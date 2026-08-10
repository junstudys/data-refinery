from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, Union
import os
import shutil


PathInput = Union[str, os.PathLike[str]]


def _workspace_root(workspace_root: Optional[PathInput]) -> Path:
    root_input = Path.cwd() if workspace_root is None else Path(workspace_root).expanduser()
    if root_input.is_symlink() or not root_input.is_dir():
        raise ValueError(f"workspace_root must be an existing directory: {root_input}")
    return root_input.resolve()


def _validate_managed_dir(
    path: PathInput, workspace_root: Optional[PathInput]
) -> Tuple[Path, Path]:
    root = _workspace_root(workspace_root)
    raw_path = Path(path).expanduser()
    lexical = root / raw_path if not raw_path.is_absolute() else raw_path

    try:
        lexical_relative = lexical.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"managed directory must be inside workspace_root: {path}") from exc
    if not lexical_relative.parts:
        raise ValueError("managed directory must be a strict descendant of workspace_root")
    if any(part in {".", ".."} for part in lexical_relative.parts):
        raise ValueError(f"managed directory cannot contain dot traversal: {path}")

    # Inspect the lexical path before resolving it, including dangling links.
    current = root
    for part in lexical_relative.parts:
        current = current / part
        if os.path.lexists(current) and current.is_symlink():
            raise ValueError(f"managed directory cannot contain symlinks: {path}")
        if os.path.lexists(current) and not current.is_dir():
            if current == lexical:
                raise ValueError(f"managed directory cannot be an ordinary file: {path}")
            raise ValueError(f"managed directory ancestor is not a directory: {path}")

    candidate = lexical.resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"managed directory must be inside workspace_root: {path}") from exc
    if os.path.lexists(candidate) and not candidate.is_dir():
        raise ValueError(f"managed directory cannot be an ordinary file: {path}")
    return candidate, root


def ensure_dir(
    path: PathInput, clear: bool = True, workspace_root: Optional[PathInput] = None
) -> Path:
    dir_path, _ = _validate_managed_dir(path, workspace_root)
    if dir_path.exists():
        if clear:
            shutil.rmtree(dir_path)
            dir_path.mkdir(parents=True, exist_ok=True)
    else:
        dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def apply_dir_policies(
    paths: Dict[str, PathInput],
    policies: Optional[Dict[str, bool]] = None,
    workspace_root: Optional[PathInput] = None,
) -> None:
    policies = policies or {}
    # Preflight every path first so an invalid later path cannot leave earlier
    # directories cleared or created.
    validated = [
        (_validate_managed_dir(path_value, workspace_root)[0], policies.get(key, False))
        for key, path_value in paths.items()
    ]
    for path, clear in validated:
        if path.exists():
            if clear:
                shutil.rmtree(path)
                path.mkdir(parents=True, exist_ok=True)
        else:
            path.mkdir(parents=True, exist_ok=True)


def _validate_failed_filename(filename: str) -> str:
    if not isinstance(filename, str) or not filename or filename in {".", ".."}:
        raise ValueError(f"failed filename must be a simple .xlsx basename: {filename!r}")
    if (
        not filename.endswith(".xlsx")
        or Path(filename).name != filename
        or "/" in filename
        or "\\" in filename
        or any(char in filename for char in "*?[]")
    ):
        raise ValueError(f"failed filename must be a simple .xlsx basename: {filename!r}")
    stem = filename[: -len(".xlsx")]
    if not stem or stem in {".", ".."}:
        raise ValueError(f"failed filename must have a non-empty stem: {filename!r}")
    return stem


def remove_failed_output_folders(
    results_folder: PathInput,
    failed_files: Iterable[str],
    workspace_root: Optional[PathInput] = None,
) -> None:
    results_path, _ = _validate_managed_dir(results_folder, workspace_root)
    # Validate the complete input before deleting any output.
    stems = [_validate_failed_filename(filename) for filename in failed_files]
    if not results_path.is_dir():
        return

    for stem in stems:
        target = results_path / stem
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        for entry in results_path.iterdir():
            if (
                entry.is_file()
                and not entry.is_symlink()
                and entry.name.startswith(f"{stem}_")
                and entry.name.endswith(".csv")
            ):
                entry.unlink()
