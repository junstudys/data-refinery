from pathlib import Path
from typing import Dict, Iterable, Optional
import shutil


def ensure_dir(path: str, clear: bool = True) -> Path:
    dir_path = Path(path)
    if dir_path.exists():
        if clear:
            shutil.rmtree(dir_path)
            dir_path.mkdir(parents=True, exist_ok=True)
    else:
        dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def apply_dir_policies(
    paths: Dict[str, str], policies: Optional[Dict[str, bool]] = None
) -> None:
    policies = policies or {}
    for key, path_value in paths.items():
        clear = policies.get(key, True)
        ensure_dir(path_value, clear=clear)


def remove_failed_output_folders(
    results_folder: Path, failed_files: Iterable[str]
) -> None:
    for filename in failed_files:
        stem = Path(filename).stem
        target = results_folder / stem
        if target.exists() and target.is_dir():
            shutil.rmtree(target)
        for csv_file in results_folder.glob(f"{stem}_*.csv"):
            csv_file.unlink()
