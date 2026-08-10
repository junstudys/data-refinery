from pathlib import Path

import pytest

from utils.path_manager import ensure_dir, apply_dir_policies


def test_ensure_dir_creates_and_clears(tmp_path: Path):
    target = tmp_path / "dir"
    target.mkdir()
    (target / "file.txt").write_text("x", encoding="utf-8")

    ensure_dir(target, clear=True, workspace_root=tmp_path)

    assert target.exists()
    assert not (target / "file.txt").exists()


def test_apply_dir_policies_respects_clear_flag(tmp_path: Path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    (a / "keep.txt").write_text("a", encoding="utf-8")
    (b / "remove.txt").write_text("b", encoding="utf-8")

    apply_dir_policies(
        {"a": a, "b": b},
        {"a": False, "b": True},
        workspace_root=tmp_path,
    )

    assert (a / "keep.txt").exists()
    assert not (b / "remove.txt").exists()


@pytest.mark.parametrize("path", [".", "..", "", "/", str(Path.home())])
def test_ensure_dir_rejects_unsafe_targets(tmp_path: Path, path: str):
    with pytest.raises(ValueError):
        ensure_dir(path, workspace_root=tmp_path)


def test_ensure_dir_rejects_outside_symlink_and_file(tmp_path: Path):
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    link = tmp_path / "link"
    link.symlink_to(outside, target_is_directory=True)
    file_path = tmp_path / "file"
    file_path.write_text("x", encoding="utf-8")

    for path in (outside, link, file_path):
        with pytest.raises(ValueError):
            ensure_dir(path, workspace_root=tmp_path)


def test_apply_dir_policies_preflights_all_paths(tmp_path: Path):
    keep = tmp_path / "keep"
    keep.mkdir()
    marker = keep / "marker"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(ValueError):
        apply_dir_policies(
            {"keep": keep, "bad": tmp_path},
            {"keep": True},
            workspace_root=tmp_path,
        )

    assert marker.exists()


def test_apply_dir_policies_missing_policy_defaults_false(tmp_path: Path):
    target = tmp_path / "target"
    target.mkdir()
    marker = target / "marker"
    marker.write_text("keep", encoding="utf-8")

    apply_dir_policies({"target": target}, workspace_root=tmp_path)

    assert marker.exists()
