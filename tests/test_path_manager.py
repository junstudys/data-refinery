from pathlib import Path

from utils.path_manager import ensure_dir, apply_dir_policies


def test_ensure_dir_creates_and_clears(tmp_path: Path):
    target = tmp_path / "dir"
    target.mkdir()
    (target / "file.txt").write_text("x", encoding="utf-8")

    ensure_dir(str(target), clear=True)

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
        {"a": str(a), "b": str(b)},
        {"a": False, "b": True},
    )

    assert (a / "keep.txt").exists()
    assert not (b / "remove.txt").exists()
