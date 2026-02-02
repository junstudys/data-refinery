import logging
import os
import shutil
from pathlib import Path
from typing import Union


def extract_files(source_dir: Union[str, Path], ignore_errors: bool = True) -> None:
    source_path = Path(source_dir).resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"目录不存在: {source_path}")

    all_files = []
    for root, _, files in os.walk(source_path):
        root_path = Path(root)
        if root_path == source_path:
            continue
        for file in files:
            all_files.append(root_path / file)

    for file_path in all_files:
        try:
            target_path = source_path / file_path.name
            if target_path.exists():
                base, ext = os.path.splitext(target_path)
                counter = 1
                while target_path.exists():
                    target_path = Path(f"{base}_{counter}{ext}")
                    counter += 1
            shutil.move(str(file_path), str(target_path))
            logging.info(f"已移动: {file_path.name} -> {target_path}")
        except Exception as exc:
            if ignore_errors:
                logging.warning(f"移动文件失败: {file_path}, 错误: {str(exc)}")
            else:
                raise

    for root, dirs, _ in os.walk(source_path, topdown=False):
        for dir_name in dirs:
            dir_path = Path(root) / dir_name
            try:
                dir_path.rmdir()
                logging.info(f"已删除空文件夹: {dir_path}")
            except OSError:
                if not ignore_errors:
                    raise
