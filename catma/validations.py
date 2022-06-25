import glob
import os
from typing import List


def get_single_validated_file_content(file_directory_path: str, suffix: str = "xml") -> str:
    suffix_single_validated_file_path = get_single_validated_file_path(file_directory_path, suffix)
    with open(suffix_single_validated_file_path) as file:
        file_content = file.read()
    return file_content


def get_single_validated_file_lines(file_directory_path: str, suffix: str = "xml") -> List[str]:
    suffix_single_validated_file_path = get_single_validated_file_path(file_directory_path, suffix)
    with open(suffix_single_validated_file_path) as file:
        file_lines = file.readlines()
    return file_lines


def get_single_validated_file_path(file_directory_path: str, suffix: str = "xml") -> str:
    suffix_file_paths = glob.glob(os.path.join(file_directory_path, f"*.{suffix}"))
    if len(suffix_file_paths) != 1:
        raise ValueError(
            f"A protocol should have a single {suffix} file. Bad directory: {file_directory_path}")
    suffix_single_validated_file_path = suffix_file_paths[0]
    return suffix_single_validated_file_path
