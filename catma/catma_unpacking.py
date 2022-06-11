import os
import glob
import shutil
from typing import List

from configuration.general_config import INPUT_CATMA_TAR_GZS_DATA_PATH, UNPACKED_PROTOCOL_ARCHIVES_PATH, \
    CATMA_XML_ANNOTATION_DIR


class CatmaUnpacking:
    def __init__(self, tar_gzs_dir: str = INPUT_CATMA_TAR_GZS_DATA_PATH,
                 unpacked_protocol_archives_dir: str = UNPACKED_PROTOCOL_ARCHIVES_PATH):
        self.tar_gzs_dir = tar_gzs_dir
        self.unpacked_protocol_archives_dir = unpacked_protocol_archives_dir

    def unpack_tar_gz_files(self) -> None:
        tar_gz_paths = glob.glob(os.path.join(self.tar_gzs_dir, "*.tar.gz"))
        if not os.path.exists(self.unpacked_protocol_archives_dir):
            os.mkdir(self.unpacked_protocol_archives_dir)
        for tar_gz_path in tar_gz_paths:
            shutil.unpack_archive(tar_gz_path, self.unpacked_protocol_archives_dir)

    def get_valid_unpacked_protocol_dirs(self) -> List[str]:
        if not os.path.exists(self.unpacked_protocol_archives_dir):
            raise RuntimeError("You need to run unpack_tar_gz_files before using the unpacked files!!!")
        protocol_dirs = glob.glob(os.path.join(self.unpacked_protocol_archives_dir, "*"))
        protocol_dirs_with_one_annotation = []
        protocol_dirs_without_annotation = []
        for protocol_dir in protocol_dirs:
            xml_paths = glob.glob(os.path.join(protocol_dir, CATMA_XML_ANNOTATION_DIR, "*.xml"))
            number_of_xml_paths = len(xml_paths)
            if number_of_xml_paths == 0:
                protocol_dirs_without_annotation.append(protocol_dir)
            elif number_of_xml_paths == 1:
                protocol_dirs_with_one_annotation.append(protocol_dir)
            elif len(xml_paths) > 1:
                raise ValueError(
                    f"A protocol shouldn't have multiple annotation xml file. Bad protocol dir: {protocol_dir}")
        print(
            f"{len(protocol_dirs_with_one_annotation)} out of {len(protocol_dirs)} unpacked tar_gz files are good")
        print(f"Protocol dirs without annotation for instance are: {protocol_dirs_without_annotation[:3]}")
        return protocol_dirs_with_one_annotation
