import os
import glob
import shutil
from typing import List

from configuration.general_config import CATMA_XML_ANNOTATION_DIR, PROTOCOL_DIRS_WITHOUT_SPEAKERS, \
    PROTOCOL_DIRS_THAT_LOOK_WEIRD


class CatmaUnpacking:
    def __init__(self, tar_gzs_dir: str,
                 unpacked_protocol_archives_dir: str):
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
        protocol_dirs_with_speakers = [protocol_dir for protocol_dir in protocol_dirs if os.path.basename(
            protocol_dir) not in PROTOCOL_DIRS_WITHOUT_SPEAKERS]  # relevant only when separating by speaker
        protocol_dirs_with_speakers = [protocol_dir for protocol_dir in protocol_dirs_with_speakers if
                                       os.path.basename(protocol_dir) not in PROTOCOL_DIRS_THAT_LOOK_WEIRD]
        protocol_dirs_with_one_annotation = []
        protocol_dirs_without_annotation = []
        protocol_dirs_with_multiple_annotations = []
        for protocol_dir in protocol_dirs_with_speakers:
            xml_paths = glob.glob(os.path.join(protocol_dir, CATMA_XML_ANNOTATION_DIR, "*.xml"))
            number_of_xml_paths = len(xml_paths)
            if number_of_xml_paths == 0:
                protocol_dirs_without_annotation.append(protocol_dir)
            elif number_of_xml_paths == 1:
                protocol_dirs_with_one_annotation.append(protocol_dir)
            elif len(xml_paths) > 1:
                longest_xml_path = self._choose_longest_xml_path(xml_paths)
                print(
                    f"A protocol shouldn't have multiple annotation xml file. Bad protocol dir: {protocol_dir}. We will rename the shorter xml files")
                protocol_dirs_with_multiple_annotations.append(protocol_dir)
                for xml_path in xml_paths:
                    if xml_path != longest_xml_path:
                        os.rename(xml_path, xml_path.replace(".xml", "_short.x_rename_m_rename_l_rename"))
        print(
            f"{len(protocol_dirs_with_one_annotation)} out of {len(protocol_dirs)} unpacked tar_gz files are good")
        print(f"Protocol dirs without annotation for instance are: {protocol_dirs_without_annotation[:3]}")
        print(
            f"Protocol dirs with multiple annotations for instance are: {protocol_dirs_with_multiple_annotations[:3]}")
        return sorted(protocol_dirs_with_one_annotation + protocol_dirs_with_multiple_annotations)

    @staticmethod
    def _choose_longest_xml_path(xml_paths):
        longest_xml_path = None
        longest_xml_word_count = -1
        for xml_path in xml_paths:
            with open(xml_path, "r") as xml_file:
                xml_file_content = xml_file.read()
            xml_word_count = len(xml_file_content.split())
            if xml_word_count > longest_xml_word_count:
                longest_xml_word_count = xml_word_count
                longest_xml_path = xml_path
        if longest_xml_path is None:
            raise ValueError(f"Could not find a longer xml path for the following xml paths: {xml_paths}")
        return longest_xml_path
