from typing import List, Dict, Optional, Tuple
import re

from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Optional

from configuration.general_config import LABEL_REPLACEMENTS_MAPPING, LABELS_TO_DROP, CATMA_XML_ANNOTATION_DIR, \
    AFTER_TEXT_CONTEXT_SIZE, BEFORE_TEXT_CONTEXT_SIZE, START_CHAR_END_CHAR_EXTRACTION_REGEX, COMMITTEE_REGEX, \
    PROTOCOL_NUMBER_REGEX
from configuration.df_columns import LABEL_COLUMN, TEXT_COLUMN, TEXT_SEGMENT_CATMA_ID_COLUMN, \
    CLEAN_TEXT_COLUMN, PROTOCOL_NUMBER_COLUMN, AFTER_TEXT_CONTEXT_COLUMN, BEFORE_TEXT_CONTEXT_COLUMN, \
    LABEL_CATMA_ID_COLUMN, FILE_COLUMN, COMMITTEE_COLUMN, LABEL_END_CHAR_COLUMN, LABEL_START_CHAR_COLUMN


class GenericDatasetCreator:
    def __init__(self, remove_new_line_and_tab: bool = False):
        self.remove_new_line_and_tab = remove_new_line_and_tab

    def get_df_from_protocol_dirs(self, protocol_dirs: List[str]):
        pass

    def get_df_from_protocol_dir(self, protocol_dir: str):
        pass

    @staticmethod
    def assert_catma_ids_intersect(text_catma_id_to_catma_label_id: Dict[str, str],
                                   text_catma_id_to_start_char_end_char: Dict[str, Tuple[int, int]]):
        catma_ids0 = list(set(text_catma_id_to_catma_label_id.keys()))
        catma_ids1 = list(set(text_catma_id_to_start_char_end_char.keys()))
        in0not1 = [catma_id for catma_id in catma_ids0 if catma_id not in catma_ids1]
        in1not0 = [catma_id for catma_id in catma_ids1 if catma_id not in catma_ids0]
        assert len(in0not1) == 0
        assert len(in1not0) == 0

    def fix_text(self, text):
        if self.remove_new_line_and_tab:
            text = text.replace("\n", "  ")
            text = text.replace("\t", " ")
        return text

    def _get_annotation_df_from_xml(self, xml_data: str) -> Optional[pd.DataFrame]:
        bs_data = BeautifulSoup(xml_data, "xml")
        text_catma_id_to_catma_label_id = create_text_catma_id_to_label_mapping(bs_data)
        if len(text_catma_id_to_catma_label_id) == 0:
            print("Empty annotation file, skipping it")
            return None
        catma_label_id_to_label = create_label_catma_id_to_label_mapping(bs_data)
        text_catma_id_to_start_char_end_char = create_text_catma_id_to_start_char_end_char_mapping(bs_data)
        self.assert_catma_ids_intersect(text_catma_id_to_catma_label_id, text_catma_id_to_start_char_end_char)
        catma_annotation_df = pd.DataFrame(text_catma_id_to_catma_label_id.items(),
                                           columns=[TEXT_SEGMENT_CATMA_ID_COLUMN, LABEL_CATMA_ID_COLUMN])
        catma_annotation_df[LABEL_COLUMN] = catma_annotation_df.apply(
            lambda row: catma_label_id_to_label[row[LABEL_CATMA_ID_COLUMN]], axis=1)
        catma_annotation_df = catma_annotation_df.apply(add_start_char_end_char_to_df,
                                                        text_catma_id_to_start_char_end_char=text_catma_id_to_start_char_end_char,
                                                        axis=1)
        return catma_annotation_df


def clean_text(text):
    # fixed_text_part1 = " ".join(text.replace(",", " ").replace(".", " ").replace("-", " ").replace("–", " ").split())
    text = " ".join(text.split())
    return " ".join(re.sub('[^\w\s]', '', text).strip().split())


def add_start_char_end_char_to_df(row, text_catma_id_to_start_char_end_char):
    start_char, end_char = text_catma_id_to_start_char_end_char[row[TEXT_SEGMENT_CATMA_ID_COLUMN]]
    row[LABEL_START_CHAR_COLUMN] = start_char
    row[LABEL_END_CHAR_COLUMN] = end_char
    return row


def extract_committee_from_text(text):
    match = COMMITTEE_REGEX.search(text)
    if not match:
        raise NotImplementedError
    return " ".join(match.group().strip().split()[:4])


def extract_protocol_number_from_text(text):
    match = PROTOCOL_NUMBER_REGEX.search(text)
    if not match:
        raise NotImplementedError
    return match.group().strip()


def create_text_catma_id_to_label_mapping(bs_data):
    text_segment_catma_id_to_label_id = dict()
    bs_text_data = bs_data.find_all("text")
    assert len(bs_text_data) == 1
    bs_text_data = bs_text_data[0]
    fss = bs_text_data.find_all("fs")
    for fs in fss:
        fs_catma_label_identifier = fs.get("type")
        fs_text_segment_catma_identifier = fs.get("xml:id")
        if fs_text_segment_catma_identifier in text_segment_catma_id_to_label_id:
            raise NotImplementedError
        text_segment_catma_id_to_label_id[fs_text_segment_catma_identifier] = fs_catma_label_identifier
    return text_segment_catma_id_to_label_id


def create_label_catma_id_to_label_mapping(bs_data):
    label_catma_id_to_label = dict()
    bs_data_labels = bs_data.find_all("encodingDesc")
    assert len(bs_data_labels) == 1
    bs_data_labels = bs_data_labels[0]
    bs_data_labels_fs_decl = bs_data_labels.find_all("fsDecl")
    for bs_data_label_fs_decl in bs_data_labels_fs_decl:
        fs_descr = bs_data_label_fs_decl.find_all("fsDescr")
        assert len(fs_descr) == 1
        fs_descr = fs_descr[0]
        label_type = fs_descr.text
        label_type_catma_id = bs_data_label_fs_decl.get("type")
        # there are main labels and sub labels in daniel's dataset, they appear before others, so we can assume they exist when others inherit from them
        label_base_type_catma_id = bs_data_label_fs_decl.get("baseTypes")
        if label_base_type_catma_id:
            # only if this type has a basetype (and it is not a basetype itself)
            label_base_type = label_catma_id_to_label[label_base_type_catma_id]
            label_type = label_base_type + "_" + label_type
        if label_type_catma_id in label_catma_id_to_label:
            raise NotImplementedError
        label_catma_id_to_label[label_type_catma_id] = label_type
    return label_catma_id_to_label


def create_text_catma_id_to_start_char_end_char_mapping(bs_data) -> Dict[str, Tuple[int, int]]:
    text_segment_catma_id_to_start_char_end_char = dict()
    bs_text_data = bs_data.find_all("text")
    assert len(bs_text_data) == 1
    bs_text_data = bs_text_data[0]
    text_segments = bs_text_data.find_all("seg")
    for text_segment in text_segments:
        catma_text_segment_ids = text_segment.get("ana").replace("#", "").split()
        ptrs = text_segment.find_all("ptr")
        assert len(ptrs) == 1
        ptrs = ptrs[0]
        match = START_CHAR_END_CHAR_EXTRACTION_REGEX.match(ptrs.get("target"))
        if not match:
            raise NotImplementedError
        start_char, end_char = map(int, match.groups())
        for catma_text_segment_id in catma_text_segment_ids:
            if catma_text_segment_id in text_segment_catma_id_to_start_char_end_char:
                old_start_char, old_end_char = text_segment_catma_id_to_start_char_end_char[catma_text_segment_id]
                if start_char != old_end_char:
                    raise NotImplementedError
                text_segment_catma_id_to_start_char_end_char[catma_text_segment_id] = (old_start_char, end_char)
            else:
                text_segment_catma_id_to_start_char_end_char[catma_text_segment_id] = (start_char, end_char)
    return text_segment_catma_id_to_start_char_end_char


def create_not_tagged_start_char_end_char(bs_text_data):
    not_tagged_start_char_end_char = []
    pointer_to_tagged_or_not_segments = bs_text_data.find_all("ptr")
    for pointer_to_tagged_or_not_segment in pointer_to_tagged_or_not_segments:
        if type(pointer_to_tagged_or_not_segment.parent.get("ana")) == str:
            continue  # it is annotated and shouldn't be none
        match = START_CHAR_END_CHAR_EXTRACTION_REGEX.match(pointer_to_tagged_or_not_segment.get("target"))
        if not match:
            raise NotImplementedError
        start_char, end_char = map(int, match.groups())
        not_tagged_start_char_end_char.append([start_char, end_char])
    return not_tagged_start_char_end_char
