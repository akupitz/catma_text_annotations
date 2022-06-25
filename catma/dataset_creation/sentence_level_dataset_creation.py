import os
import re

from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Optional

from catma.dataset_creation.generic_dataset_creation import GenericDatasetCreator
from catma.validations import get_single_validated_file_content
from configuration.general_config import LABEL_REPLACEMENTS_MAPPING, LABELS_TO_DROP, CATMA_XML_ANNOTATION_DIR, \
    AFTER_TEXT_CONTEXT_SIZE, BEFORE_TEXT_CONTEXT_SIZE, START_CHAR_END_CHAR_EXTRACTION_REGEX, COMMITTEE_REGEX, \
    PROTOCOL_NUMBER_REGEX
from configuration.df_columns import LABEL_COLUMN, TEXT_COLUMN, TEXT_SEGMENT_CATMA_ID_COLUMN, START_CHAR_COLUMN, \
    END_CHAR_COLUMN, CLEAN_TEXT_COLUMN, PROTOCOL_NUMBER_COLUMN, AFTER_TEXT_CONTEXT_COLUMN, BEFORE_TEXT_CONTEXT_COLUMN, \
    LABEL_CATMA_ID_COLUMN, FILE_COLUMN, COMMITTEE_COLUMN


def fix_text(text):
    fixed_text_part1 = " ".join(text.replace(",", " ").replace(".", " ").replace("-", " ").replace("–", " ").split())
    return " ".join(re.sub('[^\w\s]', '', fixed_text_part1).strip().split())


def add_start_char_end_char_to_df(row, text_catma_id_to_start_char_end_char):
    start_char, end_char = text_catma_id_to_start_char_end_char[row[TEXT_SEGMENT_CATMA_ID_COLUMN]]
    row[START_CHAR_COLUMN] = start_char
    row[END_CHAR_COLUMN] = end_char
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


def create_text_catma_id_to_start_char_end_char_mapping(bs_data):
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


class SentenceLevelDatasetCreator(GenericDatasetCreator):

    def get_df_from_protocol_dirs(self, protocol_dirs: List[str]) -> pd.DataFrame:
        protocol_dfs_to_concatenate = []
        for protocol_dir in protocol_dirs:
            print(f"Getting sentence level df from protocol dir: {protocol_dir}")
            protocol_matching_df = self.get_df_from_protocol_dir(protocol_dir)
            protocol_dfs_to_concatenate.append(protocol_matching_df)
        concatenated_protocols_df = pd.concat(protocol_dfs_to_concatenate, ignore_index=True)
        concatenated_protocols_df = concatenated_protocols_df.drop_duplicates(ignore_index=True)
        concatenated_protocols_df[LABEL_COLUMN].replace(LABEL_REPLACEMENTS_MAPPING, inplace=True)
        concatenated_protocols_df = concatenated_protocols_df[
            ~concatenated_protocols_df[LABEL_COLUMN].isin(LABELS_TO_DROP)]
        concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[TEXT_COLUMN])
        # this splits by "."
        concatenated_protocols_df = concatenated_protocols_df.assign(
            text=concatenated_protocols_df[TEXT_COLUMN].str.split(".")).explode(TEXT_COLUMN)
        # todo: after the explode i may need to split the start, end
        print("After explode we also need to split start_char->end_char")
        concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[TEXT_COLUMN])
        concatenated_protocols_df[CLEAN_TEXT_COLUMN] = concatenated_protocols_df.apply(
            lambda row: fix_text(row[TEXT_COLUMN]), axis=1)
        concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[CLEAN_TEXT_COLUMN])
        concatenated_protocols_df = concatenated_protocols_df[
            concatenated_protocols_df[CLEAN_TEXT_COLUMN].str.len() > 0]
        concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[CLEAN_TEXT_COLUMN])
        return concatenated_protocols_df

    def get_df_from_protocol_dir(self, protocol_dir: str) -> Optional[pd.DataFrame]:

        xml_data = get_single_validated_file_content(
            file_directory_path=os.path.join(protocol_dir, CATMA_XML_ANNOTATION_DIR),
            suffix="xml")
        txt_data = get_single_validated_file_content(file_directory_path=protocol_dir,
                                                     suffix="txt")

        bs_data = BeautifulSoup(xml_data, "xml")
        text_catma_id_to_catma_label_id = create_text_catma_id_to_label_mapping(bs_data)
        if len(text_catma_id_to_catma_label_id) == 0:
            print("Empty annotation file, skipping it")
            return None
        catma_label_id_to_label = create_label_catma_id_to_label_mapping(bs_data)
        text_catma_id_to_start_char_end_char = create_text_catma_id_to_start_char_end_char_mapping(bs_data)
        not_tagged_start_char_end_char = create_not_tagged_start_char_end_char(bs_data)

        catma_ids0 = list(set(text_catma_id_to_catma_label_id.keys()))
        catma_ids1 = list(set(text_catma_id_to_start_char_end_char.keys()))
        in0not1 = [catma_id for catma_id in catma_ids0 if catma_id not in catma_ids1]
        in1not0 = [catma_id for catma_id in catma_ids1 if catma_id not in catma_ids0]
        assert len(in0not1) == 0
        assert len(in1not0) == 0
        if self.remove_new_line_and_tab:
            txt_data = txt_data.replace("\n", "  ").replace("\t",
                                                            " ")
        print(
            '''Important: check if you need to remove new line and tab or not (depends on dataset).
            you can change it in the init of CatmaDatasetCreation.
            You can validate it using CATMA analyze with some tags to check.''')
        df = pd.DataFrame(text_catma_id_to_catma_label_id.items(),
                          columns=[TEXT_SEGMENT_CATMA_ID_COLUMN, LABEL_CATMA_ID_COLUMN])
        df[LABEL_COLUMN] = df.apply(lambda row: catma_label_id_to_label[row[LABEL_CATMA_ID_COLUMN]], axis=1)
        df = df.apply(add_start_char_end_char_to_df,
                      text_catma_id_to_start_char_end_char=text_catma_id_to_start_char_end_char, axis=1)
        not_tagged_df = pd.DataFrame(not_tagged_start_char_end_char, columns=[START_CHAR_COLUMN, END_CHAR_COLUMN])
        not_tagged_df[TEXT_SEGMENT_CATMA_ID_COLUMN] = None
        not_tagged_df[LABEL_CATMA_ID_COLUMN] = None
        not_tagged_df[LABEL_COLUMN] = None
        df = pd.concat([df, not_tagged_df], ignore_index=True)
        df[FILE_COLUMN] = os.path.basename(protocol_dir)
        committee = extract_committee_from_text(txt_data)
        df[COMMITTEE_COLUMN] = committee
        try:
            protocol_number = extract_protocol_number_from_text(txt_data)
        except Exception as exception:
            print(
                f"Using None since couldnt find protocol number for protocol dir: {protocol_dir}, committee is {committee}, exception is: {exception}")
            protocol_number = None
        df[PROTOCOL_NUMBER_COLUMN] = protocol_number
        df[TEXT_COLUMN] = df.apply(lambda row: " ".join(txt_data[row[START_CHAR_COLUMN]:row[END_CHAR_COLUMN]].split()),
                                   axis=1)
        # todo: split text in untagged dataset by ['.', ':", "?"]
        df[BEFORE_TEXT_CONTEXT_COLUMN] = df.apply(
            lambda row: txt_data[row[START_CHAR_COLUMN] + BEFORE_TEXT_CONTEXT_SIZE:row[
                                                                                       END_CHAR_COLUMN] + BEFORE_TEXT_CONTEXT_SIZE],
            axis=1)
        df[AFTER_TEXT_CONTEXT_COLUMN] = df.apply(
            lambda row: txt_data[
                        row[START_CHAR_COLUMN] + AFTER_TEXT_CONTEXT_SIZE:row[
                                                                             END_CHAR_COLUMN] + AFTER_TEXT_CONTEXT_SIZE],
            axis=1)
        df = df.sort_values(by=START_CHAR_COLUMN)
        df = df.drop_duplicates(ignore_index=True)
        return df
