import glob
import os
from copy import deepcopy

import pandas as pd
import re
from dataclasses import dataclass
from typing import Optional, List, Union
from bs4 import BeautifulSoup

from catma.archives_unpacking import CatmaUnpacking
from catma.dataset_creation.generic_dataset_creation import create_text_catma_id_to_label_mapping, \
    create_not_tagged_start_char_end_char, create_label_catma_id_to_label_mapping, \
    create_text_catma_id_to_start_char_end_char_mapping, add_start_char_end_char_to_df, extract_committee_from_text, \
    extract_protocol_number_from_text
from catma.validations import get_single_validated_file_content, get_single_validated_file_lines
from catma.dataset_creation.generic_dataset_creation import GenericDatasetCreator
from configuration.df_columns import LABEL_COLUMN, LABEL_CATMA_ID_COLUMN, TEXT_SEGMENT_CATMA_ID_COLUMN, FILE_COLUMN, \
    PROTOCOL_NUMBER_COLUMN, TEXT_COLUMN, LABEL_START_CHAR_COLUMN, COMMITTEE_COLUMN, SPEAKER_NAME_START_CHAR_COLUMN, \
    SPEAKER_TEXT_START_CHAR_COLUMN, LABEL_START_CHAR_COLUMN, LABEL_END_CHAR_COLUMN, SPEAKER_NAME_COLUMN, \
    SPEAKER_TEXT_COLUMN, SPEAKER_TEXT_END_CHAR_COLUMN, LABEL_TEXT_COLUMN, CLEAN_SPEAKER_TEXT_COLUMN, \
    CLEAN_LABEL_TEXT_COLUMN, BEFORE_SPEAKER_TEXT_CONTEXT_COLUMN, AFTER_SPEAKER_TEXT_CONTEXT_COLUMN
from configuration.general_config import ITAY_CATMA_CONFIG, CATMA_XML_ANNOTATION_DIR, NOT_SPEAKER_START_PHRASES, \
    BEFORE_TEXT_CONTEXT_SIZE, AFTER_TEXT_CONTEXT_SIZE, LABEL_REPLACEMENTS_MAPPING, LABELS_TO_DROP


@dataclass
class SpeakerMetadata:
    speaker_name_start_char: int
    speaker_text_start_char: int
    speaker_text_end_char: int


@dataclass
class AnnotationIncludingSpeakerInfo:
    speaker_name_start_char: int
    speaker_text_start_char: int
    speaker_text_end_char: int
    label: Optional[str]
    label_start_char: Optional[int]
    label_end_char: Optional[int]
    # todo: add text clean, text len, text word count


class SpeakerLevelDatasetCreator(GenericDatasetCreator):
    def get_df_from_protocol_dirs(self, protocol_dirs: List[str]) -> pd.DataFrame:
        protocol_dfs_to_concatenate = []
        for protocol_dir in protocol_dirs:
            print(f"Getting sentence level df from protocol dir: {protocol_dir}")
            protocol_matching_df = self.get_df_from_protocol_dir(protocol_dir)
            protocol_dfs_to_concatenate.append(protocol_matching_df)
        concatenated_protocols_df = pd.concat(protocol_dfs_to_concatenate, ignore_index=True)
        concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[SPEAKER_TEXT_COLUMN])
        # todo: we can add better filterations in here
        concatenated_protocols_df = concatenated_protocols_df[
                concatenated_protocols_df[SPEAKER_TEXT_COLUMN].str.len() > 0]
        concatenated_protocols_df[LABEL_COLUMN].replace(LABEL_REPLACEMENTS_MAPPING, inplace=True)
        concatenated_protocols_df = concatenated_protocols_df[
                    ~concatenated_protocols_df[LABEL_COLUMN].isin(LABELS_TO_DROP)]
        return concatenated_protocols_df

    def get_df_from_protocol_dir(self, protocol_dir: str) -> Optional[pd.DataFrame]:
        print(
            '''Important: check if you need to remove new line and tab or not (depends on dataset).
            you can change it in the init of CatmaDatasetCreation.
            You can validate it using CATMA analyze with some tags to check.''')
        txt_data_lines = get_single_validated_file_lines(file_directory_path=protocol_dir,
                                                         suffix="txt")
        txt_content = get_single_validated_file_content(file_directory_path=protocol_dir,
                                                        suffix="txt")
        xml_content = get_single_validated_file_content(
            file_directory_path=os.path.join(protocol_dir, CATMA_XML_ANNOTATION_DIR), suffix="xml")

        speaker_metadata_before_annotations_df = self._get_speaker_metadata_df(txt_data_lines)

        only_annotation_df = self._get_annotation_df_from_xml(xml_content)

        annotations_with_speaker_metadata_df = self._merge_speaker_metadata_and_annotation_df(
            speaker_metadata_before_annotations_df,
            only_annotation_df)

        annotations_with_speaker_metadata_df[FILE_COLUMN] = os.path.basename(protocol_dir)
        final_df = self._add_text_based_columns(annotations_with_speaker_metadata_df, txt_content)
        return final_df

    def _get_speaker_metadata_df(self, text_lines: List[str]) -> Optional[pd.DataFrame]:
        last_end_char = 0
        first_speaker_in_text = True
        latest_speaker_name_start_char, latest_speaker_text_start_char = None, None
        speakers_metadata = []
        for i, text_line in enumerate(text_lines):
            current_start_char = last_end_char
            current_line_text_length = len(self.fix_text(text_line))
            is_new_speaker_start = text_line.endswith(":\n") and text_line.replace(":", "").replace("\n",
                                                                                                    "") not in NOT_SPEAKER_START_PHRASES
            if is_new_speaker_start:
                if first_speaker_in_text:
                    first_speaker_in_text = False
                    latest_speaker_name_start_char = current_start_char
                    latest_speaker_text_start_char = latest_speaker_name_start_char + current_line_text_length
                else:
                    latest_speaker = SpeakerMetadata(speaker_name_start_char=latest_speaker_name_start_char,
                                                     speaker_text_start_char=latest_speaker_text_start_char,
                                                     speaker_text_end_char=last_end_char)
                    speakers_metadata.append(latest_speaker)
                    latest_speaker_name_start_char = current_start_char
                    latest_speaker_text_start_char = latest_speaker_name_start_char + current_line_text_length
            last_end_char += current_line_text_length
        if latest_speaker_text_start_char is not None:
            latest_speaker = SpeakerMetadata(speaker_name_start_char=latest_speaker_name_start_char,
                                             speaker_text_start_char=latest_speaker_text_start_char,
                                             speaker_text_end_char=last_end_char)
            speakers_metadata.append(latest_speaker)
        if len(speakers_metadata) == 0:
            return None
        speakers_metadata_df = pd.DataFrame([speaker_metadata.__dict__ for speaker_metadata in speakers_metadata])
        speakers_metadata_df = speakers_metadata_df.sort_values(by=SPEAKER_NAME_START_CHAR_COLUMN)
        speakers_metadata_df = speakers_metadata_df.drop_duplicates(ignore_index=True)
        return speakers_metadata_df  # todo: return a df of speakers

    @staticmethod
    def _merge_speaker_metadata_and_annotation_df(speaker_metadata_df: pd.DataFrame,
                                                  annotation_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if speaker_metadata_df is None or annotation_df is None or len(speaker_metadata_df) == 0 or len(
                annotation_df) == 0:
            return None
        annotations_with_speaker_info = []
        for row_index, annotation_row in annotation_df.iterrows():
            matching_speaker_level_df_row = speaker_metadata_df[
                speaker_metadata_df[SPEAKER_NAME_START_CHAR_COLUMN] <= annotation_row[
                    LABEL_START_CHAR_COLUMN]].tail(1)  # we assume the df is sorted
            speaker_name_start_char = matching_speaker_level_df_row[SPEAKER_NAME_START_CHAR_COLUMN].iloc[0]
            speaker_text_start_char = matching_speaker_level_df_row[SPEAKER_TEXT_START_CHAR_COLUMN].iloc[0]
            speaker_end_char = matching_speaker_level_df_row[SPEAKER_TEXT_END_CHAR_COLUMN].iloc[0]
            label = annotation_row[LABEL_COLUMN]
            label_start_char = annotation_row[LABEL_START_CHAR_COLUMN]
            label_end_char = annotation_row[LABEL_END_CHAR_COLUMN]
            assert speaker_name_start_char <= label_start_char
            assert speaker_end_char >= label_end_char
            annotated_row = AnnotationIncludingSpeakerInfo(speaker_name_start_char=speaker_name_start_char,
                                                           speaker_text_start_char=speaker_text_start_char,
                                                           speaker_text_end_char=speaker_end_char,
                                                           label=label,
                                                           label_start_char=label_start_char,
                                                           label_end_char=label_end_char)
            annotations_with_speaker_info.append(annotated_row)
        annotated_with_speakers_df = pd.DataFrame(
            [annotated_row.__dict__ for annotated_row in annotations_with_speaker_info])
        if len(annotated_with_speakers_df) == 0:  # todo: make sure it works
            return None
        # todo: We are adding all speakers with no annotations, it should be another function
        speakers_without_annotation = []
        already_annotated_speaker_name_start_chars = set(annotated_with_speakers_df[
                                                             SPEAKER_NAME_START_CHAR_COLUMN])
        for row_index, speaker_row in speaker_metadata_df.iterrows():
            if speaker_row[SPEAKER_NAME_START_CHAR_COLUMN] not in already_annotated_speaker_name_start_chars:
                speaker_info_without_annotation = AnnotationIncludingSpeakerInfo(
                    speaker_name_start_char=speaker_row[SPEAKER_NAME_START_CHAR_COLUMN],
                    speaker_text_start_char=speaker_row[SPEAKER_TEXT_START_CHAR_COLUMN],
                    speaker_text_end_char=speaker_row[SPEAKER_TEXT_END_CHAR_COLUMN],
                    label=None,
                    label_start_char=None, label_end_char=None)
                speakers_without_annotation.append(speaker_info_without_annotation)
        speakers_without_annotation_df = pd.DataFrame([row.__dict__ for row in speakers_without_annotation])

        speakers_annotations_df = pd.concat([annotated_with_speakers_df, speakers_without_annotation_df],
                                            ignore_index=True)
        speakers_annotations_df = speakers_annotations_df.sort_values(
            by=[SPEAKER_NAME_START_CHAR_COLUMN, LABEL_START_CHAR_COLUMN])
        speakers_annotations_df = speakers_annotations_df.drop_duplicates(ignore_index=True)
        speakers_annotations_df = speakers_annotations_df.drop_duplicates(
            subset=[SPEAKER_NAME_START_CHAR_COLUMN, LABEL_COLUMN], keep="first", ignore_index=True)
        return speakers_annotations_df

    def _add_text_based_columns(self, annotations_with_speakers_df: pd.DataFrame, txt_content: str) -> Optional[
        pd.DataFrame]:
        txt_content = self.fix_text(txt_content)
        if annotations_with_speakers_df is None or len(annotations_with_speakers_df) == 0:
            return None
        final_df = deepcopy(annotations_with_speakers_df)
        final_df[SPEAKER_NAME_COLUMN] = final_df.apply(
            lambda row: txt_content[row[SPEAKER_NAME_START_CHAR_COLUMN]:row[SPEAKER_TEXT_START_CHAR_COLUMN]], axis=1)
        final_df[SPEAKER_TEXT_COLUMN] = final_df.apply(
            lambda row: txt_content[row[SPEAKER_TEXT_START_CHAR_COLUMN]:row[SPEAKER_TEXT_END_CHAR_COLUMN]], axis=1)
        # final_df[CLEAN_SPEAKER_TEXT_COLUMN] = final_df.apply(
        #     lambda row: " ".join(row[SPEAKER_TEXT_COLUMN].split()), axis=1)
        final_df[LABEL_TEXT_COLUMN] = final_df.apply(
            lambda row: txt_content[row[LABEL_START_CHAR_COLUMN]:row[LABEL_END_CHAR_COLUMN]], axis=1)
        # final_df[CLEAN_LABEL_TEXT_COLUMN] = final_df.apply(
        #     lambda row: " ".join(row[LABEL_TEXT_COLUMN].split()), axis=1)
        # todo: if we want context we can use it:
        # final_df[BEFORE_SPEAKER_TEXT_CONTEXT_COLUMN] = final_df.apply(
        #     lambda row: txt_content[row[SPEAKER_TEXT_START_CHAR_COLUMN] + BEFORE_TEXT_CONTEXT_SIZE:row[
        #         SPEAKER_TEXT_START_CHAR_COLUMN]], axis=1)
        # final_df[AFTER_SPEAKER_TEXT_CONTEXT_COLUMN] = final_df.apply(
        #     lambda row: txt_content[
        #                 row[SPEAKER_TEXT_END_CHAR_COLUMN]:row[SPEAKER_TEXT_END_CHAR_COLUMN] + AFTER_TEXT_CONTEXT_SIZE],
        #     axis=1)
        committee = extract_committee_from_text(txt_content)
        final_df[COMMITTEE_COLUMN] = committee
        try:
            protocol_number = extract_protocol_number_from_text(txt_content)
        except Exception as exception:
            print(
                f"Using None since couldnt find protocol number for protocol dir: {final_df[FILE_COLUMN].iloc[0]}, committee is {committee}, exception is: {exception}")
            protocol_number = None
        final_df[PROTOCOL_NUMBER_COLUMN] = protocol_number
        final_df = final_df.sort_values(by=[SPEAKER_NAME_START_CHAR_COLUMN, LABEL_START_CHAR_COLUMN])
        final_df = final_df.drop_duplicates(ignore_index=True)
        return final_df


if __name__ == "__main__":
    catma_config = ITAY_CATMA_CONFIG
    catma_unpacking = CatmaUnpacking(tar_gzs_dir=catma_config.input_catma_tar_gzs_data_path,
                                     unpacked_protocol_archives_dir=catma_config.unpacked_protocol_archives_path)
    catma_unpacking.unpack_tar_gz_files()
    valid_protocol_dirs = catma_unpacking.get_valid_unpacked_protocol_dirs()
    # protocol_dir = [protocol_dir for protocol_dir in valid_protocol_dirs if
    #                 os.path.basename(protocol_dir) == "פרוטוקול_מספר_42_parts"][
    #     0]  # work on "ועדת הכספים חלקי דיון"-> "פרוטוקול מספר 42
    speaker_level_dataset_creator = SpeakerLevelDatasetCreator(
        remove_new_line_and_tab=ITAY_CATMA_CONFIG.remove_new_line_and_tab)
    # temp_df = speaker_level_dataset_creator.get_df_from_protocol_dir(protocol_dir)
    itay_speaker_level_df = speaker_level_dataset_creator.get_df_from_protocol_dirs(valid_protocol_dirs)