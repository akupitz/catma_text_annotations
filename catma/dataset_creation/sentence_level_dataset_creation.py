import os
from typing import List, Optional

import pandas as pd

from catma.dataset_creation.generic_dataset_creation import GenericDatasetCreator
from catma.validations import get_single_validated_file_lines, get_single_validated_file_content
from configuration.df_columns import FILE_COLUMN
from configuration.general_config import CATMA_XML_ANNOTATION_DIR


class SentenceLevelDatasetCreator(GenericDatasetCreator):
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

        sentence_metadata_before_annotations_df = self._get_speaker_metadata_df(txt_data_lines)

        only_annotation_df = self._get_annotation_df_from_xml(xml_content)

        annotations_with_speaker_metadata_df = self._merge_speaker_metadata_and_annotation_df(
            speaker_metadata_before_annotations_df,
            only_annotation_df)

        annotations_with_speaker_metadata_df[FILE_COLUMN] = os.path.basename(protocol_dir)
        final_df = self._add_text_based_columns(annotations_with_speaker_metadata_df, txt_content)
        return final_df


    # def get_df_from_protocol_dirs(self, protocol_dirs: List[str]) -> pd.DataFrame:
    #     protocol_dfs_to_concatenate = []
    #     for protocol_dir in protocol_dirs:
    #         print(f"Getting sentence level df from protocol dir: {protocol_dir}")
    #         protocol_matching_df = self.get_df_from_protocol_dir(protocol_dir)
    #         protocol_dfs_to_concatenate.append(protocol_matching_df)
    #     concatenated_protocols_df = pd.concat(protocol_dfs_to_concatenate, ignore_index=True)
    #     concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[SPEAKER_TEXT_COLUMN])
    #     # todo: we can add better filterations in here
    #     concatenated_protocols_df = concatenated_protocols_df[
    #         concatenated_protocols_df[SPEAKER_TEXT_COLUMN].str.len() > 0]
    #     concatenated_protocols_df[LABEL_COLUMN].replace(LABEL_REPLACEMENTS_MAPPING, inplace=True)
    #     concatenated_protocols_df = concatenated_protocols_df[
    #         ~concatenated_protocols_df[LABEL_COLUMN].isin(LABELS_TO_DROP)]
    #     return concatenated_protocols_df
#
#     def get_df_from_protocol_dirs(self, protocol_dirs: List[str]) -> pd.DataFrame:
#         protocol_dfs_to_concatenate = []
#         for protocol_dir in protocol_dirs:
#             print(f"Getting sentence level df from protocol dir: {protocol_dir}")
#             protocol_matching_df = self.get_df_from_protocol_dir(protocol_dir)
#             protocol_dfs_to_concatenate.append(protocol_matching_df)
#         concatenated_protocols_df = pd.concat(protocol_dfs_to_concatenate, ignore_index=True)
#         concatenated_protocols_df = concatenated_protocols_df.drop_duplicates(ignore_index=True)
#         concatenated_protocols_df[LABEL_COLUMN].replace(LABEL_REPLACEMENTS_MAPPING, inplace=True)
#         concatenated_protocols_df = concatenated_protocols_df[
#             ~concatenated_protocols_df[LABEL_COLUMN].isin(LABELS_TO_DROP)]
#         concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[TEXT_COLUMN])
#         # this splits by "."
#         concatenated_protocols_df = concatenated_protocols_df.assign(
#             text=concatenated_protocols_df[TEXT_COLUMN].str.split(".")).explode(TEXT_COLUMN)
#         # todo: after the explode i may need to split the start, end
#         print("After explode we also need to split start_char->end_char")
#         concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[TEXT_COLUMN])
#         concatenated_protocols_df[CLEAN_TEXT_COLUMN] = concatenated_protocols_df.apply(
#             lambda row: clean_text(row[TEXT_COLUMN]), axis=1)
#         concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[CLEAN_TEXT_COLUMN])
#         concatenated_protocols_df = concatenated_protocols_df[
#             concatenated_protocols_df[CLEAN_TEXT_COLUMN].str.len() > 0]
#         concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[CLEAN_TEXT_COLUMN])
#         return concatenated_protocols_df
#
#     def get_df_from_protocol_dir(self, protocol_dir: str) -> Optional[pd.DataFrame]:
#
#         xml_data = get_single_validated_file_content(
#             file_directory_path=os.path.join(protocol_dir, CATMA_XML_ANNOTATION_DIR),
#             suffix="xml")
#         txt_data = get_single_validated_file_content(file_directory_path=protocol_dir,
#                                                      suffix="txt")
#
#         bs_data = BeautifulSoup(xml_data, "xml")
#         text_catma_id_to_catma_label_id = create_text_catma_id_to_label_mapping(bs_data)
#         if len(text_catma_id_to_catma_label_id) == 0:
#             print("Empty annotation file, skipping it")
#             return None
#         catma_label_id_to_label = create_label_catma_id_to_label_mapping(bs_data)
#         text_catma_id_to_start_char_end_char = create_text_catma_id_to_start_char_end_char_mapping(bs_data)
#         not_tagged_start_char_end_char = create_not_tagged_start_char_end_char(bs_data)
#         self.assert_catma_ids_intersect(text_catma_id_to_catma_label_id, text_catma_id_to_start_char_end_char)
#         if self.remove_new_line_and_tab:
#             txt_data = txt_data.replace("\n", "  ").replace("\t",
#                                                             " ")
#         print(
#             '''Important: check if you need to remove new line and tab or not (depends on dataset).
#             you can change it in the init of CatmaDatasetCreation.
#             You can validate it using CATMA analyze with some tags to check.''')
#         df = pd.DataFrame(text_catma_id_to_catma_label_id.items(),
#                           columns=[TEXT_SEGMENT_CATMA_ID_COLUMN, LABEL_CATMA_ID_COLUMN])
#         df[LABEL_COLUMN] = df.apply(lambda row: catma_label_id_to_label[row[LABEL_CATMA_ID_COLUMN]], axis=1)
#         df = df.apply(add_start_char_end_char_to_df,
#                       text_catma_id_to_start_char_end_char=text_catma_id_to_start_char_end_char, axis=1)
#         not_tagged_df = pd.DataFrame(not_tagged_start_char_end_char, columns=[LABEL_START_CHAR_COLUMN, LABEL_END_CHAR_COLUMN])
#         not_tagged_df[TEXT_SEGMENT_CATMA_ID_COLUMN] = None
#         not_tagged_df[LABEL_CATMA_ID_COLUMN] = None
#         not_tagged_df[LABEL_COLUMN] = None
#         df = pd.concat([df, not_tagged_df], ignore_index=True)
#         df[FILE_COLUMN] = os.path.basename(protocol_dir)
#         committee = extract_committee_from_text(txt_data)
#         df[COMMITTEE_COLUMN] = committee
#         try:
#             protocol_number = extract_protocol_number_from_text(txt_data)
#         except Exception as exception:
#             print(
#                 f"Using None since couldnt find protocol number for protocol dir: {protocol_dir}, committee is {committee}, exception is: {exception}")
#             protocol_number = None
#         df[PROTOCOL_NUMBER_COLUMN] = protocol_number
#         df[TEXT_COLUMN] = df.apply(lambda row: " ".join(txt_data[row[LABEL_START_CHAR_COLUMN]:row[LABEL_END_CHAR_COLUMN]].split()),
#                                    axis=1)
#         # todo: split text in untagged dataset by ['.', ':", "?"]
#         df[BEFORE_TEXT_CONTEXT_COLUMN] = df.apply(
#             lambda row: txt_data[row[LABEL_START_CHAR_COLUMN] + BEFORE_TEXT_CONTEXT_SIZE:row[
#                                                                                        LABEL_END_CHAR_COLUMN] + BEFORE_TEXT_CONTEXT_SIZE],
#             axis=1)
#         df[AFTER_TEXT_CONTEXT_COLUMN] = df.apply(
#             lambda row: txt_data[
#                         row[LABEL_START_CHAR_COLUMN] + AFTER_TEXT_CONTEXT_SIZE:row[
#                                                                              LABEL_END_CHAR_COLUMN] + AFTER_TEXT_CONTEXT_SIZE],
#             axis=1)
#         df = df.sort_values(by=LABEL_START_CHAR_COLUMN)
#         df = df.drop_duplicates(ignore_index=True)
#         return df
