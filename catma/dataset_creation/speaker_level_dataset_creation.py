import glob
import os
import pandas as pd
import re
from dataclasses import dataclass
from typing import Optional, List, Union
from bs4 import BeautifulSoup

from catma.archives_unpacking import CatmaUnpacking
from catma.dataset_creation.sentence_level_dataset_creation import create_text_catma_id_to_label_mapping, \
    create_not_tagged_start_char_end_char, create_label_catma_id_to_label_mapping, \
    create_text_catma_id_to_start_char_end_char_mapping, add_start_char_end_char_to_df, extract_committee_from_text, \
    extract_protocol_number_from_text
from catma.validations import get_single_validated_file_content, get_single_validated_file_lines
from catma.dataset_creation.generic_dataset_creation import GenericDatasetCreator
from configuration.df_columns import LABEL_COLUMN, LABEL_CATMA_ID_COLUMN, TEXT_SEGMENT_CATMA_ID_COLUMN, FILE_COLUMN, \
    PROTOCOL_NUMBER_COLUMN, TEXT_COLUMN, START_CHAR_COLUMN, COMMITTEE_COLUMN
from configuration.general_config import ITAY_CATMA_CONFIG, CATMA_XML_ANNOTATION_DIR

NOT_SPEAKER_START_PHRASES = ['נכחו', 'סדר היום', 'חברי הכנסת', 'מוזמנים', 'חברי הוועדה', 'משרד האוצר', 'משרד הפנים',
                             'נוכחים',
                             'ייעוץ משפטי', 'מנהל הוועדה', 'רישום פרלמנטרי', 'מנהלת הוועדה', 'מנהל/ת הוועדה',
                             'קצרנית פרלמנטרית', 'רשמה וערכה', 'רכזת הוועדה', 'רשמת פרלמנטרית', 'נציבות המים',
                             'משרד התשתיות הלאומיות', 'המשרד לאיכות הסביבה', 'הרשות לשירותים ציבוריים מים וביוב',
                             'אדם טבע ודין']


@dataclass
class Speaker:
    speaker_name_start_char: int
    speaker_text_start_char: int
    end_char: int
    speaker_name: str
    speaker_text: str


@dataclass
class AnnotationIncludingSpeakerInfo:
    speaker_name_start_char: int
    speaker_text_start_char: int
    speaker_end_char: int
    speaker_name: str
    speaker_text: str
    label: Optional[str]
    label_start_char: Optional[int]
    label_end_char: Optional[int]
    # todo: add text clean, text len, text word count


class SpeakerLevelDatasetCreator(GenericDatasetCreator):
    # def get_df_from_protocol_dirs(self, protocol_dirs: List[str]) -> pd.DataFrame:
    #     protocol_dfs_to_concatenate = []
    #     for protocol_dir in protocol_dirs:
    #         print(f"Getting sentence level df from protocol dir: {protocol_dir}")
    #         protocol_matching_df = self.get_df_from_protocol_dir(protocol_dir)
    #         protocol_dfs_to_concatenate.append(protocol_matching_df)
    #     concatenated_protocols_df = pd.concat(protocol_dfs_to_concatenate, ignore_index=True)
    #     concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[TEXT_COLUMN])
    #     concatenated_protocols_df[CLEAN_TEXT_COLUMN] = concatenated_protocols_df.apply(
    #         lambda row: fix_text(row[TEXT_COLUMN]), axis=1)
    #     concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[CLEAN_TEXT_COLUMN])
    #     concatenated_protocols_df = concatenated_protocols_df[
    #         concatenated_protocols_df[CLEAN_TEXT_COLUMN].str.len() > 0]
    #     concatenated_protocols_df = concatenated_protocols_df.dropna(subset=[CLEAN_TEXT_COLUMN])
    #     return concatenated_protocols_df

    def get_df_from_protocol_dir(self, protocol_dir: str) -> Optional[pd.DataFrame]:
        txt_data = get_single_validated_file_content(file_directory_path=protocol_dir,
                                                     suffix="txt")
        txt_data_lines = get_single_validated_file_lines(file_directory_path=protocol_dir,
                                                         suffix="txt")
        speaker_df_before_annotations = self._get_speaker_df_from_txt_data(txt_data,
                                                                           txt_data_lines)  # todo: add speaker text clean

        xml_data = get_single_validated_file_content(
            file_directory_path=os.path.join(protocol_dir, CATMA_XML_ANNOTATION_DIR), suffix="xml")
        only_annotation_df = self._get_annotation_df_from_xml_data(xml_data)

        annotations_with_speaker_info = []
        for row_index, annotation_row in only_annotation_df.iterrows():
            # print(annotation_row["start_char"], annotation_row["end_char"])
            matching_speaker_level_df_row = speaker_df_before_annotations[
                speaker_df_before_annotations["speaker_name_start_char"] <= annotation_row["start_char"]].tail(1)
            speaker_name_start_char = matching_speaker_level_df_row["speaker_name_start_char"].iloc[0]
            speaker_text_start_char = matching_speaker_level_df_row["speaker_text_start_char"].iloc[0]
            speaker_end_char = matching_speaker_level_df_row["end_char"].iloc[0]
            speaker_name = matching_speaker_level_df_row["speaker_name"].iloc[0]
            speaker_text = matching_speaker_level_df_row["speaker_text"].iloc[0]
            label = annotation_row["label"]
            label_start_char = annotation_row["start_char"]
            label_end_char = annotation_row["end_char"]
            assert speaker_name_start_char <= label_start_char
            assert speaker_end_char >= label_end_char
            annotated_row = AnnotationIncludingSpeakerInfo(speaker_name_start_char=speaker_name_start_char,
                                                           speaker_text_start_char=speaker_text_start_char,
                                                           speaker_end_char=speaker_end_char,
                                                           speaker_name=speaker_name, speaker_text=speaker_text,
                                                           label=label,
                                                           label_start_char=label_start_char,
                                                           label_end_char=label_end_char)
            annotations_with_speaker_info.append(annotated_row)
        annotated_with_speakers_df = pd.DataFrame.from_dict(
            [annotated_row.__dict__ for annotated_row in annotations_with_speaker_info])
        # todo: add all speakers with no annotations
        speakers_without_annotation = []
        for row_index, speaker_row in speaker_df_before_annotations.iterrows():
            if speaker_row["speaker_name_start_char"] not in annotated_with_speakers_df["speaker_name_start_char"]:
                speaker_info_without_annotation = AnnotationIncludingSpeakerInfo(
                    speaker_name_start_char=speaker_row["speaker_name_start_char"],
                    speaker_text_start_char=speaker_row["speaker_text_start_char"],
                    speaker_end_char=speaker_row["end_char"],
                    speaker_name=speaker_row["speaker_name"],
                    speaker_text=speaker_row["speaker_text"],
                    label=None,
                    label_start_char=None, label_end_char=None)
                speakers_without_annotation.append(speaker_info_without_annotation)
        speakers_without_annotation_df = pd.DataFrame.from_dict([row.__dict__ for row in speakers_without_annotation])

        speakers_annotations_df = pd.concat([annotated_with_speakers_df, speakers_without_annotation_df],
                                            ignore_index=True)
        speakers_annotations_df = speakers_annotations_df.sort_values(
            by=["speaker_name_start_char", "label_start_char"])
        speakers_annotations_df = speakers_annotations_df.drop_duplicates(ignore_index=True)
        speakers_annotations_df = speakers_annotations_df.drop_duplicates(
            subset=["speaker_name_start_char", "label"], keep="first", ignore_index=True)
        # todo: add text
        if self.remove_new_line_and_tab:
            pass
            # current_text_length = len(txt_data_line.replace("\n", "  ").replace("\t", " "))
        speakers_annotations_df[FILE_COLUMN] = os.path.basename(protocol_dir)
        committee = extract_committee_from_text(txt_data)
        speakers_annotations_df[COMMITTEE_COLUMN] = committee
        try:
            protocol_number = extract_protocol_number_from_text(txt_data)
        except Exception as exception:
            print(
                f"Using None since couldnt find protocol number for protocol dir: {protocol_dir}, committee is {committee}, exception is: {exception}")
            protocol_number = None
        speakers_annotations_df[PROTOCOL_NUMBER_COLUMN] = protocol_number
        return speakers_annotations_df

    def _get_speaker_df_from_txt_data(self, txt_data: str, txt_data_lines: List[str]) -> pd.DataFrame:
        last_end_char = 0
        first_speaker_in_text = True
        speaker_name_start_char = -1
        speaker_text_start_char = -1
        speakers = []
        print(
            '''Important: check if you need to remove new line and tab or not (depends on dataset).
            you can change it in the init of CatmaDatasetCreation.
            You can validate it using CATMA analyze with some tags to check.''')
        for i, txt_data_line in enumerate(txt_data_lines):
            current_start_char = last_end_char  # todo: maybe need to add 1 here
            if self.remove_new_line_and_tab:
                current_text_length = len(txt_data_line.replace("\n", "  ").replace("\t", " "))
            else:
                current_text_length = len(txt_data_line)
            if txt_data_line.endswith(":\n") and txt_data_line.replace(":", "").replace("\n",
                                                                                        "") not in NOT_SPEAKER_START_PHRASES:  # todo: we can filter more words
                # we start a new speaker
                if first_speaker_in_text:
                    first_speaker_in_text = False
                    speaker_name_start_char = current_start_char
                    speaker_text_start_char = speaker_name_start_char + current_text_length
                else:
                    # print(
                    #     f"Adding speaker: name_start: {speaker_name_start_char}, text_start: {speaker_text_start_char}, end_char: {last_end_char}")
                    speaker_name = txt_data.replace("\n", "  ").replace("\t", " ")[
                                   speaker_name_start_char:speaker_text_start_char]
                    speaker_text = txt_data.replace("\n", "  ").replace("\t", " ")[
                                   speaker_text_start_char:last_end_char]
                    # print(f"Speaker name: {speaker_name}")
                    # print(f"Speaker text: {speaker_text}")
                    latest_speaker = Speaker(speaker_name_start_char=speaker_name_start_char,
                                             speaker_text_start_char=speaker_text_start_char, end_char=last_end_char,
                                             speaker_name=speaker_name, speaker_text=speaker_text)
                    speakers.append(latest_speaker)
                    speaker_name_start_char = current_start_char
                    speaker_text_start_char = speaker_name_start_char + current_text_length  # todo add "  " (2 chars)
                # print("new speaker start:")
                # print("text length", len(txt_data_line), "current start char", current_start_char)
                # print(
                #     f"*\nline_before: {txt_data_lines[i - 1]}line: {txt_data_line}line_after: {txt_data_lines[i + 1]}2_lines_after: {txt_data_lines[i + 2]}*\n")
            last_end_char += current_text_length
        speaker_name = txt_data.replace("\n", "  ").replace("\t", " ")[speaker_name_start_char:speaker_text_start_char]
        speaker_text = txt_data.replace("\n", "  ").replace("\t", " ")[speaker_text_start_char:last_end_char]
        # print(f"Speaker name: {speaker_name}")
        # print(f"Speaker text: {speaker_text}")
        last_speaker = Speaker(speaker_name_start_char=speaker_name_start_char,
                               speaker_text_start_char=speaker_text_start_char, end_char=last_end_char,
                               speaker_name=speaker_name, speaker_text=speaker_text)
        speakers.append(last_speaker)
        speakers_df = pd.DataFrame.from_dict([speaker.__dict__ for speaker in speakers])
        speakers_df["speaker_text_length"] = speakers_df.apply(
            lambda row: row["end_char"] - row["speaker_name_start_char"], axis=1)
        speakers_df["speaker_text_clean"] = speakers_df.apply(lambda row: " ".join(row["speaker_text"].split()), axis=1)
        speakers_df["speaker_text_clean_length"] = speakers_df.apply(lambda row: len(row["speaker_text_clean"]),
                                                                     axis=1)
        speakers_df["speaker_text_clean_number_of_words"] = speakers_df.apply(
            lambda row: len(row["speaker_text_clean"].split()),
            axis=1)
        speakers_df = speakers_df.sort_values(by="speaker_name_start_char")
        speakers_df = speakers_df.drop_duplicates(ignore_index=True)
        return speakers_df  # todo: return a df of speakers

    def _get_annotation_df_from_xml_data(self, xml_data: str) -> Optional[pd.DataFrame]:
        bs_data = BeautifulSoup(xml_data, "xml")
        text_catma_id_to_catma_label_id = create_text_catma_id_to_label_mapping(bs_data)
        if len(text_catma_id_to_catma_label_id) == 0:
            print("Empty annotation file, skipping it")
            return None
        catma_label_id_to_label = create_label_catma_id_to_label_mapping(bs_data)
        text_catma_id_to_start_char_end_char = create_text_catma_id_to_start_char_end_char_mapping(bs_data)

        catma_ids0 = list(set(text_catma_id_to_catma_label_id.keys()))
        catma_ids1 = list(set(text_catma_id_to_start_char_end_char.keys()))
        in0not1 = [catma_id for catma_id in catma_ids0 if catma_id not in catma_ids1]
        in1not0 = [catma_id for catma_id in catma_ids1 if catma_id not in catma_ids0]
        assert len(in0not1) == 0
        assert len(in1not0) == 0

        df = pd.DataFrame(text_catma_id_to_catma_label_id.items(),
                          columns=[TEXT_SEGMENT_CATMA_ID_COLUMN, LABEL_CATMA_ID_COLUMN])
        df[LABEL_COLUMN] = df.apply(lambda row: catma_label_id_to_label[row[LABEL_CATMA_ID_COLUMN]], axis=1)
        df = df.apply(add_start_char_end_char_to_df,
                      text_catma_id_to_start_char_end_char=text_catma_id_to_start_char_end_char, axis=1)
        return df


if __name__ == "__main__":
    catma_config = ITAY_CATMA_CONFIG
    catma_unpacking = CatmaUnpacking(tar_gzs_dir=catma_config.input_catma_tar_gzs_data_path,
                                     unpacked_protocol_archives_dir=catma_config.unpacked_protocol_archives_path)
    catma_unpacking.unpack_tar_gz_files()
    valid_protocol_dirs = catma_unpacking.get_valid_unpacked_protocol_dirs()
    protocol_dir = [protocol_dir for protocol_dir in valid_protocol_dirs if
                    os.path.basename(protocol_dir) == "פרוטוקול_מספר_42_parts"][
        0]  # work on "ועדת הכספים חלקי דיון"-> "פרוטוקול מספר 42
    speaker_level_dataset_creator = SpeakerLevelDatasetCreator(
        remove_new_line_and_tab=ITAY_CATMA_CONFIG.remove_new_line_and_tab)
    temp_df = speaker_level_dataset_creator.get_df_from_protocol_dir(protocol_dir)
