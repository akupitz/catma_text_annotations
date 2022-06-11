import os
import re

INPUT_CATMA_TAR_GZS_DATA_PATH = "/home/amit/Desktop/TAU/thesis/daniel_catma_data"
UNPACKED_PROTOCOL_ARCHIVES_PATH = os.path.join(INPUT_CATMA_TAR_GZS_DATA_PATH, "unpacked_protocol_archives")

BASIC_OUTPUT_TSV_PATH = os.path.join(INPUT_CATMA_TAR_GZS_DATA_PATH,
                                     "amit_data_did_not_separate_annotated_only_tagged.tsv")
OUTPUT_DATASET_TSV_PATH = os.path.join(INPUT_CATMA_TAR_GZS_DATA_PATH,
                                                             "data_with_separated_sentences.tsv")

CONTEXT_SIZE = 100
BEFORE_TEXT_CONTEXT_SIZE = -CONTEXT_SIZE
AFTER_TEXT_CONTEXT_SIZE = CONTEXT_SIZE

CATMA_XML_ANNOTATION_DIR = "annotationcollections"

LABEL_REPLACEMENTS_MAPPING = {"judicial decision turns turns": "Judicial decision",
                              "Anticipating Judicial Review turns": "Anticipating Judicial Review"}
LABELS_TO_DROP = ["Doubt"]

START_CHAR_END_CHAR_EXTRACTION_REGEX = re.compile("[\w\W]*char=(\d+),(\d+)")
COMMITTEE_REGEX = re.compile("ועדת" + "[\u0590-\u05FF ]*")
PROTOCOL_NUMBER_REGEX = re.compile("פרוטוקול מס'" + "[ ]*" + "[\d]+")
