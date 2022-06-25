import os
import re
from dataclasses import dataclass


@dataclass
class CatmaConfig:
    """
    Configuration for the CatMA project.
    """
    input_catma_tar_gzs_data_path: str = "/home/amit/Desktop/TAU/thesis/daniel_catma_data"

    def __post_init__(self):
        self.unpacked_protocol_archives_path = os.path.join(self.input_catma_tar_gzs_data_path,
                                                            "unpacked_protocol_archives")
        self.sentence_level_output_dataset_tsv_path: str = os.path.join(self.input_catma_tar_gzs_data_path,
                                                                        "dataset_with_separated_sentences.tsv")
        self.speaker_level_output_dataset_tsv_path: str = os.path.join(self.input_catma_tar_gzs_data_path,
                                                                       "dataset_with_separated_speakers.tsv")
        self.remove_new_line_and_tab: bool = True if "itay" in self.input_catma_tar_gzs_data_path else False


DANIEL_CATMA_CONFIG = CatmaConfig(input_catma_tar_gzs_data_path="/home/amit/Desktop/TAU/thesis/daniel_catma_data")
ITAY_CATMA_CONFIG = CatmaConfig(input_catma_tar_gzs_data_path="/home/amit/Desktop/TAU/thesis/itay_catma_data")

CONTEXT_SIZE = 100
BEFORE_TEXT_CONTEXT_SIZE = -CONTEXT_SIZE
AFTER_TEXT_CONTEXT_SIZE = CONTEXT_SIZE

CATMA_XML_ANNOTATION_DIR = "annotationcollections"

LABEL_REPLACEMENTS_MAPPING = {"judicial decision turns turns": "Judicial decision",
                              "Anticipating Judicial Review turns": "Anticipating Judicial Review"}
LABELS_TO_DROP = ["Doubt"]

PROTOCOL_DIRS_WITHOUT_SPEAKERS = ["9.7.18", "11.7.18", "4.7.18"]  # based on check_that_all_have_speakers.ipynb
PROTOCOL_DIRS_THAT_LOOK_WEIRD = ["5.7.18", "פרוטוקול_465_ריק"]  # based on check_that_all_have_speakers.ipynb

NOT_SPEAKER_START_PHRASES = ['נכחו', 'סדר היום', 'חברי הכנסת', 'מוזמנים', 'חברי הוועדה', 'משרד האוצר', 'משרד הפנים',
                             'נוכחים',
                             'ייעוץ משפטי', 'מנהל הוועדה', 'רישום פרלמנטרי', 'מנהלת הוועדה', 'מנהל/ת הוועדה',
                             'קצרנית פרלמנטרית', 'רשמה וערכה', 'רכזת הוועדה', 'רשמת פרלמנטרית', 'נציבות המים',
                             'משרד התשתיות הלאומיות', 'המשרד לאיכות הסביבה', 'הרשות לשירותים ציבוריים מים וביוב',
                             'אדם טבע ודין']

START_CHAR_END_CHAR_EXTRACTION_REGEX = re.compile("[\w\W]*char=(\d+),(\d+)")
COMMITTEE_REGEX = re.compile("ועדת" + "[\u0590-\u05FF ]*")
PROTOCOL_NUMBER_REGEX = re.compile("פרוטוקול מס'" + "[ ]*" + "[\d]+")
