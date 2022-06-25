# from dataset_creation.configuration import BASIC_OUTPUT_TSV_PATH
import pandas as pd

from configuration.df_columns import TEXT_SEGMENT_CATMA_ID_COLUMN
from configuration.general_config import DANIEL_CATMA_CONFIG

# todo: somehow it doesnt work anymore- check why
# def check_text1(df):
#     df_row = df[df[TEXT_SEGMENT_CATMA_ID_COLUMN] == "CATMA_70D11C17-589B-4D50-A977-98DA313EA180"]
#     assert df_row["start_char"].iloc[0] == 2303
#     assert df_row["end_char"].iloc[0] == 2544
#     assert df_row["label"].iloc[0] == "Judicial decision"
#     assert df_row["text"].iloc[0].startswith('בג"ץ, מגדלי העופות')
#
#
# if __name__ == "__main__":
#     df = pd.read_csv(DANIEL_CATMA_CONFIG.sentence_level_output_dataset_tsv_path, sep="\t")
#     check_text1(df)

# todo: check itay's ועדת הכספים חלקי דיון- > 42