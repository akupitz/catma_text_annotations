from catma.catma_unpacking import CatmaUnpacking
from catma.catma_dataset_creation import CatmaDatasetCreation
from configuration.general_config import INPUT_CATMA_TAR_GZS_DATA_PATH, UNPACKED_PROTOCOL_ARCHIVES_PATH, \
    OUTPUT_DATASET_TSV_PATH

if __name__ == "__main__":
    """
    In order to get the initial data go to: CATMA -> project -> in Documents & Annotations select files one by one and Export Documents & Collections 
    In order to validate it easily go to: CATMA -> analyze -> select document with annotation (important!!!) -> use KWIC -> select all
    """
    catma_unpacking = CatmaUnpacking(tar_gzs_dir=INPUT_CATMA_TAR_GZS_DATA_PATH,
                                     unpacked_protocol_archives_dir=UNPACKED_PROTOCOL_ARCHIVES_PATH)
    catma_unpacking.unpack_tar_gz_files()
    valid_protocol_dirs = catma_unpacking.get_valid_unpacked_protocol_dirs()
    catma_dataset_creation = CatmaDatasetCreation(remove_new_line_and_tab=False)
    sentence_level_df = catma_dataset_creation.get_sentence_level_df_from_protocol_dirs(valid_protocol_dirs)
    sentence_level_df.to_csv(OUTPUT_DATASET_TSV_PATH, sep="\t", index=False)
    # todo: concatenated df should be on a sentence level so we need to split by [".", ":", "?"]
    # todo: remove unnecessary text from documents
    # todo: when doing explode to split text to sentences i don't update start_char and end_char, so do it
    # todo: add tests
