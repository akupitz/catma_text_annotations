from catma.archives_unpacking import CatmaUnpacking
# from catma.dataset_creation.sentence_level_dataset_creation import SentenceLevelDatasetCreator
from catma.dataset_creation.speaker_level_dataset_creation import SpeakerLevelDatasetCreator
from configuration.general_config import ITAY_CATMA_CONFIG, DANIEL_6_LABELS_CATMA_CONFIG

if __name__ == "__main__":
    """
    In order to get the initial data go to: CATMA -> project -> in Documents & Annotations select files one by one and Export Documents & Collections 
    In order to validate it easily go to: CATMA -> analyze -> select document with annotation (important!!!) -> use KWIC -> select all
    """
    catma_config = DANIEL_6_LABELS_CATMA_CONFIG # ITAY_CATMA_CONFIG
    catma_unpacking = CatmaUnpacking(tar_gzs_dir=catma_config.input_catma_tar_gzs_data_path,
                                     unpacked_protocol_archives_dir=catma_config.unpacked_protocol_archives_path)
    catma_unpacking.unpack_tar_gz_files()
    valid_protocol_dirs = catma_unpacking.get_valid_unpacked_protocol_dirs()
    catma_dataset_creation = SpeakerLevelDatasetCreator(remove_new_line_and_tab=catma_config.remove_new_line_and_tab)

    speaker_level_df = catma_dataset_creation.get_df_from_protocol_dirs(valid_protocol_dirs)
    speaker_level_df.to_csv(catma_config.speaker_level_output_dataset_tsv_path, sep="\t", index=False)
    # sentence_level_df = catma_dataset_creation.get_sentence_level_df_from_protocol_dirs(valid_protocol_dirs)
    # sentence_level_df.to_csv(catma_config.sentence_level_output_dataset_tsv_path, sep="\t", index=False)
    # todo: concatenated df should be on a sentence level so we need to split by [".", ":", "?"]
    # todo: remove unnecessary text from documents
    # todo: when doing explode to split text to sentences i don't update start_char and end_char, so do it
    # todo: add tests
