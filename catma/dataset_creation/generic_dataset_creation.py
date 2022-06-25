from typing import List


class GenericDatasetCreator:
    def __init__(self, remove_new_line_and_tab: bool = False):
        self.remove_new_line_and_tab = remove_new_line_and_tab

    def get_df_from_protocol_dirs(self, protocol_dirs: List[str]):
        pass

    def get_df_from_protocol_dir(self, protocol_dir: str):
        pass
