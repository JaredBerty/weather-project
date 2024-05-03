import os
import config


class DirectoryManager:

    @staticmethod
    def json():
        os.chdir(config.JSON_DATA_DIR)
        print("changed directory to JSON data folder.")

    @staticmethod
    def root():
        os.chdir(config.ROOT_DIR)
        print("Changed directory to parent folder")
