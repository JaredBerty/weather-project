import os


class DirectoryManager:
    def __init__(self):
        self.parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.json_data_dir = os.path.join(self.parent_dir, 'json_data')

    def change_dir_to_json_data(self):
        os.chdir(self.json_data_dir)
        print("changed directory to JSON data folder.")
        print(self.json_data_dir)

    def change_dir_to_parent(self):
        os.chdir(self.parent_dir)
        print("Changed directory to parent folder")
        print(self.parent_dir)

new_manager = DirectoryManager()
new_manager.change_dir_to_json_data()
new_manager.change_dir_to_parent()