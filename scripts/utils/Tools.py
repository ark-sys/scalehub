import os

from scripts.utils.Logger import Logger


class Tools:
    def __init__(self, log: Logger):
        self.__log = log

    def create_exp_folder(self, base_path: str, date):
        # Create the base folder path
        base_folder_path = os.path.join(base_path, date)
        # Find the next available subfolder number
        subfolder_number = 1
        while True:
            subfolder_path = os.path.join(base_folder_path, str(subfolder_number))
            if not os.path.exists(subfolder_path):
                break
            subfolder_number += 1
        try:
            # Create the subfolder
            os.makedirs(subfolder_path)
        except OSError as e:
            self.__log.error(
                f"Error while creating experiment folder {subfolder_path}: {e}"
            )
            raise e

        self.__log.info(f"Created experiment folder: {subfolder_path}")
        # Return the path to the new subfolder
        return subfolder_path

    def get_timestamp_from_log(self, full_exp_path):
        # Get the log file
        log_file = os.path.join(full_exp_path, "log.txt")
        # Read the log file
        with open(log_file, "r") as file:
            lines = file.readlines()
        # Get the timestamp start and end timestamps
        start_ts = lines[0].split(":")[1].strip()
        end_ts = lines[-1].split(":")[1].strip()
        return start_ts, end_ts
