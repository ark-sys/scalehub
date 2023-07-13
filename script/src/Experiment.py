import datetime

import subprocess
from datetime import date
import os

from .utils.Config import Key as Key, Config
from .utils.Defaults import DefaultValues as Value


class Experiment:
    def __init__(self, config: Config):

        self.experiment = config.get_str(Key.NAME)
        self.topic_sources = config.get_list_str(Key.TOPIC_SOURCES)
        self.num_sensors = config.get_list_int(Key.NUM_SENSORS)
        self.interval_ms = config.get_int(Key.INTERVAL_MS)

        self.db_url = config.get_str(Key.DB_URL)

        self.experiment_base_path = config.get_str(Key.EXPERIMENTS_DATA_PATH)
        # Create the base folder if it doesn't exist
        if not os.path.exists(self.experiment_base_path):
            os.makedirs(self.experiment_base_path)

        self.start_ts = self.get_current_timestamp()

    # Creates a folder for the results of the experiments
    def create_exp_folder(self, date):
        # Create the base folder path
        base_folder_path = os.path.join(self.experiment_base_path, date)
        # Find the next available subfolder number
        subfolder_number = 1
        while True:
            subfolder_path = os.path.join(base_folder_path, str(subfolder_number))
            if not os.path.exists(subfolder_path):
                break
            subfolder_number += 1

        # Create the subfolder
        os.makedirs(subfolder_path)

        # Return the path to the new subfolder
        return subfolder_path

    def export_data_to_csv(self, exp_path, time_series_name, start_timestamp=None, end_timestamp=None,
                           format_labels='__name__,__value__,__timestamp__:unix_s'):
        # VictoriaMetrics export CSV api
        api_url = f'http://{self.db_url}/api/v1/export/csv'
        params = {
            'format': format_labels,
            'match': time_series_name,
            'start': start_timestamp,
            'end': end_timestamp
        }
        import requests
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            output_file = os.path.join(exp_path, f'{time_series_name}_export.csv')
            with open(output_file, 'wb') as file:
                file.write(response.content)
            print(f'Data exported to {output_file}')
        else:
            print(f'Error exporting data: {response.text}')

    def get_current_timestamp(self):
        return int(datetime.datetime.now().timestamp())

    def query_data(self, start_ts, end_ts, exp_folder):
        pass
