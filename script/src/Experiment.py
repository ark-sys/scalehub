from datetime import datetime
import os

from .utils.Config import Key as Key, Config
from .utils.Defaults import DefaultValues as Value


# Setup for an experiment:
# 1. Get timestamp at the beginning of the experiment.
# 2. Create base folder of the experiment based on the date of the day. Then, within this folder, create a folder for the Nth experiment.
# 3. Save timestamp file in the experiment folder.
# 4. Run job and Run transscale, wait for when transscale finishes <- maybe setup a trigger for this action?
# 5. Retrieve timestamp at the end of execution and write it to file.
# 6. Query VictoriaMetrics for csv data and save all files to experiment folder.

class Experiment:
    def __init__(self, config: Config):

        # Get setting from configuration file
        self.experiment = config.get_str(Key.NAME)
        self.topic_sources = config.get_list_str(Key.TOPIC_SOURCES)
        self.num_sensors = config.get_list_int(Key.NUM_SENSORS)
        self.interval_ms = config.get_int(Key.INTERVAL_MS)

        self.db_url = config.get_str(Key.DB_URL)

        self.experiment_base_path = config.get_str(Key.EXPERIMENTS_DATA_PATH)


        # TODO remove this if it is not necessary. Base path should exist within the container as it is mounted at runtime.
        # # Create the base folder if it doesn't exist
        # if not os.path.exists(self.experiment_base_path):
        #     os.makedirs(self.experiment_base_path)

        self.start_ts = self.get_current_timestamp()

        self.create_exp_folder(datetime.fromtimestamp(self.start_ts).strftime('%d-%m-%Y'))

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
        return int(datetime.now().timestamp())

    def query_data(self, start_ts, end_ts, exp_folder):
        pass

import requests

class Query:
    def __init__(self, metric_name, task_name):
        self.base_url = "http://localhost:8428/api/v1/export/csv"
        self.metric_name = metric_name
        self.task_name = task_name

    def execute(self, start_time, end_time):
        url = f"{self.base_url}?match[]={self.metric_name}{{task_name=~\"{self.task_name}\"}}&format=__timestamp__:unix_s,__value__&start={start_time}&end={end_time}"
        response = requests.get(url)

        if response.status_code == 200:
            # Process the response as needed
            csv_data = response.content.decode('utf-8')
            # Additional processing code can be added here
            return csv_data
        else:
            print("Failed to execute the query.")
            return None
