from utils.Data import ExperimentData
from utils.Logger import Logger
import json
import pandas as pd

queries = {
    "numTaskmanagers": "sum by(namespace) (flink_jobmanager_numRegisteredTaskManagers)",
    "parallelism": 'sum by(task_name) (count_values by() ("subtask_index", flink_taskmanager_job_task_numRecordsOutPerSecond))',
    "numRecordsInPerSecond": 'sum by(task_name) (flink_taskmanager_job_task_numRecordsInPerSecond{{job_name="{Job}", task_name="{Task}"}})',
    "numRecordsInPerSecondPerSubtask": 'sum by(task_name, subtask_index) (flink_taskmanager_job_task_numRecordsInPerSecond{{job_name="{Job}", task_name="{Task}"}})',
    "backpressureTimePerSecond": 'sum by(task_name, subtask_index) (flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond{{job_name="{Job}"}})',
    "busyTimePerSecond": 'sum by(task_name, subtask_index) (flink_taskmanager_job_task_busyTimeMsPerSecond{{job_name="{Job}", task_name="{Task}"}})',
    "lastCheckpointSize": "sum by(job_name) (flink_jobmanager_job_lastCheckpointSize)",
    "lastcheckpointDuration": "sum by(job_name) (flink_jobmanager_job_lastCheckpointDuration)",
}


def process_json_data(file_path, task_name, job_name):
    data = {}
    with open(file_path, "r") as f:
        for line in f:
            json_obj = json.loads(line)
            if (
                json_obj["metric"]["task_name"] == task_name
                and json_obj["metric"]["job_name"] == job_name
            ):
                subtask_index = json_obj["metric"]["subtask_index"]
                if subtask_index not in data:
                    data[subtask_index] = []
                for value, timestamp in zip(json_obj["values"], json_obj["timestamps"]):
                    data[subtask_index].append((timestamp, value))

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(
        {
            f"Throughput_subtask_{k}": pd.Series(
                dict(v), name=f"Throughput_subtask_{k}"
            )
            for k, v in data.items()
        }
    )

    # Sort the DataFrame by the timestamps
    df.sort_index(inplace=True)

    # Extract subtask indices from column names and sort columns by these indices
    df.columns = df.columns.str.extract("(\d+)", expand=False).astype(int)
    df = df.sort_index(axis=1)

    return df


if __name__ == "__main__":
    exp_path = "test-export/15"
    log = Logger()
    # Load experiment data
    exp_data = ExperimentData(log, exp_path)

    # task = "TumblingEventTimeWindows____Timestamps_Watermarks"
    # job = "myjoin_transscale_0_0_1"
    # query = queries["parallelism"].format(Task=task, Job=job)
    #
    # res = exp_data.perf_query(query)
    # print(res)
    #

    path = exp_data.export_timeseries_json(
        "flink_taskmanager_job_task_numRecordsInPerSecond"
    )
    df = process_json_data(
        path,
        "TumblingEventTimeWindows____Timestamps_Watermarks",
        "myjoin_transscale_0_0_1",
    )
    print(df.to_string())
