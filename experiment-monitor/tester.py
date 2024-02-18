import json

import pandas as pd

from utils.Data import ExperimentData
from utils.Logger import Logger

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


def load_json(file_path) -> [dict]:
    res = []
    with open(file_path, "r") as file:
        for line in file:
            res.append(json.loads(line))
    return res


# Extract metrics per subtask from a json exported metrics file from victoriametrics
def get_metrics_per_subtask(metrics_content, metric_name, task_name) -> pd.DataFrame:
    data = {}
    for metric in metrics_content:
        if (
                metric["metric"]["task_name"] == task_name

        ):
            subtask_index = metric["metric"]["subtask_index"]
            if subtask_index not in data:
                data[subtask_index] = []
            for value, timestamp in zip(metric["values"], metric["timestamps"]):
                # Divide by 5000 to facilitate the join of multiple columns
                data[subtask_index].append((round(timestamp / 5000), value))

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(
        {
            f"{metric_name}_{k}": pd.Series(
                dict(v), name=f"{metric_name}_{k}"
            )
            for k, v in data.items()
        }
    )

    # Sort the DataFrame by the timestamps
    df.sort_index(inplace=True)

    # Extract subtask indices from column names and sort columns by these indices
    df.columns = df.columns.str.extract("(\d+)", expand=False).astype(int)
    df = df.sort_index(axis=1)

    # Multindex subtask columns under the metric name
    df.columns = pd.MultiIndex.from_product([[metric_name], df.columns])

    # Reset timestamps
    df.index = df.index * 5000
    # Add "Timestamp" to the index
    df.index.name = "Timestamp"

    return df


def get_sources_metrics(metrics_content, metric_name) -> [pd.DataFrame]:
    # For a given job name, extract metrics for sources in a panda dataframe
    # If there are multiple sources, return a list of panda dataframes

    sources = set()
    for metric in metrics_content:
        if "Source" in metric["metric"]["task_name"]:
            sources.add(metric["metric"]["task_name"])
    res = []
    for source in sources:
        data = {}
        for metric in metrics_content:
            if (
                    metric["metric"]["task_name"] == source

            ):
                subtask_index = metric["metric"]["subtask_index"]
                if subtask_index not in data:
                    data[subtask_index] = []
                for value, timestamp in zip(metric["values"], metric["timestamps"]):
                    # Divide by 5000 to facilitate the join of multiple columns
                    data[subtask_index].append((round(timestamp / 5000), value))

        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(
            {
                f"{metric_name}_{source}_{k}": pd.Series(
                    dict(v), name=f"{metric_name}_{k}"
                )
                for k, v in data.items()
            }
        )

        # Sort the DataFrame by the timestamps
        df.sort_index(inplace=True)

        # Extract subtask indices from column names and sort columns by these indices
        df.columns = df.columns.str.extract("(\d+)", expand=False).astype(int)
        df = df.sort_index(axis=1)

        # Multindex subtask columns under the metric name
        df.columns = pd.MultiIndex.from_product([[metric_name], df.columns])

        # Reset timestamps
        df.index = df.index * 5000
        # Add "Timestamp" to the index
        df.index.name = "Timestamp"
        res.append(df)
    return res


if __name__ == "__main__":
    exp_path = "test-export/15"
    log = Logger()
    # Load experiment data
    exp_data = ExperimentData(log, exp_path)

    metrics_with_subtasks = ["flink_taskmanager_job_task_numRecordsInPerSecond",
                             "flink_taskmanager_job_task_busyTimeMsPerSecond",
                             "flink_taskmanager_job_task_numRecordsOutPerSecond"]
    sources = ["flink_taskmanager_job_task_hardBackPressuredTimeMsPerSecond"]
    job_metrics = ["flink_jobmanager_job_lastCheckpointSize",
                   "flink_jobmanager_job_lastCheckpointDuration"]

    metrics_with_subtasks_list = []
    metrics_sources_list = []
    metrics_job_list = []
    for metric in metrics_with_subtasks:
        path = exp_data.export_timeseries_json(
            metric
        )
        json_content = load_json(path)
        df = get_metrics_per_subtask(
            json_content,
            metric,
            "TumblingEventTimeWindows____Timestamps_Watermarks",
        )
        df.to_csv(f"output/{metric}.csv")
        metrics_with_subtasks_list.append(df)
    for metric in job_metrics:
        path, df = exp_data.export_timeseries_csv(
            metric
        )
        metrics_job_list.append(df)
    for metric in sources:
        path = exp_data.export_timeseries_json(
            metric
        )
        json_content = load_json(path)
        res = get_sources_metrics(
            json_content,
            metric,
            "myjoin_transscale_0_0_1",
        )
        for i, df in enumerate(res):
            df.to_csv(f"output/{metric}_{i}.csv")
            metrics_sources_list.append(df)

    # Join the dataframes in metrics_with_subtasks_list on Timestamp index
    df = pd.concat(metrics_with_subtasks_list, axis=1)

    # Join the dataframes in metrics_sources_list on Timestamp index
    df_sources = pd.concat(metrics_sources_list, axis=1)

    # # metrics_job_list: divide by 5000 the index to facilitate the join of multiple columns on Timestamp index, then join the dataframes
    # for i, df in enumerate(metrics_job_list):
    #     df.index = df.index / 5000
    #     df.index.name = "Timestamp"
    #     df.columns = pd.MultiIndex.from_product([[job_metrics[i]], df.columns])
    #     metrics_job_list[i] = df
    # df_job = pd.concat(metrics_job_list, axis=1)
    # print(df_job.to_string())
