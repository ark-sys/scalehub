import re
from .Logger import Logger


class Misc:
    def __init__(self, log: Logger):
        self.__log = log

    def add_time(self):
        command = 'ssh -t access.grid5000.fr "ssh \\$LOGNAME@rennes"'
        add_time = " oarwalltime $(oarstat -u | tail -n 1 | cut -d ' ' -f 1) +1"

    def parse_log(self, log_path: str):
        with open(log_path, "r") as log_file:
            logs = log_file.read()

        job_name_match = re.search(r"experiment.job_file = (.+)", logs)
        lg_matches = re.finditer(
            r"name = (.+?)\s+topic = (.+?)\s+num_sensors = (\d+)\s+interval_ms = (\d+)",
            logs,
            re.DOTALL,
        )
        start_match = re.search(r"Experiment start at : (\d+)", logs)
        end_match = re.search(r"Experiment end at : (\d+)", logs)

        # Check if experiment.latency_test = False
        latency_test_match = re.search(r"experiment.latency_test = (.+)", logs)

        if job_name_match:
            job_name = job_name_match.group(1)
        else:
            self.__log.error("Job name not found in log.")
            exit(1)
        if start_match and end_match:
            start_timestamp = int(start_match.group(1))
            end_timestamp = int(end_match.group(1))
        else:
            self.__log.error("Log file is incomplete: missing timestamp.")
            exit(1)
        if latency_test_match:
            latency_test = latency_test_match.group(1)
        else:
            self.__log.error("Latency test information not found in log.")
            latency_test = False
        num_sensors_sum = 0
        interval_ms_sum = 0
        lg_count = 0

        for lg_match in lg_matches:
            num_sensors = int(lg_match.group(3))
            interval_ms = int(lg_match.group(4))
            num_sensors_sum += num_sensors
            interval_ms_sum += interval_ms
            lg_count += 1

        if lg_count == 0:
            self.__log.error("No LG information found in log.")
            exit(1)

        avg_interval_ms = interval_ms_sum / lg_count

        return (
            job_name,
            num_sensors_sum,
            avg_interval_ms,
            start_timestamp,
            end_timestamp,
            latency_test,
        )
