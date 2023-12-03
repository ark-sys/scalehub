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

        # Check if experiment.delay.enable
        latency_test_match = re.search(r"experiment.delay.enable = (.+)", logs)
        latency_delay_match = re.search(r"experiment.delay.latency = (.+)", logs)
        latency_jitter_match = re.search(r"experiment.delay.jitter = (.+)", logs)
        latency_correlation_match = re.search(r"experiment.delay.correlation = (.+)", logs)

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
        # Check if latency for the experiment has been enabled
        latency_delay = "0ms"
        latency_jitter = "0ms"
        latency_correlation = "0"
        if latency_test_match:
            latency_test = latency_test_match.group(1)
            if latency_test.lower() == "true":
                latency_enabled = True
                if latency_delay_match:
                    latency_delay = latency_delay_match.group(1)
                else:
                    self.__log.error("Latency delay not found in log.")
                    exit(1)
                if latency_jitter_match:
                    latency_jitter = latency_jitter_match.group(1)
                else:
                    self.__log.error("Latency jitter not found in log.")
                    exit(1)
                if latency_correlation_match:
                    latency_correlation = latency_correlation_match.group(1)
                else:
                    self.__log.error("Latency correlation not found in log.")
                    exit(1)
            else:
                latency_enabled = False
                latency_delay = "0ms"
                latency_jitter = "0ms"
                latency_correlation = "0"
        else:
            self.__log.error("Latency test information not found in log.")
            latency_enabled = False
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
            latency_enabled,
            latency_delay,
            latency_jitter,
            latency_correlation,
        )
