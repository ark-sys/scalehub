# Change log timestamps format from [DD/MM/YYYY HH:MM:SS] to [YYYY-MM-DDTHH:MM:SS]
import os

# 1. Check if the provived log file (transscale_log.txt) has a line with the format [DD/MM/YYYY HH:MM:SS]
# 2. If the file has a line with the format [DD/MM/YYYY HH:MM:SS], change the format for all the lines to [YYYY-MM-DDTHH:MM:SS]
# 3. Save the changes to the same file
import re
from os.path import exists


def check_log_format(file):
    with open(file) as f:
        for line in f:
            if re.search(r"\[\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}\]", line):
                return True
        return False


def change_log_format(file):
    if not check_log_format(file):
        return

    with open(file, "r") as f:
        content = f.read()

    content_new_format = re.sub(
        r"\[(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2}:\d{2})\]", r"[\3-\2-\1T\4]", content
    )

    with open(file, "w") as f:
        f.write(content_new_format)


def reformat_file(log_file):
    config = {}
    with open(log_file, "r") as file_content:
        file_data = file_content.read()

    # Extract the config from the log file
    config_data = file_data.split("Experiment start ")[0]

    # Extract the timestamps. It's the diff between data and config
    timestamps = file_data[len(config_data) :]

    import configparser as cp

    dummy_header = "config"

    parser = cp.ConfigParser()
    content = f"[{dummy_header}]\n" + config_data

    parser.read_string(content)
    conf = parser[dummy_header]

    for key in conf:
        config[key] = conf[key]

    lg = parse_load_generators(config)
    config["experiment.load_generators"] = lg

    new_file = f"[CONFIG]\n{config}\n\n[TIMESTAMPS]\n{timestamps}"
    return new_file


def parse_load_generators(config: dict):
    load_generators_section = config.get("experiment.load_generators")

    load_generators = []
    current_generator = None

    for line in load_generators_section.split("\n"):
        line = line.strip()
        if not line:
            continue

        if line.startswith("- name"):
            if current_generator:
                load_generators.append(current_generator)
            current_generator = {"name": line.split("=")[1].strip()}
        else:
            key, value = map(str.strip, line.split("="))
            current_generator[key] = value

    if current_generator:
        load_generators.append(current_generator)

    return load_generators


def detect_file_version(file):
    with open(file) as f:
        # Only check if the first line contains the [CONFIG] tag
        first_line = f.readline()
        if first_line.startswith("[CONFIG]"):
            return 0
        else:
            return 1


def remove_duplicates(file):
    with open(file, "r") as f:
        content = f.read()

    print(
        f"################################### Handling file {file} ###################################"
    )
    print("Content length before: ", len(content))
    print("\n")
    print(content)
    print("\n")
    # Divide the amount of content by 2
    half = len(content) // 2
    # Split the content in two parts
    first_half = content[:half]
    print("Content length first half: ", len(first_half))
    print("First half: ", first_half)

    # Extract path from file
    path = file.split("/")
    path = "/".join(path[:-1])
    print("Path: ", path)

    # create path for the new file
    new_file = f"{path}/duplicated_log.txt"

    # Save content to "duplicated_log.txt
    with open(new_file, "w") as f:
        f.write(content)

    # path to cleaned file
    cleaned_file = f"{path}/exp_log.txt"
    with open(cleaned_file, "w") as f:
        f.write(first_half)

    print(
        "#############################################################################################"
    )


if __name__ == "__main__":
    # Iterate over all the subdirectories (3 levels deep) and change the log format
    base_dir = "paper-plots/test-convert-config"

    # data = reformat_file(config_file)
    # with open(output_file, "w") as f:
    #     f.write(data)
    # res = detect_file_version(output_file)
    # print(res)
    #
    for root, dirs, files in os.walk("."):
        for file in files:
            if file == "exp_log.txt":
                path_to_file = os.path.join(root, file)
                print(f"Processing {path_to_file}")

                ###################### Change file format. from old dict to new dict from ini ######################
                # # Check file version
                # if detect_file_version(path_to_file) == 0:
                #     print(
                #         f"Nothing to do for {path_to_file}. Already in the new format"
                #     )
                # else:
                #     # Rename file to exp_log_legacy.txt
                #     os.rename(path_to_file, path_to_file + "_legacy.txt")
                #     # Create new file with the new format
                #     new_file = reformat_file(path_to_file + "_legacy.txt")
                #     with open(path_to_file, "w") as f:
                #         f.write(new_file)
                ###################### Fix timestamps format of transscale logs ######################
                # if not check_log_format(path_to_file):
                #     continue
                # else:
                #     print(f"Changing format for {path_to_file}")
                #     change_log_format(path_to_file)

                # Change single quotes to double quotes
                try:
                    with open(path_to_file, "r+") as f:
                        content = f.read()
                    # If the file contains single quotes, change them to double quotes
                    if "'" in content:
                        # Save a copy of the original file
                        with open(path_to_file + "_original", "w") as f:
                            f.write(content)
                        content = content.replace("'", '"')
                        with open(path_to_file, "w") as f:
                            f.write(content)

                except Exception as e:
                    print(f"Error: {e}")
                    exit(1)

    # remove_duplicates(path_to_file)
