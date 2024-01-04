# shub.fish

# Define default scalehub paths
# SHUB_PATH: The root directory of the scalehub application
set -gx SHUB_PATH /app
# SHUB_CONF_PATH: The directory where the configuration files are stored
set -gx SHUB_CONF_PATH $SHUB_PATH/conf
# SHUB_PLAYBOOKS_PATH: The directory where the playbook files are stored
set -gx SHUB_PLAYBOOKS_PATH $SHUB_PATH/playbooks/project
# SHUB_EXPERIMENTS_DATA_PATH: The directory where the experiment data is stored
set -gx SHUB_EXPERIMENTS_DATA_PATH $SHUB_PATH/experiments-data

# Define the list of commands that can be used with the 'shub' script
set -l shub_commands provision destroy deploy delete run export plot
# Enable file completion for the 'shub' command
complete -c shub -f

# Enable autocompletion for the 'shub' command with the defined list of commands
complete -c shub -n "not __fish_seen_subcommand_from $shub_commands" -a "provision destroy deploy delete reload run export plot"

# Define the descriptions for each command
complete -c shub -n "__fish_use_subcommand" -a "provision" -d "Provision the platform specified in conf/scalehub.conf"
complete -c shub -n "__fish_use_subcommand" -a "destroy" -d "Destroy the platform specified in conf/scalehub.conf"
complete -c shub -n "__fish_use_subcommand" -a "deploy" -d "Execute deploy tasks of the provided playbook"
complete -c shub -n "__fish_use_subcommand" -a "delete" -d "Execute delete tasks of the provided playbook"
complete -c shub -n "__fish_use_subcommand" -a "reload" -d "Execute reload tasks of the provided playbook"
complete -c shub -n "__fish_use_subcommand" -a "experiment" -d "Start or stop an experiment"
complete -c shub -n "__fish_use_subcommand" -a "run" -d "Run action"
complete -c shub -n "__fish_use_subcommand" -a "export" -d "Export data"

# Autocompletion for 'deploy' and 'delete' commands
# This function returns the names of the playbook files
function __fish_shub_deploy_delete_complete
    set playbook_files $SHUB_PLAYBOOKS_PATH/*.yaml
    for file in $playbook_files
        set filename (basename $file .yaml)
        # If the file name begins with 'cluster-setup', skip it
        if string match -q "cluster-setup*" $filename
            continue
        end
        echo $filename
    end
end

# Enable autocompletion for the 'deploy' and 'delete' commands with the playbook file names
complete -c shub -n "__fish_seen_subcommand_from deploy delete reload" -a '(__fish_shub_deploy_delete_complete)'

# Autocompletion for 'export' command
# This function returns the names of the directories in SHUB_EXPERIMENTS_DATA_PATH
function __fish_shub_export_complete
    for dir in $SHUB_EXPERIMENTS_DATA_PATH/*
        if test -d $dir
            set dirname (basename $dir)
            # If the directory name matches the 'DD-MM-YYYY' format, echo it
            if string match -q -r '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' $dirname
                echo $dirname
                # Iterate over the subdirectories of the current directory
                for subdir in $dir/*
                    if test -d $subdir
                        set subdirname (basename $subdir)
                        # If the subdirectory name is a number, echo it with the parent directory name
                        if string match -q -r '^[0-9]+$' $subdirname
                            echo $dirname/$subdirname
                        end
                    end
                end
            end
        end
    end
end

# Enable autocompletion for the 'export' command with the directory names
complete -c shub -n "__fish_seen_subcommand_from export" -a '(__fish_shub_export_complete)'