# shub.fish
# Fish shell autocomplete configuration file for shub script

# Set subcommands
set -l commands provision destroy deploy delete run export plot
set -l playbook_commands deploy delete

# Define the command and its subcommands
complete -c shub -f
complete -c shub -n "not __fish_seen_subcommand_from $commands" -x -a "provision" -d "Provision the platform specified in conf/scalehub.co>
complete -c shub -n "not __fish_seen_subcommand_from $commands" -x -a "destroy" -d "Destroy the platform specified in conf/scalehub.conf"
complete -c shub -n "not __fish_seen_subcommand_from $commands" -x -a "deploy" -d "Execute deploy tasks of the provided playbook."
complete -c shub -n "not __fish_seen_subcommand_from $commands" -x -a "delete" -d "Execute delete tasks of the provided playbook."
complete -c shub -n "not __fish_seen_subcommand_from $commands" -x -a "run" -d "Run action."
complete -c shub -n "not __fish_seen_subcommand_from $commands" -x -a "export" -d "Export data"
complete -c shub -n "not __fish_seen_subcommand_from $commands" -x -a "plot" -d "Plot data"

# Define options
complete -c shub -s h --long-option help --description "Show this help message and exit"
complete -c shub --long-option conf --description "Specify a custom path for the configuration file of scalehub. Default configuration is specified in conf/scalehub.conf"

# Define arguments for specific subcommands
complete --command shub --arguments "(ls /app/playbooks)" --condition "__fish_seen_subcommand_from $playbook_commands"
