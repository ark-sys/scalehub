# shub.fish

set -l shub_commands provision destroy deploy delete run export plot
complete -c shub -f

complete -c shub -n "not __fish_seen_subcommand_from $shub_commands" -a "provision destroy deploy delete reload run export plot"

# Autocompletion for the 'shub' script
complete -c shub -n "__fish_use_subcommand" -a "provision" -d "Provision the platform specified in conf/scalehub.conf"
complete -c shub -n "__fish_use_subcommand" -a "destroy" -d "Destroy the platform specified in conf/scalehub.conf"
complete -c shub -n "__fish_use_subcommand" -a "deploy" -d "Execute deploy tasks of the provided playbook"
complete -c shub -n "__fish_use_subcommand" -a "delete" -d "Execute delete tasks of the provided playbook"
complete -c shub -n "__fish_use_subcommand" -a "reload" -d "Execute reload tasks of the provided playbook"
complete -c shub -n "__fish_use_subcommand" -a "run" -d "Run action"
complete -c shub -n "__fish_use_subcommand" -a "export" -d "Export data"
complete -c shub -n "__fish_use_subcommand" -a "plot" -d "Starts interactive plotter"

# Autocompletion for 'deploy' and 'delete' commands
function __fish_shub_deploy_delete_complete
    set playbook_files /app/playbooks/*.yaml
    for file in $playbook_files
        complete --no-files --arguments=(basename $file .yaml) --condition="test (count (commandline)) = 2"
    end
end

complete --command shub --arguments '(__fish_shub_deploy_delete_complete)'
#
# # Autocompletion for 'run' command
# complete --command shub --arguments '(string)' --condition="test (count (commandline)) = 2"
#
# # Autocompletion for 'export' command
# function __fish_shub_export_complete
#     set folder_names /app/experiments-data/*
#     for folder in $folder_names
#         complete --no-files --arguments=(basename $folder) --condition="test (count (commandline)) = 2"
#     end
# end
#
# complete --command shub --arguments '(__fish_shub_export_complete)'
