function update_remaining_time --description 'Update remaining time for the active reservation'
    # Retrieve user credentials from .python-grid5000.yaml file
    set -l user (grep username ~/.python-grid5000.yaml | awk '{print $2}')
    set -l password (grep password ~/.python-grid5000.yaml | awk '{print $2}')

    # Do some error checking. If no user credentials are found, or <Grid5000_username> or <Grid5000_password> are found, then exit.
    if test -z $user || test -z $password || test $user = "<Grid5000_username>" || test $password = "<Grid5000_password>"
        return 1
        echo "No user credentials found. Please run 'deploy.sh generate' from your host, then 'deploy.sh' to restart the container."
    end



#     while true
#         set -l reservation_time (shub reservation_time | sed -n 2p)
#         echo -n $reservation_time > ~/.remaining_time
#         sleep 5
#     end
end