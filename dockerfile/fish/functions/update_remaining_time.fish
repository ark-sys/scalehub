function update_remaining_time --description 'Update remaining time for the active reservation'
    while true
        set -l reservation_time (shub reservation_time | sed -n 2p)
        echo -n $reservation_time > ~/.remaining_time
        sleep 5
    end
end