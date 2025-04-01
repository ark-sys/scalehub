function get_remaining_time --description 'Get remaining time for the active reservation'
    # Check if file exists
    if [ ! -f ~/.remaining_time ]; then
        echo -n " "
        return
    else
        # Get remaining time
        set remaining_time (cat ~/.remaining_time)
        # Check if remaining time is empty
        if [ -z "$remaining_time" ]; then
            echo -n " "
            return
        fi
    fi
end