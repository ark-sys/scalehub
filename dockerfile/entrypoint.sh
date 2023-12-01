#!/bin/bash

# Function to generate SSH config
generate_config() {
    local HOME_PATH
    HOME_PATH=$(eval echo ~$USER)  # Get the home directory path
    SECRETS_PATH="/run/secrets"
    # Extract username from ~/.creds.yaml
    local username
    if [ -f $SECRETS_PATH/.python-grid5000.yaml ]; then
        username=$(cat $SECRETS_PATH/.python-grid5000.yaml | grep 'username' | awk '{print $2}')
    fi

    # Create .ssh directory if it doesn't exist
    if [ ! -d $HOME_PATH/.ssh ]; then
        mkdir $HOME_PATH/.ssh
    fi

    # Copy SSH key to ~/.ssh directory from secrets folder
    if [ -f $SECRETS_PATH/id_rsa ]; then
        cp $SECRETS_PATH/id_rsa $HOME_PATH/.ssh/id_rsa
        chmod 600 $HOME_PATH/.ssh/id_rsa
    fi

    # Check if username is present
    if [ -n "$username" ]; then
      # Check that username is not <Grid5000_username> otherwise inform the user to change it
      if [ "$username" = "<Grid5000_username>" ]; then
        echo "Please change the username in Grid5000_creds.yaml file"
        exit 1
      else
        # Generate SSH config
        bash -c "cat << 'EOF' > $HOME_PATH/.ssh/config
Host access.grid5000.fr
  User $username
  Hostname access.grid5000.fr
  IdentityFile ~/.ssh/id_rsa
  ForwardAgent no
  StrictHostKeyChecking no

Host g5k
  User $username
  Hostname access.grid5000.fr
  IdentityFile ~/.ssh/id_rsa
  ForwardAgent no
  StrictHostKeyChecking no

Host *.g5k
  User $username
  ProxyCommand ssh g5k -W \$(basename %h .g5k):%p
  IdentityFile ~/.ssh/id_rsa
  ForwardAgent no
  StrictHostKeyChecking no

Host *.*.grid5000.fr
  User $username
  ProxyCommand ssh access.grid5000.fr -W %h:%p
  IdentityFile ~/.ssh/id_rsa
  ForwardAgent no
  StrictHostKeyChecking no
EOF"
        echo "SSH config generated successfully"
      fi
    else
        echo "Username not found in ~/.python-grid5000.yaml"
    fi
}
############# Script entrypoint #############
# Change default shell to fish
sudo chsh -s /usr/bin/fish

# Generate config file based on credentials file
generate_config

# Start nginx service in background
sudo service nginx start

# Keep the container running
tail -f /dev/null