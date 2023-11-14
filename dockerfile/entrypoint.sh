#!/bin/bash

# Function to generate SSH config
generate_config() {
    local HOME_PATH
    HOME_PATH=$(eval echo ~$USER)  # Get the home directory path

    # Extract username from ~/.creds.yaml
    local username
    if [ -f $HOME_PATH/.python-grid5000.yaml ]; then
        username=$(cat $HOME_PATH/.python-grid5000.yaml | grep 'username' | awk '{print $2}')
    fi

    # Check if username is available
    if [ -n "$username" ]; then
        bash -c "cat << EOF > $HOME_PATH/.ssh/config
Host access.grid5000.fr
  User $username
  Hostname access.grid5000.fr
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
    else
        echo "Username not found in ~/.python-grid5000.yaml"
    fi
}

# Change default shell to fish
chsh -s /usr/bin/fish

# Generate config file based on credentials file
generate_config

# Keep the container running
tail -f /dev/null