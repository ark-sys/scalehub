#!/bin/bash

# Function to generate SSH config
generate_config() {
    local HOME_PATH
    HOME_PATH=$(eval echo ~$USER)  # Get the home directory path
    SECRETS_PATH="/run/secrets"
    # Extract creds
    local username
    if [ -f $SECRETS_PATH/.python-grid5000.yaml ]; then
        username=$(cat $SECRETS_PATH/.python-grid5000.yaml | grep 'username' | awk '{print $2}')
        cp $SECRETS_PATH/.python-grid5000.yaml $HOME_PATH/.python-grid5000.yaml
    fi

    if [ -f $SECRETS_PATH/.iotlabrc ]; then
        cp $SECRETS_PATH/.iotlabrc $HOME_PATH/.iotlabrc
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

    # Copy ssh pub key to ~/.ssh directory from secrets folder
    if [ -f $SECRETS_PATH/id_rsa.pub ]; then
        cp $SECRETS_PATH/id_rsa.pub $HOME_PATH/.ssh/id_rsa.pub
        chmod 600 $HOME_PATH/.ssh/id_rsa.pub
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
Host *
  AddKeysToAgent yes
  StrictHostKeyChecking no
  UserKnownHostsFile=/dev/null

Host pico2-* pico2-*.rennes.inria.fr
  User picocluster
  ProxyJump ssh-rba.inria.fr
  PreferredAuthentications publickey
  StrictHostKeyChecking no
  ForwardAgent yes

Host node-*.grenoble.iot-lab.info
  ProxyCommand ssh -q -W %h:%p grenoble.iot-lab.info
  User root
  ForwardAgent yes
  StrictHostKeyChecking no

Host *.iot-lab.info
  User arsalane
  IdentityFile ~/.ssh/fit.pk
  ForwardAgent yes

Host access.grid5000.fr g5k
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

Host 10.*.*.*
  User root
  ProxyCommand ssh access.grid5000.fr -W %h:%p
  IdentityFile ~/.ssh/id_rsa
  ForwardAgent yes
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

# Add keys from ssh-agent
ssh-add -l

# Start nginx service in background
sudo service nginx start

# Keep the container running
tail -f /dev/null