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

    # Create .ssh directory if it doesn't exist
    if [ ! -d $HOME_PATH/.ssh ]; then
        mkdir $HOME_PATH/.ssh
    fi

    # Copy SSH key to ~/.ssh directory from secrets folder. Required by Enoslib's VMonG5k provider
    if [ -f $SECRETS_PATH/id_rsa ]; then
        cp $SECRETS_PATH/id_rsa $HOME_PATH/.ssh/id_rsa
        chmod 600 $HOME_PATH/.ssh/id_rsa
    fi

    # Copy ssh pub key to ~/.ssh directory from secrets folder. Required by Enoslib's VMonG5k provider
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

check_running_k3s() {
    if kubectl cluster-info > /dev/null 2>&1; then
      cluster_ip=$(kubectl cluster-info | grep -oP 'https://\K[^:]+' | head -n 1)
      if [ -n "$cluster_ip" ]; then
        echo "K3s cluster is running at $cluster_ip"
        # Save copy of /etc/hosts
        sudo cp /etc/hosts /etc/hosts.bak
        # Use awk to update entry for ingress-upstream.k3s.scalehub.dev in /etc/hosts
        sudo awk -v domain="ingress-upstream.k3s.scalehub.dev" -v new_ip="$cluster_ip" '$2 == domain {$1 = new_ip; found = 1} {print $0} END {if (found != 1) print new_ip, domain}' /etc/hosts | sudo tee /etc/hosts > /dev/null
        echo "Updated entry 'ingress-upstream.k3s.scalehub.dev' with $cluster_ip in /etc/hosts"

        # If the file only has one line, prepend /etc/hosts.bak to it and remove the backup file
        if [ $(wc -l < /etc/hosts) -eq 1 ]; then
          sudo cat /etc/hosts.bak | sudo tee /etc/hosts > /dev/null
          sudo rm /etc/hosts.bak
        fi

      else
        echo "Failed to get cluster IP"
      fi
    else
      echo "K3s cluster is not running"
    fi
}
############# Script entrypoint #############
# Change default shell to fish
sudo chsh -s /usr/bin/fish

# Generate config file based on credentials file
generate_config

# Add keys from ssh-agent
ssh-add -l

# Check if k3s cluster is running and update /etc/hosts if so
check_running_k3s

# Start nginx server
sudo nginx -g "daemon off;"