#!/bin/bash

# Script location
BASEDIR=$(dirname $0)
IMAGE_NAME="scalehub"
SERVICE_NAME="scalehub"

# Retrieve the current user IDs and name
userid=$(id -u)
groupid=$(id -g)
username=$(whoami)

# Function to display help message
function display_help() {
    echo "Usage: ./deploy.sh [option]"
    echo " "
    echo "This script helps build and deploy a containerized environment with docker."
    echo "The built image contains ansible and enoslib."
    echo "At runtime, a python script is loaded in the container, which allows to reserve and provision nodes on Grid5000."
    echo " "
    echo "Options:"
    echo "  build             Build the Docker image"
    echo "  generate          Generate Docker secret with credentials"
    echo "  create            Create the Docker container"
    echo "  restart           Restart the Docker container"
    echo "  shell             Spawn an interactive shell in the container"
    echo "  push <registry>   Push the Docker image to a private registry"
    echo "  help              Display this help message"
}

# Function to build the Docker image
function build_image() {
  docker build --build-arg UID=$userid --build-arg GID=$groupid --build-arg UNAME=$username -t $IMAGE_NAME dockerfile
  # Check the exit code
  if [ $? -eq 0 ]; then
      echo "Docker build completed successfully."
      echo "Updating service image..."
      # Check if a Docker service with the image already exists
      if docker service ls -q --filter "name=$SERVICE_NAME" | grep -q .; then
          # If the service exists, update it to use the latest image
          docker service update --image $IMAGE_NAME $SERVICE_NAME
      else
        create_container
      fi
  else
      echo "Error occurred during Docker build. Exiting..."
      exit 1
  fi
}

# Function to generate Docker secret with credentials
function generate_secret() {
  # Check if both secrets exist
  if docker secret ls -q --filter "name=mysecretuser" | grep -q . || docker secret ls -q --filter "name=mysecretpass" | grep -q .; then
    echo "Secrets 'mysecretuser' and 'mysecretpass' already exist."
    echo "Do you want to update them? (y/n)"
    read -r update_choice

    if [[ $update_choice == "y" ]]; then
      # Remove old secrets
      echo "Remobing service and secrets..."
      docker service rm $SERVICE_NAME
#      docker service update --secret-rm mysecretuser $SERVICE_NAME
#      docker service update --secret-rm mysecretpass $SERVICE_NAME
      docker secret rm mysecretuser
      docker secret rm mysecretpass

      # Prompt the user to update the secrets via standard input
      echo "Enter updated Grid5000 username:"
      read -r mysecretuser
      echo "Enter updated Grid5000 password:"
      read -r mysecretpass

      # Update the secrets
      if [[ -n $mysecretuser ]]; then
        echo -n "$mysecretuser" | docker secret create mysecretuser - --label "updated_at=$(date +%Y-%m-%d)"
      fi
      if [[ -n $mysecretpass ]]; then
        echo -n "$mysecretpass" | docker secret create mysecretpass - --label "updated_at=$(date +%Y-%m-%d)"
      fi
      echo "Secrets 'mysecretuser' and 'mysecretpass' updated."
      create_container
    else
      echo "Exiting..."
      exit 0
    fi
  else
    # Prompt the user to provide the secrets via standard input
    echo "Enter Grid5000 username:"
    read -r mysecretuser
    echo "Enter Grid5000 password:"
    read -r mysecretpass

    # Create the secrets
    if [[ -n $mysecretuser ]]; then
      echo -n "$mysecretuser" | docker secret create mysecretuser - --label "created_at=$(date +%Y-%m-%d)"
    fi
    if [[ -n $mysecretpass ]]; then
      echo -n "$mysecretpass" | docker secret create mysecretpass - --label "created_at=$(date +%Y-%m-%d)"
    fi
    echo "Secrets 'mysecretuser' and 'mysecretpass' created."
  fi
}

function restart_container(){
    if is_service_running; then
        docker service rm $SERVICE_NAME
        create_container
    else
      echo "Service is not running."
    fi
}

# Function to create the Docker container
function create_container() {
    docker service create \
        --name $SERVICE_NAME \
        --secret mysecretuser \
        --secret mysecretpass \
        --network host \
        --mount type=bind,source=$BASEDIR/script,target=/app/script \
        --mount type=bind,source=$BASEDIR/playbooks,target=/app/playbooks \
        --mount type=bind,source=$BASEDIR/conf,target=/app/conf \
        --mount type=bind,source=$BASEDIR/experiments-data,target=/app/experiments-data \
        --mount type=bind,source=$HOME/.ssh,target=$HOME/.ssh \
        --user $userid \
        --group $groupid \
        --hostname $SERVICE_NAME \
        $IMAGE_NAME
}

# Function to check if the service is running
function is_service_running() {
    docker service ls --format '{{.Name}}' | grep -q $SERVICE_NAME
}

# Function to get an interactive shell for the service
function get_shell() {
    if is_service_running; then
        docker exec -u $username -it $(docker ps -f name=$SERVICE_NAME --quiet) fish
    else
        create_container
        get_shell
    fi
}

# Function to push the Docker image to a private registry
function push_image() {
    registry="$1"
    docker tag myproject "$registry/$IMAGE_NAME"
    docker push "$registry/$IMAGE_NAME"
}

# Parse the command line argument
case "$1" in
    build)
        build_image
        ;;
    generate)
        generate_secret
        ;;
    create)
        create_container
        ;;
    restart)
        restart_container
        ;;
    shell)
        get_shell
        ;;
    push)
        push_image "$2"
        ;;
    help)
        display_help
        ;;
    *)
        display_help
        exit 1
        ;;
esac
