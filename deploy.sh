#!/bin/bash

# Script location
export SCALEHUB_BASEDIR=$(dirname $0)
IMAGE_NAME="scalehub"
SERVICE_NAME="scalehub"

## Retrieve the current user IDs and name
export UID=$(id -u)
export GID=$(id -g)
#export username=$(whoami)

# Set credentials path
export g5k_creds_path="$SCALEHUB_BASEDIR/setup/shub/secrets/Grid5000_creds.yaml"

# Function to display help message
function display_help() {
    echo "Usage: ./deploy.sh [option]"
    echo " "
    echo "This script helps build and deploy a containerized environment with docker."
    echo "The built image contains ansible and enoslib."
    echo "At runtime, a python script is loaded in the container, which allows to reserve and provision nodes on Grid5000."
    echo " "
    echo "Options:"
    echo "  generate          Generate credentials file for grid5000"
    echo "  create            Create the Docker container"
    echo "  remove            Remove the Docker container"
    echo "  restart           Restart the Docker container"
    echo "  restart_ss <service_name>       Restart a specific service"
    echo "  shell             Spawn an interactive shell in the container"
    echo "  push <registry>   Push the Docker image to a private registry"
    echo "  help              Display this help message"
}

# Function to generate Docker secret with credentials
function generate_secret() {
    if [ -f "$g5k_creds_path" ]; then
        read -p "File exists at $g5k_creds_path. Do you want to modify it? (y/n): " answer
    else
        read -p "File doesn't exist at $g5k_creds_path. Do you want to create it? (y/n): " answer
    fi

    if [ "$answer" = "y" ]; then
        read -p "Enter username: " username
        read -s -p "Enter password: " password
        echo "username: $username" > "$g5k_creds_path"
        echo -e "\npassword: $password" >> "$g5k_creds_path"

        if [ -f "$g5k_creds_path" ]; then
            echo -e "\nFile modified successfully."
            git update-index --assume-unchanged $g5k_creds_path
        else
            echo -e "\nFile created successfully."
        fi
    else
        if [ -f "$g5k_creds_path" ]; then
            echo "No modifications made."
        else
            echo "No file created."
        fi
    fi

}

function restart_service_ss(){
  service_name="$1"

  docker-compose -p scalehub -f $SCALEHUB_BASEDIR/setup/shub/docker-compose.yaml up --build --no-deps -d --force-recreate $service_name

}

function restart_service(){
    if is_service_running; then
        remove_service
        create_service
    else
      echo "Service is not running."
    fi
}

# Function to create the Docker container
function create_service() {
  docker-compose -p scalehub -f $SCALEHUB_BASEDIR/setup/shub/docker-compose.yaml up --build -d
}

function remove_service() {
  docker-compose -p scalehub -f $SCALEHUB_BASEDIR/setup/shub/docker-compose.yaml down --rmi all --remove-orphans
}

# Function to check if the service is running
function is_service_running() {
    if docker ps --format '{{.Names}}' | grep -q "$SERVICE_NAME"; then
        echo "Container $SERVICE_NAME is running."
    else
        echo "Container $SERVICE_NAME is not running."
    fi
}

# Function to get an interactive shell for the service
function get_shell() {
    if is_service_running; then
        docker exec -it $SERVICE_NAME fish
    else
        create_service
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
        create_service
        ;;
    remove)
        remove_service
        ;;
    restart)
        restart_service
        ;;
    restart_ss)
        restart_service_ss "$2"
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
