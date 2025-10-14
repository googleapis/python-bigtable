#!/bin/bash

function filter_instances() {
    gcloud bigtable instances list | grep -v "NAME" | awk '{print $1}' | grep $1
}

# function filter_backups() {
    
# }

function delete_instances() {
    for instance in $(filter_instances $1); do
    # if [ -z "$2" ]; then
    #     for backup in $()
    # fi
        gcloud bigtable instances delete $instance
    done
}

current_project=$(gcloud config get project)

gcloud config set project precise-truck-742
delete_instances g-c-p-*
delete_instances python-bigtable-tests-*
delete_instances admin-overlay-instance-*
gcloud config set project $current_project
