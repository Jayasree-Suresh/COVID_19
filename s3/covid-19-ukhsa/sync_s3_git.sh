#!/bin/bash

# Define variables
S3_BUCKET="covid-19-ukhsa"
LOCAL_DIR="C:/Users/mylif/OneDrive/Desktop/COVID_19/s3/covid-19-ukhsa"
GIT_REPO="https://github.com/Jayasree-Suresh/COVID_19/tree/main/s3/covid-19-ukhsa/"

# Sync files from S3 to local directory
aws s3 sync s3://$S3_BUCKET $LOCAL_DIR

# Change directory to Git repository
cd $GIT_REPO

# Add and commit changes to Git repository
git add .
git commit -m "Sync files from S3"
git push origin master  # Or specify your branch name
