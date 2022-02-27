#!/usr/bin/env bash
# 
# Bootstrap script for setting up the evnironment needed

echo "Starting bootstrapping"

# installing homebrew
echo "Installing MiniConda..."
bash Miniconda3-latest-MacOSX-x86_64.sh

# creating conda env and activate
conda create --name remote python=3.8 
conda activate remote

# Installing Python packages
echo "Installing Python packages..."
pip3 install -r requirements.txt

echo "Bootstrapping complete"

echo "Running main script"
python3 main.py