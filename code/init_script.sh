#!/usr/bin/env bash
# 
# Bootstrap script for setting up the evnironment needed

echo "Starting bootstrapping"

# installing homebrew
echo "Installing MiniConda..."

echo "Enter 0 for Mac, esle any other number "
read VAR

if [[ $VAR -gt 0 ]]
then
  echo "Installing Linux based MiniConda"
  wget --no-check-certificate --output-document='../Miniconda3-latest-Linux-x86_64.sh' 'https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh'
  bash Miniconda3-latest-Linux-x86_64.sh
else
  echo "Installing MacOSX based MiniConda"
  wget --no-check-certificate --output-document='../Miniconda3-latest-MacOSX-x86_64.sh' 'https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh'
  bash Miniconda3-latest-MacOSX-x86_64.sh
fi



# creating conda env and activate
conda create --name remote python=3.8 
conda activate remote

# Installing Python packages
echo "Installing Python packages..."
pip3 install -r requirements.txt

echo "Bootstrapping complete"





echo "Running main script"
python3 main.py

echo "Pipeline done!"
