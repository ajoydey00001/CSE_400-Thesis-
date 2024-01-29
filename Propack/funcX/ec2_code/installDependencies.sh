#!/bin/bash

sudo apt install python3.8
sudo apt install python3-pip
python3.8 -m pip install --upgrade pip

echo 'deb http://download.opensuse.org/repositories/network:/messaging:/zeromq:/release-stable/xUbuntu_18.04/ /' | sudo tee /etc/apt/sources.list.d/network:messaging:zeromq:release-stable.list
curl -fsSL https://download.opensuse.org/repositories/network:messaging:zeromq:release-stable/xUbuntu_18.04/Release.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/network_messaging_zeromq_release-stable.gpg > /dev/null
sudo apt update
sudo apt install libzmq3-dev

python3.8 -m pip install funcx_endpoint
python3.8 -m pip install --no-binary=:all: 'pyzmq==19.0.0' --force-reinstall

echo "alias python='python3.8'" >> ~/.bashrc
echo "alias funcx-endpoint='python3.8 /home/ubuntu/.local/bin/funcx-endpoint"
source ~/.bashrc

