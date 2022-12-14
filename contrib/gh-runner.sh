#!/bin/bash
# Machine Setup
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get -y install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    build-essential #Libc Packages
# Install & Setup GH Actions Runner
mkdir actions-runner && cd actions-runner
if [ "$(uname -m)" = "aarch64" ]; then
    echo "Detected arm64 architecture"
    # Download the latest runner package
    curl -o actions-runner-linux-arm64-2.298.2.tar.gz -L https://github.com/actions/runner/releases/download/v2.298.2/actions-runner-linux-arm64-2.298.2.tar.gz
    # Optional: Validate the hash
    echo "803e4aba36484ef4f126df184220946bc151ae1bbaf2b606b6e2f2280f5042e8  actions-runner-linux-arm64-2.298.2.tar.gz" | shasum -a 256 -c
    # Extract the installer
    tar xzf ./actions-runner-linux-arm64-2.298.2.tar.gz
elif [ "$(uname -m)" = "x86_64" ]; then
    echo "Detected amd64 architecture"
    # Download the latest runner package
    curl -o actions-runner-linux-x64-2.298.2.tar.gz -L https://github.com/actions/runner/releases/download/v2.298.2/actions-runner-linux-x64-2.298.2.tar.gz
    # Optional: Validate the hash
    echo "0bfd792196ce0ec6f1c65d2a9ad00215b2926ef2c416b8d97615265194477117  actions-runner-linux-x64-2.298.2.tar.gz" | shasum -a 256 -c 
    # Extract the installer
    tar xzf ./actions-runner-linux-x64-2.298.2.tar.gz
else
    echo "Unrecognized architecture"
    exit 1
fi
# Create the runner and start the configuration experience
./config.sh --url https://github.com/dgraph-io/dgraph --token $TOKEN
# Start GH Actions
sudo ./svc.sh install
sudo ./svc.sh start
