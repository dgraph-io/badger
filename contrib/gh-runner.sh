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
    curl -o actions-runner-linux-arm64-2.299.1.tar.gz -L https://github.com/actions/runner/releases/download/v2.299.1/actions-runner-linux-arm64-2.299.1.tar.gz
    # Optional: Validate the hash
    echo "debe1cc9656963000a4fbdbb004f475ace5b84360ace2f7a191c1ccca6a16c00  actions-runner-linux-arm64-2.299.1.tar.gz" | shasum -a 256 -c
    # Extract the installer
    tar xzf ./actions-runner-linux-arm64-2.299.1.tar.gz
elif [ "$(uname -m)" = "x86_64" ]; then
    echo "Detected amd64 architecture"
    # Download the latest runner package
    curl -o actions-runner-linux-x64-2.299.1.tar.gz -L https://github.com/actions/runner/releases/download/v2.299.1/actions-runner-linux-x64-2.299.1.tar.gz
    # Optional: Validate the hash
    echo "147c14700c6cb997421b9a239c012197f11ea9854cd901ee88ead6fe73a72c74  actions-runner-linux-x64-2.299.1.tar.gz" | shasum -a 256 -c
    # Extract the installer
    tar xzf ./actions-runner-linux-x64-2.299.1.tar.gz
else
    echo "Unrecognized architecture"
    exit 1
fi
# Create the runner and start the configuration experience
./config.sh --url https://github.com/dgraph-io/dgraph --token $TOKEN
# Start GH Actions
sudo ./svc.sh install
sudo ./svc.sh start
