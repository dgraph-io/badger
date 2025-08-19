#!/bin/bash

# Run this script from its directory, so that badgerpb4.proto is where it's expected to
# be. Only run this script if you've made changes to the API in badgerpb4.proto.

# Check if protoc version matches expected version first
EXPECTED_PROTOC_VERSION="3.21.12"
ACTUAL_PROTOC_VERSION=$(protoc --version 2>/dev/null | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' || true)

if [[ ${ACTUAL_PROTOC_VERSION} != "${EXPECTED_PROTOC_VERSION}" ]]; then
	echo "Warning: protoc version mismatch"
	echo "Expected: ${EXPECTED_PROTOC_VERSION}"
	echo "Actual: ${ACTUAL_PROTOC_VERSION:-'not found'}"
	echo "Skipping generation"
	exit 0
fi

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0

# Get the GOPATH and add the bin directory to PATH
GOPATH=$(go env GOPATH)
export PATH="${GOPATH}/bin:${PATH}"

protoc --go_out=. --go_opt=paths=source_relative badgerpb4.proto
