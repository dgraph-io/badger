#!/bin/bash

# Run this script from its directory, so that badgerpb2.proto is where it's expected to
# be.

# You might need to go get -v google.golang.org/protobuf/cmd/protoc-gen-go

protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative -I=. badgerpb3.proto
