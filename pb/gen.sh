#!/bin/bash

# Run this script from its directory, so that badgerpb2.proto is where it's expected to
# be.

# You might need to go get -v github.com/gogo/protobuf/...
go get -v github.com/gogo/protobuf/protoc-gen-gogofaster
protoc --gogofaster_out=. --gogofaster_opt=paths=source_relative -I=. badgerpb3.proto
