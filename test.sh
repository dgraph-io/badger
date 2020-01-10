#!/bin/bash

set -e

go version

# Ensure that we can compile the binary.
pushd badger
go build -v .
popd

# Run the memory intensive tests first.
go test -v --manual=true -run='TestBigKeyValuePairs$'
go test -v --manual=true -run='TestPushValueLogLimit'

# Run the special Truncate test.
rm -rf p
go test -v --manual=true -run='TestTruncateVlogNoClose$' .
truncate --size=4096 p/000000.vlog
go test -v --manual=true -run='TestTruncateVlogNoClose2$' .
go test -v --manual=true -run='TestTruncateVlogNoClose3$' .
rm -rf p

# Then the normal tests.
echo
echo "==> Starting tests with value log mmapped..."
sleep 5
# Run top level package tests with mmap flag.
go test -v --vlog_mmap=true -race github.com/dgraph-io/badger/v2
echo
echo "==> Starting tests with value log not mmapped..."
sleep 5
go test -v --vlog_mmap=false -race github.com/dgraph-io/badger/v2

# Run test for rest of the packages.
go test -v -race $(go list ./... | grep github.com/dgraph-io/badger/v2/.)

