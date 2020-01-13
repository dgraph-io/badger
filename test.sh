#!/bin/bash

set -e

go version

if [[ ! -z "$TEAMCITY_VERSION" ]]; then
  export GOFLAGS="-json"
fi

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
echo "==> Starting test for table, skl and y package"
sleep 5
go test -v -race github.com/dgraph-io/badger/v2/skl
# Run test for all package except the top level packge. The top level package support the
# `vlog_mmap` flag which rest of the packages don't support.
go test -v -race $(go list ./... | grep github.com/dgraph-io/badger/v2/. | tr -d '"')

echo
echo "==> Starting tests with value log mmapped..."
sleep 5
# Run top level package tests with mmap flag.
go test -v --vlog_mmap=true -race github.com/dgraph-io/badger/v2
echo
echo "==> Starting tests with value log not mmapped..."
sleep 5
go test -v --vlog_mmap=false -race github.com/dgraph-io/badger/v2

