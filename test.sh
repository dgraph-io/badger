#!/bin/bash

set -e

go version

packages=$(go list ./... | grep github.com/dgraph-io/badger/v2/)

if [[ ! -z "$TEAMCITY_VERSION" ]]; then
  export GOFLAGS="-json"
fi

# Ensure that we can compile the binary.
pushd badger
go build -v .
popd

# Run the memory intensive tests first.
go test -v -run='TestBigKeyValuePairs$' --manual=true
go test -v -run='TestPushValueLogLimit' --manual=true

# Run the special Truncate test.
rm -rf p
go test -v -run='TestTruncateVlogNoClose$' --manual=true
truncate --size=4096 p/000000.vlog
go test -v -run='TestTruncateVlogNoClose2$' --manual=true
go test -v -run='TestTruncateVlogNoClose3$' --manual=true
rm -rf p

# Run the normal tests.
echo "==> Starting tests.. "
# go test -timeout=25m -v -race github.com/dgraph-io/badger/v2/...
for pkg in $packages; do
  echo "===> Testing $pkg"
  go test -timeout=25m -v -race $pkg
done

echo "===> Testing root level"
go test -timeout=25m -v . -race
