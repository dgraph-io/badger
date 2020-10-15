#!/bin/bash

set -e

go version

packages=$(go list ./... | grep github.com/dgraph-io/badger/v2/)

if [[ ! -z "$TEAMCITY_VERSION" ]]; then
  export GOFLAGS="-json"
fi

function InstallJemalloc() {
  pushd .
  if [ ! -f /usr/local/lib/libjemalloc.a ]; then
		USER_ID=`id -u`
    	JEMALLOC_URL="https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2"

		mkdir -p /tmp/jemalloc-temp && cd /tmp/jemalloc-temp ;
		echo "Downloading jemalloc" ;
		curl -s -L ${JEMALLOC_URL} -o jemalloc.tar.bz2 ;
		tar xjf ./jemalloc.tar.bz2 ;
		cd jemalloc-5.2.1 ;
		./configure --with-jemalloc-prefix='je_' ;
		make ;
		if [ "$USER_ID" -eq "0" ]; then
			make install ;
		else
			echo "==== Need sudo access to install jemalloc" ;
			sudo make install ;
		fi
	fi
  popd
}

# Ensure that we can compile the binary.
pushd badger
go build -v .
popd

# tags="-tags=jemalloc"
tags=""

# Run the memory intensive tests first.
go test -v $tags -run='TestBigKeyValuePairs$' --manual=true
go test -v $tags -run='TestPushValueLogLimit' --manual=true

# Run the special Truncate test.
rm -rf p
go test -v $tags -run='TestTruncateVlogNoClose$' --manual=true
truncate --size=4096 p/000000.vlog
go test -v $tags -run='TestTruncateVlogNoClose2$' --manual=true
go test -v $tags -run='TestTruncateVlogNoClose3$' --manual=true
rm -rf p

InstallJemalloc
# Run the key count test for stream writer.
go test -v -tags jemalloc -run='TestKeyCount' --manual=true

# Run the normal tests.
echo "==> Starting tests.. "
# go test -timeout=25m -v -race github.com/dgraph-io/badger/v2/...
for pkg in $packages; do
  echo "===> Testing $pkg"
  go test $tags -timeout=25m -v -race $pkg
done

echo "===> Testing root level"
go test $tags -timeout=25m -v . -race
