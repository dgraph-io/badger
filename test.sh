#!/bin/bash

set -e

go version


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

tags="-tags=jemalloc"
# tags=""
InstallJemalloc

# Run the memory intensive tests first.
manual() {
  echo "==> Running manual tests"
  # Run the special Truncate test.
  rm -rf p
  set -e
  go test -v $tags -run='TestTruncateVlogNoClose$' --manual=true
  truncate --size=4096 p/000000.vlog
  go test -v $tags -run='TestTruncateVlogNoClose2$' --manual=true
  go test -v $tags -run='TestTruncateVlogNoClose3$' --manual=true
  rm -rf p

  go test -v $tags -run='TestBigKeyValuePairs$' --manual=true
  go test -v $tags -run='TestPushValueLogLimit' --manual=true
  go test -v $tags -run='TestKeyCount' --manual=true
  go test -v $tags -run='TestIteratePrefix' --manual=true
  go test -v $tags -run='TestIterateParallel' --manual=true
  go test -v $tags -run='TestBigStream' --manual=true
  go test -v $tags -run='TestGoroutineLeak' --manual=true

  echo "==> DONE manual tests"
}

pkgs() {
  packages=$(go list ./... | grep github.com/dgraph-io/badger/v2/)
  echo "==> Running package tests for $packages"
  set -e
  for pkg in $packages; do
    echo "===> Testing $pkg"
    go test $tags -timeout=25m -v -race $pkg -parallel 16
  done
  echo "==> DONE package tests"
}

root() {
  # Run the normal tests.
  # go test -timeout=25m -v -race github.com/dgraph-io/badger/v2/...

  echo "==> Running root level tests."
  set -e
  go test $tags -timeout=25m -v . -race -parallel 16
  echo "==> DONE root level tests"
}

export -f manual
export -f pkgs
export -f root

parallel --halt now,fail=1 --progress --line-buffer ::: manual pkgs root
