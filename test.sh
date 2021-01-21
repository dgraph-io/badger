#!/bin/bash

set -eo pipefail

go version

# Run `go list` BEFORE setting GOFLAGS so that the output is in the right
# format for grep.
# export packages because the test will run in a sub process.
export packages=$(go list ./... | grep "github.com/dgraph-io/badger/v3/")

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

tags="-tags=jemalloc"

# Ensure that we can compile the binary.
pushd badger
go build -v $tags .
popd

# tags=""
InstallJemalloc

# Run the memory intensive tests first.
manual() {
  timeout="-timeout 2m"
  echo "==> Running package tests for $packages"
  set -e
  for pkg in $packages; do
    echo "===> Testing $pkg"
    go test $tags -timeout=25m -race $pkg -parallel 16
  done
  echo "==> DONE package tests"

  echo "==> Running manual tests"
  # Run the special Truncate test.
  rm -rf p
  set -e
  go test $tags $timeout -run='TestTruncateVlogNoClose$' --manual=true
  truncate --size=4096 p/000000.vlog
  go test $tags $timeout -run='TestTruncateVlogNoClose2$' --manual=true
  go test $tags $timeout -run='TestTruncateVlogNoClose3$' --manual=true
  rm -rf p

  # TODO(ibrahim): Let's make these tests have Manual prefix.
  # go test $tags -run='TestManual' --manual=true --parallel=2
  # TestWriteBatch
  # TestValueGCManaged
  # TestDropPrefix
  # TestDropAllManaged
  go test $tags $timeout -run='TestBigKeyValuePairs$' --manual=true
  go test $tags $timeout -run='TestPushValueLogLimit' --manual=true
  go test $tags $timeout -run='TestKeyCount' --manual=true
  go test $tags $timeout -run='TestIteratePrefix' --manual=true
  go test $tags $timeout -run='TestIterateParallel' --manual=true
  go test $tags $timeout -run='TestBigStream' --manual=true
  go test $tags $timeout -run='TestGoroutineLeak' --manual=true
  go test $tags $timeout -run='TestGetMore' --manual=true

  echo "==> DONE manual tests"
}

root() {
  # Run the normal tests.
  # go test -timeout=25m -v -race github.com/dgraph-io/badger/v3/...

  echo "==> Running root level tests."
  set -e
  go test $tags -timeout=25m . -v -race -parallel 16
  echo "==> DONE root level tests"
}

stream() {
  set -eo pipefail
  pushd badger
  baseDir=$(mktemp -d -p .)
  ./badger benchmark write -s --dir=$baseDir/test | tee $baseDir/log.txt
  ./badger benchmark read --dir=$baseDir/test --full-scan | tee --append $baseDir/log.txt
  ./badger benchmark read --dir=$baseDir/test -d=30s | tee --append $baseDir/log.txt
  ./badger stream --dir=$baseDir/test -o "$baseDir/test2" | tee --append $baseDir/log.txt
  count=$(cat "$baseDir/log.txt" | grep "at program end: 0 B" | wc -l)
  rm -rf $baseDir
  if [ $count -ne 4 ]; then
    echo "LEAK detected in Badger stream."
    return 1
  fi
  echo "==> DONE stream test"
  return 0
}

export -f stream
export -f manual
export -f root

parallel --halt now,fail=1 --progress --line-buffer ::: stream manual root
