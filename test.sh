go test -v --vlog_mmap=true -race ./...
go test -v --vlog_mmap=false -race ./...

# Run the special Truncate test.
rm -R p
go test -v --manual=true -run='TestTruncateVlogNoClose$' .
truncate --size=4096 p/000000.vlog
go test -v --manual=true -run='TestTruncateVlogNoClose2$' .
go test -v --manual=true -run='TestTruncateVlogNoClose3$' .
