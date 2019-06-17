Size of table is 127,618,890 bytes for all benchmarks.

# BenchmarkRead
```
$ go test -bench Read$ -count 3
goos: linux
goarch: amd64
pkg: github.com/dgraph-io/badger/table
BenchmarkRead-16    	      10	 163993633 ns/op
BenchmarkRead-16    	      10	 162365329 ns/op
BenchmarkRead-16    	      10	 162801148 ns/op
PASS
ok  	github.com/dgraph-io/badger/table	30.095s
```

Size of table is 127,618,890 bytes, which is ~122MB.

The rate is ~742MB/s using LoadToRAM(when table is in RAM).

To read a 64M table, this would take ~0.0863s, which is negligible.

# BenchmarkReadAndBuild
```go
$ go test -bench BenchmarkReadAndBuild -count 3
goos: linux
goarch: amd64
pkg: github.com/dgraph-io/badger/table
BenchmarkReadAndBuild-16   	       1	1004708276 ns/op
BenchmarkReadAndBuild-16  	       1	1009918585 ns/op
BenchmarkReadAndBuild-16  	       1	1030127321 ns/op
PASS
ok  	github.com/dgraph-io/badger/table	18.732s
```

The rate is ~118MB/s. To build a ~64M table, this would take ~0.54s. Note that this
does NOT include the flushing of the table to disk. All we are doing above is
reading one table (which is in RAM) and write one table in memory.

The table building takes 0.54-0.0.0863s ~ 0.4537s.

# BenchmarkReadMerged
Below, we merge 5 tables. The total size remains unchanged at ~122M.

```go
$ go test -bench ReadMerged -count 3
BenchmarkReadMerged-16   	       2	954475788 ns/op
BenchmarkReadMerged-16   	       2	955252462 ns/op
BenchmarkReadMerged-16  	       2	956857353 ns/op
PASS
ok  	github.com/dgraph-io/badger/table	33.327s
```

The rate is ~127M/s. To read a 64M table using merge iterator, this would take ~0.5s.

# BenchmarkRandomRead

```go
go test -bench BenchmarkRandomRead$ -count 3
goos: linux
goarch: amd64
pkg: github.com/dgraph-io/badger/table
BenchmarkRandomRead-16    	  300000	      3596 ns/op
BenchmarkRandomRead-16    	  300000	      3621 ns/op
BenchmarkRandomRead-16    	  300000	      3596 ns/op
PASS
ok  	github.com/dgraph-io/badger/table	44.727s
```

For random read benchmarking, we are randomly reading a key and verifying its value.
