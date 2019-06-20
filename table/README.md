Size of table is 127,618,890 bytes for all benchmarks.

# BenchmarkRead
```
$ go test -bench ^BenchmarkRead$ -run ^$ -count 3
goos: linux
goarch: amd64
pkg: github.com/dgraph-io/badger/table
BenchmarkRead-16    	      10	 153281932 ns/op
BenchmarkRead-16    	      10	 153454443 ns/op
BenchmarkRead-16    	      10	 155349696 ns/op
PASS
ok  	github.com/dgraph-io/badger/table	23.549s
```

Size of table is 127,618,890 bytes, which is ~122MB.

The rate is ~783MB/s using LoadToRAM (when table is in RAM).

To read a 64MB table, this would take ~0.0817s, which is negligible.

# BenchmarkReadAndBuild
```go
$ go test -bench BenchmarkReadAndBuild -run ^$ -count 3
goos: linux
goarch: amd64
pkg: github.com/dgraph-io/badger/table
BenchmarkReadAndBuild-16    	       2	 945041628 ns/op
BenchmarkReadAndBuild-16    	       2	 947120893 ns/op
BenchmarkReadAndBuild-16    	       2	 954909506 ns/op
PASS
ok  	github.com/dgraph-io/badger/table	26.856s
```

The rate is ~127MB/s. To build a 64MB table, this would take ~0.5s. Note that this
does NOT include the flushing of the table to disk. All we are doing above is
reading one table (which is in RAM) and write one table in memory.

The table building takes 0.5-0.0817s ~ 0.4183s.

# BenchmarkReadMerged
Below, we merge 5 tables. The total size remains unchanged at ~122M.

```go
$ go test -bench ReadMerged -run ^$ -count 3
BenchmarkReadMerged-16   	       2	954475788 ns/op
BenchmarkReadMerged-16   	       2	955252462 ns/op
BenchmarkReadMerged-16  	       2	956857353 ns/op
PASS
ok  	github.com/dgraph-io/badger/table	33.327s
```

The rate is ~127MB/s. To read a 64MB table using merge iterator, this would take ~0.5s.

# BenchmarkRandomRead

```go
go test -bench BenchmarkRandomRead$ -run ^$ -count 3
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

# DB Open benchmark
1. Create badger DB with 2 billion key-value pairs (about 380GB of data)
```
badger fill -m 2000 --dir="/tmp/data" --sorted
```
2. Clear buffers and swap memory
```
free -mh && sync && echo 3 | sudo tee /proc/sys/vm/drop_caches && sudo swapoff -a && sudo swapon -a && free -mh
```
Also flush disk buffers
```
blockdev --flushbufs /dev/nvme0n1p4
```
3. Run the benchmark
```
go test -run=^$ github.com/dgraph-io/badger -bench ^BenchmarkDBOpen$ -benchdir="/tmp/data" -v

badger 2019/06/04 17:15:56 INFO: 126 tables out of 1028 opened in 3.017s
badger 2019/06/04 17:15:59 INFO: 257 tables out of 1028 opened in 6.014s
badger 2019/06/04 17:16:02 INFO: 387 tables out of 1028 opened in 9.017s
badger 2019/06/04 17:16:05 INFO: 516 tables out of 1028 opened in 12.025s
badger 2019/06/04 17:16:08 INFO: 645 tables out of 1028 opened in 15.013s
badger 2019/06/04 17:16:11 INFO: 775 tables out of 1028 opened in 18.008s
badger 2019/06/04 17:16:14 INFO: 906 tables out of 1028 opened in 21.003s
badger 2019/06/04 17:16:17 INFO: All 1028 tables opened in 23.851s
badger 2019/06/04 17:16:17 INFO: Replaying file id: 1998 at offset: 332000
badger 2019/06/04 17:16:17 INFO: Replay took: 9.81µs
goos: linux
goarch: amd64
pkg: github.com/dgraph-io/badger
BenchmarkDBOpen-16    	       1	23930082140 ns/op
PASS
ok  	github.com/dgraph-io/badger	24.076s

```
It takes about 23.851s to open a DB with 2 billion sorted key-value entries.
