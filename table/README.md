# BenchmarkRead

```
$ go test -bench Read$ -count 3

Size of table: 105843444
BenchmarkRead-8   	3	 343846914 ns/op
BenchmarkRead-8   	3	 351790907 ns/op
BenchmarkRead-8   	3	 351762823 ns/op
```

Size of table is 105,843,444 bytes, which is ~101M.

The rate is ~287M/s which matches our read speed. This is using mmap.

To read a 64M table, this would take ~0.22s, which is negligible.

```
$ go test -bench BenchmarkReadAndBuild -count 3

BenchmarkReadAndBuild-8   	       1	2341034225 ns/op
BenchmarkReadAndBuild-8   	       1	2346349671 ns/op
BenchmarkReadAndBuild-8   	       1	2364064576 ns/op
```

The rate is ~43M/s. To build a ~64M table, this would take ~1.5s. Note that this
does NOT include the flushing of the table to disk. All we are doing above is
to read one table (mmaped) and write one table in memory.

The table building takes 1.5-0.22 ~ 1.3s.

If we are writing out up to 10 tables, this would take 1.5*10 ~ 15s, and ~13s
is spent building the tables.

When running populate, building one table in memory tends to take ~1.5s to ~2.5s
on my system. Where does this overhead come from? Let's investigate the merging.

Below, we merge 5 tables. The total size remains unchanged at ~101M.

```
$ go test -bench ReadMerged -count 3
BenchmarkReadMerged-8   	       1	1321190264 ns/op
BenchmarkReadMerged-8   	       1	1296958737 ns/op
BenchmarkReadMerged-8   	       1	1314381178 ns/op
```

The rate is ~76M/s. To build a 64M table, this would take ~0.84s. The writing
takes ~1.3s as we saw above. So in total, we expect around 0.84+1.3 ~ 2.1s.
This roughly matches what we observe when running populate. There might be
some additional overhead due to the concurrent writes going on, in flushing the
table to disk. Also, the tables tend to be slightly bigger than 64M/s.

# DB Open benchmark
1. Create badger DB with some data
```
badger fill -m 300 --dir="/tmp/data" --sorted
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

badger 2019/05/28 21:31:20 INFO: 92 tables out of 155 opened in 3.005s
badger 2019/05/28 21:31:22 INFO: All 155 tables opened in 5.132s
badger 2019/05/28 21:31:22 INFO: Replaying file id: 299 at offset: 116366000
badger 2019/05/28 21:31:22 INFO: Replay took: 355.887µs
goos: linux
goarch: amd64
pkg: github.com/dgraph-io/badger
BenchmarkDBOpen-8   	       1	5228268412 ns/op
PASS
ok  	github.com/dgraph-io/badger	5.291s
```
It takes about 5.132s to open a DB with 300 million sorted key-value entries.
