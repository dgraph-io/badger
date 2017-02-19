For comparison with simple skiplist:

```
BenchmarkReadParallel-4   	  100000	     14729 ns/op

BenchmarkWriteParallelAlt-4   	      50	  21515948 ns/op
```

These benchmarks do not look good.

ReadParallel is much slower than the normal skiplist. Note that ReadParallel freezes the skiplist
and then do concurrent reads.

For WriteParallelAlt, we expect the lock-free skiplist to shine but it did not. This is for a
Macbook with that many cores though.

