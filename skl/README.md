This is better than `skiplist` and `slist`.

WriteParallel is about 3X faster and ReadParallel is 2X faster than `skiplist` and 20X faster
than `slist`.

```
BenchmarkWriteParallelAlt-4   	     200	   7700008 ns/op
BenchmarkReadParallel-4       	 2000000	       787 ns/op
```