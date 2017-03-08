This is much better than `skiplist` and `slist`.

```
BenchmarkReadWrite/frac_0-4         	 1000000	      1516 ns/op
BenchmarkReadWrite/frac_1-4         	 1000000	      1456 ns/op
BenchmarkReadWrite/frac_2-4         	 1000000	      1354 ns/op
BenchmarkReadWrite/frac_3-4         	 1000000	      1295 ns/op
BenchmarkReadWrite/frac_4-4         	 1000000	      1142 ns/op
BenchmarkReadWrite/frac_5-4         	 1000000	      1077 ns/op
BenchmarkReadWrite/frac_6-4         	 1000000	      1003 ns/op
BenchmarkReadWrite/frac_7-4         	 2000000	      1054 ns/op
BenchmarkReadWrite/frac_8-4         	 2000000	       929 ns/op
BenchmarkReadWrite/frac_9-4         	 3000000	       815 ns/op
BenchmarkReadWrite/frac_10-4        	 5000000	       472 ns/op
```

But compared to a simple map with read-write lock, it is still slower.

```
BenchmarkReadWriteMap/frac_0-4         	 2000000	       883 ns/op
BenchmarkReadWriteMap/frac_1-4         	 2000000	       830 ns/op
BenchmarkReadWriteMap/frac_2-4         	 2000000	       658 ns/op
BenchmarkReadWriteMap/frac_3-4         	 2000000	       662 ns/op
BenchmarkReadWriteMap/frac_4-4         	 2000000	       657 ns/op
BenchmarkReadWriteMap/frac_5-4         	 2000000	       650 ns/op
BenchmarkReadWriteMap/frac_6-4         	 3000000	       592 ns/op
BenchmarkReadWriteMap/frac_7-4         	 3000000	       573 ns/op
BenchmarkReadWriteMap/frac_8-4         	 3000000	       539 ns/op
BenchmarkReadWriteMap/frac_9-4         	 3000000	       521 ns/op
BenchmarkReadWriteMap/frac_10-4        	 3000000	       479 ns/op
```