Ran on macbook.

```
BenchmarkWrite/branch_2-4  	 1000000	     44162 ns/op
BenchmarkWrite/branch_3-4  	 1000000	      6306 ns/op
BenchmarkWrite/branch_4-4  	 1000000	      6922 ns/op
BenchmarkWrite/branch_5-4  	 1000000	      9222 ns/op
BenchmarkWrite/branch_6-4  	 1000000	      7646 ns/op
BenchmarkWrite/branch_7-4  	  500000	      6604 ns/op
BenchmarkWriteParallel/branch_2-4         	  500000	     24437 ns/op
BenchmarkWriteParallel/branch_3-4         	 1000000	      7390 ns/op
BenchmarkWriteParallel/branch_4-4         	 1000000	      8112 ns/op
BenchmarkWriteParallel/branch_5-4         	  500000	      7885 ns/op
BenchmarkWriteParallel/branch_6-4         	  500000	      8459 ns/op
BenchmarkWriteParallel/branch_7-4         	  500000	      8593 ns/op
BenchmarkRead/branch_2-4                  	  200000	      8491 ns/op
BenchmarkRead/branch_3-4                  	  500000	      3108 ns/op
BenchmarkRead/branch_4-4                  	  500000	      3223 ns/op
BenchmarkRead/branch_5-4                  	  500000	      3586 ns/op
BenchmarkRead/branch_6-4                  	  500000	      4118 ns/op
BenchmarkRead/branch_7-4                  	  300000	      4729 ns/op
BenchmarkReadParallel/branch_2-4          	  500000	      2680 ns/op
BenchmarkReadParallel/branch_3-4          	 1000000	      1909 ns/op
BenchmarkReadParallel/branch_4-4          	 1000000	      1928 ns/op
BenchmarkReadParallel/branch_5-4          	 1000000	      1663 ns/op
BenchmarkReadParallel/branch_6-4          	 1000000	      1400 ns/op
BenchmarkReadParallel/branch_7-4          	 1000000	      1733 ns/op
BenchmarkReadWrite/frac_0-4               	 1000000	      6719 ns/op
BenchmarkReadWrite/frac_1-4               	 1000000	      6177 ns/op
BenchmarkReadWrite/frac_2-4               	 1000000	      6239 ns/op
BenchmarkReadWrite/frac_3-4               	 1000000	      5616 ns/op
BenchmarkReadWrite/frac_4-4               	 1000000	      5103 ns/op
BenchmarkReadWrite/frac_5-4               	 1000000	      4962 ns/op
BenchmarkReadWrite/frac_6-4               	 1000000	      4172 ns/op
BenchmarkReadWrite/frac_7-4               	 1000000	      3175 ns/op
BenchmarkReadWrite/frac_8-4               	 1000000	      2070 ns/op
BenchmarkReadWrite/frac_9-4               	 1000000	      1201 ns/op
BenchmarkReadWrite/frac_10-4              	 5000000	       456 ns/o
```

For comparison with lock-free skiplist

```
BenchmarkReadParallel/branch_4-4         	 1000000	      1358 ns/op
BenchmarkWriteParallelAlt-4   	     100	  19644625 ns/op
```
