Ran on macbook.

```
BenchmarkWrite/branch_2-4  	 1000000	     35556 ns/op
BenchmarkWrite/branch_3-4  	 1000000	      5286 ns/op
BenchmarkWrite/branch_4-4  	  500000	      5113 ns/op
BenchmarkWrite/branch_5-4  	 1000000	      6270 ns/op
BenchmarkWrite/branch_6-4  	 1000000	      6019 ns/op
BenchmarkWrite/branch_7-4  	 1000000	      6720 ns/op

BenchmarkWriteParallel/branch_2-4         	 1000000	     40336 ns/op
BenchmarkWriteParallel/branch_3-4         	 1000000	      5751 ns/op
BenchmarkWriteParallel/branch_4-4         	 1000000	      5915 ns/op
BenchmarkWriteParallel/branch_5-4         	 1000000	      6291 ns/op
BenchmarkWriteParallel/branch_6-4         	 1000000	      6775 ns/op
BenchmarkWriteParallel/branch_7-4         	 1000000	      7307 ns/op

BenchmarkRead/branch_2-4                  	  200000	      7337 ns/op
BenchmarkRead/branch_3-4                  	  500000	      3083 ns/op
BenchmarkRead/branch_4-4                  	  500000	      3318 ns/op
BenchmarkRead/branch_5-4                  	  500000	      3688 ns/op
BenchmarkRead/branch_6-4                  	  500000	      3721 ns/op
BenchmarkRead/branch_7-4                  	  300000	      4219 ns/op

BenchmarkReadParallel/branch_2-4          	  500000	      2649 ns/op
BenchmarkReadParallel/branch_3-4          	 2000000	      1005 ns/op
BenchmarkReadParallel/branch_4-4          	 1000000	      1065 ns/op
BenchmarkReadParallel/branch_5-4          	 1000000	      1152 ns/op
BenchmarkReadParallel/branch_6-4          	 1000000	      1188 ns/op
BenchmarkReadParallel/branch_7-4          	 1000000	      1248 ns/op

BenchmarkReadWrite/frac_0-4         	 1000000	      5825 ns/op
BenchmarkReadWrite/frac_1-4         	 1000000	      5669 ns/op
BenchmarkReadWrite/frac_2-4         	 1000000	      5556 ns/op
BenchmarkReadWrite/frac_3-4         	 1000000	      5231 ns/op
BenchmarkReadWrite/frac_4-4         	 1000000	      4772 ns/op
BenchmarkReadWrite/frac_5-4         	 1000000	      4194 ns/op
BenchmarkReadWrite/frac_6-4         	 1000000	      3700 ns/op
BenchmarkReadWrite/frac_7-4         	 1000000	      2923 ns/op
BenchmarkReadWrite/frac_8-4         	 1000000	      2124 ns/op
BenchmarkReadWrite/frac_9-4         	 1000000	      1218 ns/op
BenchmarkReadWrite/frac_10-4        	 3000000	       455 ns/op
```