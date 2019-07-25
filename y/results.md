- Page size 1 MB
- Data size 1 MB
- No preallocation in bytes.buffer
```
name                    old time/op  new time/op  delta
Buffer/write-1.0_MB-16  22.3ms ± 4%  22.5ms ± 2%  +0.84%  (p=0.016 n=29+28)

name                    old time/op  new time/op  delta
Buffer/write-1.0_MB-16  22.3ms ± 3%  22.5ms ± 3%  +0.96%  (p=0.006 n=30+30)

name                    old time/op  new time/op  delta
Buffer/write-1.0_MB-16  22.3ms ± 3%  22.3ms ± 3%   ~     (p=0.697 n=30+30)
```
Conclusion- New implementation slower


- Page size 1 MB
- Data size 1 MB
- Preallocate page size in bytes.buffer
```
name                    old time/op  new time/op  delta
Buffer/write-1.0_MB-16  22.4ms ± 2%  22.5ms ± 3%   ~     (p=0.323 n=30+29)

name                    old time/op  new time/op  delta
Buffer/write-1.0_MB-16  22.6ms ± 2%  22.6ms ± 3%   ~     (p=0.301 n=30+29)

name                    old time/op  new time/op  delta
Buffer/write-1.0_MB-16  22.2ms ± 4%  22.5ms ± 2%  +1.13%  (p=0.003 n=30+28)

```


- Page size 1 MB
- data size 10 MB
- No Preallocation page size
```
name                   old time/op  new time/op  delta
Buffer/write-10_MB-16   222ms ± 3%   226ms ± 3%  +1.85%  (p=0.000 n=30+30)

name                   old time/op  new time/op  delta
Buffer/write-10_MB-16   223ms ± 3%   227ms ± 4%  +1.44%  (p=0.003 n=30+29)

name                   old time/op  new time/op  delta
Buffer/write-10_MB-16   225ms ± 4%   226ms ± 4%   ~     (p=0.182 n=30+30)
```

- Page size 1 MB
- data size 10 MB
- Preallocate page size
```
name                   old time/op  new time/op  delta
Buffer/write-10_MB-16   220ms ± 3%   222ms ± 3%  +1.09%  (p=0.003 n=30+28)

name                   old time/op  new time/op  delta
Buffer/write-10_MB-16   219ms ± 4%   225ms ± 4%  +2.55%  (p=0.000 n=30+30)

name                   old time/op  new time/op  delta
Buffer/write-10_MB-16   220ms ± 3%   226ms ± 3%  +2.59%  (p=0.000 n=29+30)
```

## Bytes() call
- Page size 1 MB
- data size 10 MB
- Preallocate page size
```
name                   old time/op  new time/op        delta
Buffer/bytes-10_MB-16  0.54ns ± 4%  2619897.50ns ± 3%  +486066226.53%  (p=0.000 n=10+8)
```