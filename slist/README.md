Work in progress. This is quite hard to port from Java.

The RocksDB version (inlined skiplist) does some tricks with memory
allocation and is not nice to port. We will consider that later.

Meanwhile, let's try to implement memtable using the simpler skiplist.