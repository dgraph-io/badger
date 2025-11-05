# Design

We wrote Badger with these design goals in mind:

- Write a key-value database in pure Go
- Use latest research to build the fastest KV database for data sets spanning
  terabytes
- Optimize for modern storage devices

Badger’s design is based on a paper titled
[WiscKey: Separating Keys from Values in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).

## References

The following blog posts are a great starting point for learning more about
Badger and the underlying design principles:

- [Introducing Badger: A fast key-value store written natively in Go](https://hypermode.com/blog/badger/)
- [Make Badger crash resilient with ALICE](https://hypermode.com/blog/alice/)
- [Badger vs LMDB vs BoltDB: Benchmarking key-value databases in Go](https://hypermode.com/blog/badger-lmdb-boltdb/)
- [Concurrent ACID Transactions in Badger](https://hypermode.com/blog/badger-txn/)

## Comparisons

| Feature                       | Badger                                 | RocksDB                      | BoltDB  |
| ----------------------------- | -------------------------------------- | ---------------------------- | ------- |
| Design                        | LSM tree with value log                | LSM tree only                | B+ tree |
| High Read throughput          | Yes                                    | No                           | Yes     |
| High Write throughput         | Yes                                    | Yes                          | No      |
| Designed for SSDs             | Yes (with latest research<sup>1</sup>) | Not specifically<sup>2</sup> | No      |
| Embeddable                    | Yes                                    | Yes                          | Yes     |
| Sorted KV access              | Yes                                    | Yes                          | Yes     |
| Pure Go (no Cgo)              | Yes                                    | No                           | Yes     |
| Transactions                  | Yes                                    | Yes                          | Yes     |
| ACID-compliant                | Yes, concurrent with SSI<sup>3</sup>   | No                           | Yes     |
| Snapshots                     | Yes                                    | Yes                          | Yes     |
| TTL support                   | Yes                                    | Yes                          | No      |
| 3D access (key-value-version) | Yes<sup>4</sup>                        | No                           | No      |

<sup>1</sup> The WiscKey paper (on which Badger is based) saw big wins with
separating values from keys, significantly reducing the write amplification
compared to a typical LSM tree.

<sup>2</sup> RocksDB is an SSD-optimized version of LevelDB, which was designed
specifically for rotating disks. As such RocksDB's design isn't aimed at SSDs.

<sup>3</sup> SSI: Serializable Snapshot Isolation. For more details, see the
blog post [Concurrent ACID Transactions in
Badger](https://hypermode.com/blog/badger-txn/)

<sup>4</sup> Badger provides direct access to value versions via its Iterator
API. Users can also specify how many versions to keep per key via Options.

## Benchmarks

We've run comprehensive benchmarks against RocksDB, BoltDB, and LMDB. The
benchmarking code with detailed logs are in the
[badger-bench](https://github.com/dgraph-io/badger-bench) repo.
