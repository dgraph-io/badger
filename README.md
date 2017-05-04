# Badger

An embeddable, simple and fast key value (KV) store, written natively in Go.

## About

Badger is written out of frustration with existing KV stores which are either natively written in Go and slow, or fast but require usage of Cgo.
Badger aims to provide an equal or better speed compared to industry leading KV stores (like RocksDB), while maintaining the entire code base in Go natively.

## Design Goals

Badger has these design goals in mind:

- Write it natively in Go.
- Use latest research to build the fastest KV store for data sets spanning terabytes.
- Keep it simple, stupid. No support for transactions, versioning or snapshots -- anything that can be done outside of the store should be done outside.
- Optimize for SSDs (more below).

### Non-Goals

- Try to be a database.

## Design

In simplest terms, keys are stored in LSM trees along with pointers to values, which would be stored in write-ahead log files, aka value logs.
Keys would typically be kept in RAM, values would be served directly off SSD.

### Optimizations for SSD

SSDs are best at doing serial writes (like HDD) and random reads (unlike HDD).
Each write reduces the lifecycle of an SSD, so Badger aims to reduce the write amplification of a typical LSM tree.
It achieves this by separating the keys from values. Keys tend to be smaller in size and are stored in the LSM tree.
Values (which tend to be larger in size) are stored in value logs, which also double as write ahead logs for fault tolerance.
Only a pointer to the value is stored along with the key, which significantly reduces the size of each KV pair in LSM tree.
This allows storing lot more KV pairs per table. For e.g., a table of size 64MB can store 2 million KV pairs assuming an average key size of 16 bytes, and a value pointer of 16 bytes (with prefix diffing in Badger, the average key sizes stored in a table would be lower).

### Nature of LSM trees

Because only keys (and value pointers) are stored in LSM tree, Badger generates much smaller LSM trees.
Even for huge datasets, these smaller trees can fit nicely in RAM allowing for lot quicker accesses to and iteration through keys.
For random gets, keys can be quickly looked up from within RAM, and only pointed reads from SSD are done to retrieve value.
This improves random get performance significantly compared to traditional LSM tree design used by other KV stores.

## Comparisons

| Feature             | Badger                                       | RocksDB            | BoltDB    |
| -------             | ------                                       | -------            | ------    |
| Design              | LSM tree with value log                      | LSM tree only      | B+ tree   |
| High RW Performance | Yes                                          | Yes                | No        |
| Designed for SSDs   | Yes (with latest research*)                  | Not specifically   | No        |
| Embeddable          | Yes                                          | Yes                | Yes       |
| Sorted KV access    | Yes                                          | Yes                | Yes       |
| Go Native (no Cgo)  | Yes                                          | No                 | Yes       |
| Transactions        | No (but provides compare and set operations) | Yes (but non-ACID) | Yes, ACID |
| Snapshots           | No                                           | Yes                | Yes       |

* Badger is based on a paper called [WiscKey by University of Wisconsin, Madison](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), where they separate values from keys, which significantly reduces the write amplification compared to a typical LSM tree.
