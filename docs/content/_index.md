---
title: "BadgerDB Documentation"
date: 2020-07-06T17:43:29+05:30
draft: false
---

![Badger mascot](/images/diggy-shadow.png)

BadgerDB is an embeddable, persistent and fast key-value (KV) database written
in pure Go. It is the underlying database for [Dgraph](https://dgraph.io), a
fast, distributed graph database. It's meant to be a performant alternative to
non-Go-based key-value stores like RocksDB.

## Project Status [March 24, 2020]

Badger is stable and is being used to serve data sets worth hundreds of
terabytes. Badger supports concurrent ACID transactions with serializable
snapshot isolation (SSI) guarantees. A Jepsen-style bank test runs nightly for
8h, with `--race` flag and ensures the maintenance of transactional guarantees.
Badger has also been tested to work with filesystem level anomalies, to ensure
persistence and consistency. Badger is being used by a number of projects which
includes Dgraph, Jaeger Tracing, UsenetExpress, and many more.

The list of projects using Badger can be found [here]({{<relref "projects-using-badger/index.md" >}}).

Badger v1.0 was released in Nov 2017, and the latest version that is data-compatible
with v1.0 is v1.6.0.

Badger v2.0 was released in Nov 2019 with a new storage format which won't
be compatible with all of the v1.x. Badger v2.0 supports compression, encryption and uses a cache to speed up lookup.

The [Changelog] is kept fairly up-to-date.

For more details on our version naming schema please read [Choosing a version]({{< relref "get-started/index.md#choosing-a-version" >}}).

[Changelog]:https://github.com/dgraph-io/badger/blob/master/CHANGELOG.md
