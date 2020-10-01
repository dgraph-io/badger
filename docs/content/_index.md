---
title: "BadgerDB Documentation"
date: 2020-07-06T17:43:29+05:30
draft: false
---

![Badger mascot](/images/diggy-shadow.png)

**Welcome to the official Badger documentation.**

BadgerDB is an embeddable, persistent and fast key-value (KV) database written
in pure Go. It is the underlying database for [Dgraph](https://dgraph.io), a
fast, distributed graph database. It's meant to be a performant alternative to
non-Go-based key-value stores like RocksDB.

## Table of Contents

<section class="toc">
  <div class="container">
    <div class="row row-no-padding">
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="{{< relref "get-started/_index.md">}}">
              Quickstart Guide
            </a>
          </div>
          <p class="section-desc">
            A single page quickstart guide to get started with BadgerDB
          </p>
        </div>
      </div>
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="{{< relref "resources/_index.md">}}">
              Resources
            </a>
          </div>
          <p class="section-desc">
            Additional resources and information
          </p>
        </div>
      </div>
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="{{< relref "design/_index.md">}}">
              Design
            </a>
          </div>
          <p class="section-desc">
            Design goals behind BadgerDB
          </p>
        </div>
      </div>
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="{{< relref "projects-using-badger/_index.md">}}">
              Projects using Badger
            </a>
          </div>
          <p class="section-desc">
            A list of known projects that use BadgerDB
          </p>
        </div>
      </div>
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="/faq">
              FAQ
            </a>
          </div>
          <p class="section-desc">
            Frequently asked questions
          </p>
        </div>
      </div>
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="https://dgraph.io/badger">
              Badger
            </a>
          </div>
          <p class="section-desc">
            Embeddable, persistent and fast key-value database that powers Dgraph
          </p>
        </div>
      </div>
    </div>
  </div>
</section>

## Changelog

The [Changelog] is kept fairly up-to-date.

- Badger v1.0 was released in Nov 2017, and the latest version that is data-compatible
with v1.0 is v1.6.0.
- Badger v2.0 was released in Nov 2019 with a new storage format which won't
be compatible with all of the v1.x. Badger v2.0 supports compression, encryption and uses a cache to speed up lookup.

For more details on our version naming schema please read [Choosing a version]({{< relref "get-started/index.md#choosing-a-version" >}}).

[Changelog]:https://github.com/dgraph-io/badger/blob/master/CHANGELOG.md

## Contribute

<section class="toc">
  <div class="container">
    <div class="row row-no-padding">
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="https://github.com/dgraph-io/badger/blob/master/CONTRIBUTING.md">
              Contribute to Badger
            </a>
          </div>
          <p class="section-desc">
            Get started with contributing fixes and enhancements to Badger and related software.
          </p>
        </div>
      </div>
      </div>
  </div>
</section>

## Our Community

**Badger is made better every day by the growing community and the contributors all over the world.**

<section class="toc">
  <div class="container">
    <div class="row row-no-padding">
      <div class="col-12 col-sm-6">
        <div class="section-item">
          <div class="section-name">
            <a href="https://discuss.dgraph.io">
              Community
            </a>
          </div>
          <p class="section-desc">
            Discuss Badger on the official community.
          </p>
        </div>
      </div>
    </div>
  </div>
</section>
