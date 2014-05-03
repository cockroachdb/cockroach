# Cockroach [![Build Status](https://secure.travis-ci.org/cockroachdb/cockroach.png)](http://travis-ci.org/cockroachdb/cockroach) [![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.png)](https://godoc.org/github.com/cockroachdb/cockroach)

A Scalable, Geo-Replicated, Transactional Datastore

## Contributing

We use [Phabricator](http://phabricator.andybons.com/) for code reviews. Phabricator
uses your GitHub credentials, just click on the "Login or Register" button. The Phabricator
development model is similar to the GitHub pull request model in that changes are
typically developed on their own branch and then uploaded for review. When a change is
uploaded to Phabricator (via `arc diff`), it is not merged with master until
the review is complete and submitted.

+ Make sure your [Go environment is set up](http://golang.org/doc/code.html).
+ Retrieve the code: `go get github.com/cockroachdb/cockroach`
+ Within `$GOPATH/src/github.com/cockroachdb/cockroach/`, run `./bootstrap.sh`. This will install the git hooks and any prerequisite binaries for them.
+ Hack away...
+ Commit your changes locally using `git add` and `git commit`.
+ Upload your change for review using `arc diff`.

## Installing Arcanist
To install Arcanist (the code review tool)...

Create a dir that will hold the two repos required.

`$ mkdir somewhere`

then clone the required repos into that folder:

```
somewhere/ $ git clone git://github.com/facebook/libphutil.git
somewhere/ $ git clone git://github.com/facebook/arcanist.git
```

Then add somewhere/arcanist/bin/ to your $PATH

Now within the cockroach directory...

```
$ git checkout -b newbranch
... make changes ...
$ git commit -a -m 'my awesome changes'
$ arc diff
```

Say you’ve updated your diff to account for some suggestions, you just commit those on the same branch and do:

```
$ arc diff
```

again and it will take care of uploading things.

Once you’re ready to land a change (it has been approved).

```
$ arc land
```

it will squash all commits, update the commit message with the original headline and description, and have a link back to the review. It will also clean up (delete) your feature branch.

## Design

Cockroach is a distributed key:value datastore which supports ACID
transactional semantics and versioned values as first-class
features. The primary design goal is global consistency and
survivability, hence the name. Cockroach aims to tolerate disk,
machine, rack, and even datacenter failures with minimal latency
disruption and no manual intervention. Cockroach nodes are symmetric;
a design goal is one binary with minimal configuration and no required
auxiliary services.

Cockroach implements a single, monolithic sorted map from key to value
where both keys and values are byte strings (not unicode). Cockroach
scales linearly (theoretically up to 4 exabytes (4E) of logical
data). The map is composed of one or more ranges and each range is
backed by data stored in rocksDB (a variant of leveldb), and is
replicated to a total of three or more cockroach servers. Ranges are
defined by start and end keys. Ranges are merged and split to maintain
total byte size within a globally configurable min/max size
interval. Range sizes default to target 64M in order to facilitate
quick splits and merges and to distribute load at hotspots within a
key range. Range replicas are intended to be located in disparate
datacenters for survivability (e.g. { US-East, US-West, Japan }, {
Ireland, US-East, US-West}, { Ireland, US-East, US-West, Japan,
Australia }).

Single mutations to ranges are mediated via an instance of a
distributed consensus algorithm to ensure consistency. We’ve chosen to
use the Raft consensus algorithm. All consensus state is stored in
rocksDB.

A single logical mutation may affect multiple key/value pairs. Logical
mutations have ACID transactional semantics. If all keys affected by a
logical mutation fall within the same range, atomicity and consistency
are guaranteed by Raft; this is the fast commit path. Otherwise, a
non-locking distributed commit protocol is employed between affected
ranges.

Cockroach provides snapshot isolation (SI) and serializable snapshot
isolation (SSI) semantics, allowing externally consistent, lock-free
reads and writes--both from an historical snapshot timestamp and from
the current wall clock time. SI provides lock-free reads and writes
but still allows write skew. SSI eliminates write skew, but introduces
a performance hit in the case of a contentious system. SSI is the
default isolation; clients must consciously decide to trade
correctness for performance. Cockroach implements a limited form of
linearalizability, providing ordering for any observer or chain of
observers.

Similar to Spanner directories, Cockroach allows configuration of
arbitrary zones of data. This allows replication factor, storage
device type, and/or datacenter location to be chosen to optimize
performance and/or availability. Unlike Spanner, zones are monolithic
and don’t allow movement of fine grained data on the level of entity
groups.

A Megastore-like message queue mechanism is also provided to 1)
efficiently sideline updates which can tolerate asynchronous execution
and 2) provide an integrated message queuing system for asynchronous
communication between distributed system components.

-----------------

For design details visit: https://docs.google.com/document/d/11k2EmhLGSbViBvi6_zFEiKzuXxYF49ZuuDJLe6O8gBU/edit?usp=sharing

#### SQL - NoSQL - SpanSQL Capabilities

![SQL - NoSQL - SpanSQL Capabilities](/resources/doc/sql-nosql-spansql.png?raw=true)
