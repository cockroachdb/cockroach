- Feature Name: Asynchronous Global Tables
- Status: **rough** draft
- Start Date: 2021-03-16
- Authors: Andrew Werner
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary



# Motivation

Use cases arise where customers would like the following mix of properties:

 * low latency writes and reads
    * Cannot leave the region to service reads or writes
 * high availability reads of all data in the face of regional failure

Given the CAP theorem and all of that, something has to give. In this case,
these customers are fining giving up on consistency, at least, selectively.
In particular, it is totally fine for a write to one region to not be visible
immediately, or in any particular order to readers in other regions requiring
low latency. 

The proposal here attempts to fit this concept neatly into Cockroach's suite of
global tools while leveraging concepts and machinery cockroach already has. The
proposal puts forth a new type of table multi-region table called `ASYNCHRONOUS
GLOBAL` which has a cheeky formulation that allows it to provide both
strong consistency and isolation when desired while simultaneously exposing an
asynchronously updated view of the data due to writes performed other regions
for highly available reads.

# Technical design

For the remainder of this discussion, let's say we're in the following database
```
CREATE DATABASE db 
 PRIMARY REGION "us-west-1"
        REGIONS "us-east-1", "us-central-1"
        SURVIVE ZONE;
```

## Preface to talk about REGIONAL BY ROW

Let's imagine the following table:

```
CREATE TABLE kv (
    k STRING PRIMARY KEY,
    v STRING,
) LOCALITY REGIONAL BY ROW;
```

This table has a primary key that is a region and a user-specified key. This
allows the user to write a key into the current region without any coordination.
If the user knows that the key is in the current region, then reads can be
fast but if the key is in another region, then reads will be slow. 

This table really expands to something more like:

```
CREATE TABLE kv (
    crdb_internal_region crdb_region HIDDEN DEFAULT gateway_region(), 
    k STRING,
    v STRING,
    PRIMARY KEY (crdb_internal_region, k),
    UNIQUE WITHOUT INDEX (k)
) LOCALITY REGIONAL BY ROW;
```

One additional and important constraint is that we don't want the system to
enforce that the user is providing globally unique keys. In order to deal with
that, we'll be utilizing the `crdb_region` column as a part of primary keys --
notice that this isn't a fundamental thing. In other cases where `UUID`s are a
reasonable key choice, we can utilize them and generate them to ensure that we
can have globally unique primary keys without the coordination burden.

So, in this case, we might want to do:

```
CREATE TABLE kv (
    k STRING,
    v STRING,
    PRIMARY KEY (crdb_internal_region, k)
) LOCALITY REGIONAL BY ROW;
```

This will permit multiple rows with the same `k` but not with the same region
value.

## LOCALITY ASYNCHRONOUS GLOBAL

The thrust of this proposal is a new type of multi-region table called an
`ASYNCHRONOUS GLOBAL` table. 

Let's look at the following:

```sql
CREATE TABLE kv (
    k STRING,
    v STRING,
    PRIMARY KEY (crdb_internal_region, k)
) LOCALITY ASYNCHRONOUS GLOBAL;
```

This table actually expands to a very different thing from the `REGIONAL BY ROW`
table. It will expand to:

```sql
CREATE TABLE kv (
    crdb_internal_region crdb_region DEFAULT gateway_region() HIDDEN,
    crdb_internal_source_region crdb_region DEFAULT gateway_region() HIDDEN,
    k STRING,
    v STRING,
    updated DECIMAL HIDDEN,
    PRIMARY KEY (crdb_internal_region, crdb_internal_source_region, k)
) LOCALITY REGIONAL BY ROW;
```

Now, what's super interesting about these tables is that users will always
insert into the table by inserting into the table with the prefix of the enum
for their region for both the region and the source region. 

When a user wants fast reads, they can get them by doing the following:

```sql
SELECT * FROM kv WHERE crdb_internal_region = gateway_region()
```

What's amazing, is that you can achieve consistent reads on the table by doing
the following:

```sql
SELECT * FROM kv WHERE crdb_internal_region = crdb_internal_source_region
```

You can even create partial indexes which carry this predicate in order to
maintain indexes over just the fresh data!

## Asynchronous updates

Well, now you may be thinking to yourself, well, if everybody is writing into
the prefix (gateway_region(), gateway_region()) for each gateway region, how
does the data from one region make it into another region? This, here, is the
main trick of the entire scheme:

For each region, we create an internal job to watch for writes to one region and
copy them over to all of the other regions. This infrastructure can utilize 
internal primitives very similar to changefeeds, but we can handle it all
entirely internally and transparently.

## Filled out example:

Imagine that we run the following in `us-east-1` that we'll say occurs at `t1`.
This write is local only. 
```
INSERT INTO kv VALUES ('a', 'a'), ('b', 'b');
```

This will insert the following data (note below that the encoding is presented
for readability):

```
/Table/kv/1/us-east-1/us-east-1/a@t1: {"v": "a", "updated": null}
/Table/kv/1/us-east-1/us-east-1/b@t1: {"v": "b", "updated": null}
```

Now at t2, we might do a read from `us-west-1` of just it's local data which
will be:
```sql
SELECT v FROM kv WHERE k = 'a' AND crdb_internal_region = gateway_region()
```

This is going to generate the following spans:

```
/Table/kv/1/us-west-1/us-east-1/{a-}
/Table/kv/1/us-west-1/us-west-1/{a-}
/Table/kv/1/us-west-1/us-central-1/{a-}
```

Fortunately, these are all local reads! However, note that this query is going
to not find our previous write. That's because it hasn't been replicated yet!

## Replication 

For each region, we're going to have jobs for each other region which is going
to watch writes to the current region and write them to the target regions. It
will do this by creating rangefeeds over the local paritition with the prefix
that contains the writes with both the region and source region of the local
region. 

Imagine that the replication process for `us-east-1`->`us-west-1` sees the
following write:
```
/Table/kv/1/us-east-1/us-east-1/b@t1: {"v": "b", "updated": null}
``` 

It will issue a write to `us-west-1` that looks like:

```
/Table/kv/1/us-east-1/us-east-1/b@t3: {"v": "b", "updated": t@1}
```

Note that the t3 is just the current timestamp when the replication occurred.
A similar write will occur for `us-central-1`. At the end up replication, the
table state will look like:

```
/Table/kv/1/us-east-1/us-east-1/a@t1: {"v": "a", "updated": null}
/Table/kv/1/us-east-1/us-east-1/b@t1: {"v": "b", "updated": null}
/Table/kv/1/us-central-1/us-east-1/a@t3.2: {"v": "a", "updated": t1}
/Table/kv/1/us-central-1/us-east-1/b@t2.9: {"v": "b", "updated": t1}
/Table/kv/1/us-west-1/us-east-1/a@t3: {"v": "a", "updated": t1}
/Table/kv/1/us-west-1/us-east-1/b@t3.1: {"v": "b", "updated": t1}
```

The timestamp in the row value will help the replication process understand when
it should not perform writes. This is an optimization.


## Drawbacks

* Machinery?
* N^2 Jobs
* Doesn't really help us to do DR things that improving resolved timestamp infra
  might.

## Rationale and Alternatives

Alternatives:

* Inconsistent reads off of followers
* Something about bounded staleness and fanning out to each region from clients.

# Explain it to folk outside of your team

TODO

# Unresolved questions

* Do we want to add syntax for indexes which are only over the fresh data?
   * In fact, should we *only* permit secondary indexes over fresh data and
     then make those all `REGIONAL BY ROW`?
* These predicates to indicate local and global are pretty gnarly, should there
  be syntactic sugar? This feels like it could really benefit from a bunch
* Lots about observability of the replication process
* Schema changes?
   * They actually shouldn't be too bad -- by making it all in one table
     we can really narrow the scope.