- Feature Name: Disaggregated Shared Storage
- Status: draft
- Start Date: 2021-09-14
- Authors: Sumeer Bhola
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Motivation and Summary

CockroachDB nodes currently own the persistent state of all the range
replicas at the node. Adding/moving a replica involves sending a copy
of the replica state to another node. We have a 512MB limit on range
size which is hard to increase significantly, partly because of this
need to send a copy of the replica state. Also, reducing the cost of
range rebalancing, by not needing to copy state, could allow the
system to be more responsive to shifting load by doing more aggressive
rebalancing, which could generally improve the user experience.

The maximum recommended storage capacity per node is
[2.5TB](https://www.cockroachlabs.com/docs/stable/recommended-production-settings.html#storage).
Users would like the storage capacity to be much higher, especially
with significant amount of cold state (not often being touched by
reads or writes), since it reduces their cost. We believe that there
are OLTP contexts where older state is cold e.g. old orders in order
processing. Infrequent accesses is also likely going to be true for
the long tail of serverless users, and a decrease in cost can
translate to wider adoption. A combination of lower cost storage for
range replica state, and increase in storage capacity per node, would
achieve lower costs. Note that increasing storage capacity per node is
not necessarily the primary motivation for this proposal: it is likely
that average range sizes are (or will be) significantly smaller than
the existing 512MB limit because of small tenants, or regional by row
tables that slice up the key space of the table into smaller ranges at
region boundaries. The latter can be addressed by allowing for
[non-contiguous
ranges](https://github.com/cockroachdb/cockroach/issues/65726).  One
should consider this proposal as helpful when other constraints that
are forcing a range boundary are no longer preventing range size
increases.

Full backups and restores involve copying to and from cheaper shared
storage. Making these operations faster and cheaper can improve RPO
and RTO. There are also use cases that involve branching/copying
tables to use for other purposes, like testing, or data sharing
(potentially even across customers), that cannot be accomplished in
the current storage architecture. This is because making a read-only
copy of a table to be served by another set of replica nodes is
costly. If we had the ability to make cheap read-only copies, by
taking a cheap snapshot of shared state, these use cases would become
viable (with additional work at layers above the storage layer).

This RFC proposes moving most of the state machine state for a range
to shared storage (e.g. S3, GCS), such that that storage can be shared
across nodes when needed. The shared storage is cheaper per stored
byte, and the fact that it can be shared across nodes allows for cheap
rebalances/backups/restores/branches etc. We do not consider disk
sharing options like [shared persistent
disks](https://cloud.google.com/compute/docs/disks/sharing-disks-between-vms)
since they currently come with restrictions, and we desire more
fine-grained sharing. This RFC ignores detailed cost analysis, which
will need to be performed since shared storage additionally charges
for reads and writes, and without such cost analysis it is hard to be
certain that this approach will actually be cheaper. The stored byte
cost for shared storage is as expected significantly lower:
approximately 12% of that of external SSD block storage, based on
comparing the costs
[here](https://cloud.google.com/storage/pricing#price-tables) and
[here](https://cloud.google.com/compute/disks-image-pricing#disk).

One concern with this approach is that while the durability SLO for
the aforementioned shared storage is sufficiently high, the
availability SLO is lower than block storage (e.g. EBS) currently
employed in CockroachDB deployments. There are various ways to deal
with it:

- [O1] Transparently improve availability for non-cold data by caching
  it in block storage (or local SSDs). Users would need to be aware
  that this transparent caching cannot make perfect decisions on what
  to cache, so there could be occassional drops in availability. 
- [O2] Allow users to mark critical tables as mandatorily cached.
- [O3] Allow users to opt out of the use of shared storage for certain
  data.

Options O1 and O2 are complementary to each other. Option O3 is our
current solution, and it may increase code complexity by requiring
maintenance of two different code paths, and so may not be
desirable. One challenge will be ensuring that O2 is not significantly
more expensive than O3, for users that mark vast swaths of their data
as mandatorily cached. Note that when using local SSD for the cache,
versus using external block storage (e.g. EBS or Persistent Disk) for
the LSM (option O1), the former costs ~50% of the [latter](https://cloud.google.com/compute/disks-image-pricing#disk).


Our current approach of placing the state machine state on external
replicated block devices, like EBS or GCP PD, means that there is a
multiplier effect on number of copies of data: N range replicas \* M
block device "replicas" (may not be standard replication, but there is
redundancy). The approach outlined in this RFC to use shared storage
does not directly try to address this: each replica will continue to
do its own writes to the shared storage to maintain its own state
machine. However, if two replicas happened to start with a significant
number of shared files, because one replica used the state of the
other at initialization time, *and* the range has insignificant write
traffic, they could continue to share files for a significant duration
of time.

# Terminology

We adopt the following terms for the rest of this document:

- LSM: Refers to a single CockroachDB store maintained by a node, that
  is implemented as a log-structured merge tree (LSM tree),
  specifically Pebble. In the design sketched here, some files in an
  LSM may be in shared storage. For convenience, and without loss of
  generality, the discussion in this document usually does not
  consider multiple LSMs per node.

- Key spans and ranges: Key span refers to a [start, end) span of the
  global key space in CockroachDB. A range refers to a CockroachDB
  range, which is also defined by a key span, and has a number of
  replicas.

- Local-store and shared-store: The current LSM implementation is said
  to maintain all files in a local-store. The following design will
  also use a shared-store. Details of the shared-store implementation
  are omitted, and we assume the following capabilities (which are
  included in blob storage systems like S3 and GCS):
  - Write a file once, with no subsequent mutations.
  - Read a specific byte range in a file.
  - Strong consistency for reads after writes: This is needed for (a)
    data, so that the contents of sstables that have been written and
    are now part of the LSM state are correctly read, (b) metadata,
    created files should be visible to reads and to operations that
    list files. These requirements are satisfied based on
    documentation
    [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
    and [here](https://cloud.google.com/storage/docs/consistency).

  These operations are sufficient for how a LSM produces and consumes
  sstable files.

# Limitations

This document is not a detailed design. It also omits software
engineering details on how we could modify Pebble to implement this
design, while retaining existing users who want to continue using
Pebble in the current manner.  These will need to be fleshed out in
future refinements of this RFC or companion RFCs.

# Technical Design

We first consider some prerequisites, then explore some design
difficulties, and finally sketch out the design.

## Design constraints and prerequisites

Given the lower availability SLO for shared storage, we must not place
it on the critical path of user-facing writes. This means the raft log
and initial application to the state machine should happen to
local-store.

Additionally, past experience suggests that blob storage systems may
not respond well to high write amplification workloads, or result in
excessive operation costs, so it is desirable to pay some of the write
amplification cost in local-store. In theory, an LSM which has all
levels L0-L6 populated with files has 60x write amplification. Levels
L5 and L6 would account for 99% of the data. So we decide to place all
L5 and L6 sstables in shared-store and the higher levels in
local-store. This 99% value is based on the current compaction logic
which tries to maintain a 10x size multiplier between all levels -- it
is possible we could increase this multiplier for the lower levels
when we expect significant volume of cold data. Which may mean that
keeping only L6 in the shared-store would suffice to achieve a 100x
reduction in data volume that needs to be shipped from one node to
another. For purposes of this document we assume the current size
multipliers.

For the LSM that is split across local-store and shared-store, we
assume the absence of a WAL. That is, its full state consists only of
memtables, local-store sstables, shared-store sstables, and the loss
of the memtables on failure is not a correctness issue. This fits well
with our existing plans to separate the raft log into a separate LSM
from the state machine and turn off the WAL on the state machine
LSM. This separation relies on the `RaftAppliedIndex` stored in the
state machine, which is monotonically increasing, and represents the
last raft log index whose change has been applied to the state
machine. By reading the persisted value of the `RaftAppliedIndex` from
the state machine LSM, one can compute how much of the raft log is
safe to truncate. We assume that this change is already made for the
rest of this design. A node would have a single or few LSMs, on
local-store, that would keep the raft logs for all ranges at that
node. Since the raft logs are frequently truncated, the size of this
raft log LSM(s) should be a tiny fraction of the sum of the state
machine sizes of the corresponding ranges. Additionally, the size of
this raft log LSM(s) will not increase if the amount of cold data at
the node increases. Generalizing this, we assume that all unreplicated
state is in a separate LSM.

In hindsight, the assumption of raft log separation is not strictly
needed for the design sketched below (it was needed in some abandoned
design ideas), but it simplifies our mental model, so we continue
assuming it in this doc. It may be possible to relax this assumption.

## Problem statement

We refer to shared-store sstables as shared sstables and local-store
sstables as local sstables. Local sstables by definition must have
been produced in the context of the LSM (this includes sstable
ingestion into the LSM via `DB.Ingest`). Shared sstables could have
been produced within the LSM via ingestion or compaction, or could
have been produced in a different LSM and **imported** into this LSM
(as we will describe in detail later). We refer to these imported
shared sstables (imported into L5 and L6) as foreign sstables.

The problem is now reduced to:
- Manage an LSM consisting of memtables, local sstables, and shared
  sstables, that contains the state machine state for many range
  replicas at a node.
- Allow for the shared sstables to be directly used by another node
  that wants to initialize its state to become a range replica. This
  is the case where some of these shared sstables will become foreign
  sstables in the other node's LSM.

Note that the second bullet does not include cheap
backups/restores/branches of tables, but we claim that the design we
put together for this second bullet is an appropriate building block
for those additional features. We will discuss this briefly near the
end of this doc.

For the first bullet, the design is quite straightforward:
- Memtable flushes and higher-level compactions write local sstables
  (levels L0-L4).
- Compactions writing to L5 and L6 write to the shared-store, so write
  shared sstables. A few seconds of unavailability are harmless for
  such compactions.
- Ingested sstables, caused by bulk building of new indexes etc., can
  go into the lowest level that permits it based on the level
  invariant. Those that can go into L5 or L6 are written as shared
  sstables. Again, we can tolerate few seconds of unavailability.
- A manifest keeps track of the latest version of the LSM, and the
  manifest is maintained in the local-store.
- We need some caching of these shared sstables, which we will discuss
  later in this document.

## Design Challenges

We face some design challenges:

- The LSM is shared across many ranges, and sstables can contain data
  from multiple ranges. A node desiring to use a foreign sstable may
  not actually want all the data contained in the sstable. This is not
  really a hard problem. One can hide data in an sstable that does not
  fall within the desired key span, and the hiding can be done at
  iteration time (for all reads). Other systems have adopted this
  approach when sharing sstables after splitting a range R into
  sub-ranges R1 and R2: the iterator for R1 hides the data in the
  common sstable, that was built for R, that is not in the key span of
  R1.

- Constructing a consistent set of sstables to be used by another
  node: Consider the set of shared sstables in the latest version of
  the LSM at node N1. These do not represent a contiguous sequence of
  LSM sequence numbers, since each level in the LSM can have many
  files and newer writes can sink to lower levels before older
  writes. For the same reason, they do not represent a contiguous
  prefix of the raft log for that range. That is, these sstables are
  missing mutations corresponding to an arbitrary set of raft log
  indexes.
  - Tracking what is missing is hard: we do not know what LSM sequence
    numbers are missing **and** we do not have a mapping from sequence
    numbers to raft log indexes. If we did somehow know what raft log
    indexes were missing, we could potentially use these sstables
    along with newly generated higher level sstables. These higher
    level sstables would be generated by doing raft log writes
    corresponding to the missing indexes. But generating such sstables
    is also hard (a) the raft log may be truncated (though that can be
    handled by shipping the raft log to the shared-store before
    truncating), (b) we will incur the write amplification for levels
    L1-L4 again, which we have already paid at the time the mutations
    first happened (we do not want to pay the write amplification cost
    for old mutations on new nodes).
  - We cannot simply take a union of sstables that together cover all
    sequence numbers since some deletes may have been elided. For
    example, consider an older sstable that contained `a.SET.4:foo`,
    and subsequenly `a.DEL.10` was written. The shared sstables may
    contain `a.SET.4:foo`, but not the DEL. If we looked at the latest
    sstables for the higher levels (at an arbitrary replica) it is
    possible that the `a.DEL.10` has deleted the `a.SET.4` and been
    elided, so there is no remaining record of the
    `a.DEL.10`. Unioning these two sets of sstables would cause the
    `a.SET.4` to incorrectly become visible again.
  
  Note that the problem would be simpler in a system with a separate
  LSM per range and a single sstable per level, since then L5 and L6
  would represent a contiguous prefix of the raft log.

- The LSM sequence numbers (seqnums) in the shared sstables of nodes
  N1 may be incompatible with the seqnums in the existing LSM of node
  N2, where node N2 is the one trying to use these sstables to
  initialize its replica. This issue is a consequence of the fact that
  we do not maintain a separate LSM per range, so the seqnums are a
  function of the history of the LSM at the node and not that of the
  history of the range.

We do not want to switch to a separate LSM per range design given the
cons: small L0 sstables when flushing, and consequently higher write
amplification. Similarly we also do not want to change to a single
sstable per level design since multiple sstables per level behave
better when some parts of the key space are hotter than others.

## Simplifications to address challenges

We adopt the following simplifications:

- If node N1 has an LSM containing range R and node N2 desires to use
  the shared sstables from N1's LSM, it can do so only with
  cooperation from N1. That is, node N1 must be alive at the time the
  sharing starts (but can subsequently die). This is in contrast to
  shared sstable reuse in systems like Bigtable, where N1 can be dead
  -- but that reuse comes at the cost of a separate LSM per range and
  single file per level, both of which are not design points we want
  to switch to (as discussed in the previous section). One reason this
  is an acceptable assumption is that CockroachDB expects a quorum of
  nodes to be alive, so there should be some N1 that has a replica of
  range R that can serve the role above. However in a multi-region
  setting it is possible that the N1 is not in the same region as N2,
  in which case there will be inter region data transfer. We contend
  that this can be made no worse than the current copying of data,
  which would also be inter region. Additionally, production
  deployments tend to have extremely low unplanned individual node
  failures compared to planned node shutdowns (TODO: confirm this
  assumption). Given advance warning, a dying node N1 can efficiently
  help N2 get a consistent state for range R.

- Shared sstables do not span range boundaries. Note that this is
  best-effort behavior and does not eliminate the need to apply a key
  span constraint at iteration time (due to range splits). With 512MB
  ranges and 90% of data in L6 and 9% in L5, it gives a maximum
  sstable size of 472MB and 47MB respectively, which is not too
  small. And these maximum sizes will increase when we increase the
  maximum range size.

- A range R uses five non-contiguous parts of the replicated key
  space: the global key space which contains most of the state, range
  local keys, range-id local keys and range lock table keys (which use
  two non contiguous sections). We cannot afford for the local key
  space sstables to be per range since they will likely be
  tiny. Instead we will mark the local key space as maintaining all
  files in the local-store, even for levels L5 and L6 (see options
  [O3] described earlier). For simplicity of exposition, the rest of
  the doc does not discuss these.

## LSM structure

The LSM maintained by a node looks similar to our current
approach. There is a manifest maintained in the local-store that is
incrementally updated as new *versions* of the LSM are installed. The
manifest refers to two kinds of sstables (as expected):

- L0-L4 sstables in local-store.

- L5-L6 sstables in shared-store: For each of these, the LSM maintains
  a reference that is known to the shared-store, in addition to the
  per-sstable reference counts maintained within the LSM. When the
  LSM's reference count for an sstable drops to 0, instead of deleting
  the sstable (that we do for local sstables), the reference
  maintained in the shared-store is removed. We will discuss the
  design of this shared-store reference later in the document.

- Each shared sstable in this LSM is a *virtual* sstable consisting of
  the pair (key-span-constraint, shared sstable). The same shared
  sstable can appear in multiple places in this LSM in different
  virtual sstables. The LSM will maintain this repeated appearance as
  part of its internal reference count. Note that the
  key-span-constraint for a shared sstable replaces the file span
  maintained in the manifest for a local sstable. The file span uses
  the LSM's internal keys, which include the LSM seqnum and key kind,
  while the key-span-constraint is solely defined using user keys
  (more on that later). Part of the reason for this is that the
  seqnums inside a foreign shared sstable are arbitrary compared to
  the seqnums generated by this LSM.

- The virtual shared sstables are setup in a way to prevent any
  sstable from spanning across range boundaries. If a range is split,
  the LSM is informed and if there are any virtual sstables that span
  ranges, they will be split into multiple adjacent virtual sstables
  and a new version of the manifest installed. The virtualization of
  the sstable is local to this LSM. A different LSM in a different or
  same node may have virtualized the sstable with a different key
  span.

For example, consider an LSM with range R which has key span [a, z),
and a virtual sstable corresponding to R that is represented as
([a, z), F), where F is the name of the shared sstable. This range is
then split into R1 and R2, which have key spans [a, m) and [m, z),
resulting in two adjacent virtual sstables in this LSM ([a, m), F),
([m, z), F). Then [a, m) is further split into [a, f), [f, m),
resulting in ([a, f), F), ([f, m), F), ([m, z), F). Finally,
rebalancing causes the range [f, m) to be moved to another LSM, and
the initialization there is done with the cooperation of this
node. This node will have ([a, f), F), ([m, z), F) and the other node
([f, m), F). Soon after, this LSM again becomes a replica for the
range [f, m) and initializes itself using a virtual sstable from a
different node, say ([f, m), F2). We then have the following sequence
of sstables in a level ([a, f), F), ([f, m), F2), ([m, z), F).

We have presented a virtual sstable in a simple manner with a single
key span. It is possible to optimize this by keeping multiple key
spans per virtual sstable such that each key span does not cross range
boundaries. This could be useful if we need to iterate across multiple
ranges, since it can reduce the number of sstable iterators we need to
initialize.

Since splits may not be frequent, relative to the lifetime of L5 and
L6 files, we expect most sstables will have all keys within the
key-span-constraint. In such a case we do not need to do any extra key
comparisons at iteration time to impose the constraint. Also, even
when we need to impose the constraint, the decision can be made on a
block level, and for many blocks the constraint key comparisons would
be avoidable (like how we avoid iterator lower and upper bound
comparisons).

## Dropping range state from a node

Dropping the range state from a node consists of 2 steps:
- Remove the virtual sstables corresponding to the range in L5 and L6
  from the latest manifest version. Note that by definition the
  virtual sstables must be fully contained in the range or be
  non-intersecting with the range.
- Use an iterator over the memtables plus L0-L4 to decide whether to
  write point deletes or range deletes to delete data in those
  levels. Semantically these point or range deletes also apply to any
  data in L5 and L6, but there is no data there.

We use sstable removal to drop state in L5 and L6 not just because it
is potentially more efficient. The main motivation is clearing out the
file key space for this range from those levels, so that if the next
instant we want to add back this range, we can import foreign shared
files into those levels since the key space has been cleared out. We
will discuss the details later.

There is an unfortunate implication of this approach: LSM snapshots
cannot preserve state in L5/L6 for a range that has been dropped. This
is because LSM snapshots do not hold onto a specific LSM version, and
instead work with the latest LSM version, from which the dropped range
files will be gone. This is probably not an issue for CockroachDB: the
current uses of LSM snapshots are related to copying data for range
data transfers, for which we will no longer need the data in shared
sstables, and anyway will be done in a different manner (as discussed
later). Another possibility would be to eliminate support for LSM
snapshots: the primary reason for existence of snapshots is to not
cause a huge increase in disk bytes due to files being retained by
iterators, thereby causing an out-of-disk problem. With most of the
data being on shared storage, there is lower risk of this since one
could afford a higher percentage of head-room when provisioning the
local-store.


## File structure and refinement of key-span-constraint

The shared sstables are internally structured in the same way as local
sstables. However, there is a difference when looking across files in
a shared level (L5/L6):
when writing shared sstables we additionally impose the requirement
that different versions of the same user key do not span multiple
sstables. Hence, all shared sstables being written to a shared level
as part of a compaction have non-overlapping [start, end] user key
bounds. Note that the key-span-constraint we mentioned earlier is not
the full story of a shared sstable. The shared sstable itself has a
[start, end] file bound specified as user keys (not internal keys),
and then there is the key-span-constraint for the virtual sstable that
is due to the range. The requirement specified here implies that for
all virtual sstables in a level, the effective file bounds after
overlaying the key-span-constraint over the file bounds makes them
non-overlapping.

For example, consider the following sstables in a shared level:

```
([a, f), bound [a, b]), ([a, f), bound [c, c]), ([a, f), bound [e, h]), ([f, j), bound [e, h])
```

The effective file bounds are `[a, b], [c, c], [e, f), [f, h]`, which
are non-overlapping. Note that not all upper bound values in the
effective file bounds are exclusive.

This requirement removes the need for any inter file seqnum
comparisons for a user key in a level, which we will utilize later. We
will also properly truncate range deletes for such sstables and avoid
any complexities related to improperly truncated range deletes that
are outlined in the doc
[here](https://github.com/cockroachdb/pebble/blob/master/docs/range_deletions.md).
Which means we can cleanly consider the semantics of the data in an
sstable as purely a function of the key-span-constraint plus file
contents (which is not true for local sstables, where the file bounds
in the manifest must also be utilized).

This requirement can theoretically lead to much larger files if a
particular key has a huge number of versions. In the CockroachDB
context this is not a problem since different MVCC versions for a
single `roachpb.Key` do not use the same LSM user key, and intents
written for transactional writes also do not reuse the same LSM user
key. In any case, many versions should be rare since versions do get
collapsed by compactions except when there is an LSM snapshot
preventing the collapse. Use of long-lived LSM snapshots is
discouraged, and CockroachDB follows this best practice.

## Reads

Reads can be internal reads for compactions or reads by a client of
the DB. At a high-level both can use the usual `InternalIterator` tree
with some modifications:

- The key-span-constraint is imposed over file level iterators on
  shared sstables.

- The LSM invariant used in the current iterators is that the higher
  level must have newer data, and we keep that invariant. However,
  there are places where seqnums are compared for the same user key
  across levels or sstables within a level, for validating that this
  invariant has not been violated (which would imply data
  corruption). We omit such comparisons across levels when they
  involve any shared sstable. This is because foreign sstables do not
  follow the same sequence numbering as the LSM. Additionally, there
  is no need to compare seqnums across shared sstables in the same
  level since by definition they must have different user keys (as
  discussed in the previous section).
  - Within an sstable the seqnums do need to be compared. This
    includes the case of range deletes within a file. This is
    necessary because LSM snapshots can prevent deletion of versions
    when writing shared sstables just like in the current system.


Clients of the DB can request two additional iteration modes:
- Iteration that only involves memtables + local levels. This was used
  in a previous section when dropping a range.

- Iteration that only involves memtables + local levels that exposes
  point deletes if they are the latest version for a key, and range
  deletes if there could be any data in the shared levels that could
  be deleted by those range deletes. We will later describe how this
  is used by node N1 that is helping node N2 acquire the state for a
  range.

Time bound iteration is orthogonal to this discussion and is
unaffected.

### Manufacturing sequence numbers for reads

The full story for reads is somewhat more complicated. The
`InternalIterator` tree that contains the heart of the logic for
iteration does expose the seqnums for each point. The iterator that
is used by clients of this DB, that wraps this InternalIterator, does
not really care about the seqnums. Let us consider why these seqnums
are useful during iteration:

- LSM snapshots: seqnums are useful for reads over LSM snapshots,
  since they may want to read earlier versions of a user key.
- Concurrent memtable mutations: seqnums allow handling of concurrent
  and later memtable mutations, by preventing them from becoming
  visible to an iterator that was constructed earlier.
- Output of compactions: seqnums are written back to the system in the
  output of compactions.

We need a way to reconcile seqnums in the shared sstables that were
not written while in this LSM (foreign sstables) with what was written
in this LSM. We observe that we have 56 bits for seqnums in the
current system. In practice we use the bit corresponding to 2^55 for
batch seqnums during iteration, so only 2^55-1 seqnums are
available. We don't need more than 5 bits for the key kinds, so if we
were willing to make an incompatible change (which would not require
rewriting all existing sstables), we could expand to 59 bits for
seqnums and use the bit corresponding to 2^58 for batch seqnums,
giving us 2^57-1 maximum seqnums for writes. If the write load on the
LSM was 1 billion seqnums/sec (this extremely high number should be a
very loose upper bound), that gives us approximately 2^27 seconds
before we run out (in the current system), which is approximately 4
years.

Let us assume that this highest seqnum is N -- it may be the current
2^55-1 or expanded to 2^57-1. We split this into two segments of
seqnums [0, M) and [M, N). M is the lower bound on what a new LSM will
use when accepting writes, when it is created. So [M, N) should be
large enough to last a long time duration. [0, M) is used for
manufacturing seqnums for foreign shared sstables. Note that M is a
constant, so importing foreign sstables is changing the "history" of
the LSM -- we consider this acceptable since LSMs are not designed to
keep any history beyond what is supported by an LSM snapshot (and
we've discussed earlier the limitations on LSM snapshots due to the
shared levels).


#### Failed solution: InternalIterator is merging exactly one foreign sstable with native sstables

Let us say we keep metadata about the smallest and largest actual
seqnum in the foreign sstable. We will refer to these as [s, l]. For
now, let us assume `l-s < M`. Then for every seqnum x in this file we
can compute a manufactured seqnum as follows:
```
manufactured-seq-num(x) = M-1-(l-x)
```

It is tempting to think we can do better by simply resetting the
seqnum to M-1 when we switch from one user key to the next. This is
not generally possible because of range deletes. For example, consider
a foreign sstable with the following contents: points a#10, b#9, c#8,
..., j#1 and range deletes [a,k)#10, [a,k)#9, ..., [a,k)#2.  If we
output a#M-1, b#M-1, ..., j#M-1 we would not have viable seqnums to
assign to the range deletes which are slicing up the seqnum space in
this file.

This simple approach, in which we have not even considered multiple
foreign sstables, has a problem. The output of a compaction involving
the above manufactured seqnums for a foreign sstable and many native
sstables can have seqnums anywhere in the interval [0, N). Now if this
output sstable becomes a foreign sstable in a different LSM, the
invariant could be as weak as `l-s < N`. One way to address this would
be to maintain information about holes in the seqnum space of an
sstable and use that to adjust the computation of the
`manufactured-seq-num`, in that it can avoid wasting parts of the
manufactured space on these holes. We could maintain these holes in an
approximate and space efficient manner while incrementally writing the
file. But there is a possibility that the holes are evenly distributed
across the key space, in which case the number of holes we maintain
for an sstable could be equal to the number of keys in that sstable.

#### Solution

The following observations help us devise the solution:
- A foreign sstable can have multiple versions of the same key because
  of an LSM snapshot in the LSM where it was a native sstable.
- An iterator over a foreign sstable is not affected by any LSM
  snapshots since all such snapshots are trying to preserve some
  history for this node, while the foreign sstable represents the
  starting state for the range at this node. Specifically, snapshots
  will use a seqnum >= M, which means everything <= M-1 can be
  collapsed.

An iterator over a foreign sstable in L5 has to expose:
- at most one point version for a user key, representing what is live
  in that sstable, after applying any deletes and range deletes.
- the range deletes, since they could apply to foreign sstables in L6.

All the point keys can use seqnum M-1 and all the range deletes can
use seqnum M-2.

An iterator over a foreign sstable in L6 is a subset of the above in
that it does not expose any range deletes. The point keys can all use
seqnum M-3.

So we can use M=3, and have not stolen many seqnums from the regular
LSM, which can use up to N-3 seqnums.

## Writes, Ingestion, Flushes and Compactions

Writes go the memtable and are flushed to L0 -- there is no change.

For ingesting an sstable that is sent as bytes over the network, the
LSM will compute the lowest level it can occupy, similar to the
current logic. If it is a shared level, the bytes will be written as a
shared sstable and then ingested. Note that such sstables already
conform to the range boundaries.

### Concurrency of range splits and drops with background operations

Background operations like flushes, compaction, ingestion are not
immediately affected by range splits and drops. But when these
operations finish and are going to install a new manifest version, we
ensure consistency with the latest set of ranges. This may involve
changing the key-span-constraint on a virtual file, or splitting a
virtual file into multiple virtual files, or completely discarding a
virtual file.

## Initializing range state at different node

Consider that node N2 is asking node N1 for the state of range R. The
steps are:

- N1 uses the latest manifest version number and locally refs this
  version. This ref is time bounded. It returns to N2 the version
  number of the manifest (local to N1's LSM) and the list of shared
  sstables in N1's LSM that overlap with R. Note that these files will
  not be deleted while the time bounded ref exists. The ref also
  prevents deletion of memtables and local sstables for that version.

- N2 creates a shared-store reference for each of those shared
  sstables. Note that we make N2 create these shared-store references
  since we want every LSM to have complete knowledge of shared-store
  references it has taken, so that it can do complete cleanup except
  for the case where the node is permanently removed. This record of
  shared-store references can be maintained in the LSM manifest of N2.

- N2 sends another RPC to N1 with the manifest version number that N1
  had originally provided, and asks for the local state.

  - N1 transfers the time bounded ref to the handler for the RPC (the
    ref will be removed when the RPC finishes, successfully or is
    canceled).

  - The RPC handler creates an iterator over only the memtables and
    local sstables in the manifest version, and unrefs (since the
    iterator now hold the relevant ref).

  - The iterator is used to read the state and respond to N2. The
    state read includes the (a) latest point operation on every user
    key where the latest operation is not already subsumed by a range
    delete, (b) range deletes that could delete something in the
    shared levels. These latest point operations include SET, DEL,
    SINGLEDEL, and includes converting SINGLEDEL to DEL if the points
    below the SINGLEDEL can together be converted to a SETWITHDEL.

- N2 imports the shared sstables for which it has acquired
  shared-store references directly to the relevant shared levels, as
  foreign sstables. Note that these imported sstables are placed in L5
  and L6, like they were in N1, and not subject to level determination
  for normal ingests. Additionally, it takes the data returned in the
  RPC response, and uses it to generate one or more sstables that it
  ingests into the LSM.

Note that the current use of range delete when generating sstables at
N2 to ingest into the LSM is eliminated. We have already guaranteed
that N2 has no data for R when it dropped the range (or never had any
of it in its history). Using a range delete would incorrectly hide all
the state in the foreign sstables.

Also note that sstable generated for ingestion can have a point user
key and range delete user key that are identical. Since the range
delete has already hidden the points that are irrelevant, the point
keys and range delete keys can share the same seqnum, which will the
seqnum assigned at ingestion time. We already have logic in the LSM
that semantically applies a range delete with seqnum N only to points
with seqnum < N, so this new usage is consistent with those semantics.

## Reference counting shared sstables

There are references maintained in the shared-store on behalf of each
node's LSM. As long as the node is alive it is guaranteed to cleanup
references it no longer needs. There is periodic scanning of these
references to see if any are held by nodes no longer in the
cluster. These references can be deleted.  When the last reference to
a shared-store file is removed, the correponding file can also be
deleted. The periodic scanning will also check for files that have no
references and remove them.

Ideally, we could rely on some mechanism in the shared-store
implementation for such references. Certain blob storage systems allow
creation of file snapshots, which would allow us to embed the name of
the node that owns the snapshot (or the original file) in the file or
file snapshot name. Then the file itself serves as the reference. This
does not seem possible in S3 or GCS. Another possibility would be to
use an empty reference file with the name of the reference holder in
the reference file name. A file with n references would result in n+1
files, the original containing the data, and n reference files.

## Caching

Caching is needed to improve the availability guarantees, and to
reduce the number of operations against the shared-store which can
help control costs. Note that reads of an MVCC key need to merge
across all levels of the LSM. This is because the CockroachDB
workload, even for a single MVCC key, has to merge across different
versions of the key, which are different LSM keys. So the common case
looks like a scan (possibly with bloom filters on the non-versioned
part of the key) from the LSM's perspective. A critical assumption
made here is that parts of the key space are cold.  That is, it is
insufficient to have older versions of each key being cold, but every
part of the key space being touched by queries. Also note that we
cannot generally assume that data that was written a long time ago,
and has sunk to L6, is not the latest version. Therefore it could be
actively being read, and not just as a side-effect of the fact that
the code is written to make single MVCC key reads appear as a scan to
the LSM.

We consider two kinds of caching.

### Distributed caching of whole shared sstables

We do not want more than one copy per region. There are two aspects to
this cache:

- Which node is responsible for caching the sstable: We embed the node
  number of the original creator of the shared sstable in the sstable
  name. By default, that is the node responsible for it -- this
  eliminates a network hop in the common case of the sstable creator
  node being the one that reads the sstable. If the node is dead, or
  the node requesting the file from the cache is in a different
  region, consistent hashing using the nodes in the region is used to
  decide who is responsible for the file.

- When is the cache populated:
  - We need to have a policy that determines whether a cache miss (or
    set of cache misses) will cause the file to be added to the
    cache. We can possibly repurpose caching policies used in block
    caches. If it should be added to the cache, the caching node will
    need to read the whole file in the background and add it (since
    the request that encountered the cache miss was requesting a
    subset of the file bytes).
  - For ranges that are mandatorily cached, the cache can be populated
    at the same time as the file is being written to shared-store
    since the cache responsible for the file is the same node.

We could also consider separating the cache into a separate service if
this disaggregated shared storage architecture is only deployed in
managed CockroachDB offerings, where the complexity of having an
additional binary is offset by the ability to more easily make cache
deployment changes, or isolate cache load, without affecting the
CockroachDB cluster.

### Local cache of part of shared sstables

An LSM with shared sstables may want to cache some part of all these
sstables. For example, caching the footer can avoid repeated reading
of the location of the index block. Caching the metaindex block could
also be useful since it contains the location of the range delete and
bloom filter blocks. Note that all these blocks can be cached in
compressed form and the cache can be on-disk, since it is being used
to save a roundtrip to the shared-store. Structuring an on-disk cache
that is caching O(50KB) per entry needs some more thought. We do not
want to put these cache entries in another LSM since we do not want to
pay the cost of compacting cache entries. We could manage this cache
directly as local files, with reuse of space that is freed up, to
reduce fragmentation. The RocksDB
[persistent](https://github.com/facebook/rocksdb/tree/master/utilities/persistent_cache)
[cache](https://github-wiki-see.page/m/facebook/rocksdb/wiki/Persistent-Read-Cache)
implements a simple LRU model that evicts whole cache files that
contain many blocks -- we could begin with a simple approach like
this.

## Shared sstables and multi-region clusters

Blob storage systems are different in terms of multi-region support.
S3 buckets are single region, while GCS supports dual-region and
multi-region (within a continent).

For multi-region GCS, there is no network cost of reading in any of
the regions, while the per byte storage cost is only 30% higher. It is
simple, and likely cheap enough, for single continent, multi-region
CockroachDB to use multi-region buckets and not concern itself with
copying of data when rebalancing a replica of a range across regions.

If we are limited to single region storage and the cluster spans
multiple regions, we have two cases:
- Ranges that have replica placement preferences that typically
  prevent cross region rebalancing: in this case it is likely that a
  node receiving a replica can find another node in the same region
  whose shared files it can use.
- Cross region rebalancing of replicas: The data will need to be
  copied.

# Other use cases: backups/restores/branches

The use cases of backing up a table or branching the contents of a
table represent the logical state of a table at a particular MVCC
timestamp. What we have described earlier is all the state of a range,
including history, and is not a consistent cut at a particular MVCC
timestamp. If one wants a full backup without history, what we
describe below may represent too much extra state, since the physical
state cannot avoid including the history. Current full backups involve
a logical scan of the data, and what we outline below is potentially
cheaper in avoiding such a scan, and then making a copy.

Creating a full backup will involve the following steps:

- Pick an MVCC timestamp T that is protected (so cannot be affected by
  GC).
- Wait until the closed timestamp of a range is beyond T, so no new
  writes can happen below T.
- Wait until resolution of all intents below T. This can be done by
  cheaply scanning the separated lock table.
- Grab the state of the range in the manner described earlier: the
  shared sstables will be referenced by the backup, which prevents
  them from being deleted, and the state generated from the local
  levels (L0-L4) can be used to write backup-specific sstables.

Using the branched state and restoring from backup are similar in the
sense that the timestamps after T need to be removed. This can be done
using a non-MVCC compliant RevertRange operation that deletes all
intents, provisional values and committed values > T. The elements to
be reverted can be identified cheaply using a time-bound
iterator. Branching has one additional step: reads need to change the
key prefix to handle the different table identifier. There would be
metadata per sstable indicating whether it needs a change to the key
prefix.

The backup steps would happen on exactly one replica for each range
for backups, since the likelihood of restoring from backup is low. For
branching a multi-region table we may choose to do this once per
replica per region so as to point to shared sstables that are in the
same region.

If a range can span multiple tables and indexes, the approach
described here may not actually be cheaper than copying to contents of
a table/index. What we have outlined here is meant to work for very
large tables, which may span 100+ ranges -- if the first and last
range are not exclusive to the table they can be copied.

# Drawbacks and Alternatives

TBD, though there is some discussion already inline in the doc.

## Related Work

- Discuss Rockset and add links
  [here](https://github.com/rockset/rocksdb-cloud) and
  [here](https://rockset.com/blog/how-we-use-rocksdb-at-rockset/). Rockset
  is replicating everthing to S3, so simpler solution, though could
  incur higher costs. Rockset achieves similar goals of another node
  taking over, but does it for the entire DB, which is different from
  CockroachDB's range sharded setup where only some of the ranges in
  the shared Pebble DB may be moving to a different node.

## Alternatives


# Unresolved questions

Need cost calculations informed by actual workloads to figure out
actual cost relative to current approach.

