-   Feature Name: Raft consistency checker
-   Status: completed
-   Start Date: 2016-02-10
-   Authors: Ben Darnell, David Eisenstat, Bram Gruneir, Vivek Menezes
-   RFC PR: [#4317](https://github.com/cockroachdb/cockroach/pull/4317),
            [#8032](https://github.com/cockroachdb/cockroach/pull/8032)
-   Cockroach Issues: [#837](https://github.com/cockroachdb/cockroach/issues/837),
                      [#7739](https://github.com/cockroachdb/cockroach/issues/7739)

Summary
=======

An online consistency checker that periodically compares snapshots of
range replicas at a specific point in the Raft log. These snapshots
should be the same.

An API for direct invocation of the checker, to be used in tests and the
CLI.

Motivation
==========

Consistency! Correctness at scale.

Design
======

Each node scans continuously through its local range replicas,
periodically initiating a consistency check on ranges for which it is
currently the lease holder.

1.  The initiator of the check invokes the Raft command
    `ComputeChecksum` (in `roachpb.RequestUnion`), marking the point at
    which all replicas take a snapshot and compute its checksum.

2.  Outside of Raft, the initiator invokes `CollectChecksum` (in
    `service internal`) on the other replicas. The request message
    includes the initiator's checksum so that whenever a replica's
    checksum is inconsistent, both parties can log that fact.

3.  If the initiator discovers an inconsistency, it immediately retries
    the check with the `snapshot` option set to true. In this mode,
    inconsistent replicas include their full snapshot in their
    `CollectChecksum` response. The initiator retains its own snapshot
    long enough to log the diffs and panic (so that someone
    will notice).

Details
-------

The initiator of a consistency check chooses a UUID that relates its
`CollectChecksum` requests to its `ComputeChecksum` request
(`checksum_id`). Retried checks use a different UUID.

Replicas store information about ongoing consistency checks in a map
keyed by UUID. The entries of this map expire after some time so that
failures don't cause memory leaks.

To avoid blocking Raft, replicas handle `ComputeChecksum` requests
asynchronously via MVCC. `CollectChecksum` calls are outside of Raft and
block until the response checksum is ready. Because the channels are
separate, replicas may receive related requests out of order.

`ComputeChecksum` requests have a `version` field, which specifies the
checksum algorithm. This allows us to switch algorithms without
downtime. The current algorithm is to apply SHA-512 to all of the KV
pairs returned from `replicaDataIterator`.

If the initiator needs to retry a consistency check but finds that the
range has been split or merged, it logs an error instead.

API
---

A cockroach node will support a command through which an admin or a test
can check the consistency of all ranges for which it is a lease holder
using the same mechanism provided for the periodic consistency checker.
This will be used in all acceptance tests.

Later if needed it will be useful to support a CLI command for an admin
to run consistency checks over a section of the KV map: e.g.,
\[roachpb.KeyMin, roachpb.KeyMax). Since the underlying ranges within a
specified KV section of the map can change while consistency is being
checked, this command will be implemented through kv.DistSender to allow
command retries in the event of range splits/merges.

Failure scenarios
-----------------

If the initiator of a consistency check dies, the check dies with it.
This is acceptable because future range lease holders will initiate new
checks. Replicas that compute a checksum anyway store it until it
expires.

It doesn't matter whether the initiator remains the range lease holder.
The reason that the lease holder initiates is to avoid concurrent
consistency checks on the same range, but there is no correctness issue.

Replicas that die cause their `CollectChecksum` call to time out. The
initiator logs the error and moves on. Replicas that restart without
replaying the `ComputeChecksum` command also cause `CollectChecksum` to
time out, since they have no record of the consistency check. Replicas
that do replay the command are fine.

Drawbacks
=========

There could be some performance drawbacks of periodically computing the
checksum. We eliminate them by running the consistency checks
infrequently (once a day), and by spacing them out in time for different
ranges.

A bug in the consistency checker can spring false alerts.

Alternatives
============

1.  A consistency checker that runs offline, or only in tests.

2.  An online consistency checker that collects checksums from all the
    replicas, computes the majority agreed upon checksum, and supplies
    it down to the replicas. While this could be a better solution, we
    feel that we cannot depend on a majority vote because new replicas
    brought up with a bad lease holder supplying them with a snapshot
    would agree with the bad lease holder, resulting in a bad
    majority vote. This method is slightly more complex and does not
    necessarily improve upon the current design.

3.  A protocol where the initiator gets the diff of an inconsistent
    replica on the first pass. The performance cost of retaining
    snapshot engines is unknown, so we'd rather complicate the
    implementation of the consistency checker.

Unresolved questions
====================

None.
