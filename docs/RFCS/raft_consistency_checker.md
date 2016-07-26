-   Feature Name: Raft consistency checker
-   Status: draft
-   Start Date: 2016-02-10
-   Authors: Ben Darnell, David Eisenstat, Bram Gruneir, Vivek Menezes
-   RFC PR: (PR \# after acceptance of initial draft)
-   Cockroach Issue: \#837

Summary
=======

An online system that periodically compares snapshots of range replicas
at a specific point in the Raft log. These snapshots should be the same.

An API to be used for direct invocation of the checker, to be used in
finding consistency problems in tests and CLI, will also be developed
and used in the acceptance tests.

Motivation
==========

Consistency! Correctness at scale.

Detailed design
===============

Each node loops continuously through its local replicas, requesting a
consistency check on the next replica every 10 seconds.

1.  The requester invokes the Raft command `ComputeChecksum` (in
    `roachpb.RequestUnion`), marking the point at which all replicas
    take a snapshot and compute its checksum.
2.  The lease holder invokes `CollectChecksum` (in `service internal`)
    on the other replicas, thereby exchanging checksums. If there is an
    inconsistency, both parties log it.
3.  (optional, flag-controlled) Inconsistent replicas attach their
    snapshot to their `CollectChecksum` response. The lease holder logs
    the differently valued keys.

Each consistency check has a UUID chosen by the requester. All related
requests have this UUID in the `checksum_id` field.

On receiving a `ComputeChecksum` request, replicas reply to Raft
immediately and begin computing their checksum asynchronously. Replicas
other than the lease holder store uncollected checksums in a local map
whose entries expire after 30 minutes.

`CollectChecksum` requests are blocking in tests (to avoid flakes) and
nonblocking in production. Before the lease holder makes these requests,
it sleeps for as long as it took to compute its checksum in order to
give the other replicas a chance to finish.

`ComputeChecksum` requests have a `version` field, which specifies the
checksum algorithm. This allows us to switch algorithms without
downtime. The current algorithm is to apply SHA-512 to all of the KV
pairs returned from `replicaDataIterator`, ignoring the Raft `HardState`
and any log entries with index greater than the applied index of the
`ComputeChecksum` command. If the replica is split or merged before
`CollectChecksum` is received, the replica returns a `RANGE_CHANGED`
error.

Exposing consistency checker through an API for direct invocation:

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

Noteworthy scenarios:

1.  The lease holder dies between ComputeChecksum and CollectChecksum:
    The replicas, including the new lease holder, will continue
    computing the SHA. The new lease holder will not send a
    CollectChecksum command.
2.  The lease holdership change occurs between ComputeChecksum and
    CollectChecksum: Same as 1.
3.  The lease holder dies and returns (still with the lease) between
    ComputeChecksum and CollectChecksum: The restored lease holder
    doesn’t compute the SHA, and the replicas never
    receive CollectChecksum.
4.  The lease holder dies after sending the CollectChecksum: The new
    lease holder might replay the CollectChecksum, with each replica
    reporting its SHA.
5.  A replica dies after receiving ComputeChecksum and receives the
    CollectChecksum later when it comes back up: Since it died it will
    not have the computed SHA, and will reply with a
    NOT\_COMPUTED status.
6.  A replica dies after receiving ComputeChecksum and never receives
    the CollectChecksum: Nothing.
7.  A new replica is added between ComputeChecksum and CollectChecksum:
    The new replica receives the CollectChecksum and
    returns NOT\_COMPUTED.
8.  A replica is removed between ComputeChecksum and
    CollectChecksum: Nothing.

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

Unresolved questions
====================

None.
