- Feature Name: Raft consistency checker
- Status: draft
- Start Date: 2016-02-10
- Authors: Ben Darnell, Bram Gruneir, Vivek Menezes
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #837

# Summary

An online system that periodically collects a SHA on the range replicas to
check for consistency; a snapshot for a range at a specific point in the raft log
should have the same SHA on all replicas. An API to be used for direct
invocation of the checker, to be used in finding consistency problems in tests
and CLI, will also be developed and used in the acceptance tests.

# Motivation

Consistency! Correctness at scale.

# Detailed design

The proposal is for each range replica leader to periodically collect a SHA of
each replica to keep track of consistency problems and report them. The
consistency checker runs in three phases:

1. All replicas agree on the snapshot at which a SHA is to be computed.
2. A SHA is computed on all replicas.
3. The leader collects the SHA from all replicas and reports an error if all
SHAs do not match.

The above is implemented by introducing two raft commands in
roachpb.RequestUnion:

1. ComputeChecksum: A replica on receiving this command will reply
immediately and begin computing the SHA asynchronously for the current
snapshot. It will continue executing commands while it computes the SHA
concurrently. This command will contain a UUID under which the result is to be
made available for subsequent querying via CollectChecksum below. A version
number is also supplied to identify the method used in computing the SHA, and
the version number is checked against the receivers  version and an error
returned on mismatch. The computed SHA is stored in a map by UUID, and reported
back to the leader through the next command.
2. CollectChecksum: The leader collects the SHA via this command from all
replicas, citing the UUID identifing the ComputeChecksum command snapshot. The
leader waits for a short delay (minutes after sending ComputeChecksum) before
sending a CollectChecksum to all replicas, to give the replicas plenty
of time to compute the SHA. If a replica has not computed the SHA it returns
a NOT_COMPUTED status. In tests this command will block until the checksum is
computed. If this command never arrives the computed SHA is deleted via a 30
minute timeout.

The periodic consistency checker is run within a scanner that scans through all
the local replicas and runs the consistency checker for all leader replicas.
The scanner runs in a continuous loop with equal time spacing between
replicas such that a single iteration spans the entire periodicity interval. It
will log an error for all consistency problems seen.

The SHA over a range replica is computed over all the KV pairs using
replicaDataIterator, ignoring the raft HardState and any log entries with index
greater than the applied index of the ComputeChecksum command. If the replica is
split or merged before CollectChecksum is received, the replica will return a
RANGE_CHANGED error.

Exposing consistency checker through an API for direct invocation:

A cockroach node will support a command through which an admin or a test can
check the consistency of all ranges for which it is a leader using the same
mechanism provided for the periodic consistency checker. This will be used
in all acceptance tests.

Later if needed it will be useful to support a CLI command for an admin to run
consistency checks over a section of the KV map: e.g., [roachpb.KeyMin,
roachpb.KeyMax). Since the underlying ranges within a specified KV section of
the map can change while consistency is being checked, this command will be
implemented through kv.DistSender to allow command retries in the event of
range splits/merges.

Noteworthy scenarios:

1. The leader dies between ComputeChecksum and CollectChecksum: The replicas,
including the new leader, will continue computing the SHA. The new leader will
not send a CollectChecksum command.
2. The leadership change occurs between ComputeChecksum and CollectChecksum:
Same as 1.
3. The leader dies and is restored (still as leader) between ComputeChecksum
and CollectChecksum: The restored leader doesn’t compute the SHA, and the
replicas never receive CollectChecksum.
4. The leader dies after sending the CollectChecksum: The new leader might
replay the CollectChecksum, with each replica reporting its SHA.
5. A replica dies after receiving ComputeChecksum and receives the
CollectChecksum later when it comes back up: Since it died it will not have the
computed SHA, and will reply with a NOT_COMPUTED status.
6. A replica dies after receiving ComputeChecksum and never receives the
CollectChecksum: Nothing.
7. A new replica is added between ComputeChecksum and CollectChecksum: The new
replica receives the CollectChecksum and returns NOT_COMPUTED.
8. A replica is removed between ComputeChecksum and CollectChecksum: Nothing.

# Drawbacks

There could be some performance drawbacks of periodically computing the
checksum. We eliminate them by running the consistency checks infrequently
(once a day), and by spacing them out in time for different ranges.

A bug in the consistency checker can spring false alerts.

# Alternatives

1. A consistency checker that runs offline, or only in tests.

2. An online consistency checker that collects checksums from all the replicas,
computes the majority agreed upon checksum, and supplies it down to the
replicas. While this could be a better solution, we feel that we cannot depend
on a majority vote because new replicas brought up with a bad leader supplying
them with a snapshot would agree with the bad leader, resulting in a bad
majority vote. This method is slightly more complex and does not necessarily
improve upon the current design.

# Unresolved questions

None.
