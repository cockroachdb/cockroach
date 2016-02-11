- Feature Name: Raft consistency checker
- Status: draft
- Start Date: 2016-02-10
- Authors: Ben Darnell, Bram Gruneir, Vivek Menezes
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #837

# Summary

An online system that periodically checks that range replicas
are consistent; a snapshot for a range at a specific point in the raft log
has the same SHA on all replicas.

# Motivation

Consistency! Correctness at scale.

# Detailed design

The proposal is for each range replica leader to periodically run the
consistency checker. The consistency checker runs in three phases:

1. All replicas agree on the snapshot at which a SHA is to be computed.
2. A SHA is computed on all replicas.
3. The leader sends its SHA to all replicas; any replica seeing an inconsistent
SHA will report an error and euthanize itself (configurable).

The above is implemented by introducing two raft commands in
roachpb.RequestUnion:

1. ChecksumCompute: A replica on receiving this command will reply
immediately, and asynchronously compute the SHA for the current snapshot. It
will continue executing commands while it computes the SHA on the side. 
2. ChecksumVerify: The leader will supply its SHA for the latest
ChecksumCompute snapshot via this command to all replicas. A replica on
receiving this command will reply immediately and handoff the checksum to the
goroutine computing the latest checksum.

The periodic consistency checker is run within a scanner that scans through all
the local replicas and runs the consistency checker for all leader replicas.
The scanner runs in a continuous loop with equal time spacing between
replicas such that a single iteration spans the entire periodicity interval.

The goroutine computing the checksum on all non-leader replicas will accept
a checksum supplied through ChecksumVerify. On not hearing from the leader
for a long time (10 minutes), it will terminate. A new leader might trigger the
creation of a checksum computing goroutine by sending a ChecksumCompute while
an existing goroutine created by a previous leader exists. This is safe because
the previous goroutine will simply timeout.

Noteworthy scenarios:

1. The leader dies between ChecksumCompute and ChecksumVerify: The replicas,
including the new leader, will continue computing the SHA. The new leader will
not send a ChecksumVerify command with the SHA and the goroutines on the
replicas will timeout and log a warning.
2. The leadership change occurs between ChecksumCompute and ChecksumVerify:
Same as 1.
3. The leader dies and is restored (still as leader) between ChecksumCompute
and ChecksumVerify: The restored leader doesn’t compute the SHA, and the
replicas never receive ChecksumVerify.
4. The leader dies after sending the ChecksumVerify: Replicas will continue
computing the SHA, and will compared it against the one supplied. If the
replica itself has been elected leader and has noted a different SHA, it will
log an alert but will stubbornly remain alive.
5. A replica dies after receiving ChecksumCompute and receives the
ChecksumVerify later when it comes back up: Since it died it will not have the
computed SHA, and will ignore this message.
6. A replica dies after receiving ChecksumCompute and never receives the
ChecksumVerify: Nothing.
7. A new replica is added between ChecksumCompute and ChecksumVerify: The new
replica receives the ChecksumVerify and ignores it.
8. A replica is removed between ChecksumCompute and ChecksumVerify: Nothing.

# Drawbacks

There could be some performance drawbacks of periodically computing the
checksum. We eliminate them by running the consistency checks infrequently
(once a day), and by spacing them out in time for different ranges.

# Alternatives

1. A consistency checker that runs offline, or only in tests.

2. An online consistency checker that collects checksums from all the replicas,
computes the majority agreed upon checksum, and supplies it down to the
replicas. While this could be a better solution, we feel that we cannot depend
on a majority vote because new replicas brought up with a bad leader supplying
them with a snapshot would agree with the bad leader, resulting in a bad
majority vote. This method is slightly more complex and doesn't necessarily
improve upon the current design.

# Unresolved questions

None.
