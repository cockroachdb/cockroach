- Feature Name: Raft consistency checker
- Status: draft
- Start Date: 2016-02-10
- Authors: Ben Darnell, Bram Grunier, Vivek Menezes
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #837

# Summary

An online system that checks that periodically checks that range replicas
are consistent; a snapshot for a range at a specific point in the raft log
 has the same sha on all replicas.

# Motivation

Consistency! Correctness at scale.

# Detailed design

The proposal is for each range replica leader to periodically run the
consistency checker. The consistency checker runs in three phases:

1. All replicas agree on the snapshot at which a sha is to be computed.
2. A sha is computed on all replicas.
3. The leader shares its sha with all replicas; a replica computing a
different sha from the one supplied by the leader alerts and eutanizes
the node.

The above is implemented by introducing two raft commands:

1. CHECK_CONSISTENCY_BEGIN (BEGIN): A replica on receiving this command
immediately acknowledges it has executed it, and asynchronously computes the sha
for the current snapshot. It continues executing commands while it computes the
sha on the side. Once the sha is computed it compares the BEGIN command id along
with the computed sha to the one received from the leader in the second command
below.
2. CHECK_CONSISTENCY_CHECKSUM (CHECKSUM): This command contains the id for the
BEGIN command it is referring to, and the sha that it has computed at the BEGIN
command snapshot on the leader. On receiving this command the replica saves the
command id and sha supplied, and compares the sha against the one it eventually
computes. Under the rare circumstances that it sees a difference, it will alert
and euthanize the node.

Each range replica has the ability to keep track of multiple sha computations
that are being computed or recently computed. The shas get GC-ed after about
30 minutes.

The periodic consistency checker is run within a scanner that scans through all
the local replicas and runs the consistency checker for all leader replicas.
The scanner runs in a continuous loop with equal time spacing between
replicas such that it spans the entire periodicity interval.

Noteworthy scenarios:

1. The leader dies between BEGIN and CHECKSUM: The replicas, including the new
leader, will continue computing the sha. The new leader will send a CHECKSUM
command with the sha.
2. The leadership change occurs between BEGIN and CHECKSUM: same as 1
3. The leader dies and is restored (still as leader) between BEGIN and CHECK: The
new leader doesn’t compute the sha, and the replicas never receive the sha and
eventually GC their sha.
4. The leader dies after sending the CHECKSUM: Replicas will continue computing
the sha, and will compared it against the one supplied. If the replica
itself has been elected leader and has noted a different sha, it will log an
alert but will stubbornly remain alive.
5. A replica dies after receiving BEGIN and receives the CHECKSUM later when it
comes back up: Since it died it will not have the computed sha, and will ignore
the CHECKSUM command. This behavior is unaffected by whether the replica is
being restored from a snapshot or not.
6. A replica dies after receiving BEGIN and never receives the CHECKSUM:
Nothing.
7. A new replica is added between BEGIN and CHECKSUM: The new replica receives
the CHECKSUM and ignores it.
8. A replica is removed between BEGIN and CHECK: Nothing.

# Drawbacks

None.

# Alternatives

A consistency checker that runs offline, or only in tests.

# Unresolved questions

None.
