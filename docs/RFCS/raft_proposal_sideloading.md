- Feature Name: Raft Proposal Sideloading
- Status: draft/in-progress/completed/rejected/obsolete
- Start Date: 2017-05-24
- Authors: Dan Harrison and Tobias Schottdorf, original suggestion Ben Darnell
- RFC PR: #16159
- Cockroach Issue: TBD

# Summary and non-goals

Allow proposers to tag Raft proposals as sideloadable to avoid the write
amplification associated with storing them in the Raft log, and to allow RESTORE
to directly link SSTables into the LSM. Proposals tagged as such are stored
directly on the file system (i.e. not in the Raft log), with only metadata in
the Raft log. This happens transparently on each node, i.e. the proposal is sent
over the wire. This proposal special-cases for the ingestion of SSTables, but is
presented in some generality to apply also to large proposals created otherwise,
though this generality is only maintained for as long as it is convenient, and
the actual implementation may be specialized to apply to SSTable ingestion only.

It is explicitly a non-goal to deal with (overly) large proposals, as none of
the Raft interfaces expect proposals of nontrivial size. It is not expected that
sideloadable proposals will exceed a few MB in size.

# Motivation

`RESTORE` needs a fast mechanism to ingest data into a Range. It wants to rely
on Raft for simplicity, but the naive approach of sending a (chunked)
`WriteBatch` has downsides:

1. all of the data is written to the Raft log, where it incurs a large write
   amplification, and
1. applying a large `WriteBatch` to RocksDB incurs another sizable write
   amplification factor: it is far better to link SSTables directly into the
   LSM.

Instead, `RESTORE` sends small (initially ~2MB) SSTables which are to be linked
directly into RocksDB. This eliminates the latter point above. However, it does
not address the former, and this is what this RFC is about: achieving the
optimal write amplification of 1 or 2 (the latter amounting to dodging some
technical difficulties discussed at the end).

Note also that housing the Raft log outside of RocksDB only addresses the former
point if it results in a SSTable being stored inside of the RocksDB directory,
which is likely to be a non-goal of that project.

## Detailed design

The mechanism proposed here is the following:

1. `storagebase.RaftCommand` gains fields `SideloadedSha512 []byte` (on
   `ReplicatedEvalResult`) and `SideloadedData []byte` (next to `WriteBatch`)
   which are unused for regular Raft proposals.
1. When a proposal is to be sideloaded, a regular proposal is generated, but the
   sideloaded data and its hash populated. In the concrete case of `SSTable`
   this means that `evalIngestSSTable` creates an essentially empty proposal
   (perhaps accounting for an MVCC stats delta).
1.  On its way into Raft, the
   `SideloadedData` bytes are then written to disk and stripped from the
   proposal. On disk, the payload is stored in the `Store`'s directory,
   accessible via its log index, using the following scheme (for an explanation
   on why this scheme was chosen, see later sections):

   ```
   <storeDir>/sideloaded_proposals/<rangeID>.<replicaID>/<logIndex>.<term>
   ```

   where the file contains the raw sideloaded data, e.g. an `SSTable` that will
   populate `(storagebase.RaftCommand).SideloadedData`.
1. Whenever the content of a sideloaded proposal is required, a `raftpb.Entry`
   is reconstituted from the corresponding file, while verifying the checksum in
   `SideloadedSha512`. A failure of either operation is treated as fatal (a
   `ReplicaCorruptionError`). Note that restoring an entry is expensive: load
   the payload from disk, unmarshal the `RaftCommand`, compute the checksum and
   compare it to that stored in `RaftCommand`, put the payload into the
   `RaftCommand`, marshal the `RaftCommand`. The Raft entries cache should help
   mitigate this cost, though we should watch the overhead closely to check
   whether we can perhaps eagerly populate the cache when proposing.
1. When such an entry is sent to the follower over the network
   (`sendRaftMessage`), the original proposal is sent (via the mechanism above).
   Note that this could happen via `append` or via the snapshotting mechanism
   (`sendSnapshot` or rather its child `iterateEntries`), which sends a part of
   the Raft log along with the snapshot.
1. Apply-time side effects have access to the payload, and can make use of it
   under the contract that they do not alter the on-disk file. For the SSTable
   use case, when a sideloaded entry is applied to the state machine, the
   corresponding file is hard-linked into its RocksDB directory and, from there,
   ingested by RocksDB. That way, even if RocksDB decided to remove the file, it
   would still be available to Raft due to the way hardlinks work.

## Determining whether a `raftpb.Entry` is sideloaded

The principal difficulty is sniffing sideloadable proposals from a
`(raftpb.Entry).Data` byte slice. This is necessary since sideloading happens at
that fairly low level, and unmarshalling every single message squarely in the
hot write path is clearly not an option. `(raftpb.Entry).Data` equals
`encodeRaftCommand(p.idKey, data)`, where `data` is a marshaled
`storagebase.RaftCommand`, resulting in

```
(raftpb.Entry).Data = append(raftCommandEncodingVersion, idKey, data)
```

We can't hide information in `data` as we would have to unmarshal it too often,
and so we make the idiomatic choice: Sideloaded Raft proposals are sent using a
new `raftCommandEncodingVersion`. See the next section for migrations.

Armed with that, `(*Replica).append` and the other methods above can assure that
there is no externally visible change to the way Raft proposals are handled, and
we can drop sideloaded proposals directly on disk, where they are accessible via
their log index.

TODO: use cmdID instead? Or can we do something else entirely?

## Migration story

The feature will initially only be used for SSTable ingestion, which remains
behind a feature flag. An upgrade is carried out as follows:

1. rolling cluster restart to new version
1. enable the feature flag
1. restore now uses SSTable ingestion

A downgrade is slightly less stable but should work out OK in practice:

1. disable the feature flag
1. wait until it's clear that no SSTable ingestions are active anywhere (i.e.
   any such proposals have been applied by all replicas affected by them)
1. rolling cluster restart to old version.
1. in the unlikely event of a node crashing, upgrade that node again and back to
   the first step

Due to the feature flag, rolling updates are possible as usual.

## Details on file creation and removal

There are three procedures that need to mutate sideloaded files. These are, in
increasing difficulty, replica GC, log truncation, and `append()`. The main
objectives here are making sure that we move the bulk of disk I/O outside of
critical locks, and that all files are eventually cleaned up.

### Replica GC

When a Replica is garbage collected, we know that if it is ever recreated, then
it will be at a higher `ReplicaID`. For that reason, the sideloaded proposals
are disambiguated by `ReplicaID`; we can postpone cleanup of these files until
we don't hold any locks. During node startup, we check for sideloaded proposals
that do not correspond to a Replica of their Store and remove them.

Concretely,

- after GC'ing the Replica, Replica GC wholesale removes the directory
  `<storeDir>/sideloaded_proposals/<rangeID>.<replicaID>` after releasing all
  locks.
- on node startup, after all Replicas have been instantiated but before
  receiving traffic, delete those directories which do not correspond to a live
  replica. Assert that there is no directory for a replicaID larger than what we
  have instantiated.

### Log truncation

Similarly to Replica GC, once the log is truncated to some new first index `i`,
we know that no older sideloaded data is ever going to be loaded again, and we
can lazily and without any locks unlink all files for which `term <=
currentTerm` *or* `index < i`.

### append()

This is the interesting one. `append()` is called by Raft when it wants us to
store new entries into the log.

Once we commit the corresponding RocksDB batch, any sideloaded payloads must be
on disk as well, or an ill-timed crash would lead to log entries which are
acknowledged but have no payload associated to them, a situation impossible to
recover from (OK, not impossible since we crash before sending a message out to
the lease holder - we could remove the message from the log again at node
startup, but it's a bad idea). So we have to make sure that all sideloaded
payloads are written to disk before the batch passed to `append()` is committed,
and that obsolete payloads are removed *after*.

Initially, we will write the files directly in `append()` which means they are
all on disk when the batch commits, but they are written under the `raftMu` and
`replicaMu` locks, which is not ideal. However, it should be relatively easy to
optimize it by eagerly creating the files much earlier outside of the lock; this
is made possible since we disambiguate by term, and once a payload for a given
index and term has been written, it will not be changed in that term.

An interesting subcase arises when Raft wants us to **replace** our tail of the
log, which would then have a term bump associated with it. In particular, we may
need to replace a sideloaded entry with a different higher-term sideloaded
entry. Thanks to disambiguation by term, both payloads can exist side by side,
and we can write first the higher-term payload and then remove the replaced once.

We must tolerate a file with a higher term than we know existing, though its
existence should be short-lived as our Replica should learn about that higher
term shortly.

In summary, what we do is:

- write the new payloads as early as possible; initially in `append()` but
  theoretically before acquiring any locks.
- tolerate existing files - they are identical (as a `(term,index)` can be
  assigned only one log entry). Note how this encourages the write-early
  optimization.
- remove outdated payloads as late as possible; initially after `append()`'s
  batch commits, later after releasing any locks.

## Details on reconstituting a `raftpb.Entry`

Whenever a `raftpb.Entry` is required for transmission to a follower, we have to
reconstitute it (i.e. inline the payload). This is straightforward, though a bit
expensive.

- check the RaftCommandVersion (can be sniffed from the `Data` slice); if it's
  not a sideloaded entry, do nothing. Otherwise:
- decode the command, unmarshal the contained `cmd storagebase.RaftCommand`
- load the on-disk payload into `cmd.SideloadedData` (term, replicaID and log
  index are known at this point) and compare its sha512 with
  `cmd.SideloadedSha`, failing on mismatch.
- marshal and encode the command into a new `raftpb.Entry`, using the new raft
  command version only if the SSTable ingestion feature flag is active (old
  version otherwise).

# Drawbacks

There is some overlap with the project to move the Raft log out of RocksDB.
However, the end goal of that is not necessarily compatible with this RFC, and
the changes proposed here are agnostic of changes in the Raft log backend.

# Unresolved questions

## Investigate: Could RocksDB alter the file?

If we are aiming high and want 1x write amplification, then we do not want to
copy the file to RocksDB; we want to hard-link it there. However, RocksDB may
*alter* the SSTable. One case in which it does this is that it may set an
internal sequence number used for RocksDB transactions; this happens when there
are open RocksDB snapshots, for example.

Knowing that this can happen, we can avoid it: the SSTable is always created
with a zero sequence number, and we can ignore any updated on-disk sequence
number when reading the file from disk for treating it as a log entry.

However, we need to be very sure that RocksDB will not perform other
modifications to the file that could trip the consistency checker.

## Complications of using hard links

The design above uses hard linking for SSTable ingestion for minimal write
amplification. Hard links may not be supported across the board (think:
virtualized environments), and we crucially rely on the fact that a file is only
deleted after all referencing hardlinks have been. In these situations, an extra
copy can be made; this will be exposed as either a hidden knob or cluster
setting.

## Usefulness of generalization

Sideloading could be useful for bulk INSERTs (non-RESTORE data loading) and
DeleteRange, or more generally any proposal that's large enough to profit from
reduced write amplification compared to what we have today. However, moving the
raft log out of rocksdb likely already addresses that suitably.

## Deleting sideloaded files when tail of log is replaced

The unstable part of the Raft log may be overwritten. This means that an entry
that already holds a sideloaded file may be overwritten with another entry
(which in turn may or may not be sideloaded). This means that either we must be
able to overwrite files on disk, or we must make sure that whenever `append()`
overwrites an existing part of the log, any associated on-disk payloads are
removed.

Either option seems feasible.

[1]: https://github.com/cockroachdb/cockroach/pull/9459/files#diff-2967750a9f426e20041d924947ff1d46R707
