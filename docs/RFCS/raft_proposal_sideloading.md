- Feature Name: Raft Proposal Sideloading
- Status: draft/in-progress/completed/rejected/obsolete
- Start Date: 2017-05-24
- Authors: Dan Harrison and Tobias Schottdorf, original suggestion Ben Darnell
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary and non-goals

Allow proposers to tag Raft proposals as sideloadable to avoid the write
amplification associated with storing them in the Raft log, and to allow RESTORE
to directly link SSTables into the LSM. Proposals tagged as such are stored
directly on the file system (i.e. not in the Raft log), with only metadata in
the Raft log. This happens transparently on each node, i.e. the proposal is sent
over the wire. This proposal special-cases for the ingestion of SSTables. If
there is potential for different use cases, these should be discussed with this
RFC; the mechanism should then be generalized from the start. This seems
feasible but has been omitted for clarity.

Two alternatives are presented here. The simpler one, discussed in the
alternatives section, is a simple reduction of the somewhat more involved one,
discussed at length, and incurs an extra copy of the file.

It is explicitly a non-goal to deal with (overly) large proposals, as none of
the Raft interfaces expect proposals of nontrivial size. It is not expected that
sideloadable proposals will exceed a few MB in size.

# Motivation

`RESTORE` needs a fast mechanism to ingest data into a Range. It wants to rely
on Raft for simplicity, but the naive approach of sending a (chunked)
`WriteBatch` has downsides:

1. all of the data is written to the Raft log, where it incurs a large write
   amplification, and
1. applying `WriteBatch`es to RocksDB incurs another sizable write amplification
   factor: it is far better to link SSTables directly into the LSM.

Instead, `RESTORE` sends small (initially ~2MB) SSTables which are to be linked
directly into RocksDB. This eliminates the latter point above. However, it does
not address the former, and this is what this RFC is about: achieving the
optimal write amplification of 1, with the simpler alternative discussed at the
end incurring an extra copy, for a write amplification of 2.

Note also that housing the Raft log outside of RocksDB only addresses the former
point if it results in a SSTable being stored inside of the RocksDB directory,
which is likely to be a non-goal of that project.

The proposal (original suggested by @bdarnell) is to special-case the
proposals containing SSTables for ingestion:

1. When they are put into the Raft log (`append()`), what's actually put in the
   Raft log is a small piece of metadata, and
1. the actual payload is stored to the `Store`'s temp dir, accessible via its
   log index. More precisely, we likely actually want to store a few files: at
   least the payload and a checksum.
1. When such an entry is sent to the follower over the network
   (`sendRaftMessage`), the original proposal is sent (that is, the data
   inlined). Note that this could happen via `append` or via the snapshotting
   mechanism (`sendSnapshot` or rather its child `iterateEntries`), which sends
   a part of the Raft log along with the snapshot.
1. When a sideloaded entry is applied to the state machine, the corresponding
   file is hard-linked into its RocksDB directory and, from there, ingested
   by RocksDB. That way, even if RocksDB decided to remove the file, it would
   still be available to Raft due to the way hardlinks work.
   TODO: windows and other systems? Would we need to special-case anything?
   We could fall-back to the simpler proposal in those cases and simply copy.
1. When Raft log truncation runs and truncates the log up to and including index
   `i`, then any sideloaded files for such indexes in the temp dir (all of which
   must at that point be hardlinked into RocksDB already) are unlinked.
1. The sideloaded data must be included in checksumming, consistency checks,
   etc.

# Detailed design

The principal difficulty is sniffing sideloadable proposals from a
`(raftpb.Entry).Bytes` byte slice. This is necessary since sideloading happens
at that fairly low level, and unmarshalling every single message squarely in the
hot write path is clearly not an option. Instead, we make use of the fact that
`(raftpb.Entry).Bytes` equals `encodeRaftCommand(p.idKey, data)`, which
essentially prepends the data slice with a version byte and the idKey.

We propose to encode the information that this proposal should be sideloaded in
the version byte. Naively, this breaks migrations: while decoding Raft commands,
the version byte is checked and on mismatch, a panic ensues. There are two ways
out:

1. We don't provide a migration path. A different version is only sent for
   sideloadable proposals, which implies use of BACKUP-RESTORE, and we simply
   mandate that no such commands may be issued on a mixed cluster (or in
   temporal proximity to a rolling upgrade). However, with a little trickery we
   can do better:
1. For historical reasons, we already ignore the 7th bit of the Raft command
   version[1] when decoding it. Thus, if we repurpose that bit, we can even run
   BACKUP-RESTORE on mixed clusters (though obviously old versions that don't
   support the respective Raft command would still fail, so it's not generally a
   great idea).

Either way, we propose to flag sideloadable commands in this manner. Armed with
that, `(*Replica).append` and the other methods above can assure that there is
no externally visible change to the way Raft proposals are handled, and we can
drop sideloaded proposals directly on disk, where they are accessible via their
log index.

Next, when applying a sideloaded proposal, we synthesize a side effect which
recovers the file's location from the log position, and performs a suitable
action, which must be inferred from the file. If the feature is restricted to
handle the SSTable ingestion required for `RESTORE`, it will simply treat the
file as a SSTable, hard link it into the RocksDB directory, and ingest it there.

That leaves describing how SSTable ingestion commands are proposed: They will
arrive at the Replica in a regular command, with the SSTable in a byte slice.
Since we envision the payload to be *only* the SSTable (i.e. not the marshalled
command containing that table), we must special-case this command.

To this end, we add a new `SSTable []byte` field to `ProposalData` which is read
in `defaultSubmitProposalLocked` and, if set, triggers the special handling.

TODO: when applying the chunks one after another, the range isn't in a valid
state. For example, MVCCStats don't match up, etc. Maybe ok? Could set
`MVCCStats.ContainsEstimates=true`. Alternative: let last SSTable pull the
trigger, link them at the same time. More complex - what if trigger never shows
up? When are these files GC'ed? Can no more be tied to truncation. Probably
non-issue - if backup fails, range never "used". Could some queue pick it up and
panic, though? Or we could compute MVCCStats on the side and send for each
chunk. Either way, none of these problems are new with this proposal.

# Drawbacks

There is some overlap with the project to move the Raft log out of RocksDB.
However, the end goal of that is not necessarily compatible with this RFC, and
the changes proposed here are agnostic of changes in the Raft log backend.

# Alternatives

The alternative is not linking the sideloaded SSTable into RocksDB; instead,
it is copied there wholesale. The only difference is that we don't have to
worry about the feasability of hard linking as much, and we also avoid the
problem (discussed below) of RocksDB mutating the file.

On the downside, the write amplification is now 2x.

# Unresolved questions

## RocksDB may alter the file

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

They may not be supported across the board, and we crucially rely on the fact
that a file is only deleted after all referencing hardlinks have been. However,
it seems that once we detect these systems reliably (conservatively), we can
always fall back to making an extra copy instead of a hard link.

## Useful generalization?

If multiple use of sideloading is envisioned, additional information has to be
hidden in the file. For example, sideloadable payloads could be cut into sevaral
parts, one of which dictates what kind of payload this is. It's probably best to
think hard about whether there's any other use for this; probably not, not even
for snapshots.


[1]: https://github.com/cockroachdb/cockroach/pull/9459/files#diff-2967750a9f426e20041d924947ff1d46R707
