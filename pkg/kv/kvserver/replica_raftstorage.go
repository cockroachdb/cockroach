// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/redact"
)

var snapshotIngestAsWriteThreshold = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot.ingest_as_write_threshold",
	"size below which a range snapshot ingestion will be performed as a normal write",
	metamorphic.ConstantWithTestChoice[int64](
		"kv.snapshot.ingest_as_write_threshold",
		100<<10, /* default value is 100KiB */
		1<<30,   /* 1GiB causes everything to be a normal write */
		0 /* 0B causes everything to be an ingest */),
)

// replicaRaftStorage implements the raft.Storage interface.
//
// All mutating calls to raft.RawNode require that r.mu is held. All read-only
// calls to raft.RawNode require that r.mu is held at least for reads.
//
// All methods implementing raft.Storage are called from within, or on behalf of
// a RawNode. When called from within RawNode, r.mu is held necessarily (and
// maybe r.raftMu). Conceptually, r.mu only needs to be locked for reading, but
// implementation details may require an exclusive lock (see method comments).
// When called from outside RawNode (on behalf of a RawNode "snapshot"), the
// caller must hold r.raftMu and/or r.mu.
//
// RawNode has the in-memory "unstable" state which services most of its needs.
// Most RawNode.Step updates are completed in memory, while only holding r.mu.
//
// RawNode falls back to reading from Storage when it does not have the needed
// state in memory. For example, the leader may need to read log entries from
// storage to construct a log append request for a follower, or a follower may
// need to interact with its storage upon receiving such a request to check
// whether the appended log slice is consistent with raft rules.
//
// (1) RawNode guarantees that everything it reads from Storage has no in-flight
// writes. Raft always reads state that it knows to be stable (meaning it does
// not have pending writes) and, in some cases, also synced / durable. Storage
// acknowledges completed writes / syncs back to RawNode, under r.mu, so that
// RawNode can correctly implement this guarantee.
//
// (2) The content of raft.Storage is always mutated while holding r.raftMu,
// which is an un-contended "IO" mutex and is allowed to be held longer. Most
// writes are extracted from RawNode while holding r.raftMu and r.mu (in the
// Ready() loop), and handed over to storage under r.raftMu. There are a few
// cases when CRDB synthesizes the writes (e.g. during a range split / merge, or
// raft log truncations) under r.raftMu.
//
// The guarantees explain why holding only r.mu is sufficient for RawNode or its
// snapshot to be in a consistent state. Under r.mu, new writes are blocked,
// because of (2), and by (1) reads never conflict with the in-flight writes.
//
// However, r.mu is a widely used mutex, and not recommended for IO. When doing
// work on behalf RawNode that involves IO (like constructing log appends for a
// follower), we would like to release r.mu. The two guarantees make it possible
// to observe a consistent RawNode snapshot while only holding r.raftMu.
//
// While both r.raftMu and r.mu are held, we can take a shallow / COW copy of
// the RawNode or its relevant subset (e.g. the raft log; the Ready struct is
// also considered such). A subsequent release of r.mu allows RawNode to resume
// making progress. The raft.Storage does not observe any new writes while
// r.raftMu is still held, by the guarantee (2). Combined with guarantee (1), it
// means that both the original and the snapshot RawNode remain consistent. The
// shallow copy represents a valid past state of the RawNode.
//
// All the implementation methods assume that the required locks are held, and
// don't acquire them. The specific locking requirements are noted in each
// method's comment. The method names do not follow our "Locked" naming
// conventions, due to being an implementation of raft.Storage interface from a
// different package.
//
// Many of the methods defined in this file are wrappers around static
// functions. This is done to facilitate their use from Replica.Snapshot(),
// where it is important that all the data that goes into the snapshot comes
// from a consistent view of the database, and not the replica's in-memory state
// or via a reference to Replica.store.Engine().
type replicaRaftStorage = replicaLogStorage

var _ raft.Storage = (*replicaRaftStorage)(nil)

// InitialState implements the raft.Storage interface.
func (r *replicaRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	// The call must synchronize with raft IO. Called when raft is initialized
	// under both r.raftMu and r.mu. We don't technically need r.mu here, but we
	// know it is held.
	r.raftMu.AssertHeld()
	r.mu.AssertHeld()

	ctx := r.AnnotateCtx(context.TODO())
	hs, err := r.raftMu.stateLoader.LoadHardState(ctx, r.store.TODOEngine())
	if err != nil {
		r.reportRaftStorageError(err)
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	// For uninitialized ranges, membership is unknown at this point.
	if raft.IsEmptyHardState(hs) {
		return raftpb.HardState{}, raftpb.ConfState{}, nil
	}
	// NB: r.mu.state is guarded by both r.raftMu and r.mu.
	cs := r.shMu.state.Desc.Replicas().ConfState()
	return hs, cs, nil
}

// GetLeaseAppliedIndex returns the lease index of the last applied command.
func (r *Replica) GetLeaseAppliedIndex() kvpb.LeaseAppliedIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.shMu.state.LeaseAppliedIndex
}

// Snapshot implements the raft.Storage interface.
// Snapshot requires that r.mu is held for writing because it requires exclusive
// access to r.mu.stateLoader.
//
// Note that the returned snapshot is a placeholder and does not contain any of
// the replica data. The snapshot is actually generated (and sent) by the Raft
// snapshot queue.
//
// More specifically, this method is called by etcd/raft in
// (*raftLog).snapshot. Raft expects that it generates the snapshot (by
// calling Snapshot) and that "sending" the result actually sends the
// snapshot. In CockroachDB, that message is intercepted (at the sender) and
// instead we add the replica (the raft leader) to the raft snapshot queue,
// and when its turn comes we look at the raft state for followers that want a
// snapshot, and then send one. That actual sending path does not call this
// Snapshot method.
func (r *replicaRaftStorage) Snapshot() (raftpb.Snapshot, error) {
	r.mu.AssertHeld()
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: uint64(r.shMu.state.RaftAppliedIndex),
			Term:  uint64(r.shMu.state.RaftAppliedIndexTerm),
		},
	}, nil
}

var raftStorageErrorLogger = log.Every(30 * time.Second)

func (r *replicaRaftStorage) reportRaftStorageError(err error) {
	if raftStorageErrorLogger.ShouldLog() {
		log.Errorf(r.raftCtx, "error in raft.Storage %v", err)
	}
	r.store.metrics.RaftStorageError.Inc(1)
}

// raftSnapshotLocked requires that r.mu is held for writing.
func (r *Replica) raftSnapshotLocked() (raftpb.Snapshot, error) {
	return (*replicaRaftStorage)(r).Snapshot()
}

// GetSnapshot returns a snapshot of the replica appropriate for sending to a
// replica. If this method returns without error, callers must eventually call
// OutgoingSnapshot.Close.
func (r *Replica) GetSnapshot(
	ctx context.Context, snapUUID uuid.UUID,
) (_ *OutgoingSnapshot, err error) {
	// Get a snapshot while holding raftMu to make sure we're not seeing "half
	// an AddSSTable" (i.e. a state in which an SSTable has been linked in, but
	// the corresponding Raft command not applied yet).
	var snap storage.Reader
	r.raftMu.Lock()
	startKey := r.shMu.state.Desc.StartKey
	if r.store.cfg.SharedStorageEnabled || storage.ShouldUseEFOS(&r.ClusterSettings().SV) {
		var ss *spanset.SpanSet
		spans := rditer.MakeAllKeySpans(r.shMu.state.Desc) // needs unreplicated to access Raft state
		if util.RaceEnabled {
			ss = rditer.MakeAllKeySpanSet(r.shMu.state.Desc)
			defer ss.Release()
		}
		efos := r.store.TODOEngine().NewEventuallyFileOnlySnapshot(spans)
		if util.RaceEnabled {
			snap = spanset.NewEventuallyFileOnlySnapshot(efos, ss)
		} else {
			snap = efos
		}
	} else {
		snap = r.store.TODOEngine().NewSnapshot()
	}
	r.raftMu.Unlock()

	defer func() {
		if err != nil {
			snap.Close()
		}
	}()
	rangeID := r.RangeID

	ctx, sp := r.AnnotateCtxWithSpan(ctx, "snapshot")
	defer sp.Finish()

	log.Eventf(ctx, "new engine snapshot for replica %s", r)

	// Delegate to a static function to make sure that we do not depend
	// on any indirect calls to r.store.Engine() (or other in-memory
	// state of the Replica). Everything must come from the snapshot.
	//
	// NB: we don't hold either of locks, so can't use Replica.mu.stateLoader or
	// Replica.raftMu.stateLoader. This call is not performance sensitive, so
	// create a new state loader.
	snapData, err := snapshot(ctx, snapUUID, stateloader.Make(rangeID), snap, startKey)
	if err != nil {
		log.Errorf(ctx, "error generating snapshot: %+v", err)
		return nil, err
	}
	return &snapData, nil
}

// OutgoingSnapshot contains the data required to stream a snapshot to a
// recipient. Once one is created, it needs to be closed via Close() to prevent
// resource leakage.
type OutgoingSnapshot struct {
	SnapUUID uuid.UUID
	// The Raft snapshot message to send. Contains SnapUUID as its data.
	RaftSnap raftpb.Snapshot
	// The Pebble snapshot that will be streamed from.
	EngineSnap storage.Reader
	// The replica state within the snapshot.
	State          kvserverpb.ReplicaState
	sharedBackings []objstorage.RemoteObjectBackingHandle
	onClose        func()
}

func (s OutgoingSnapshot) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s OutgoingSnapshot) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("snapshot %s at applied index %d",
		redact.Safe(s.SnapUUID.Short()), s.State.RaftAppliedIndex)
}

// Close releases the resources associated with the snapshot.
func (s *OutgoingSnapshot) Close() {
	s.EngineSnap.Close()
	for i := range s.sharedBackings {
		s.sharedBackings[i].Close()
	}
	if s.onClose != nil {
		s.onClose()
	}
}

// IncomingSnapshot contains the data for an incoming streaming snapshot message.
type IncomingSnapshot struct {
	SnapUUID uuid.UUID
	// The storage interface for the underlying SSTs.
	SSTStorageScratch *SSTSnapshotStorageScratch
	FromReplica       roachpb.ReplicaDescriptor
	// The descriptor in the snapshot, never nil.
	Desc *roachpb.RangeDescriptor
	// Size of the key-value pairs.
	DataSize int64
	// Size of the ssts containing these key-value pairs.
	SSTSize                     int64
	SharedSize                  int64
	placeholder                 *ReplicaPlaceholder
	raftAppliedIndex            kvpb.RaftIndex      // logging only
	msgAppRespCh                chan raftpb.Message // receives MsgAppResp if/when snap is applied
	sharedSSTs                  []pebble.SharedSSTMeta
	externalSSTs                []pebble.ExternalFile
	doExcise                    bool
	includesRangeDelForLastSpan bool
	// clearedSpans represents the key spans in the existing store that will be
	// cleared by doing the Ingest*. This is tracked so that we can convert the
	// ssts into a WriteBatch if the total size of the ssts is small.
	clearedSpans []roachpb.Span
}

func (s IncomingSnapshot) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s IncomingSnapshot) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("snapshot %s from %s at applied index %d",
		redact.Safe(s.SnapUUID.Short()), s.FromReplica, s.raftAppliedIndex)
}

// snapshot creates an OutgoingSnapshot containing a pebble snapshot for the
// given range. Note that snapshot() is called without Replica.raftMu held.
func snapshot(
	ctx context.Context,
	snapUUID uuid.UUID,
	rsl stateloader.StateLoader,
	snap storage.Reader,
	startKey roachpb.RKey,
) (OutgoingSnapshot, error) {
	var desc roachpb.RangeDescriptor
	// We ignore intents on the range descriptor (consistent=false) because we
	// know they cannot be committed yet; operations that modify range
	// descriptors resolve their own intents when they commit.
	ok, err := storage.MVCCGetProto(ctx, snap, keys.RangeDescriptorKey(startKey),
		hlc.MaxTimestamp, &desc, storage.MVCCGetOptions{
			Inconsistent: true, ReadCategory: fs.RangeSnapshotReadCategory})
	if err != nil {
		return OutgoingSnapshot{}, errors.Wrap(err, "failed to get desc")
	}
	if !ok {
		return OutgoingSnapshot{}, errors.Mark(errors.Errorf("couldn't find range descriptor"), errMarkSnapshotError)
	}

	// TODO(sep-raft-log): do not load the state that is not useful for sending
	// snapshots, e.g. Lease, GCThreshold, etc. Only applied indices and the
	// descriptor are used, everything else is included in the snapshot data.
	state, err := rsl.Load(ctx, snap, &desc)
	if err != nil {
		return OutgoingSnapshot{}, err
	}
	// There is no need in sending TruncatedState because the receiver assigns it
	// to match snap.RaftSnap.Metadata.{Index,Term}. See (*Replica).applySnapshot.
	state.TruncatedState = nil

	return OutgoingSnapshot{
		EngineSnap: snap,
		State:      state,
		SnapUUID:   snapUUID,
		RaftSnap: raftpb.Snapshot{
			Data: snapUUID.GetBytes(),
			Metadata: raftpb.SnapshotMetadata{
				Index: uint64(state.RaftAppliedIndex),
				Term:  uint64(state.RaftAppliedIndexTerm),
				// Synthesize our raftpb.ConfState from desc.
				ConfState: desc.Replicas().ConfState(),
			},
		},
	}, nil
}

// updateRangeInfo is called whenever a range is updated by ApplySnapshot
// or is created by range splitting to setup the fields which are
// uninitialized or need updating.
func (r *Replica) updateRangeInfo(ctx context.Context, desc *roachpb.RangeDescriptor) error {
	// RangeMaxBytes should be updated by looking up Zone Config in two cases:
	// 1. After applying a snapshot, if the zone config was not updated for
	// this key range, then maxBytes of this range will not be updated either.
	// 2. After a new range is created by a split, only copying maxBytes from
	// the original range wont work as the original and new ranges might belong
	// to different zones.
	// Load the system config.
	confReader, err := r.store.GetConfReader(ctx)
	if errors.Is(err, errSpanConfigsUnavailable) {
		// This could be before the span config subscription was ever
		// established.
		log.Warningf(ctx, "unable to retrieve conf reader, cannot determine range MaxBytes")
		return nil
	}
	if err != nil {
		return err
	}

	// Find span config for this range.
	conf, sp, err := confReader.GetSpanConfigForKey(ctx, desc.StartKey)
	if err != nil {
		return errors.Wrapf(err, "%s: failed to lookup span config", r)
	}

	changed := r.SetSpanConfig(conf, sp)
	if changed {
		r.MaybeQueue(ctx, r.store.cfg.Clock.NowAsClockTimestamp())
	}

	return nil
}

// applySnapshot updates the replica and its store based on the given
// (non-empty) snapshot and associated HardState. All snapshots must pass
// through Raft for correctness, i.e. the parameters to this method must be
// taken from a raft.Ready. Any replicas specified in subsumedRepls will be
// destroyed atomically with the application of the snapshot.
//
// If there is a placeholder associated with r, applySnapshot will remove that
// placeholder from the store if and only if it does not return an error.
//
// This method requires that r.raftMu is held, as well as the raftMus of any
// replicas in subsumedRepls.
//
// TODO(benesch): the way this replica method reaches into its store to update
// replicasByKey is unfortunate, but the fix requires a substantial refactor to
// maintain the necessary synchronization.
func (r *Replica) applySnapshot(
	ctx context.Context,
	inSnap IncomingSnapshot,
	nonemptySnap raftpb.Snapshot,
	hs raftpb.HardState,
	subsumedRepls []*Replica,
) (err error) {
	desc := inSnap.Desc
	if desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "unexpected range ID %d", desc.RangeID)
	}
	if !desc.IsInitialized() {
		return errors.AssertionFailedf("applying snapshot with uninitialized desc: %s", desc)
	}

	// NB: since raftMu is held, this is not going to change out under us for the
	// duration of this method. In particular, if this is true, then the replica
	// must be in the store's uninitialized replicas map.
	isInitialSnap := !r.IsInitialized()
	{
		var from, to roachpb.RKey
		if isInitialSnap {
			// For uninitialized replicas, there must be a placeholder that covers
			// the snapshot's bounds, so basically check that. A synchronous check
			// here would be simpler but this works well enough.
			d := inSnap.placeholder.Desc()
			from, to = d.StartKey, d.EndKey
			defer r.store.maybeAssertNoHole(ctx, from, to)()
		} else {
			// For snapshots to existing replicas, from and to usually match (i.e.
			// nothing is asserted) but if the snapshot spans a merge then we're
			// going to assert that we're transferring the keyspace from the subsumed
			// replicas to this replica seamlessly.
			d := r.Desc()
			from, to = d.EndKey, inSnap.Desc.EndKey
			defer r.store.maybeAssertNoHole(ctx, from, to)()
		}
	}
	defer func() {
		if e := recover(); e != nil {
			// Re-panic to avoid the log.Fatal() below.
			panic(e)
		}
		if err == nil {
			desc, err := r.GetReplicaDescriptor()
			if err != nil {
				log.Fatalf(ctx, "could not fetch replica descriptor for range after applying snapshot: %v", err)
			}
			if isInitialSnap {
				r.store.metrics.RangeSnapshotsAppliedForInitialUpreplication.Inc(1)
			} else {
				switch desc.Type {
				// NB: A replica of type LEARNER can receive a non-initial snapshot (via
				// the snapshot queue) if we end up truncating the raft log before it
				// gets promoted to a voter. We count such snapshot applications as
				// "applied by voters" here, since the LEARNER will soon be promoted to
				// a voting replica.
				case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.VOTER_DEMOTING_LEARNER,
					roachpb.VOTER_OUTGOING, roachpb.LEARNER, roachpb.VOTER_DEMOTING_NON_VOTER:
					r.store.metrics.RangeSnapshotsAppliedByVoters.Inc(1)
				case roachpb.NON_VOTER:
					r.store.metrics.RangeSnapshotsAppliedByNonVoters.Inc(1)
				default:
					log.Fatalf(ctx, "unexpected replica type %s while applying snapshot", desc.Type)
				}
			}
		}
	}()

	if raft.IsEmptyHardState(hs) {
		// Raft will never provide an empty HardState if it is providing a
		// nonempty snapshot because we discard snapshots that do not increase
		// the commit index.
		log.Fatalf(ctx, "found empty HardState for non-empty Snapshot %+v", nonemptySnap)
	}

	var stats struct {
		// Time to process subsumed replicas.
		subsumedReplicas time.Time
		// Time to ingest SSTs.
		ingestion time.Time
	}
	log.KvDistribution.Infof(ctx, "applying %s", inSnap)
	appliedAsWrite := false
	defer func(start time.Time) {
		var logDetails redact.StringBuilder
		logDetails.Printf("total=%0.0fms", timeutil.Since(start).Seconds()*1000)
		logDetails.Printf(" data=%s", humanizeutil.IBytes(inSnap.DataSize))
		if len(subsumedRepls) > 0 {
			logDetails.Printf(" subsumedReplicas=%d@%0.0fms",
				len(subsumedRepls), stats.subsumedReplicas.Sub(start).Seconds()*1000)
		}
		if len(inSnap.sharedSSTs) > 0 {
			logDetails.Printf(" shared=%d sharedSize=%s", len(inSnap.sharedSSTs), humanizeutil.IBytes(inSnap.SharedSize))
		}
		if inSnap.doExcise {
			logDetails.Printf(" excise=true")
		}
		logDetails.Printf(" ingestion=%d@%0.0fms", len(inSnap.SSTStorageScratch.SSTs()),
			stats.ingestion.Sub(stats.subsumedReplicas).Seconds()*1000)
		var appliedAsWriteStr string
		if appliedAsWrite {
			appliedAsWriteStr = "as write "
		}
		log.Infof(ctx, "applied %s %s(%s)", inSnap, appliedAsWriteStr, logDetails)
	}(timeutil.Now())

	// Clear the raft state and reset it. The log starts from the applied entry ID
	// of the snapshot.
	truncState := kvserverpb.RaftTruncatedState{
		Index: kvpb.RaftIndex(nonemptySnap.Metadata.Index),
		Term:  kvpb.RaftTerm(nonemptySnap.Metadata.Term),
	}
	clearedSpans := inSnap.clearedSpans
	unreplicatedSSTFile, clearedSpan, err := writeUnreplicatedSST(
		ctx, r.ID(), r.ClusterSettings(), truncState, hs, &r.raftMu.stateLoader.StateLoader,
	)
	if err != nil {
		return err
	}
	clearedSpans = append(clearedSpans, clearedSpan)
	// TODO(itsbilal): Write to SST directly in unreplicatedSST rather than
	// buffering in a MemObject first.
	if err := inSnap.SSTStorageScratch.WriteSST(ctx, unreplicatedSSTFile.Data()); err != nil {
		return err
	}
	// Update Raft entries.
	r.store.raftEntryCache.Drop(r.RangeID)

	subsumedDescs := make([]*roachpb.RangeDescriptor, 0, len(subsumedRepls))
	for _, sr := range subsumedRepls {
		// We mark the replica as destroyed so that new commands are not
		// accepted. This destroy status will be detected after the batch
		// commits by clearSubsumedReplicaInMemoryData() to finish the removal.
		//
		// We need to mark the replicas as being destroyed *before* we ingest the
		// SSTs, so that, e.g., concurrent reads served by the replica don't
		// erroneously return empty data.
		sr.readOnlyCmdMu.Lock()
		sr.mu.Lock()
		sr.mu.destroyStatus.Set(
			kvpb.NewRangeNotFoundError(sr.RangeID, sr.store.StoreID()),
			destroyReasonRemoved)
		sr.mu.Unlock()
		sr.readOnlyCmdMu.Unlock()

		subsumedDescs = append(subsumedDescs, sr.Desc())
	}

	// If we're subsuming a replica below, we don't have its last NextReplicaID,
	// nor can we obtain it. That's OK: we can just be conservative and use the
	// maximum possible replica ID. preDestroyRaftMuLocked will write a replica
	// tombstone using this maximum possible replica ID, which would normally be
	// problematic, as it would prevent this store from ever having a new replica
	// of the removed range. In this case, however, it's copacetic, as subsumed
	// ranges _can't_ have new replicas.
	clearedSubsumedSpans, err := clearSubsumedReplicaDiskData(
		// TODO(sep-raft-log): needs access to both engines.
		ctx, r.store.ClusterSettings(), r.store.TODOEngine(), inSnap.SSTStorageScratch.WriteSST,
		desc, subsumedDescs, mergedTombstoneReplicaID,
	)
	if err != nil {
		return err
	}
	clearedSpans = append(clearedSpans, clearedSubsumedSpans...)
	stats.subsumedReplicas = timeutil.Now()

	// Ingest all SSTs atomically.
	if fn := r.store.cfg.TestingKnobs.BeforeSnapshotSSTIngestion; fn != nil {
		if err := fn(inSnap, inSnap.SSTStorageScratch.SSTs()); err != nil {
			return err
		}
	}
	var ingestStats pebble.IngestOperationStats
	var writeBytes uint64
	// TODO: separate ingestions for log and statemachine engine. See:
	//
	// https://github.com/cockroachdb/cockroach/issues/93251
	if len(inSnap.externalSSTs) > 0 || len(inSnap.sharedSSTs) > 0 {
		if !inSnap.doExcise {
			return errors.AssertionFailedf("expected snapshot with remote files to have excise=true")
		}
		exciseSpan := desc.KeySpan().AsRawSpanWithNoLocals()
		if ingestStats, err = r.store.TODOEngine().IngestAndExciseFiles(
			ctx,
			inSnap.SSTStorageScratch.SSTs(),
			inSnap.sharedSSTs,
			inSnap.externalSSTs,
			exciseSpan,
			inSnap.includesRangeDelForLastSpan,
		); err != nil {
			return errors.Wrapf(err, "while ingesting %s and excising %s-%s",
				inSnap.SSTStorageScratch.SSTs(), exciseSpan.Key, exciseSpan.EndKey)
		}
	} else {
		if inSnap.SSTSize > snapshotIngestAsWriteThreshold.Get(&r.ClusterSettings().SV) {
			if inSnap.doExcise {
				exciseSpan := desc.KeySpan().AsRawSpanWithNoLocals()
				if ingestStats, err = r.store.TODOEngine().IngestAndExciseFiles(
					ctx,
					inSnap.SSTStorageScratch.SSTs(),
					nil, /* sharedSSTs */
					nil, /* externalSSTs */
					exciseSpan,
					inSnap.includesRangeDelForLastSpan,
				); err != nil {
					return errors.Wrapf(err, "while ingesting %s and excising %s-%s",
						inSnap.SSTStorageScratch.SSTs(), exciseSpan.Key, exciseSpan.EndKey)
				}
			} else {
				if ingestStats, err =
					r.store.TODOEngine().IngestLocalFilesWithStats(ctx, inSnap.SSTStorageScratch.SSTs()); err != nil {
					return errors.Wrapf(err, "while ingesting %s", inSnap.SSTStorageScratch.SSTs())
				}
			}
		} else {
			appliedAsWrite = true
			err := r.store.TODOEngine().ConvertFilesToBatchAndCommit(
				ctx, inSnap.SSTStorageScratch.SSTs(), clearedSpans)
			if err != nil {
				return errors.Wrapf(err, "while applying as batch %s", inSnap.SSTStorageScratch.SSTs())
			}
			// Admission control wants the writeBytes to be roughly equivalent to
			// the bytes in the SST when these writes are eventually flushed. We use
			// the SST size of the incoming snapshot as that approximation. We've
			// written additional SSTs to clear some data earlier in this method,
			// but we ignore those since the bulk of the data is in the incoming
			// snapshot.
			writeBytes = uint64(inSnap.SSTSize)
		}
	}
	// The "ignored" here is to ignore the writes to create the AC linear models
	// for LSM writes. Since these writes typically correspond to actual writes
	// onto the disk, we account for them separately in
	// kvBatchSnapshotStrategy.Receive().
	if r.store.cfg.KVAdmissionController != nil {
		r.store.cfg.KVAdmissionController.SnapshotIngestedOrWritten(
			r.store.StoreID(), ingestStats, writeBytes)
	}
	stats.ingestion = timeutil.Now()

	// The on-disk state is now committed, but the corresponding in-memory state
	// has not yet been updated. Any errors past this point must therefore be
	// treated as fatal.

	sl := stateloader.Make(desc.RangeID)
	state, err := sl.Load(ctx, r.store.TODOEngine(), desc)
	if err != nil {
		log.Fatalf(ctx, "unable to load replica state: %s", err)
	}

	if uint64(state.RaftAppliedIndex) != nonemptySnap.Metadata.Index {
		log.Fatalf(ctx, "snapshot RaftAppliedIndex %d doesn't match its metadata index %d",
			state.RaftAppliedIndex, nonemptySnap.Metadata.Index)
	}
	if uint64(state.RaftAppliedIndexTerm) != nonemptySnap.Metadata.Term {
		log.Fatalf(ctx, "snapshot RaftAppliedIndexTerm %d doesn't match its metadata term %d",
			state.RaftAppliedIndexTerm, nonemptySnap.Metadata.Term)
	}

	// Read the prior read summary for this range, which was included in the
	// snapshot. We may need to use it to bump our timestamp cache if we
	// discover that we are the leaseholder as of the snapshot's log index.
	prioReadSum, err := readsummary.Load(ctx, r.store.TODOEngine(), r.RangeID)
	if err != nil {
		log.Fatalf(ctx, "failed to read prior read summary after applying snapshot: %+v", err)
	}

	// The necessary on-disk state is read. Update the in-memory Replica and Store
	// state now.

	subPHs, err := r.clearSubsumedReplicaInMemoryData(ctx, subsumedRepls, mergedTombstoneReplicaID)
	if err != nil {
		log.Fatalf(ctx, "failed to clear in-memory data of subsumed replicas while applying snapshot: %+v", err)
	}

	// Atomically swap the placeholder, if any, for the replica, and update the
	// replica's state. Note that this is intentionally in one critical section.
	// to avoid exposing an inconsistent in-memory state. We did however already
	// consume the SSTs above, meaning that at this point the in-memory state lags
	// the on-disk state.

	r.store.mu.Lock()
	if inSnap.placeholder != nil {
		subPHs = append(subPHs, inSnap.placeholder)
	}
	for _, ph := range subPHs {
		_, err := r.store.removePlaceholderLocked(ctx, ph, removePlaceholderFilled)
		if err != nil {
			log.Fatalf(ctx, "unable to remove placeholder %s: %s", ph, err)
		}
	}

	// NB: we lock `r.mu` only now because removePlaceholderLocked operates on
	// replicasByKey and this may end up calling r.Desc().
	r.mu.Lock()
	if isInitialSnap {
		// NB: this will also call setDescLockedRaftMuLocked.
		if err := r.initFromSnapshotLockedRaftMuLocked(ctx, desc); err != nil {
			r.mu.Unlock()
			log.Fatalf(ctx, "unable to initialize replica while applying snapshot: %+v", err)
		} else if err := r.store.markReplicaInitializedLockedReplLocked(ctx, r); err != nil {
			r.mu.Unlock()
			log.Fatalf(ctx, "unable to mark replica initialized while applying snapshot: %+v", err)
		}
	} else {
		r.setDescLockedRaftMuLocked(ctx, desc)
	}
	// NOTE: even though we acquired the store mutex first (according to the
	// lock ordering rules described on Store.mu), it is safe to drop it first
	// without risking a lock-ordering deadlock.
	r.store.mu.Unlock()

	// The log has been cleared and reset to start at the snapshot's applied
	// index/term. Update the in-memory metadata accordingly.
	r.asLogStorage().updateStateRaftMuLockedMuLocked(logstore.RaftState{
		LastIndex: truncState.Index,
		LastTerm:  truncState.Term,
		ByteSize:  0, // the log is empty now
	})
	r.shMu.raftTruncState = truncState
	// Snapshots typically have fewer log entries than the leaseholder. The next
	// time we hold the lease, recompute the log size before making decisions.
	//
	// TODO(pav-kv): does this assume that snapshots can contain log entries,
	// which is no longer true? The comment needs an update, and the decision to
	// set this flag to false revisited.
	r.shMu.raftLogSizeTrusted = false

	// Update the store stats for the data in the snapshot.
	r.store.metrics.subtractMVCCStats(ctx, r.tenantMetricsRef, *r.shMu.state.Stats)
	r.store.metrics.addMVCCStats(ctx, r.tenantMetricsRef, *state.Stats)
	lastKnownLease := r.shMu.state.Lease
	// Update the rest of the Raft state. Changes to r.mu.state.Desc must be
	// managed by r.setDescRaftMuLocked and changes to r.mu.state.Lease must be handled
	// by r.leasePostApply, but we called those above, so now it's safe to
	// wholesale replace r.mu.state.
	r.shMu.state = state
	if r.shMu.state.ForceFlushIndex != (roachpb.ForceFlushIndex{}) {
		r.flowControlV2.ForceFlushIndexChangedLocked(ctx, r.shMu.state.ForceFlushIndex.Index)
	}

	// Invoke the leasePostApply method to ensure we properly initialize the
	// replica according to whether it holds the lease. We allow jumps in the
	// lease sequence because there may be multiple lease changes accounted for
	// in the snapshot.
	r.leasePostApplyLocked(ctx, lastKnownLease, state.Lease /* newLease */, prioReadSum, allowLeaseJump)

	// Similarly, if we subsumed any replicas through the snapshot (meaning that
	// we missed the application of a merge) and we are the new leaseholder, we
	// make sure to update the timestamp cache using the prior read summary to
	// account for any reads that were served on the right-hand side range(s).
	if len(subsumedRepls) > 0 && state.Lease.Replica.ReplicaID == r.replicaID && prioReadSum != nil {
		applyReadSummaryToTimestampCache(ctx, r.store.tsCache, r.descRLocked(), *prioReadSum)
	}

	// Inform the concurrency manager that this replica just applied a snapshot.
	r.concMgr.OnReplicaSnapshotApplied()

	if fn := r.store.cfg.TestingKnobs.AfterSnapshotApplication; fn != nil {
		desc, _ := r.getReplicaDescriptorRLocked()
		fn(desc, r.shMu.state, inSnap)
	}

	r.mu.Unlock()

	// Assert that the in-memory and on-disk states of the Replica are congruent
	// after the application of the snapshot. Do so under a read lock, as this
	// operation can be expensive. This is safe, as we hold the Replica.raftMu
	// across both Replica.mu critical sections.
	r.mu.RLock()
	r.assertStateRaftMuLockedReplicaMuRLocked(ctx, r.store.TODOEngine())
	r.mu.RUnlock()

	// The rangefeed processor is listening for the logical ops attached to
	// each raft command. These will be lost during a snapshot, so disconnect
	// the rangefeed, if one exists.
	r.disconnectRangefeedWithReason(
		kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
	)

	// Update the replica's cached byte thresholds. This is a no-op if the system
	// config is not available, in which case we rely on the next gossip update
	// to perform the update.
	if err := r.updateRangeInfo(ctx, desc); err != nil {
		log.Fatalf(ctx, "unable to update range info while applying snapshot: %+v", err)
	}

	return nil
}

// writeUnreplicatedSST creates an SST for snapshot application that
// covers the RangeID-unreplicated keyspace. A range tombstone is
// laid down and the Raft state provided by the arguments is overlaid
// onto it.
func writeUnreplicatedSST(
	ctx context.Context,
	id storage.FullReplicaID,
	st *cluster.Settings,
	ts kvserverpb.RaftTruncatedState,
	hs raftpb.HardState,
	sl *logstore.StateLoader,
) (_ *storage.MemObject, clearedSpan roachpb.Span, _ error) {
	unreplicatedSSTFile := &storage.MemObject{}
	unreplicatedSST := storage.MakeIngestionSSTWriter(
		ctx, st, unreplicatedSSTFile,
	)
	defer unreplicatedSST.Close()
	// Clear the raft state/log, and initialize it again with the provided
	// HardState and RaftTruncatedState.
	clearedSpan, err := rewriteRaftState(ctx, id, hs, ts, sl, &unreplicatedSST)
	if err != nil {
		return nil, roachpb.Span{}, err
	}
	if err := unreplicatedSST.Finish(); err != nil {
		return nil, roachpb.Span{}, err
	}
	return unreplicatedSSTFile, clearedSpan, nil
}

// rewriteRaftState clears and rewrites the unreplicated rangeID-local key space
// of the given replica with the provided raft state. Note that it also clears
// the raft log contents.
//
// The caller must make sure the log does not have entries newer than the
// snapshot entry ID, and that clearing the log is applied atomically with the
// snapshot write, or after the latter is synced.
func rewriteRaftState(
	ctx context.Context,
	id storage.FullReplicaID,
	hs raftpb.HardState,
	ts kvserverpb.RaftTruncatedState,
	sl *logstore.StateLoader,
	w storage.Writer,
) (clearedSpan roachpb.Span, _ error) {
	// Clearing the unreplicated state.
	//
	// NB: We do not expect to see range keys in the unreplicated state, so
	// we don't drop a range tombstone across the range key space.
	unreplicatedPrefixKey := keys.MakeRangeIDUnreplicatedPrefix(id.RangeID)
	unreplicatedStart := unreplicatedPrefixKey
	unreplicatedEnd := unreplicatedPrefixKey.PrefixEnd()
	clearedSpan = roachpb.Span{Key: unreplicatedStart, EndKey: unreplicatedEnd}
	if err := w.ClearRawRange(
		unreplicatedStart, unreplicatedEnd, true /* pointKeys */, false, /* rangeKeys */
	); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "error clearing the unreplicated space")
	}

	// Update HardState.
	if err := sl.SetHardState(ctx, w, hs); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "unable to write HardState")
	}
	// We've cleared all the raft state above, so we are forced to write the
	// RaftReplicaID again here.
	if err := sl.SetRaftReplicaID(ctx, w, id.ReplicaID); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "unable to write RaftReplicaID")
	}
	// Update the log truncation state.
	if err := sl.SetRaftTruncatedState(ctx, w, &ts); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "unable to write RaftTruncatedState")
	}
	return clearedSpan, nil
}

// clearSubsumedReplicaDiskData clears the on disk data of the subsumed
// replicas by creating SSTs with range deletion tombstones. We have to be
// careful here not to have overlapping ranges with the SSTs we have already
// created since that will throw an error while we are ingesting them. This
// method requires that each of the subsumed replicas raftMu is held, and that
// the Reader reflects the latest I/O each of the subsumed replicas has done
// (i.e. Reader was instantiated after all raftMu were acquired).
func clearSubsumedReplicaDiskData(
	ctx context.Context,
	st *cluster.Settings,
	reader storage.Reader,
	writeSST func(context.Context, []byte) error,
	desc *roachpb.RangeDescriptor,
	subsumedDescs []*roachpb.RangeDescriptor,
	subsumedNextReplicaID roachpb.ReplicaID,
) (clearedSpans []roachpb.Span, _ error) {
	// NB: we don't clear RangeID local key spans here. That happens
	// via the call to preDestroyRaftMuLocked.
	getKeySpans := func(d *roachpb.RangeDescriptor) []roachpb.Span {
		return rditer.Select(d.RangeID, rditer.SelectOpts{
			ReplicatedBySpan: d.RSpan(),
		})
	}
	keySpans := getKeySpans(desc)
	totalKeySpans := append([]roachpb.Span(nil), keySpans...)
	for _, subDesc := range subsumedDescs {
		// We have to create an SST for the subsumed replica's range-id local keys.
		subsumedReplSSTFile := &storage.MemObject{}
		subsumedReplSST := storage.MakeIngestionSSTWriter(
			ctx, st, subsumedReplSSTFile,
		)
		//nolint:deferloop TODO(#137605)
		defer subsumedReplSST.Close()
		// NOTE: We set mustClearRange to true because we are setting
		// RangeTombstoneKey. Since Clears and Puts need to be done in increasing
		// order of keys, it is not safe to use ClearRangeIter.
		opts := kvstorage.ClearRangeDataOptions{
			ClearReplicatedByRangeID:   true,
			ClearUnreplicatedByRangeID: true,
			MustUseClearRange:          true,
		}
		subsumedClearedSpans := rditer.Select(subDesc.RangeID, rditer.SelectOpts{
			ReplicatedByRangeID:   opts.ClearReplicatedByRangeID,
			UnreplicatedByRangeID: opts.ClearUnreplicatedByRangeID,
		})
		clearedSpans = append(clearedSpans, subsumedClearedSpans...)
		if err := kvstorage.DestroyReplica(ctx, subDesc.RangeID, reader, &subsumedReplSST, subsumedNextReplicaID, opts); err != nil {
			subsumedReplSST.Close()
			return nil, err
		}
		if err := subsumedReplSST.Finish(); err != nil {
			return nil, err
		}
		if subsumedReplSST.DataSize > 0 {
			// TODO(itsbilal): Write to SST directly in subsumedReplSST rather than
			// buffering in a MemObject first.
			if err := writeSST(ctx, subsumedReplSSTFile.Data()); err != nil {
				return nil, err
			}
		}

		srKeySpans := getKeySpans(subDesc)
		// Compute the total key space covered by the current replica and all
		// subsumed replicas.
		for i := range srKeySpans {
			if srKeySpans[i].Key.Compare(totalKeySpans[i].Key) < 0 {
				totalKeySpans[i].Key = srKeySpans[i].Key
			}
			if srKeySpans[i].EndKey.Compare(totalKeySpans[i].EndKey) > 0 {
				totalKeySpans[i].EndKey = srKeySpans[i].EndKey
			}
		}
	}

	// We might have to create SSTs for the range local keys, lock table keys,
	// and user keys depending on if the subsumed replicas are not fully
	// contained by the replica in our snapshot. The following is an example to
	// this case happening.
	//
	// a       b       c       d
	// |---1---|-------2-------|  S1
	// |---1-------------------|  S2
	// |---1-----------|---3---|  S3
	//
	// Since the merge is the first operation to happen, a follower could be down
	// before it completes. It is reasonable for a snapshot for r1 from S3 to
	// subsume both r1 and r2 in S1.
	for i := range keySpans {
		if totalKeySpans[i].EndKey.Compare(keySpans[i].EndKey) > 0 {
			subsumedReplSSTFile := &storage.MemObject{}
			subsumedReplSST := storage.MakeIngestionSSTWriter(
				ctx, st, subsumedReplSSTFile,
			)
			//nolint:deferloop TODO(#137605)
			defer subsumedReplSST.Close()
			if err := storage.ClearRangeWithHeuristic(
				ctx,
				reader,
				&subsumedReplSST,
				keySpans[i].EndKey,
				totalKeySpans[i].EndKey,
				kvstorage.ClearRangeThresholdPointKeys,
				kvstorage.ClearRangeThresholdRangeKeys,
			); err != nil {
				subsumedReplSST.Close()
				return nil, err
			}
			clearedSpans = append(clearedSpans,
				roachpb.Span{Key: keySpans[i].EndKey, EndKey: totalKeySpans[i].EndKey})
			if err := subsumedReplSST.Finish(); err != nil {
				return nil, err
			}
			if subsumedReplSST.DataSize > 0 {
				// TODO(itsbilal): Write to SST directly in subsumedReplSST rather than
				// buffering in a MemObject first.
				if err := writeSST(ctx, subsumedReplSSTFile.Data()); err != nil {
					return nil, err
				}
			}
		}
		// The snapshot must never subsume a replica that extends the range of the
		// replica to the left. This is because splits and merges (the only
		// operation that change the key bounds) always leave the start key intact.
		// Extending to the left implies that either we merged "to the left" (we
		// don't), or that we're applying a snapshot for another range (we don't do
		// that either). Something is severely wrong for this to happen.
		if totalKeySpans[i].Key.Compare(keySpans[i].Key) < 0 {
			log.Fatalf(ctx, "subsuming replica to our left; key span: %v; total key span %v",
				keySpans[i], totalKeySpans[i])
		}
	}
	return clearedSpans, nil
}

// clearSubsumedReplicaInMemoryData clears the in-memory data of the subsumed
// replicas. This method requires that each of the subsumed replicas raftMu is
// held.
func (r *Replica) clearSubsumedReplicaInMemoryData(
	ctx context.Context, subsumedRepls []*Replica, subsumedNextReplicaID roachpb.ReplicaID,
) ([]*ReplicaPlaceholder, error) {
	//
	var phs []*ReplicaPlaceholder
	for _, sr := range subsumedRepls {
		// We already hold sr's raftMu, so we must call removeReplicaImpl directly.
		// As the subsuming range is planning to expand to cover the subsumed ranges,
		// we introduce corresponding placeholders and return them to the caller to
		// consume. Without this, there would be a risk of errant snapshots being
		// allowed in (perhaps not involving any of the RangeIDs known to the merge
		// but still touching its keyspace) and causing corruption.
		ph, err := r.store.removeInitializedReplicaRaftMuLocked(ctx, sr, subsumedNextReplicaID, RemoveOptions{
			// The data was already destroyed by clearSubsumedReplicaDiskData.
			DestroyData:       false,
			InsertPlaceholder: true,
		})
		if err != nil {
			return nil, err
		}
		phs = append(phs, ph)
		// We removed sr's data when we committed the batch. Finish subsumption by
		// updating the in-memory bookkeping.
		if err := sr.postDestroyRaftMuLocked(ctx, sr.GetMVCCStats()); err != nil {
			return nil, err
		}
	}
	return phs, nil
}
