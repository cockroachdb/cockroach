// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// How long to keep consistency checker checksums in-memory for collection.
// Typically a long-poll waits for the result of the computation, so it's almost
// immediately collected. However, the consistency checker synchronously
// collects the first replica's checksum before all others, so if the first one
// is slow the checksum may not be collected right away, and that first
// consistency check can take a long time due to rate limiting and range size.
const replicaChecksumGCInterval = time.Hour

// fatalOnStatsMismatch, if true, turns stats mismatches into fatal errors. A
// stats mismatch is the event in which
// - the consistency checker finds that all replicas are consistent
//   (i.e. byte-by-byte identical)
// - the (identical) stats tracked in them do not correspond to a recomputation
//   via the data, i.e. the stats were incorrect
// - ContainsEstimates==false, i.e. the stats claimed they were correct.
//
// Before issuing the fatal error, the cluster bootstrap version is verified.
// We know that old versions of CockroachDB sometimes violated this invariant,
// but we want to exclude these violations, focusing only on cases in which we
// know old CRDB versions (<19.1 at time of writing) were not involved.
var fatalOnStatsMismatch = envutil.EnvOrDefaultBool("COCKROACH_ENFORCE_CONSISTENT_STATS", false)

// replicaChecksum contains progress on a replica checksum computation.
type replicaChecksum struct {
	CollectChecksumResponse
	// started is true if the checksum computation has started.
	started bool
	// If gcTimestamp is nonzero, GC this checksum after gcTimestamp. gcTimestamp
	// is zero if and only if the checksum computation is in progress.
	gcTimestamp time.Time
	// This channel is closed after the checksum is computed, and is used
	// as a notification.
	notify chan struct{}
}

// CheckConsistency runs a consistency check on the range. It first applies a
// ComputeChecksum through Raft and then issues CollectChecksum commands to the
// other replicas. These are inspected and a CheckConsistencyResponse is assembled.
//
// When args.Mode is CHECK_VIA_QUEUE and an inconsistency is detected and no
// diff was requested, the consistency check will be re-run to collect a diff,
// which is then printed before calling `log.Fatal`. This behavior should be
// lifted to the consistency checker queue in the future.
func (r *Replica) CheckConsistency(
	ctx context.Context, args roachpb.CheckConsistencyRequest,
) (roachpb.CheckConsistencyResponse, *roachpb.Error) {
	startKey := r.Desc().StartKey.AsRawKey()

	checkArgs := roachpb.ComputeChecksumRequest{
		RequestHeader: roachpb.RequestHeader{Key: startKey},
		Version:       batcheval.ReplicaChecksumVersion,
		Snapshot:      args.WithDiff,
		Mode:          args.Mode,
		Checkpoint:    args.Checkpoint,
		Terminate:     args.Terminate,
	}

	isQueue := args.Mode == roachpb.ChecksumMode_CHECK_VIA_QUEUE

	results, err := r.RunConsistencyCheck(ctx, checkArgs)
	if err != nil {
		return roachpb.CheckConsistencyResponse{}, roachpb.NewError(err)
	}

	res := roachpb.CheckConsistencyResponse_Result{}
	res.RangeID = r.RangeID

	shaToIdxs := map[string][]int{}
	var missing []ConsistencyCheckResult
	for i, result := range results {
		if result.Err != nil {
			missing = append(missing, result)
			continue
		}
		s := string(result.Response.Checksum)
		shaToIdxs[s] = append(shaToIdxs[s], i)
	}

	// When replicas diverge, anecdotally often the minority (usually of size
	// one) is in the wrong. If there's more than one smallest minority (for
	// example, if three replicas all return different hashes) we pick any of
	// them.
	var minoritySHA string
	if len(shaToIdxs) > 1 {
		for sha, idxs := range shaToIdxs {
			if minoritySHA == "" || len(shaToIdxs[minoritySHA]) > len(idxs) {
				minoritySHA = sha
			}
		}
	}

	// There is an inconsistency if and only if there is a minority SHA.

	if minoritySHA != "" {
		var buf redact.StringBuilder
		buf.Printf("\n") // New line to align checksums below.
		for sha, idxs := range shaToIdxs {
			minority := redact.Safe("")
			if sha == minoritySHA {
				minority = redact.Safe(" [minority]")
			}
			for _, idx := range idxs {
				buf.Printf("%s: checksum %x%s\n"+
					"- stats: %+v\n"+
					"- stats.Sub(recomputation): %+v\n",
					&results[idx].Replica,
					redact.Safe(sha),
					minority,
					&results[idx].Response.Persisted,
					&results[idx].Response.Delta,
				)
			}
			minoritySnap := results[shaToIdxs[minoritySHA][0]].Response.Snapshot
			curSnap := results[shaToIdxs[sha][0]].Response.Snapshot
			if sha != minoritySHA && minoritySnap != nil && curSnap != nil {
				diff := diffRange(curSnap, minoritySnap)
				if report := r.store.cfg.TestingKnobs.ConsistencyTestingKnobs.BadChecksumReportDiff; report != nil {
					report(*r.store.Ident, diff)
				}
				buf.Printf("====== diff(%x, [minority]) ======\n%v", redact.Safe(sha), diff)
			}
		}

		if isQueue {
			log.Errorf(ctx, "%v", &buf)
		}
		res.Detail += buf.String()
	} else {
		res.Detail += fmt.Sprintf("stats: %+v\n", results[0].Response.Persisted)
	}
	for _, result := range missing {
		res.Detail += fmt.Sprintf("%s: error: %v\n", result.Replica, result.Err)
	}

	delta := enginepb.MVCCStats(results[0].Response.Delta)
	var haveDelta bool
	{
		d2 := delta
		d2.AgeTo(0)
		haveDelta = d2 != enginepb.MVCCStats{}
	}

	res.StartKey = []byte(startKey)
	res.Status = roachpb.CheckConsistencyResponse_RANGE_CONSISTENT
	if minoritySHA != "" {
		res.Status = roachpb.CheckConsistencyResponse_RANGE_INCONSISTENT
	} else if args.Mode != roachpb.ChecksumMode_CHECK_STATS && haveDelta {
		if delta.ContainsEstimates > 0 {
			// When ContainsEstimates is set, it's generally expected that we'll get a different
			// result when we recompute from scratch.
			res.Status = roachpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_ESTIMATED
		} else {
			// When ContainsEstimates is unset, we expect the recomputation to agree with the stored stats.
			// If that's not the case, that's a problem: it could be a bug in the stats computation
			// or stats maintenance, but it could also hint at the replica having diverged from its peers.
			res.Status = roachpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT
		}
		res.Detail += fmt.Sprintf("stats - recomputation: %+v\n", enginepb.MVCCStats(results[0].Response.Delta))
	} else if len(missing) > 0 {
		// No inconsistency was detected, but we didn't manage to inspect all replicas.
		res.Status = roachpb.CheckConsistencyResponse_RANGE_INDETERMINATE
	}
	var resp roachpb.CheckConsistencyResponse
	resp.Result = append(resp.Result, res)

	// Bail out at this point except if the queue is the caller. All of the stuff
	// below should really happen in the consistency queue to keep CheckConsistency
	// itself self-contained.
	if !isQueue {
		return resp, nil
	}

	if minoritySHA == "" {
		// The replicas were in sync. Check that the MVCCStats haven't diverged from
		// what they should be. This code originated in the realization that there
		// were many bugs in our stats computations. These are being fixed, but it
		// is through this mechanism that existing ranges are updated. Hence, the
		// logging below is relatively timid.

		// If there's no delta, there's nothing else to do.
		if !haveDelta {
			return resp, nil
		}

		if delta.ContainsEstimates <= 0 && fatalOnStatsMismatch {
			// We just found out that the recomputation doesn't match the persisted stats,
			// so ContainsEstimates should have been strictly positive.

			var v roachpb.Version
			if err := r.store.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				return txn.GetProto(ctx, keys.BootstrapVersionKey, &v)
			}); err != nil {
				log.Infof(ctx, "while retrieving cluster bootstrap version: %s", err)
				// Intentionally continue with the assumption that it's the current version.
				v = r.store.cfg.Settings.Version.ActiveVersion(ctx).Version
			}
			// For clusters that ever ran <19.1, we're not so sure that the stats
			// are consistent. Verify this only for clusters that started out on 19.1 or
			// higher.
			if !v.Less(roachpb.Version{Major: 19, Minor: 1}) {
				// If version >= 19.1 but < 20.1-14 (AbortSpanBytes before its removal),
				// we want to ignore any delta in AbortSpanBytes when comparing stats
				// since older versions will not be tracking abort span bytes.
				if v.Less(roachpb.Version{Major: 20, Minor: 1, Internal: 14}) {
					delta.AbortSpanBytes = 0
					haveDelta = delta != enginepb.MVCCStats{}
				}
				if !haveDelta {
					return resp, nil
				}
				log.Fatalf(ctx, "found a delta of %+v", redact.Safe(delta))
			}
		}

		// We've found that there's something to correct; send an RecomputeStatsRequest. Note that this
		// code runs only on the lease holder (at the time of initiating the computation), so this work
		// isn't duplicated except in rare leaseholder change scenarios (and concurrent invocation of
		// RecomputeStats is allowed because these requests block on one another). Also, we're
		// essentially paced by the consistency checker so we won't call this too often.
		log.Infof(ctx, "triggering stats recomputation to resolve delta of %+v", results[0].Response.Delta)

		req := roachpb.RecomputeStatsRequest{
			RequestHeader: roachpb.RequestHeader{Key: startKey},
		}

		var b kv.Batch
		b.AddRawRequest(&req)

		err := r.store.db.Run(ctx, &b)
		return resp, roachpb.NewError(err)
	}

	if args.WithDiff {
		// A diff was already printed. Return because all the code below will do
		// is request another consistency check, with a diff and with
		// instructions to terminate the minority nodes.
		log.Errorf(ctx, "consistency check failed")
		return resp, nil
	}

	// No diff was printed, so we want to re-run with diff.
	// Note that this recursive call will be terminated in the `args.WithDiff`
	// branch above.
	args.WithDiff = true
	args.Checkpoint = true
	for _, idxs := range shaToIdxs[minoritySHA] {
		args.Terminate = append(args.Terminate, results[idxs].Replica)
	}
	// args.Terminate is a slice of properly redactable values, but
	// with %v `redact` will not realize that and will redact the
	// whole thing. Wrap it as a ReplicaSet which is a SafeFormatter
	// and will get the job done.
	//
	// TODO(knz): clean up after https://github.com/cockroachdb/redact/issues/5.
	{
		var tmp redact.SafeFormatter = roachpb.MakeReplicaSet(args.Terminate)
		log.Errorf(ctx, "consistency check failed; fetching details and shutting down minority %v", tmp)
	}

	// We've noticed in practice that if the snapshot diff is large, the
	// log file to which it is printed is promptly rotated away, so up
	// the limits while the diff printing occurs.
	//
	// See:
	// https://github.com/cockroachdb/cockroach/issues/36861
	defer log.TemporarilyDisableFileGCForMainLogger()()

	if _, pErr := r.CheckConsistency(ctx, args); pErr != nil {
		log.Errorf(ctx, "replica inconsistency detected; could not obtain actual diff: %s", pErr)
	}

	return resp, nil
}

// A ConsistencyCheckResult contains the outcome of a CollectChecksum call.
type ConsistencyCheckResult struct {
	Replica  roachpb.ReplicaDescriptor
	Response CollectChecksumResponse
	Err      error
}

func (r *Replica) collectChecksumFromReplica(
	ctx context.Context, replica roachpb.ReplicaDescriptor, id uuid.UUID, checksum []byte,
) (CollectChecksumResponse, error) {
	conn, err := r.store.cfg.NodeDialer.Dial(ctx, replica.NodeID, rpc.DefaultClass)
	if err != nil {
		return CollectChecksumResponse{},
			errors.Wrapf(err, "could not dial node ID %d", replica.NodeID)
	}
	client := NewPerReplicaClient(conn)
	req := &CollectChecksumRequest{
		StoreRequestHeader: StoreRequestHeader{NodeID: replica.NodeID, StoreID: replica.StoreID},
		RangeID:            r.RangeID,
		ChecksumID:         id,
		Checksum:           checksum,
	}
	resp, err := client.CollectChecksum(ctx, req)
	if err != nil {
		return CollectChecksumResponse{}, err
	}
	return *resp, nil
}

// RunConsistencyCheck carries out a round of CheckConsistency/CollectChecksum
// for the members of this range, returning the results (which it does not act
// upon). The first result will belong to the local replica, and in particular
// there is a first result when no error is returned.
func (r *Replica) RunConsistencyCheck(
	ctx context.Context, req roachpb.ComputeChecksumRequest,
) ([]ConsistencyCheckResult, error) {
	// Send a ComputeChecksum which will trigger computation of the checksum on
	// all replicas.
	res, pErr := kv.SendWrapped(ctx, r.store.db.NonTransactionalSender(), &req)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	ccRes := res.(*roachpb.ComputeChecksumResponse)

	var orderedReplicas []roachpb.ReplicaDescriptor
	{
		desc := r.Desc()
		localReplica, err := r.GetReplicaDescriptor()
		if err != nil {
			return nil, errors.Wrap(err, "could not get replica descriptor")
		}

		// Move the local replica to the front (which makes it the "master"
		// we're comparing against).
		orderedReplicas = append(orderedReplicas, desc.Replicas().Descriptors()...)

		sort.Slice(orderedReplicas, func(i, j int) bool {
			return orderedReplicas[i] == localReplica
		})
	}

	resultCh := make(chan ConsistencyCheckResult, len(orderedReplicas))
	var results []ConsistencyCheckResult
	var wg sync.WaitGroup

	for _, replica := range orderedReplicas {
		wg.Add(1)
		replica := replica // per-iteration copy for the goroutine
		if err := r.store.Stopper().RunAsyncTask(ctx, "storage.Replica: checking consistency",
			func(ctx context.Context) {
				defer wg.Done()

				var masterChecksum []byte
				if len(results) > 0 {
					masterChecksum = results[0].Response.Checksum
				}
				resp, err := r.collectChecksumFromReplica(ctx, replica, ccRes.ChecksumID, masterChecksum)
				resultCh <- ConsistencyCheckResult{
					Replica:  replica,
					Response: resp,
					Err:      err,
				}
			}); err != nil {
			wg.Done()
			// If we can't start tasks, the node is likely draining. Just return the error verbatim.
			return nil, err
		}

		// Collect the master result eagerly so that we can send a SHA in the
		// remaining requests (this is used for logging inconsistencies on the
		// remote nodes only).
		if len(results) == 0 {
			wg.Wait()
			result := <-resultCh
			if err := result.Err; err != nil {
				// If we can't compute the local checksum, give up.
				return nil, errors.Wrap(err, "computing own checksum")
			}
			results = append(results, result)
		}
	}

	wg.Wait()
	close(resultCh)

	// Collect the remaining results.
	for result := range resultCh {
		results = append(results, result)
	}

	return results, nil
}

func (r *Replica) gcOldChecksumEntriesLocked(now time.Time) {
	for id, val := range r.mu.checksums {
		// The timestamp is valid only if set.
		if !val.gcTimestamp.IsZero() && now.After(val.gcTimestamp) {
			delete(r.mu.checksums, id)
		}
	}
}

// getChecksum waits for the result of ComputeChecksum and returns it.
// It returns false if there is no checksum being computed for the id,
// or it has already been GCed.
func (r *Replica) getChecksum(ctx context.Context, id uuid.UUID) (replicaChecksum, error) {
	now := timeutil.Now()
	r.mu.Lock()
	r.gcOldChecksumEntriesLocked(now)
	c, ok := r.mu.checksums[id]
	if !ok {
		// TODO(tbg): we need to unconditionally set a gcTimestamp or this
		// request can simply get stuck forever or cancel anyway and leak an
		// entry in r.mu.checksums.
		if d, dOk := ctx.Deadline(); dOk {
			c.gcTimestamp = d
		}
		c.notify = make(chan struct{})
		r.mu.checksums[id] = c
	}
	r.mu.Unlock()

	// Wait for the checksum to compute or at least to start.
	computed, err := r.checksumInitialWait(ctx, id, c.notify)
	if err != nil {
		return replicaChecksum{}, err
	}
	// If the checksum started, but has not completed commit
	// to waiting the full deadline.
	if !computed {
		_, err = r.checksumWait(ctx, id, c.notify, nil)
		if err != nil {
			return replicaChecksum{}, err
		}
	}

	if log.V(1) {
		log.Infof(ctx, "waited for compute checksum for %s", timeutil.Since(now))
	}
	r.mu.RLock()
	c, ok = r.mu.checksums[id]
	r.mu.RUnlock()
	// If the checksum wasn't found or the checksum could not be computed, error out.
	// The latter case can occur when there's a version mismatch or, more generally,
	// when the (async) checksum computation fails.
	if !ok || c.Checksum == nil {
		return replicaChecksum{}, errors.Errorf("no checksum found (ID = %s)", id)
	}
	return c, nil
}

// Waits for the checksum to be available or for the checksum to start computing.
// If we waited for 10% of the deadline and it has not started, then it's
// unlikely to start because this replica is most likely being restored from
// snapshots.
func (r *Replica) checksumInitialWait(
	ctx context.Context, id uuid.UUID, notify chan struct{},
) (bool, error) {
	d, dOk := ctx.Deadline()
	// The max wait time should be 5 seconds, so we dont end up waiting for
	// minutes for a huge range.
	maxInitialWait := 5 * time.Second
	var initialWait <-chan time.Time
	if dOk {
		duration := time.Duration(timeutil.Until(d).Nanoseconds() / 10)
		if duration > maxInitialWait {
			duration = maxInitialWait
		}
		initialWait = time.After(duration)
	} else {
		initialWait = time.After(maxInitialWait)
	}
	return r.checksumWait(ctx, id, notify, initialWait)
}

// checksumWait waits for the checksum to be available or for the computation
// to start  within the initialWait time. The bool return flag is used to
// indicate if a checksum is available (true) or if the initial wait has expired
// and the caller should wait more, since the checksum computation started.
func (r *Replica) checksumWait(
	ctx context.Context, id uuid.UUID, notify chan struct{}, initialWait <-chan time.Time,
) (bool, error) {
	// Wait
	select {
	case <-r.store.Stopper().ShouldQuiesce():
		return false,
			errors.Errorf("store quiescing while waiting for compute checksum (ID = %s)", id)
	case <-ctx.Done():
		return false,
			errors.Wrapf(ctx.Err(), "while waiting for compute checksum (ID = %s)", id)
	case <-initialWait:
		{
			r.mu.Lock()
			started := r.mu.checksums[id].started
			r.mu.Unlock()
			if !started {
				return false,
					errors.Errorf("checksum computation did not start in time for (ID = %s)", id)
			}
			return false, nil
		}
	case <-notify:
		return true, nil
	}
}

// computeChecksumDone adds the computed checksum, sets a deadline for GCing the
// checksum, and sends out a notification.
func (r *Replica) computeChecksumDone(
	ctx context.Context, id uuid.UUID, result *replicaHash, snapshot *roachpb.RaftSnapshotData,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.mu.checksums[id]; ok {
		if result != nil {
			c.Checksum = result.SHA512[:]

			delta := result.PersistedMS
			delta.Subtract(result.RecomputedMS)
			c.Delta = enginepb.MVCCStatsDelta(delta)
			c.Persisted = result.PersistedMS
		}
		c.gcTimestamp = timeutil.Now().Add(replicaChecksumGCInterval)
		c.Snapshot = snapshot
		r.mu.checksums[id] = c
		// Notify
		close(c.notify)
	} else {
		// ComputeChecksum adds an entry into the map, and the entry can
		// only be GCed once the gcTimestamp is set above. Something
		// really bad happened.
		log.Errorf(ctx, "no map entry for checksum (ID = %s)", id)
	}
}

type replicaHash struct {
	SHA512                    [sha512.Size]byte
	PersistedMS, RecomputedMS enginepb.MVCCStats
}

// sha512 computes the SHA512 hash of all the replica data at the snapshot.
// It will dump all the kv data into snapshot if it is provided.
func (*Replica) sha512(
	ctx context.Context,
	desc roachpb.RangeDescriptor,
	snap storage.Reader,
	snapshot *roachpb.RaftSnapshotData,
	mode roachpb.ChecksumMode,
	limiter *quotapool.RateLimiter,
) (*replicaHash, error) {
	statsOnly := mode == roachpb.ChecksumMode_CHECK_STATS

	// Iterate over all the data in the range.
	var alloc bufalloc.ByteAllocator
	var intBuf [8]byte
	var legacyTimestamp hlc.LegacyTimestamp
	var timestampBuf []byte
	hasher := sha512.New()

	visitor := func(unsafeKey storage.MVCCKey, unsafeValue []byte) error {
		// Rate Limit the scan through the range
		if err := limiter.WaitN(ctx, int64(len(unsafeKey.Key)+len(unsafeValue))); err != nil {
			return err
		}

		if snapshot != nil {
			// Add (a copy of) the kv pair into the debug message.
			kv := roachpb.RaftSnapshotData_KeyValue{
				Timestamp: unsafeKey.Timestamp,
			}
			alloc, kv.Key = alloc.Copy(unsafeKey.Key, 0)
			alloc, kv.Value = alloc.Copy(unsafeValue, 0)
			snapshot.KV = append(snapshot.KV, kv)
		}

		// Encode the length of the key and value.
		binary.LittleEndian.PutUint64(intBuf[:], uint64(len(unsafeKey.Key)))
		if _, err := hasher.Write(intBuf[:]); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(intBuf[:], uint64(len(unsafeValue)))
		if _, err := hasher.Write(intBuf[:]); err != nil {
			return err
		}
		if _, err := hasher.Write(unsafeKey.Key); err != nil {
			return err
		}
		legacyTimestamp = unsafeKey.Timestamp.ToLegacyTimestamp()
		if size := legacyTimestamp.Size(); size > cap(timestampBuf) {
			timestampBuf = make([]byte, size)
		} else {
			timestampBuf = timestampBuf[:size]
		}
		if _, err := protoutil.MarshalTo(&legacyTimestamp, timestampBuf); err != nil {
			return err
		}
		if _, err := hasher.Write(timestampBuf); err != nil {
			return err
		}
		_, err := hasher.Write(unsafeValue)
		return err
	}

	var ms enginepb.MVCCStats
	// In statsOnly mode, we hash only the RangeAppliedState. In regular mode, hash
	// all of the replicated key space.
	if !statsOnly {
		// Do not want the lock table ranges since the iter has been constructed
		// using MVCCKeyAndIntentsIterKind.
		//
		// TODO(sumeer): When we have replicated locks other than exclusive locks,
		// we will probably not have any interleaved intents so we could stop
		// using MVCCKeyAndIntentsIterKind and consider all locks here.
		for _, span := range rditer.MakeReplicatedKeyRangesExceptLockTable(&desc) {
			iter := snap.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind,
				storage.IterOptions{UpperBound: span.End})
			spanMS, err := storage.ComputeStatsForRange(
				iter, span.Start, span.End, 0 /* nowNanos */, visitor,
			)
			iter.Close()
			if err != nil {
				return nil, err
			}
			ms.Add(spanMS)
		}
	}

	var result replicaHash
	result.RecomputedMS = ms

	rangeAppliedState, err := stateloader.Make(desc.RangeID).LoadRangeAppliedState(ctx, snap)
	if err != nil {
		return nil, err
	}
	result.PersistedMS = rangeAppliedState.RangeStats.ToStats()

	if statsOnly {
		b, err := protoutil.Marshal(&rangeAppliedState)
		if err != nil {
			return nil, err
		}
		if snapshot != nil {
			// Add LeaseAppliedState to the diff.
			kv := roachpb.RaftSnapshotData_KeyValue{
				Timestamp: hlc.Timestamp{},
			}
			kv.Key = keys.RangeAppliedStateKey(desc.RangeID)
			var v roachpb.Value
			if err := v.SetProto(&rangeAppliedState); err != nil {
				return nil, err
			}
			kv.Value = v.RawBytes
			snapshot.KV = append(snapshot.KV, kv)
		}
		if _, err := hasher.Write(b); err != nil {
			return nil, err
		}
	}

	hasher.Sum(result.SHA512[:0])

	// We're not required to do so, but it looks nicer if both stats are aged to
	// the same timestamp.
	result.RecomputedMS.AgeTo(result.PersistedMS.LastUpdateNanos)

	return &result, nil
}

func (r *Replica) computeChecksumPostApply(ctx context.Context, cc kvserverpb.ComputeChecksum) {
	stopper := r.store.Stopper()
	now := timeutil.Now()
	r.mu.Lock()
	var notify chan struct{}
	if c, ok := r.mu.checksums[cc.ChecksumID]; !ok {
		// There is no record of this ID. Make a new notification.
		notify = make(chan struct{})
	} else if !c.started {
		// A CollectChecksumRequest is waiting on the existing notification.
		notify = c.notify
	} else {
		log.Fatalf(ctx, "attempted to apply ComputeChecksum command with duplicated checksum ID %s",
			cc.ChecksumID)
	}

	r.gcOldChecksumEntriesLocked(now)

	// Create an entry with checksum == nil and gcTimestamp unset.
	r.mu.checksums[cc.ChecksumID] = replicaChecksum{started: true, notify: notify}
	desc := *r.mu.state.Desc
	r.mu.Unlock()

	if cc.Version != batcheval.ReplicaChecksumVersion {
		r.computeChecksumDone(ctx, cc.ChecksumID, nil, nil)
		log.Infof(ctx, "incompatible ComputeChecksum versions (requested: %d, have: %d)",
			cc.Version, batcheval.ReplicaChecksumVersion)
		return
	}

	// Caller is holding raftMu, so an engine snapshot is automatically
	// Raft-consistent (i.e. not in the middle of an AddSSTable).
	snap := r.store.engine.NewSnapshot()
	if cc.Checkpoint {
		sl := stateloader.Make(r.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, snap)
		if err != nil {
			log.Warningf(ctx, "unable to load applied index, continuing anyway")
		}
		// NB: the names here will match on all nodes, which is nice for debugging.
		tag := fmt.Sprintf("r%d_at_%d", r.RangeID, as.RaftAppliedIndex)
		if dir, err := r.store.checkpoint(ctx, tag); err != nil {
			log.Warningf(ctx, "unable to create checkpoint %s: %+v", dir, err)
		} else {
			log.Warningf(ctx, "created checkpoint %s", dir)
		}
	}

	// Compute SHA asynchronously and store it in a map by UUID.
	// Don't use the proposal's context for this, as it likely to be canceled very
	// soon.
	if err := stopper.RunAsyncTask(r.AnnotateCtx(context.Background()), "storage.Replica: computing checksum", func(ctx context.Context) {
		func() {
			defer snap.Close()
			var snapshot *roachpb.RaftSnapshotData
			if cc.SaveSnapshot {
				snapshot = &roachpb.RaftSnapshotData{}
			}

			result, err := r.sha512(ctx, desc, snap, snapshot, cc.Mode, r.store.consistencyLimiter)
			if err != nil {
				log.Errorf(ctx, "%v", err)
				result = nil
			}
			r.computeChecksumDone(ctx, cc.ChecksumID, result, snapshot)
		}()

		var shouldFatal bool
		for _, rDesc := range cc.Terminate {
			if rDesc.StoreID == r.store.StoreID() && rDesc.ReplicaID == r.replicaID {
				shouldFatal = true
			}
		}

		if shouldFatal {
			// This node should fatal as a result of a previous consistency
			// check (i.e. this round is carried out only to obtain a diff).
			// If we fatal too early, the diff won't make it back to the lease-
			// holder and thus won't be printed to the logs. Since we're already
			// in a goroutine that's about to end, simply sleep for a few seconds
			// and then terminate.
			auxDir := r.store.engine.GetAuxiliaryDir()
			_ = r.store.engine.MkdirAll(auxDir)
			path := base.PreventedStartupFile(auxDir)

			const attentionFmt = `ATTENTION:

this node is terminating because a replica inconsistency was detected between %s
and its other replicas. Please check your cluster-wide log files for more
information and contact the CockroachDB support team. It is not necessarily safe
to replace this node; cluster data may still be at risk of corruption.

A checkpoints directory to aid (expert) debugging should be present in:
%s

A file preventing this node from restarting was placed at:
%s
`
			preventStartupMsg := fmt.Sprintf(attentionFmt, r, auxDir, path)
			if err := fs.WriteFile(r.store.engine, path, []byte(preventStartupMsg)); err != nil {
				log.Warningf(ctx, "%v", err)
			}

			if p := r.store.cfg.TestingKnobs.ConsistencyTestingKnobs.OnBadChecksumFatal; p != nil {
				p(*r.store.Ident)
			} else {
				time.Sleep(10 * time.Second)
				log.Fatalf(r.AnnotateCtx(context.Background()), attentionFmt, r, auxDir, path)
			}
		}

	}); err != nil {
		defer snap.Close()
		log.Errorf(ctx, "could not run async checksum computation (ID = %s): %v", cc.ChecksumID, err)
		// Set checksum to nil.
		r.computeChecksumDone(ctx, cc.ChecksumID, nil, nil)
	}
}
