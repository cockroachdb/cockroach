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
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// How long to keep consistency checker checksums in-memory for collection.
// Typically a long-poll waits for the result of the computation, so it's almost
// immediately collected.
//
// Up to 22.1, the consistency check initiator used to synchronously collect the
// first replica's checksum before all others, so checksum collection requests
// could arrive late if the first one was slow. Since 22.2, all requests are
// parallel and likely arrive quickly.
//
// TODO(pavelkalinnikov): Consider removing GC behaviour in 23.1+, when all the
// incoming requests are from 22.2+ nodes (hence arrive timely).
const replicaChecksumGCInterval = time.Hour

// fatalOnStatsMismatch, if true, turns stats mismatches into fatal errors. A
// stats mismatch is the event in which
//   - the consistency checker finds that all replicas are consistent
//     (i.e. byte-by-byte identical)
//   - the (identical) stats tracked in them do not correspond to a recomputation
//     via the data, i.e. the stats were incorrect
//   - ContainsEstimates==false, i.e. the stats claimed they were correct.
//
// Before issuing the fatal error, the cluster bootstrap version is verified.
// We know that old versions of CockroachDB sometimes violated this invariant,
// but we want to exclude these violations, focusing only on cases in which we
// know old CRDB versions (<19.1 at time of writing) were not involved.
var fatalOnStatsMismatch = envutil.EnvOrDefaultBool("COCKROACH_ENFORCE_CONSISTENT_STATS", false)

// replicaChecksum contains progress on a replica checksum computation.
type replicaChecksum struct {
	// started is closed when the checksum computation has started. If the start
	// was successful, passes a function that can be used by the receiver to stop
	// the computation, otherwise is closed immediately.
	started chan context.CancelFunc
	// result passes a single checksum computation result from the task.
	// INVARIANT: result is written to or closed only if started is closed.
	result chan CollectChecksumResponse
	// A non-zero gcTimestamp means this tracker is "inactive", i.e. either the
	// computation task completed/failed, or the checksum collection request
	// returned. A tracker is deleted from the state when both participants have
	// learnt about it, or gcTimestamp passes, whichever happens first.
	gcTimestamp time.Time
}

// CheckConsistency runs a consistency check on the range. It first applies a
// ComputeChecksum through Raft and then issues CollectChecksum commands to the
// other replicas. These are inspected and a CheckConsistencyResponse is assembled.
//
// When req.Mode is CHECK_VIA_QUEUE and an inconsistency is detected, the
// consistency check will be re-run to collect a diff, which is then printed
// before calling `log.Fatal`. This behavior should be lifted to the consistency
// checker queue in the future.
func (r *Replica) CheckConsistency(
	ctx context.Context, req roachpb.CheckConsistencyRequest,
) (roachpb.CheckConsistencyResponse, *roachpb.Error) {
	return r.checkConsistencyImpl(ctx, roachpb.ComputeChecksumRequest{
		RequestHeader: roachpb.RequestHeader{Key: r.Desc().StartKey.AsRawKey()},
		Version:       batcheval.ReplicaChecksumVersion,
		Mode:          req.Mode,
	})
}

func (r *Replica) checkConsistencyImpl(
	ctx context.Context, args roachpb.ComputeChecksumRequest,
) (roachpb.CheckConsistencyResponse, *roachpb.Error) {
	isQueue := args.Mode == roachpb.ChecksumMode_CHECK_VIA_QUEUE

	results, err := r.runConsistencyCheck(ctx, args)
	if err != nil {
		return roachpb.CheckConsistencyResponse{}, roachpb.NewError(err)
	}

	res := roachpb.CheckConsistencyResponse_Result{RangeID: r.RangeID}

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
		// The Persisted stats are covered by the SHA computation, so if all the
		// hashes match, we can take an arbitrary one that succeeded.
		res.Detail += fmt.Sprintf("stats: %+v\n", results[0].Response.Persisted)
	}
	for _, result := range missing {
		res.Detail += fmt.Sprintf("%s: error: %v\n", result.Replica, result.Err)
	}

	// NB: delta is examined only when minoritySHA == "", i.e. all the checksums
	// match. It helps to further check that the recomputed MVCC stats match the
	// stored stats.
	//
	// Both Persisted and Delta stats were computed deterministically from the
	// data fed into the checksum, so if all checksums match, we can take the
	// stats from an arbitrary replica that succeeded.
	//
	// TODO(pavelkalinnikov): Compare deltas to assert this assumption anyway.
	delta := enginepb.MVCCStats(results[0].Response.Delta)
	var haveDelta bool
	{
		d2 := delta
		d2.AgeTo(0)
		haveDelta = d2 != enginepb.MVCCStats{}
	}

	res.StartKey = []byte(args.Key)
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
			RequestHeader: roachpb.RequestHeader{Key: args.Key},
		}

		var b kv.Batch
		b.AddRawRequest(&req)

		err := r.store.db.Run(ctx, &b)
		return resp, roachpb.NewError(err)
	}

	if args.Snapshot {
		// A diff was already printed. Return because all the code below will do
		// is request another consistency check, with a diff and with
		// instructions to terminate the minority nodes.
		log.Errorf(ctx, "consistency check failed")
		return resp, nil
	}

	// No diff was printed, so we want to re-run the check with snapshots
	// requested, to build the diff. Note that this recursive call will be
	// terminated in the `args.Snapshot` branch above.
	args.Snapshot = true
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

	if _, pErr := r.checkConsistencyImpl(ctx, args); pErr != nil {
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
	ctx context.Context, replica roachpb.ReplicaDescriptor, id uuid.UUID, withSnap bool,
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
		WithSnapshot:       withSnap,
	}
	resp, err := client.CollectChecksum(ctx, req)
	if err != nil {
		return CollectChecksumResponse{}, err
	}
	return *resp, nil
}

// runConsistencyCheck carries out a round of ComputeChecksum/CollectChecksum
// for the members of this range, returning the results (which it does not act
// upon). Requires that the computation succeeds on at least one replica, and
// puts an arbitrary successful result first in the returned slice.
func (r *Replica) runConsistencyCheck(
	ctx context.Context, req roachpb.ComputeChecksumRequest,
) ([]ConsistencyCheckResult, error) {
	// Send a ComputeChecksum which will trigger computation of the checksum on
	// all replicas.
	res, pErr := kv.SendWrapped(ctx, r.store.db.NonTransactionalSender(), &req)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	ccRes := res.(*roachpb.ComputeChecksumResponse)

	replicas := r.Desc().Replicas().Descriptors()
	resultCh := make(chan ConsistencyCheckResult, len(replicas))
	results := make([]ConsistencyCheckResult, 0, len(replicas))

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)

	defer close(resultCh) // close the channel when
	defer wg.Wait()       // writers have terminated
	defer cancel()        // but cancel them first
	// P.S. Have you noticed the Haiku?

	for _, replica := range replicas {
		wg.Add(1)
		replica := replica // per-iteration copy for the goroutine
		if err := r.store.Stopper().RunAsyncTask(ctx, "storage.Replica: checking consistency",
			func(ctx context.Context) {
				defer wg.Done()
				resp, err := r.collectChecksumFromReplica(ctx, replica, ccRes.ChecksumID, req.Snapshot)
				resultCh <- ConsistencyCheckResult{
					Replica:  replica,
					Response: resp,
					Err:      err,
				}
			},
		); err != nil {
			// If we can't start tasks, the node is likely draining. Return the error
			// verbatim, after all the started tasks are stopped.
			wg.Done()
			return nil, err
		}
	}

	// Collect the results from all replicas, while the tasks are running.
	for result := range resultCh {
		results = append(results, result)
		// If it was the last request, don't wait on the channel anymore.
		if len(results) == len(replicas) {
			break
		}
	}
	// Find any successful result, and put it first.
	for i, res := range results {
		if res.Err == nil {
			results[0], results[i] = res, results[0]
			return results, nil
		}
	}
	return nil, errors.New("could not collect checksum from any replica")
}

func (r *Replica) gcOldChecksumEntriesLocked(now time.Time) {
	for id, val := range r.mu.checksums {
		// The timestamp is valid only if set.
		if !val.gcTimestamp.IsZero() && now.After(val.gcTimestamp) {
			delete(r.mu.checksums, id)
		}
	}
}

// getReplicaChecksum returns replicaChecksum tracker for the given ID, and
// whether it is still active (i.e. has a zero GC timestamp).
func (r *Replica) getReplicaChecksum(id uuid.UUID, now time.Time) (*replicaChecksum, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gcOldChecksumEntriesLocked(now)
	c := r.mu.checksums[id]
	if c == nil {
		c = &replicaChecksum{
			started: make(chan context.CancelFunc, 1),      // allow an async send
			result:  make(chan CollectChecksumResponse, 1), // allow an async send
		}
		r.mu.checksums[id] = c
	}
	return c, c.gcTimestamp.IsZero()
}

// gcReplicaChecksum schedules GC to remove the given replicaChecksum from the
// state after replicaChecksumGCInterval passes from now, or removes immediately
// if it is no longer active.
//
// Each user of replicaChecksum (at most two during its lifetime: sender and
// receiver; in any order) must call this method exactly once when they finish
// working on this tracker.
//
// The guarantee: both parties see the same tracker iff neither of them arrives
// at it (by calling getReplicaChecksum) later than GC timeout past the moment
// when the other left it (by calling gcReplicaChecksum).
func (r *Replica) gcReplicaChecksum(id uuid.UUID, rc *replicaChecksum) {
	// TODO(pavelkalinnikov): Avoid locking, use atomics.
	r.mu.Lock()
	defer r.mu.Unlock()
	// If the tracker is inactive (GC is already scheduled) then the counterparty
	// has abandoned this tracker, and will not come back to it. Remove it then.
	if !rc.gcTimestamp.IsZero() {
		delete(r.mu.checksums, id)
	} else { // otherwise give the counterparty some time to see this tracker
		rc.gcTimestamp = timeutil.Now().Add(replicaChecksumGCInterval)
	}
}

// getChecksum waits for the result of ComputeChecksum and returns it. Returns
// an error if there is no checksum being computed for the ID, it has already
// been GC-ed, or an error happened during the computation.
func (r *Replica) getChecksum(ctx context.Context, id uuid.UUID) (CollectChecksumResponse, error) {
	now := timeutil.Now()
	c, _ := r.getReplicaChecksum(id, now)
	defer r.gcReplicaChecksum(id, c)

	// Wait for the checksum computation to start.
	var taskCancel context.CancelFunc
	select {
	case <-ctx.Done():
		return CollectChecksumResponse{},
			errors.Wrapf(ctx.Err(), "while waiting for compute checksum (ID = %s)", id)
	case <-time.After(r.checksumInitialWait(ctx)):
		return CollectChecksumResponse{},
			errors.Errorf("checksum computation did not start in time for (ID = %s)", id)
	case taskCancel = <-c.started:
		// Happy case, the computation has started.
	}
	if taskCancel == nil { // but it may have started with an error
		return CollectChecksumResponse{}, errors.Errorf("checksum task failed to start (ID = %s)", id)
	}

	// Wait for the computation result.
	select {
	case <-ctx.Done():
		taskCancel()
		return CollectChecksumResponse{},
			errors.Wrapf(ctx.Err(), "while waiting for compute checksum (ID = %s)", id)
	case c, ok := <-c.result:
		if log.V(1) {
			log.Infof(ctx, "waited for compute checksum for %s", timeutil.Since(now))
		}
		if !ok || c.Checksum == nil {
			return CollectChecksumResponse{}, errors.Errorf("no checksum found (ID = %s)", id)
		}
		return c, nil
	}
}

// checksumInitialWait returns the amount of time to wait until the checksum
// computation has started. It is set to min of 5s and 10% of the remaining time
// in the passed-in context (if it has a deadline).
//
// If it takes longer, chances are that the replica is being restored from
// snapshots, or otherwise too busy to handle this request soon.
func (*Replica) checksumInitialWait(ctx context.Context) time.Duration {
	wait := 5 * time.Second
	if d, ok := ctx.Deadline(); ok {
		if dur := time.Duration(timeutil.Until(d).Nanoseconds() / 10); dur < wait {
			wait = dur
		}
	}
	return wait
}

// computeChecksumDone sends the checksum computation result to the receiver.
func (*Replica) computeChecksumDone(
	rc *replicaChecksum, result *replicaHash, snapshot *roachpb.RaftSnapshotData,
) {
	c := CollectChecksumResponse{Snapshot: snapshot}
	if result != nil {
		c.Checksum = result.SHA512[:]
		delta := result.PersistedMS
		delta.Subtract(result.RecomputedMS)
		c.Delta = enginepb.MVCCStatsDelta(delta)
		c.Persisted = result.PersistedMS
	}

	// Sending succeeds because the channel is buffered, and there is at most one
	// computeChecksumDone per replicaChecksum. In case of a bug, another writer
	// closes the channel, so this send panics instead of deadlocking. By design.
	rc.result <- c
	close(rc.result)
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

	pointKeyVisitor := func(unsafeKey storage.MVCCKey, unsafeValue []byte) error {
		// Rate limit the scan through the range.
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

	rangeKeyVisitor := func(rangeKV storage.MVCCRangeKeyValue) error {
		// Rate limit the scan through the range.
		err := limiter.WaitN(ctx,
			int64(len(rangeKV.RangeKey.StartKey)+len(rangeKV.RangeKey.EndKey)+len(rangeKV.Value)))
		if err != nil {
			return err
		}

		if snapshot != nil {
			// Add (a copy of) the range key into the debug message.
			rkv := roachpb.RaftSnapshotData_RangeKeyValue{
				Timestamp: rangeKV.RangeKey.Timestamp,
			}
			alloc, rkv.StartKey = alloc.Copy(rangeKV.RangeKey.StartKey, 0)
			alloc, rkv.EndKey = alloc.Copy(rangeKV.RangeKey.EndKey, 0)
			alloc, rkv.Value = alloc.Copy(rangeKV.Value, 0)
			snapshot.RangeKV = append(snapshot.RangeKV, rkv)
		}

		// Encode the length of the start key and end key.
		binary.LittleEndian.PutUint64(intBuf[:], uint64(len(rangeKV.RangeKey.StartKey)))
		if _, err := hasher.Write(intBuf[:]); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(intBuf[:], uint64(len(rangeKV.RangeKey.EndKey)))
		if _, err := hasher.Write(intBuf[:]); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(intBuf[:], uint64(len(rangeKV.Value)))
		if _, err := hasher.Write(intBuf[:]); err != nil {
			return err
		}
		if _, err := hasher.Write(rangeKV.RangeKey.StartKey); err != nil {
			return err
		}
		if _, err := hasher.Write(rangeKV.RangeKey.EndKey); err != nil {
			return err
		}
		legacyTimestamp = rangeKV.RangeKey.Timestamp.ToLegacyTimestamp()
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
		_, err = hasher.Write(rangeKV.Value)
		return err
	}

	var ms enginepb.MVCCStats
	// In statsOnly mode, we hash only the RangeAppliedState. In regular mode, hash
	// all of the replicated key space.
	if !statsOnly {
		var err error
		ms, err = rditer.ComputeStatsForRangeWithVisitors(&desc, snap, 0, /* nowNanos */
			pointKeyVisitor, rangeKeyVisitor)
		if err != nil {
			return nil, err
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
		b, err := protoutil.Marshal(rangeAppliedState)
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
			if err := v.SetProto(rangeAppliedState); err != nil {
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

func (r *Replica) computeChecksumPostApply(
	ctx context.Context, cc kvserverpb.ComputeChecksum,
) (err error) {
	// Note: all exit paths must call gcReplicaChecksum.
	c, active := r.getReplicaChecksum(cc.ChecksumID, timeutil.Now())
	defer func() {
		if err != nil {
			close(c.started) // send nothing to signal that the task failed to start
			r.gcReplicaChecksum(cc.ChecksumID, c)
		}
	}()
	if !active {
		return errors.New("checksum collection request gave up")
	}
	if req, have := cc.Version, uint32(batcheval.ReplicaChecksumVersion); req != have {
		return errors.Errorf("incompatible versions (requested: %d, have: %d)", req, have)
	}

	// Capture the current range descriptor, as it may change by the time the
	// async task below runs.
	desc := *r.Desc()

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

	// Compute SHA asynchronously and store it in a map by UUID. Concurrent checks
	// share the rate limit in r.store.consistencyLimiter, so we also limit the
	// number of concurrent checks via r.store.consistencySem.
	//
	// Don't use the proposal's context for this, as it likely to be canceled very
	// soon.
	const taskName = "kvserver.Replica: computing checksum"
	sem := r.store.consistencySem
	if cc.Mode == roachpb.ChecksumMode_CHECK_STATS {
		// Stats-only checks are cheap, and the DistSender parallelizes these across
		// ranges (in particular when calling crdb_internal.check_consistency()), so
		// they don't count towards the semaphore limit.
		sem = nil
	}
	stopper := r.store.Stopper()
	taskCtx, taskCancel := stopper.WithCancelOnQuiesce(r.AnnotateCtx(context.Background()))
	if err := stopper.RunAsyncTaskEx(taskCtx, stop.TaskOpts{
		TaskName:   taskName,
		Sem:        sem,
		WaitForSem: false,
	}, func(ctx context.Context) {
		defer taskCancel()
		// There is only one writer to c.started (this task), so this doesn't block.
		// But if by mistake there is another writer, one of us closes the channel
		// eventually, and other send/close ops will crash. This is by design.
		c.started <- taskCancel
		close(c.started)

		if err := contextutil.RunWithTimeout(ctx, taskName, consistencyCheckAsyncTimeout,
			func(ctx context.Context) error {
				defer snap.Close()
				var snapshot *roachpb.RaftSnapshotData
				if cc.SaveSnapshot {
					snapshot = &roachpb.RaftSnapshotData{}
				}

				result, err := r.sha512(ctx, desc, snap, snapshot, cc.Mode, r.store.consistencyLimiter)
				if err != nil {
					result = nil
				}
				r.computeChecksumDone(c, result, snapshot)
				r.gcReplicaChecksum(cc.ChecksumID, c)
				return err
			},
		); err != nil {
			log.Errorf(ctx, "checksum computation failed: %v", err)
		}

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
		taskCancel()
		snap.Close()
		return err
	}
	return nil
}
