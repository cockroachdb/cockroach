// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
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

// replicaChecksum contains progress on a replica checksum computation.
type replicaChecksum struct {
	// started is closed when the checksum computation has started. If the start
	// was successful, passes a function that can be used by the receiver to stop
	// the computation, otherwise is closed immediately.
	started chan context.CancelFunc
	// result passes a single checksum computation result from the task.
	// INVARIANT: result is written to or closed only if started is closed.
	result chan CollectChecksumResponse
}

// CheckConsistency runs a consistency check on the range. It first applies a
// ComputeChecksum through Raft and then issues CollectChecksum commands to the
// other replicas. These are inspected and a CheckConsistencyResponse is assembled.
//
// When req.Mode is CHECK_VIA_QUEUE and an inconsistency is detected, the
// consistency check will be re-run to save storage engine checkpoints and
// terminate suspicious nodes. This behavior should be lifted to the consistency
// checker queue in the future.
func (r *Replica) CheckConsistency(
	ctx context.Context, req kvpb.CheckConsistencyRequest,
) (kvpb.CheckConsistencyResponse, *kvpb.Error) {
	return r.checkConsistencyImpl(ctx, kvpb.ComputeChecksumRequest{
		RequestHeader: kvpb.RequestHeader{Key: r.Desc().StartKey.AsRawKey()},
		Version:       batcheval.ReplicaChecksumVersion,
		Mode:          req.Mode,
	})
}

func (r *Replica) checkConsistencyImpl(
	ctx context.Context, args kvpb.ComputeChecksumRequest,
) (kvpb.CheckConsistencyResponse, *kvpb.Error) {
	isQueue := args.Mode == kvpb.ChecksumMode_CHECK_VIA_QUEUE

	results, err := r.runConsistencyCheck(ctx, args)
	if err != nil {
		return kvpb.CheckConsistencyResponse{}, kvpb.NewError(err)
	}

	res := kvpb.CheckConsistencyResponse_Result{RangeID: r.RangeID}

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
	res.Status = kvpb.CheckConsistencyResponse_RANGE_CONSISTENT
	if minoritySHA != "" {
		res.Status = kvpb.CheckConsistencyResponse_RANGE_INCONSISTENT
	} else if args.Mode != kvpb.ChecksumMode_CHECK_STATS && haveDelta {
		if delta.ContainsEstimates > 0 {
			// When ContainsEstimates is set, it's generally expected that we'll get a different
			// result when we recompute from scratch.
			res.Status = kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_ESTIMATED
		} else {
			// When ContainsEstimates is unset, we expect the recomputation to agree with the stored stats.
			// If that's not the case, that's a problem: it could be a bug in the stats computation
			// or stats maintenance, but it could also hint at the replica having diverged from its peers.
			res.Status = kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT
		}
		res.Detail += fmt.Sprintf("delta (stats-computed): %+v\n",
			enginepb.MVCCStats(results[0].Response.Delta))
	} else if len(missing) > 0 {
		// No inconsistency was detected, but we didn't manage to inspect all replicas.
		res.Status = kvpb.CheckConsistencyResponse_RANGE_INDETERMINATE
	}
	var resp kvpb.CheckConsistencyResponse
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

		// We've found that there's something to correct; send an RecomputeStatsRequest. Note that this
		// code runs only on the lease holder (at the time of initiating the computation), so this work
		// isn't duplicated except in rare leaseholder change scenarios (and concurrent invocation of
		// RecomputeStats is allowed because these requests block on one another). Also, we're
		// essentially paced by the consistency checker so we won't call this too often.
		log.Infof(ctx, "triggering stats recomputation to resolve delta of %+v", results[0].Response.Delta)

		var b kv.Batch
		b.AddRawRequest(&kvpb.RecomputeStatsRequest{
			RequestHeader: kvpb.RequestHeader{Key: args.Key},
		})
		err := r.store.db.Run(ctx, &b)
		return resp, kvpb.NewError(err)
	}

	if args.Checkpoint {
		// A checkpoint/termination request has already been sent. Return because
		// all the code below will do is request another consistency check, with
		// instructions to make a checkpoint and to terminate the minority nodes.
		log.Errorf(ctx, "consistency check failed")
		return resp, nil
	}

	// No checkpoint was requested, so we want to re-run the check with
	// checkpoints and termination of suspicious nodes. Note that this recursive
	// call will be terminated in the `args.Checkpoint` branch above.
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
	// TODO(pavelkalinnikov): remove this now that diffs are not printed?
	defer log.TemporarilyDisableFileGCForMainLogger()()

	if _, pErr := r.checkConsistencyImpl(ctx, args); pErr != nil {
		log.Errorf(ctx, "replica inconsistency detected; second round failed: %s", pErr)
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
	ctx context.Context, replica roachpb.ReplicaDescriptor, id uuid.UUID,
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
	ctx context.Context, req kvpb.ComputeChecksumRequest,
) ([]ConsistencyCheckResult, error) {
	// Send a ComputeChecksum which will trigger computation of the checksum on
	// all replicas.
	res, pErr := kv.SendWrapped(ctx, r.store.db.NonTransactionalSender(), &req)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	ccRes := res.(*kvpb.ComputeChecksumResponse)

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
				resp, err := r.collectChecksumFromReplica(ctx, replica, ccRes.ChecksumID)
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

// trackReplicaChecksum returns replicaChecksum tracker for the given ID, and
// the corresponding cleanup function that the caller must invoke when finished
// working on this tracker.
func (r *Replica) trackReplicaChecksum(id uuid.UUID) (*replicaChecksum, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.mu.checksums[id]
	if c == nil {
		c = &replicaChecksum{
			started: make(chan context.CancelFunc),         // require send/recv sync
			result:  make(chan CollectChecksumResponse, 1), // allow an async send
		}
		r.mu.checksums[id] = c
	}
	return c, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		// Delete from the map only if it still holds the same record. Otherwise,
		// someone has already deleted and/or replaced it. This should not happen, but
		// we guard against it anyway, for clearer semantics.
		if r.mu.checksums[id] == c {
			delete(r.mu.checksums, id)
		}
	}
}

// getChecksum waits for the result of ComputeChecksum and returns it. Returns
// an error if there is no checksum being computed for the ID, it has already
// been GC-ed, or an error happened during the computation.
func (r *Replica) getChecksum(ctx context.Context, id uuid.UUID) (CollectChecksumResponse, error) {
	now := timeutil.Now()
	c, cleanup := r.trackReplicaChecksum(id)
	defer cleanup()

	// Wait for the checksum computation to start.
	dur := r.checksumInitialWait(ctx)
	var t timeutil.Timer
	t.Reset(dur)
	defer t.Stop()
	var taskCancel context.CancelFunc
	select {
	case <-ctx.Done():
		return CollectChecksumResponse{},
			errors.Wrapf(ctx.Err(), "while waiting for compute checksum (ID = %s)", id)
	case <-t.C:
		t.Read = true
		return CollectChecksumResponse{},
			errors.Errorf("checksum computation did not start in time for (ID = %s, wait=%s)", id, dur)
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
// computation has started. It is set to min of consistencyCheckSyncTimeout and
// 10% of the remaining time in the passed-in context (if it has a deadline).
//
// If it takes longer, chances are that the replica is being restored from
// snapshots, or otherwise too busy to handle this request soon.
func (*Replica) checksumInitialWait(ctx context.Context) time.Duration {
	wait := consistencyCheckSyncTimeout
	if d, ok := ctx.Deadline(); ok {
		if dur := time.Duration(timeutil.Until(d).Nanoseconds() / 10); dur < wait {
			wait = dur
		}
	}
	return wait
}

// computeChecksumDone sends the checksum computation result to the receiver.
func (*Replica) computeChecksumDone(rc *replicaChecksum, result *ReplicaDigest) {
	var c CollectChecksumResponse
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

// ReplicaDigest holds a summary of the replicated state on a replica.
type ReplicaDigest struct {
	SHA512       [sha512.Size]byte
	PersistedMS  enginepb.MVCCStats
	RecomputedMS enginepb.MVCCStats
}

// CalcReplicaDigest computes the SHA512 hash and MVCC stats of the replica data
// at the given snapshot. Depending on the mode, it either considers the full
// replicated state, or only RangeAppliedState (including MVCC stats).
func CalcReplicaDigest(
	ctx context.Context,
	desc roachpb.RangeDescriptor,
	snap storage.Reader,
	mode kvpb.ChecksumMode,
	limiter *quotapool.RateLimiter,
	settings *cluster.Settings,
) (*ReplicaDigest, error) {
	statsOnly := mode == kvpb.ChecksumMode_CHECK_STATS

	// Iterate over all the data in the range.
	var intBuf [8]byte
	var timestamp hlc.Timestamp
	var timestampBuf []byte
	var uuidBuf [uuid.Size]byte
	hasher := sha512.New()

	// Request quota from the limiter in chunks of at least targetBatchSize, to
	// amortize the overhead of the limiter when reading many small KVs.
	var batchSize int64
	const targetBatchSize = int64(256 << 10) // 256 KiB
	wait := func(size int64) error {
		if batchSize += size; batchSize < targetBatchSize {
			return nil
		}
		tokens := batchSize
		batchSize = 0
		return limiter.WaitN(ctx, tokens)
	}

	var visitors storage.ComputeStatsVisitors

	visitors.PointKey = func(unsafeKey storage.MVCCKey, unsafeValue []byte) error {
		// Rate limit the scan through the range.
		if err := wait(int64(len(unsafeKey.Key) + len(unsafeValue))); err != nil {
			return err
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
		// Encode the key.
		if _, err := hasher.Write(unsafeKey.Key); err != nil {
			return err
		}
		timestamp = unsafeKey.Timestamp
		if size := timestamp.Size(); size > cap(timestampBuf) {
			timestampBuf = make([]byte, size)
		} else {
			timestampBuf = timestampBuf[:size]
		}
		if _, err := protoutil.MarshalToSizedBuffer(&timestamp, timestampBuf); err != nil {
			return err
		}
		if _, err := hasher.Write(timestampBuf); err != nil {
			return err
		}
		// Encode the value.
		_, err := hasher.Write(unsafeValue)
		return err
	}

	visitors.RangeKey = func(rangeKV storage.MVCCRangeKeyValue) error {
		// Rate limit the scan through the range.
		err := wait(
			int64(len(rangeKV.RangeKey.StartKey) + len(rangeKV.RangeKey.EndKey) + len(rangeKV.Value)))
		if err != nil {
			return err
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
		// Encode the key.
		if _, err := hasher.Write(rangeKV.RangeKey.StartKey); err != nil {
			return err
		}
		if _, err := hasher.Write(rangeKV.RangeKey.EndKey); err != nil {
			return err
		}
		timestamp = rangeKV.RangeKey.Timestamp
		if size := timestamp.Size(); size > cap(timestampBuf) {
			timestampBuf = make([]byte, size)
		} else {
			timestampBuf = timestampBuf[:size]
		}
		if _, err := protoutil.MarshalToSizedBuffer(&timestamp, timestampBuf); err != nil {
			return err
		}
		if _, err := hasher.Write(timestampBuf); err != nil {
			return err
		}
		// Encode the value.
		_, err = hasher.Write(rangeKV.Value)
		return err
	}

	visitors.LockTableKey = func(unsafeKey storage.LockTableKey, unsafeValue []byte) error {
		// Assert that the lock is not an intent. Intents are handled by the
		// PointKey visitor function, not by the LockTableKey visitor function.
		if unsafeKey.Strength == lock.Intent {
			return errors.AssertionFailedf("unexpected intent lock in LockTableKey visitor: %s", unsafeKey)
		}
		// Rate limit the scan through the lock table.
		if err := wait(int64(len(unsafeKey.Key) + len(unsafeValue))); err != nil {
			return err
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
		// Encode the key.
		if _, err := hasher.Write(unsafeKey.Key); err != nil {
			return err
		}
		// NOTE: this is not the same strength encoding that the actual lock
		// table version uses. For that, see getByteForReplicatedLockStrength.
		strengthBuf := intBuf[:1]
		strengthBuf[0] = byte(unsafeKey.Strength)
		if _, err := hasher.Write(strengthBuf); err != nil {
			return err
		}
		copy(uuidBuf[:], unsafeKey.TxnUUID.GetBytes())
		if _, err := hasher.Write(uuidBuf[:]); err != nil {
			return err
		}
		// Encode the value.
		_, err := hasher.Write(unsafeValue)
		return err
	}

	// In statsOnly mode, we hash only the RangeAppliedState. In regular mode, hash
	// all of the replicated key space.
	var result ReplicaDigest
	if !statsOnly {
		ms, err := rditer.ComputeStatsForRangeWithVisitors(
			ctx, &desc, snap, 0 /* nowNanos */, visitors)
		// Consume the remaining quota borrowed in the visitors. Do it even on
		// iteration error, but prioritize returning the latter if it occurs.
		if wErr := limiter.WaitN(ctx, batchSize); wErr != nil && err == nil {
			err = wErr
		}
		if err != nil {
			return nil, err
		}
		result.RecomputedMS = ms
	}

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
	c, cleanup := r.trackReplicaChecksum(cc.ChecksumID)
	defer func() {
		if err != nil {
			close(c.started) // send nothing to signal that the task failed to start
			cleanup()
		}
	}()
	if req, have := cc.Version, uint32(batcheval.ReplicaChecksumVersion); req != have {
		return errors.Errorf("incompatible versions (requested: %d, have: %d)", req, have)
	}

	// Capture the current range descriptor, as it may change by the time the
	// async task below runs.
	desc := *r.Desc()

	// Caller is holding raftMu, so an engine snapshot is automatically
	// Raft-consistent (i.e. not in the middle of an AddSSTable).
	spans := rditer.MakeReplicatedKeySpans(&desc)
	var snap storage.Reader
	if r.store.cfg.SharedStorageEnabled || storage.ShouldUseEFOS(&r.ClusterSettings().SV) {
		efos := r.store.TODOEngine().NewEventuallyFileOnlySnapshot(spans)
		if util.RaceEnabled {
			ss := rditer.MakeReplicatedKeySpanSet(&desc)
			defer ss.Release()
			snap = spanset.NewEventuallyFileOnlySnapshot(efos, ss)
		} else {
			snap = efos
		}
	} else {
		snap = r.store.TODOEngine().NewSnapshot()
	}
	if cc.Checkpoint {
		sl := stateloader.Make(r.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, snap)
		if err != nil {
			log.Warningf(ctx, "unable to load applied index, continuing anyway")
		}
		// NB: the names here will match on all nodes, which is nice for debugging.
		tag := fmt.Sprintf("r%d_at_%d", r.RangeID, as.RaftAppliedIndex)
		spans := r.store.checkpointSpans(&desc)
		log.Warningf(ctx, "creating checkpoint %s with spans %+v", tag, spans)
		if dir, err := r.store.checkpoint(tag, spans); err != nil {
			log.Warningf(ctx, "unable to create checkpoint %s: %+v", tag, err)
		} else {
			log.Warningf(ctx, "created checkpoint %s", dir)
		}
	}

	// Compute SHA asynchronously and store it in a map by UUID. Concurrent checks
	// share the rate limit in r.store.consistencyLimiter, so if too many run at
	// the same time, chances are they will time out.
	//
	// Each node's consistency queue runs a check for one range at a time, which
	// it broadcasts to all replicas, so the average number of incoming in-flight
	// collection requests per node is equal to the replication factor (typ. 3-7).
	// Abandoned tasks are canceled eagerly within a few seconds, so there is very
	// limited room for running above this figure. Thus we don't limit the number
	// of concurrent tasks here.
	//
	// NB: CHECK_STATS checks are cheap and the DistSender will parallelize them
	// across all ranges (notably when calling crdb_internal.check_consistency()).
	const taskName = "kvserver.Replica: computing checksum"
	stopper := r.store.Stopper()
	// Don't use the proposal's context, as it is likely to be canceled very soon.
	taskCtx, taskCancel := stopper.WithCancelOnQuiesce(r.AnnotateCtx(context.Background()))
	if err := stopper.RunAsyncTaskEx(taskCtx, stop.TaskOpts{
		TaskName: taskName,
	}, func(ctx context.Context) {
		defer taskCancel()
		defer snap.Close()
		defer cleanup()

		// Wait until the CollectChecksum request handler joins in and learns about
		// the starting computation, and then start it.
		if err := timeutil.RunWithTimeout(ctx, taskName, consistencyCheckSyncTimeout,
			func(ctx context.Context) error {
				// There is only one writer to c.started (this task), buf if by mistake
				// there is another writer, one of us closes the channel eventually, and
				// other writes to c.started will crash. By design.
				defer close(c.started)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case c.started <- taskCancel:
					return nil
				}
			},
		); err != nil {
			log.Errorf(ctx, "checksum collection did not join: %v", err)
		} else {
			result, err := CalcReplicaDigest(ctx, desc, snap, cc.Mode, r.store.consistencyLimiter, r.ClusterSettings())
			if err != nil {
				log.Errorf(ctx, "checksum computation failed: %v", err)
				result = nil
			}
			r.computeChecksumDone(c, result)
		}
		var shouldFatal bool
		for _, rDesc := range cc.Terminate {
			if rDesc.StoreID == r.store.StoreID() && rDesc.ReplicaID == r.replicaID {
				shouldFatal = true
				break
			}
		}
		if !shouldFatal {
			return
		}

		// This node should fatal as a result of a previous consistency check (i.e.
		// this round only saves checkpoints and kills some nodes). If we fatal too
		// early, the reply won't make it back to the leaseholder, so it will not be
		// certain of completing the check. Since we're already in a goroutine
		// that's about to end, just sleep for a few seconds and then terminate.
		auxDir := r.store.TODOEngine().GetAuxiliaryDir()
		_ = r.store.TODOEngine().Env().MkdirAll(auxDir, os.ModePerm)
		path := base.PreventedStartupFile(auxDir)

		const attentionFmt = `ATTENTION:

This node is terminating because a replica inconsistency was detected between %s
and its other replicas: %v. Please check your cluster-wide log files for more
information and contact the CockroachDB support team. It is not necessarily safe
to replace this node; cluster data may still be at risk of corruption.

A checkpoints directory to aid (expert) debugging should be present in:
%s

A file preventing this node from restarting was placed at:
%s

Checkpoints are created on each node/store hosting this range, to help
investigate the cause. Only nodes that are more likely to have incorrect data
are terminated, and usually a majority of replicas continue running. Checkpoints
are partial, i.e. contain only the data from to the inconsistent range, and
possibly its neighbouring ranges.

The storage checkpoint directories can/should be deleted when no longer needed.
They are very helpful in debugging this issue, so before deleting them, please
consider alternative actions:

- If the store has enough capacity, hold off the deletion until CRDB staff has
  diagnosed the issue.
- Back up the checkpoints for later investigation.
- If the stores are nearly full, but the cluster has enough capacity, consider
  gradually decommissioning the affected nodes, to retain the checkpoints.

To inspect the checkpoints, one can use the cockroach debug range-data tool, and
command line tools like diff. For example:

$ cockroach debug range-data --replicated data/auxiliary/checkpoints/rN_at_M N

Note that a directory that ends with "_pending" might not represent a valid
checkpoint. Such directories can exist if the node fails during checkpoint
creation. These directories should be deleted, or inspected with caution.
`
		attentionArgs := []any{r, desc.Replicas(), redact.Safe(auxDir), redact.Safe(path)}
		preventStartupMsg := fmt.Sprintf(attentionFmt, attentionArgs...)
		if err := fs.WriteFile(r.store.TODOEngine().Env(), path, []byte(preventStartupMsg), fs.UnspecifiedWriteCategory); err != nil {
			log.Warningf(ctx, "%v", err)
		}

		if p := r.store.cfg.TestingKnobs.ConsistencyTestingKnobs.OnBadChecksumFatal; p != nil {
			p(*r.store.Ident)
		} else {
			time.Sleep(10 * time.Second)
			log.Fatalf(r.AnnotateCtx(context.Background()), attentionFmt, attentionArgs...)
		}
	}); err != nil {
		taskCancel()
		snap.Close()
		return err
	}
	return nil
}
