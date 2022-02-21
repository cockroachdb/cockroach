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
	"sync/atomic"
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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
				log.Fatalf(ctx, "found a delta of %+v", log.Safe(delta))
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
				if ctx.Err() != nil {
					// If our Context has timed out, we still want to visit all of the replicas to
					// terminate their checksum computations. A 1s timeout is enough to get the RPC
					// onto the other Replica, where the cancellation will then short-circuit the
					// in-progress cancellation.
					var cancel func()
					ctx, cancel = context.WithTimeout(r.AnnotateCtx(context.Background()), time.Second) // nolint:context
					defer cancel()
				}
				resp, err := r.collectChecksumFromReplica(ctx, replica, ccRes.ChecksumID, masterChecksum)
				resultCh <- ConsistencyCheckResult{
					Replica:  replica,
					Response: resp,
					Err:      err,
				}
			}); err != nil {
			wg.Done()
			// If we can't start tasks, the store is draining. Just return the error verbatim.
			return nil, err
		}

		// Collect the master result eagerly so that we can send a SHA in the
		// remaining requests (this is used for logging inconsistencies on the
		// remote nodes only).
		if len(results) == 0 {
			wg.Wait()
			result := <-resultCh
			// It's tempting to return early if we can't "even" collect the local
			// checksum, but we want to collect checksums to make sure there aren't
			// any consistency check computations left running when this consistency
			// check wraps up.
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

// replicaChecksum contains progress on a replica checksum computation.
type replicaChecksum struct {
	deadline time.Time // once in the past, entry is considered abandoned

	cancel atomic.Value // func(); non-nil iff computation started

	notify                  chan struct{} // closed when computation done/failed
	CollectChecksumResponse               // immutable on close(notify)
}

// SignalStart must be called only once. It indicates that the consistency
// checksum computation is now ongoing, signalling WaitStarted. The provided
// cancel function aborts the computation.
func (c *replicaChecksum) SignalStart(cancel func()) {
	c.cancel.Store(cancel)
}

// SignalResult must be called only once, and after SignalStart. It populates
// the replicaChecksum with the result of the checksum computation, and signals
// any waiter(s) on WaitResult.
func (c *replicaChecksum) SignalResult(res *replicaHash, snapData *roachpb.RaftSnapshotData) {
	c.deadline = timeutil.Now().Add(replicaChecksumGCInterval)
	if res != nil {
		c.Checksum = res.SHA512[:]
		delta := res.PersistedMS
		delta.Subtract(res.RecomputedMS)
		c.Delta = enginepb.MVCCStatsDelta(delta)
		c.Persisted = res.PersistedMS
		c.Snapshot = snapData
	}
	close(c.notify)
}

// WaitStarted waits for up to maxWait for the checksum computation to begin. If
// this does not occur, or the Context cancels, returns an error. As a special
// case, a zero maxWait disables the corresponding upper limit.
func (c *replicaChecksum) WaitStarted(ctx context.Context, maxWait time.Duration) error {
	var ch <-chan time.Time
	if maxWait > 0 {
		ch = time.After(maxWait)
	}
	// Wait
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		if c.cancel.Load() == nil {
			return errors.Errorf("max wait of %.2fs exceeded", maxWait.Seconds())
		}
		return nil
	case <-c.notify:
		// Not only did it start, it finished.
		return nil
	}
}

// WaitResult waits until the result of the checksum computation is available.
// Callers should invoke WaitStarted before (and receive a nil error from it) to
// avoid waiting for a computation that will never even begin. WaitResult
// respects context cancellation.
func (c *replicaChecksum) WaitResult(ctx context.Context) (*CollectChecksumResponse, error) {
	if err := c.WaitStarted(ctx, 0 /* maxWait */); err != nil {
		return nil, err
	}
	if len(c.CollectChecksumResponse.Checksum) == 0 {
		return nil, errors.New("no checksum found")
	}
	return &c.CollectChecksumResponse, nil
}

type checksumStorage struct {
	mu struct {
		syncutil.Mutex
		m map[uuid.UUID]*replicaChecksum
	}
}

// GetOrInit returns the checksum computation, creating it with the supplied
// deadline if necessary. Once the deadline is elapsed, calls to GC will remove
// the computation. When the entry already exists and the provided deadine is
// stricter, it will replace the previous deadline.
func (css *checksumStorage) GetOrInit(id uuid.UUID, deadline time.Time) *replicaChecksum {
	css.mu.Lock()
	defer css.mu.Unlock()

	if css.mu.m == nil {
		// "Make the zero value useful".
		css.mu.m = map[uuid.UUID]*replicaChecksum{}
	}

	c, ok := css.mu.m[id]
	if !ok {
		c = &replicaChecksum{
			notify:   make(chan struct{}),
			deadline: deadline,
		}
		css.mu.m[id] = c
	}
	if c.deadline.After(deadline) {
		c.deadline = deadline
	}
	return c
}

// Delete removes a checksum computation, canceling it if it has already started.
func (css *checksumStorage) Delete(id uuid.UUID) (existed bool) {
	css.mu.Lock()
	defer css.mu.Unlock()
	return css.delLocked(id)
}

func (css *checksumStorage) delLocked(id uuid.UUID) (existed bool) {
	c, existed := css.mu.m[id]
	if !existed {
		return false
	}
	delete(css.mu.m, id)
	if f := c.cancel.Load(); f != nil {
		f.(func())()
	}
	return true // existed
}

func (css *checksumStorage) GC(now time.Time) {
	css.mu.Lock()
	defer css.mu.Unlock()

	for id, c := range css.mu.m {
		if c.deadline.Before(now) {
			css.delLocked(id)
		}
	}
}

// getChecksum waits for the result of ComputeChecksum and returns it. An error
// is returned if the checksum could not be obtained. This method should only be
// called once per computation as it cleans up the in-memory state, including
// cancelling the inflight computation upon return if necessary (in the case of
// ctx cancellation).
func getChecksum(
	ctx context.Context, css *checksumStorage, id uuid.UUID,
) (*CollectChecksumResponse, error) {
	tBegin := timeutil.Now()
	defer css.GC(tBegin)

	deadline := tBegin.Add(replicaChecksumGCInterval)
	if t, ok := ctx.Deadline(); ok && t.Before(deadline) {
		deadline = t
	}

	c := css.GetOrInit(id, deadline)
	// Whether the consistency check is still running or will
	// never happen is immaterial; if we are no longer waiting
	// for the result, stop tracking the computation. This is
	// important, see:
	//
	// https://github.com/cockroachdb/cockroach/pull/75448
	defer css.Delete(id)

	maxWait := 5 * time.Second
	if dur := deadline.Sub(tBegin); dur < maxWait && dur > 0 {
		maxWait = dur
	}
	if err := c.WaitStarted(ctx, maxWait); err != nil {
		return nil, errors.Wrapf(err, "waiting for checksum %s to start", id.Short())
	}
	// Checksum computation has started, so commit to waiting for its completion.
	return c.WaitResult(ctx)
}

type replicaHash struct {
	SHA512                    [sha512.Size]byte
	PersistedMS, RecomputedMS enginepb.MVCCStats
}

// sha512Checksum computes the SHA512 hash of all the replica data at the snapshot.
// It will dump all the kv data into snapshot if it is provided.
func sha512Checksum(
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
	css := &r.checksumStorage
	tBegin := timeutil.Now()
	css.GC(tBegin)

	// NB: we are careful in this method to not tie `ctx` to the computation,
	// as we are intentionally spawning a possibly long-running task below.
	c := css.GetOrInit(cc.ChecksumID, tBegin.Add(replicaChecksumGCInterval))

	if cc.Version != batcheval.ReplicaChecksumVersion {
		c.SignalResult(nil /* hash */, nil /* snap */)
		return
	}

	// Caller is holding raftMu, so an engine snapshot is automatically
	// Raft-consistent (i.e. not in the middle of an AddSSTable or descriptor
	// change).
	snap := r.store.engine.NewSnapshot()
	desc := *r.Desc()
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
	if err := r.store.stopper.RunAsyncTask(
		r.AnnotateCtx(context.Background()), "storage.Replica: computing checksum", func(ctx context.Context) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			c.SignalStart(cancel)
			func() {
				defer snap.Close()
				var raftSnapData *roachpb.RaftSnapshotData
				if cc.SaveSnapshot {
					raftSnapData = &roachpb.RaftSnapshotData{}
				}

				hash, err := sha512Checksum(ctx, desc, snap, raftSnapData, cc.Mode, r.store.consistencyLimiter)
				if err != nil {
					log.Errorf(ctx, "%v", err)
					hash, raftSnapData = nil, nil
				}
				c.SignalResult(hash, raftSnapData)
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
		snap.Close()
		log.Errorf(ctx, "could not run async checksum computation (ID = %s): %v", cc.ChecksumID, err)
		c.SignalResult(nil, nil /* snap */)
	}
}
