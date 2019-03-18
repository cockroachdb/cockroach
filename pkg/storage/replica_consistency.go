// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

var testingFatalOnStatsMismatch = envutil.EnvOrDefaultBool("COCKROACH_FATAL_ON_STATS_MISMATCH", false)

const (
	// collectChecksumTimeout controls how long we'll wait to collect a checksum
	// for a CheckConsistency request. We need to bound the time that we wait
	// because the checksum might never be computed for a replica if that replica
	// is caught up via a snapshot and never performs the ComputeChecksum
	// operation.
	collectChecksumTimeout = 15 * time.Second
)

// ReplicaChecksum contains progress on a replica checksum computation.
type ReplicaChecksum struct {
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
// other replicas. When an inconsistency is detected and no diff was requested,
// the consistency check will be re-run to collect a diff, which is then printed
// before calling `log.Fatal`.
func (r *Replica) CheckConsistency(
	ctx context.Context, args roachpb.CheckConsistencyRequest, fatalOnMismatch bool,
) (roachpb.CheckConsistencyResponse, *roachpb.Error) {
	startKey := r.Desc().StartKey.AsRawKey()

	checkArgs := roachpb.ComputeChecksumRequest{
		RequestHeader: roachpb.RequestHeader{Key: startKey},
		Version:       batcheval.ReplicaChecksumVersion,
		Snapshot:      args.WithDiff,
		Quick:         args.Quick,
	}

	results, err := r.RunConsistencyCheck(ctx, checkArgs)
	if err != nil {
		return roachpb.CheckConsistencyResponse{}, roachpb.NewError(err)
	}

	var inconsistencyCount int
	var missingCount int

	res := roachpb.CheckConsistencyResponse_Result{}
	res.RangeID = r.RangeID
	for _, result := range results {
		expResponse := results[0].Response
		if result.Err != nil || bytes.Equal(expResponse.Checksum, result.Response.Checksum) {
			missingCount++
			continue
		}
		inconsistencyCount++
		var buf bytes.Buffer
		_, _ = fmt.Fprintf(&buf, "replica %s is inconsistent: expected checksum %x, got %x",
			result.Replica, expResponse.Checksum, result.Response.Checksum)
		if expResponse.Snapshot != nil && result.Response.Snapshot != nil {
			diff := diffRange(expResponse.Snapshot, result.Response.Snapshot)
			if report := r.store.cfg.ConsistencyTestingKnobs.BadChecksumReportDiff; report != nil {
				report(*r.store.Ident, diff)
			}
			buf.WriteByte('\n')
			_, _ = diff.WriteTo(&buf)
		}
		log.Error(ctx, buf.String())
		res.Detail += buf.String()
	}

	res.Status = roachpb.CheckConsistencyResponse_RANGE_INDETERMINATE
	if inconsistencyCount != 0 {
		res.Status = roachpb.CheckConsistencyResponse_RANGE_INCONSISTENT
	} else if !args.Quick {
		res.Status = roachpb.CheckConsistencyResponse_RANGE_CONSISTENT
	}
	var resp roachpb.CheckConsistencyResponse
	resp.Result = append(resp.Result, res)

	if !fatalOnMismatch {
		return resp, nil
	}

	if inconsistencyCount == 0 {
		// The replicas were in sync. Check that the MVCCStats haven't diverged from
		// what they should be. This code originated in the realization that there
		// were many bugs in our stats computations. These are being fixed, but it
		// is through this mechanism that existing ranges are updated. Hence, the
		// logging below is relatively timid.
		delta := enginepb.MVCCStats(results[0].Response.Delta)
		delta.LastUpdateNanos = 0
		// If there's no delta (or some nodes in the cluster may not know
		// RecomputeStats, in which case sending it to them could crash them),
		// there's nothing else to do.
		if delta == (enginepb.MVCCStats{}) || !r.ClusterSettings().Version.IsActive(cluster.VersionRecomputeStats) {
			return roachpb.CheckConsistencyResponse{}, nil
		}

		if !delta.ContainsEstimates && testingFatalOnStatsMismatch {
			// ContainsEstimates is true if the replica's persisted MVCCStats had ContainsEstimates set.
			// If this was *not* the case, the replica believed it had accurate stats. But we just found
			// out that this isn't true.
			log.Fatalf(ctx, "found a delta of %+v", log.Safe(delta))
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

		var b client.Batch
		b.AddRawRequest(&req)

		err := r.store.db.Run(ctx, &b)
		return roachpb.CheckConsistencyResponse{}, roachpb.NewError(err)
	}

	logFunc := log.Fatalf
	if p := r.store.cfg.ConsistencyTestingKnobs.BadChecksumPanic; p != nil {
		if !args.WithDiff {
			// We'll call this recursively with WithDiff==true; let's let that call
			// be the one to trigger the handler.
			p(*r.store.Ident)
		}
		logFunc = log.Errorf
	}

	// Diff was printed above, so call logFunc with a short message only.
	if args.WithDiff {
		logFunc(ctx, "consistency check failed with %d inconsistent replicas", inconsistencyCount)
		return resp, nil
	}

	// No diff was printed, so we want to re-run with diff.
	// Note that this will call Fatal recursively in `CheckConsistency` (in the code above).
	log.Errorf(ctx, "consistency check failed with %d inconsistent replicas; fetching details",
		inconsistencyCount)
	args.WithDiff = true
	if _, pErr := r.CheckConsistency(ctx, args, fatalOnMismatch); pErr != nil {
		logFunc(ctx, "replica inconsistency detected; could not obtain actual diff: %s", pErr)
	}

	// Not reached except in tests.
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
	conn, err := r.store.cfg.NodeDialer.Dial(ctx, replica.NodeID)
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
	res, pErr := client.SendWrapped(ctx, r.store.db.NonTransactionalSender(), &req)
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
		orderedReplicas = append(orderedReplicas, desc.Replicas...)

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

				var resp CollectChecksumResponse
				err := contextutil.RunWithTimeout(ctx, "collect checksum", collectChecksumTimeout,
					func(ctx context.Context) error {
						var masterChecksum []byte
						if len(results) > 0 {
							masterChecksum = results[0].Response.Checksum
						}
						var err error
						resp, err = r.collectChecksumFromReplica(ctx, replica, ccRes.ChecksumID, masterChecksum)
						return err
					})
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

// getChecksum waits for the result of ComputeChecksum and returns it.
// It returns false if there is no checksum being computed for the id,
// or it has already been GCed.
func (r *Replica) getChecksum(ctx context.Context, id uuid.UUID) (ReplicaChecksum, error) {
	now := timeutil.Now()
	r.mu.Lock()
	r.gcOldChecksumEntriesLocked(now)
	c, ok := r.mu.checksums[id]
	if !ok {
		if d, dOk := ctx.Deadline(); dOk {
			c.gcTimestamp = d
		}
		c.notify = make(chan struct{})
		r.mu.checksums[id] = c
	}
	r.mu.Unlock()
	// Wait
	select {
	case <-r.store.Stopper().ShouldStop():
		return ReplicaChecksum{},
			errors.Errorf("store has stopped while waiting for compute checksum (ID = %s)", id)
	case <-ctx.Done():
		return ReplicaChecksum{},
			errors.Wrapf(ctx.Err(), "while waiting for compute checksum (ID = %s)", id)
	case <-c.notify:
	}
	if log.V(1) {
		log.Infof(ctx, "waited for compute checksum for %s", timeutil.Since(now))
	}
	r.mu.RLock()
	c, ok = r.mu.checksums[id]
	r.mu.RUnlock()
	if !ok {
		return ReplicaChecksum{}, errors.Errorf("no map entry for checksum (ID = %s)", id)
	}
	if c.Checksum == nil {
		return ReplicaChecksum{}, errors.Errorf(
			"checksum is nil, most likely because the async computation could not be run (ID = %s)", id)
	}
	return c, nil
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
		}
		c.gcTimestamp = timeutil.Now().Add(batcheval.ReplicaChecksumGCInterval)
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
func (r *Replica) sha512(
	ctx context.Context,
	desc roachpb.RangeDescriptor,
	snap engine.Reader,
	snapshot *roachpb.RaftSnapshotData,
	quick bool,
) (*replicaHash, error) {
	// Iterate over all the data in the range.
	iter := snap.NewIterator(engine.IterOptions{UpperBound: desc.EndKey.AsRawKey()})
	defer iter.Close()

	var alloc bufalloc.ByteAllocator
	var intBuf [8]byte
	var legacyTimestamp hlc.LegacyTimestamp
	var timestampBuf []byte
	hasher := sha512.New()

	visitor := func(unsafeKey engine.MVCCKey, unsafeValue []byte) error {
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
		legacyTimestamp = hlc.LegacyTimestamp(unsafeKey.Timestamp)
		if size := legacyTimestamp.Size(); size > cap(timestampBuf) {
			timestampBuf = make([]byte, size)
		} else {
			timestampBuf = timestampBuf[:size]
		}
		if _, err := protoutil.MarshalToWithoutFuzzing(&legacyTimestamp, timestampBuf); err != nil {
			return err
		}
		if _, err := hasher.Write(timestampBuf); err != nil {
			return err
		}
		_, err := hasher.Write(unsafeValue)
		return err
	}

	var ms enginepb.MVCCStats
	// In quick mode, we hash only the RangeAppliedState. In regular mode, hash
	// all of the replicated key space.
	if !quick {
		for _, span := range rditer.MakeReplicatedKeyRanges(&desc) {
			spanMS, err := engine.ComputeStatsGo(
				iter, span.Start, span.End, 0 /* nowNanos */, visitor,
			)
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

	if quick {
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
