// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

type worker struct {
	t   T
	o   writeOptions
	rng *rand.Rand
	s   *aggStats

	durabilityCallbackCount *atomic.Int64
}

func (w *worker) doWorkForReplica(ctx context.Context, r *replicaWriteState) error {
	durCBCount := w.durabilityCallbackCount.Load()
	// Do any pending truncs.
	if r.pendingTruncIndex != 0 {
		const truncLag = 1 // if truncation is requested, it happens on the next flush
		if durCBCount-r.pendingTruncCallbackCount >= truncLag {
			if err := w.doTruncForReplica(w.t, r, r.pendingTruncIndex); err != nil {
				return err
			}
			r.pendingTruncIndex = 0
		}
	}
	// Consider doing another truncation.
	if r.logSizeBytes >= w.o.cfg.TruncThresholdBytes {
		pendingTrunc := r.nextRaftLogIndex - 1
		r.logSizeBytes = 0
		if !w.o.cfg.LooseTrunc {
			// Loose approximation of CRDB today where "everything" is truncated. and
			// we rely on single engine. This wouldn't be correct with two engines.
			if err := w.doTruncForReplica(w.t, r, pendingTrunc); err != nil {
				return err
			}
		} else {
			// Somewhat suboptimal approximation of what loosely coupled truncations
			// could do. When the state machine flushes next time, we'll truncate up
			// to the (then stale) last index of the log. But we could truncate up to
			// the latest index that has been made durable. So this is not as
			// aggressive as we could be.
			r.pendingTruncIndex = pendingTrunc
			r.pendingTruncCallbackCount = durCBCount
		}
	}
	batches, keyBytes, valBytes, err := generateBatches(ctx, w.o, r, w.rng)
	if err != nil {
		return err
	}
	if err = batches.raftBatch.Commit(!w.o.cfg.RaftNoSync); err != nil {
		return err
	}
	if err = batches.smBatch.Commit(false); err != nil {
		return err
	}
	w.s.keyBytes.Add(keyBytes)
	w.s.valBytes.Add(valBytes)
	if w.s.ops.Add(1) >= w.o.cfg.NumWrites {
		return io.EOF
	}

	return nil
}

func (w *worker) doTruncForReplica(t T, r *replicaWriteState, truncIndex uint64) error {
	if false {
		logf(t, "r%d: truncIndex %d, nextIndex: %d", r.rangeID, truncIndex,
			r.nextRaftLogIndex)
	}
	raftBatch := w.o.raftEng.NewUnindexedBatch()
	for i := r.truncatedLogIndex + 1; i <= truncIndex; i++ {
		key := r.rangeIDPrefixBuf.RaftLogKey(kvpb.RaftIndex(i))
		if w.o.cfg.SingleDel {
			if err := raftBatch.SingleClearEngineKey(storage.EngineKey{Key: key}); err != nil {
				return err
			}
		} else if err := raftBatch.ClearUnversioned(key, storage.ClearOptions{}); err != nil {
			return err
		}
	}
	if err := raftBatch.Commit(false); err != nil {
		return err
	}
	w.s.truncs.Add(1)
	r.truncatedLogIndex = truncIndex
	return nil
}

func (w *worker) run(t T, q *replicaQueue, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	for {
		r := q.pop()
		err := w.doWorkForReplica(ctx, r)
		q.push(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			t.Fatal(err)
		}
		time.Sleep(w.o.cfg.DelayDur)
	}
}
