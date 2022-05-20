// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/dustin/go-humanize"
)

var (
	singleEngine        = flag.Bool("single-engine", false, "")
	numReplicas         = flag.Int("num-replicas", 2, "")
	numSteps            = flag.Int("num-steps", 500000, "")
	looseTrunc          = flag.Bool("loose-trunc", true, "")
	memtableSize        = flag.Int("memtable-size", 64, "")
	delayDur            = flag.Duration("delay-dur", 500*time.Microsecond, "")
	walMetrics          = flag.Bool("wal-metrics", false, "")
	singleDel           = flag.Bool("single-del", false, "")
	syncSMWALBytes      = flag.Int("sync-sm-wal-bytes", 0, "")
	dir                 = flag.String("dir", "/tmp", "")
	raftL0Threshold     = flag.Int("raft-l0-threshold", 2, "")
	raftMetricsInterval = flag.Int("raft-metrics-interval", 0, "")
)

type replicaWriteState struct {
	keyPrefix        []byte
	rangeID          roachpb.RangeID
	rangeIDPrefixBuf keys.RangeIDPrefixBuf

	nextRaftLogIndex  uint64
	truncatedLogIndex uint64
	logSizeBytes      int64

	pendingTruncIndex         uint64
	pendingTruncCallbackCount int64

	buf []byte
}

type writeOptions struct {
	keyLen   int
	valueLen int
	batchLen int
	raftEng  *storage.Pebble
	smEng    *storage.Pebble
	truncLag int64
}

type writeBatches struct {
	raftBatch storage.Batch
	smBatch   storage.Batch
}

func generateBatches(
	ctx context.Context, o writeOptions, r *replicaWriteState, rng *rand.Rand,
) (writeBatches, error) {
	size := len(r.keyPrefix) + o.keyLen + o.valueLen
	if cap(r.buf) < size {
		r.buf = make([]byte, 0, size)
	}
	raftBatch := o.raftEng.NewBatch()
	smBatch := o.smEng.NewBatch()
	for i := 0; i < o.batchLen; i++ {
		r.buf = r.buf[:0]
		key := append(r.buf, r.keyPrefix...)
		randKey := key[len(key) : len(key)+o.keyLen]
		rng.Read(randKey)
		key = key[0 : len(key)+o.keyLen]
		value := r.buf[len(key):size]
		rng.Read(value)
		tmpBatch := o.smEng.NewBatch()
		var ms enginepb.MVCCStats
		err := storage.MVCCBlindPut(ctx, tmpBatch, &ms, roachpb.Key(key),
			hlc.Timestamp{WallTime: int64(r.nextRaftLogIndex)}, hlc.ClockTimestamp{},
			roachpb.Value{RawBytes: value}, nil)
		if err != nil {
			return writeBatches{}, err
		}
		batchBytes := tmpBatch.Repr()
		smBatch.ApplyBatchRepr(batchBytes, false)
		err = raftBatch.PutUnversioned(r.rangeIDPrefixBuf.RaftLogKey(r.nextRaftLogIndex), batchBytes)
		if err != nil {
			return writeBatches{}, err
		}
		r.nextRaftLogIndex++
		r.logSizeBytes += int64(len(batchBytes))
		tmpBatch.Close()
	}
	appliedState := enginepb.RangeAppliedState{RaftAppliedIndex: r.nextRaftLogIndex - 1}
	appliedStateBytes, err := protoutil.Marshal(&appliedState)
	if err != nil {
		return writeBatches{}, err
	}
	if err = smBatch.PutUnversioned(r.rangeIDPrefixBuf.RangeAppliedStateKey(), appliedStateBytes); err != nil {
		return writeBatches{}, err
	}
	return writeBatches{smBatch: smBatch, raftBatch: raftBatch}, nil
}

type worker struct {
	o   writeOptions
	rng *rand.Rand
	mu  struct {
		syncutil.Mutex
		durabilityCallbackCount int64
	}
}

const truncThreshold = 64 * 1024

func (w *worker) getDurabilityCallbackCount() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.durabilityCallbackCount
}

func (w *worker) doWorkForReplica(ctx context.Context, r *replicaWriteState) error {
	durCBCount := w.getDurabilityCallbackCount()
	// Do any pending truncs.
	if r.pendingTruncIndex != 0 {
		if durCBCount-r.pendingTruncCallbackCount >= w.o.truncLag {
			// Do the truncation
			if err := w.doTruncForReplica(ctx, r, r.pendingTruncIndex); err != nil {
				return err
			}
			r.pendingTruncIndex = 0
		}
	}
	// Consider doing another truncation.
	if r.logSizeBytes >= truncThreshold {
		// Unrelated printing of metrics.
		if *walMetrics {
			raftMetrics := w.o.raftEng.GetInternalIntervalMetrics()
			printLogWriterMetrics("raft", raftMetrics)
			if !*singleEngine {
				smMetrics := w.o.smEng.GetInternalIntervalMetrics()
				printLogWriterMetrics("sm", smMetrics)
			}
		}
		pendingTrunc := r.nextRaftLogIndex - 1
		r.logSizeBytes = 0
		if w.o.truncLag == 0 {
			if err := w.doTruncForReplica(ctx, r, pendingTrunc); err != nil {
				return err
			}
		} else {
			r.pendingTruncIndex = pendingTrunc
			r.pendingTruncCallbackCount = durCBCount
		}
	}
	batches, err := generateBatches(ctx, w.o, r, w.rng)
	if err != nil {
		return err
	}
	// Not using sync=true for raft since very slow on macos and this experiment
	// is focusing on write costs and not throughput.
	// Switched back to true since not running locally any more.
	if err = batches.raftBatch.Commit(true); err != nil {
		return err
	}
	if err = batches.smBatch.Commit(false); err != nil {
		return err
	}
	return nil
}

func printLogWriterMetrics(prefix string, m *pebble.InternalIntervalMetrics) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s logWriter: rate(peak): %s(%s) util(pending,sync): (%.2f,%.2f)",
		prefix, humanize.IBytes(uint64(m.LogWriter.WriteThroughput.Rate())),
		humanize.IBytes(uint64(m.LogWriter.WriteThroughput.PeakRate())),
		m.LogWriter.PendingBufferUtilization, m.LogWriter.SyncQueueUtilization)
	slm := m.LogWriter.SyncLatencyMicros
	if slm != nil {
		fmt.Fprintf(&b, " sync: mean: %.2f, p50,p90,p99: %d,%d,%d",
			slm.Mean(), slm.ValueAtQuantile(50), slm.ValueAtQuantile(90),
			slm.ValueAtQuantile(99))
	}
	fmt.Printf("%s\n", b.String())
}

func (w *worker) doTruncForReplica(
	ctx context.Context, r *replicaWriteState, truncIndex uint64,
) error {
	fmt.Printf("r%d: truncIndex %d, nextIndex: %d\n", r.rangeID, truncIndex,
		r.nextRaftLogIndex)
	raftBatch := w.o.raftEng.NewUnindexedBatch(true)
	for i := r.truncatedLogIndex + 1; i <= truncIndex; i++ {
		if *singleDel {
			if err := raftBatch.SingleClearEngineKey(
				storage.EngineKey{Key: r.rangeIDPrefixBuf.RaftLogKey(i)}); err != nil {
				return err
			}
		} else {
			if err := raftBatch.ClearUnversioned(r.rangeIDPrefixBuf.RaftLogKey(i)); err != nil {
				return err
			}
		}
	}
	if err := raftBatch.Commit(false); err != nil {
		return err
	}
	r.truncatedLogIndex = truncIndex
	return nil
}

func (w *worker) run(t testing.TB, q *replicaQueue, wg *sync.WaitGroup) {
	ctx := context.Background()
	i := 0
	for {
		r := q.pop()
		if r == nil {
			break
		}
		err := w.doWorkForReplica(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if *raftMetricsInterval > 0 && !*singleEngine && i%*raftMetricsInterval == 0 {
			fmt.Printf("raft%d:\n%s\n", i, w.o.raftEng.GetMetrics().String())
		}
		q.push(r)
		time.Sleep(*delayDur)
		i++
	}
	wg.Done()
}

type replicaQueue struct {
	q        chan *replicaWriteState
	maxSteps int
	mu       struct {
		syncutil.Mutex
		numSteps int
	}
}

func makeReplicaQueue(maxSteps int, numReplicas int) *replicaQueue {
	q := &replicaQueue{
		q:        make(chan *replicaWriteState, numReplicas),
		maxSteps: maxSteps,
	}
	return q
}

func (q *replicaQueue) pop() *replicaWriteState {
	q.mu.Lock()
	if q.mu.numSteps > q.maxSteps {
		q.mu.Unlock()
		return nil
	}
	q.mu.numSteps++
	q.mu.Unlock()
	return <-q.q
}

func (q *replicaQueue) push(r *replicaWriteState) {
	q.q <- r
}

func TestRaftStateMachineStorage(t *testing.T) {
	flag.Parse()
	fmt.Printf("single-engine: %t, num-replicas: %d, num-steps: %d, loose-trunc: %t\n",
		*singleEngine, *numReplicas, *numSteps, *looseTrunc)

	totalMaxCompactions := 8
	smMaxCompactions := 4
	raftMaxCompactions := 4
	fs := vfs.Default
	path := *dir + "/foo"
	fs.RemoveAll(path)
	fs.MkdirAll(path, os.ModePerm)
	var smEng, raftEng *storage.Pebble
	maxCompactions := smMaxCompactions
	if *singleEngine {
		maxCompactions = totalMaxCompactions
	}
	var copts []storage.ConfigOption
	copts = append(copts, storage.MaxConcurrentCompactions(maxCompactions),
		storage.MemtableSize(*memtableSize<<20))
	if !*singleEngine {
		copts = append(copts, storage.WALBytesPerSync(*syncSMWALBytes))
	}
	smEng, err := storage.Open(context.Background(), storage.Filesystem(path),
		copts...)
	if err != nil {
		t.Fatal(err)
	}
	defer smEng.Close()
	if *singleEngine {
		raftEng = smEng
	} else {
		raftPath := *dir + "/raft"
		fs.RemoveAll(raftPath)
		fs.MkdirAll(raftPath, os.ModePerm)
		raftEng, err = storage.Open(context.Background(), storage.Filesystem(raftPath),
			storage.MaxConcurrentCompactions(raftMaxCompactions),
			storage.MemtableSize(*memtableSize<<20),
			storage.L0CompactionThreshold(*raftL0Threshold))
		if err != nil {
			t.Fatal(err)
		}
		defer raftEng.Close()
	}

	truncLag := 0
	if *looseTrunc {
		truncLag = 1
	}
	o := writeOptions{
		keyLen:   50,
		valueLen: 1000,
		batchLen: 1,
		raftEng:  raftEng,
		smEng:    smEng,
		truncLag: int64(truncLag),
	}

	// Warm up the memtable size to 64MB, so results are easier to understand,
	// especially when we do a run with small num-steps. 256KB is the initial
	// memtable size, and doubles with each flush, so 20 flushes allows us to
	// reach 2^20*256KB = 256GB memtable which is way bigger than anyone will
	// configure using -memtable-size.
	for i := 0; i < 20; i++ {
		if i%2 == 0 {
			o.raftEng.PutUnversioned(roachpb.Key("zoo"), nil)
			o.smEng.PutUnversioned(roachpb.Key("zoo"), nil)
		} else {
			o.raftEng.ClearUnversioned(roachpb.Key("zoo"))
			o.smEng.ClearUnversioned(roachpb.Key("zoo"))
		}
		// Each flush doubles the next memtable size.
		o.raftEng.Flush()
		o.smEng.Flush()
	}

	maxSteps := *numSteps
	numReplicas := *numReplicas
	q := makeReplicaQueue(maxSteps, numReplicas)

	prefixLen := 10
	dedupRanges := make(map[string]bool)
	keyPrefix := make([]byte, prefixLen)
	for i := 0; i < numReplicas; i++ {
		for {
			rand.Read(keyPrefix)
			// Don't accidentally mix with the local keys.
			keyPrefix[0] = 'a'
			if !dedupRanges[string(keyPrefix)] {
				dedupRanges[string(keyPrefix)] = true
				break
			}
		}
		r := &replicaWriteState{
			keyPrefix:        keyPrefix,
			rangeID:          roachpb.RangeID(i + 1),
			rangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(roachpb.RangeID(i + 1)),
			nextRaftLogIndex: 1,
		}
		q.push(r)
	}

	numWorkers := 1
	var wg sync.WaitGroup
	var workers []*worker
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		w := &worker{
			o:   o,
			rng: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		workers = append(workers, w)
		go w.run(t, q, &wg)
	}
	var callbackCount int64
	o.smEng.RegisterFlushCompletedCallback(func() {
		callbackCount++
		fmt.Printf("flush callback count: %d\n", callbackCount)
		for i := 0; i < len(workers); i++ {
			workers[i].mu.Lock()
			workers[i].mu.durabilityCallbackCount = callbackCount
			workers[i].mu.Unlock()
		}
	})
	wg.Wait()
	if *singleEngine {
		fmt.Printf("before-eng:\n%s\n", o.smEng.GetMetrics().Metrics.String())
	} else {
		fmt.Printf("before-raft-eng:\n%s\n", o.raftEng.GetMetrics().Metrics.String())
		fmt.Printf("before-sm-eng:\n%s\n", o.smEng.GetMetrics().Metrics.String())
	}
	if err := o.raftEng.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := o.smEng.Compact(); err != nil {
		t.Fatal(err)
	}
	smMetrics := o.smEng.GetMetrics()
	var walWritten, compactionWritten uint64
	walWritten = smMetrics.WAL.BytesWritten
	for i := 0; i < len(smMetrics.Levels); i++ {
		compactionWritten += smMetrics.Levels[i].BytesFlushed + smMetrics.Levels[i].BytesCompacted
	}
	if *singleEngine {
		fmt.Printf("eng:\n%s\n", smMetrics.String())
	} else {
		raftMetrics := o.raftEng.GetMetrics()
		fmt.Printf("raft-eng:\n%s\n", raftMetrics.String())
		fmt.Printf("sm-eng:\n%s\n", smMetrics.String())
		walWritten += raftMetrics.WAL.BytesWritten
		for i := 0; i < len(raftMetrics.Levels); i++ {
			compactionWritten += raftMetrics.Levels[i].BytesFlushed + raftMetrics.Levels[i].BytesCompacted
		}
	}
	fmt.Printf("\n\nWrite Bytes WAL: %s, Compaction: %s, W-Amp(C/W): %.2f\n", humanize.IBytes(walWritten),
		humanize.IBytes(compactionWritten), float64(compactionWritten)/float64(walWritten))
}
