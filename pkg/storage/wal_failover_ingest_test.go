// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// walCountingFS wraps a vfs.FS and keeps track of how many WAL files are
// currently live, along with the high watermark of that count.
type walCountingFS struct {
	vfs.FS
	mu struct {
		syncutil.Mutex
		live          int
		highWatermark int
		created       int
	}
}

var _ vfs.FS = (*walCountingFS)(nil)

func isWALFile(name string) bool { return strings.HasSuffix(name, ".log") }

func (f *walCountingFS) Create(name string, cat vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, cat)
	if err == nil && isWALFile(name) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.mu.live++
		f.mu.created++
		if f.mu.live > f.mu.highWatermark {
			f.mu.highWatermark = f.mu.live
		}
	}
	return file, err
}

func (f *walCountingFS) Remove(name string) error {
	err := f.FS.Remove(name)
	if err == nil && isWALFile(name) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.mu.live--
	}
	return err
}

// Unwrap must be implemented explicitly; the embedded FS's implementation would
// skip a level.
func (f *walCountingFS) Unwrap() vfs.FS { return f.FS }

func (f *walCountingFS) counts() (live, highWatermark, created int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.live, f.mu.highWatermark, f.mu.created
}

// TestFlushableIngestLimitDuringWALFailover verifies that the number of WAL
// files accumulated in the failover (secondary) location stays bounded when a
// store is subjected to a long stream of ingestions while its primary
// filesystem is slow.
//
// Ingestions that overlap the flushable queue are added to that queue as
// "flushable ingests"; each one rotates the memtable and hence creates a new
// WAL. During WAL failover Pebble tolerates a longer flushable queue (so that
// commits are not stalled by a flush that is itself waiting on the stalled
// disk), but the queue must still be bounded, otherwise WALs pile up in the
// secondary location without limit.
func TestFlushableIngestLimitDuringWALFailover(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Number of sstables that are generated and ingested one by one.
	const numIngests = 300
	// Delay injected into writes of the primary WAL. This must exceed
	// storage.wal_failover.unhealthy_op_threshold so that we fail over to the
	// secondary location. It is only paid a couple of times: once we have failed
	// over, no more WAL writes go to the primary.
	const walDelay = 250 * time.Millisecond
	// Delay injected into each operation against the failover prober's file in
	// the primary directory. The prober does a Create + Write + Sync every
	// second and we must keep the observed latency above Pebble's
	// HealthyProbeLatencyThreshold (25ms) so that we never fail back to the
	// primary for the duration of the test.
	const probeDelay = 15 * time.Millisecond
	// Delay injected into other writes against the primary (the MANIFEST and
	// sstables), which throttles flushes. Note that this directly bounds how
	// fast the test can run: once the flushable queue is capped, each ingestion
	// has to wait for a flush.
	const flushDelay = 2 * time.Millisecond
	// Bound on the number of live WAL files we tolerate in the secondary
	// location: two per allowed flushable queue entry, with the queue limited to
	// MemTableStopWritesThreshold (4) times the failover multiplier (4). This is
	// a generous bound; the point is to distinguish "tens" from the "hundreds"
	// that an unbounded queue produces.
	const maxSecondaryWALs = 2 * 4 * 4

	// delaysEnabled is flipped on after the sstables have been generated, so
	// that generating them is not slowed down.
	var delaysEnabled atomic.Bool
	primaryFS := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if !delaysEnabled.Load() || op.Kind.ReadOrWrite() != errorfs.OpIsWrite {
			return nil
		}
		switch op.Kind {
		case errorfs.OpLink, errorfs.OpRemove, errorfs.OpRemoveAll:
			// Don't slow down ingestion itself (which links the sstable into the
			// store directory) or obsolete file deletion; we want to measure the
			// size of the flushable queue, not the speed of these operations.
			return nil
		}
		switch {
		case isWALFile(op.Path):
			time.Sleep(walDelay)
		case strings.HasSuffix(op.Path, "failover_source"):
			time.Sleep(probeDelay)
		default:
			time.Sleep(flushDelay)
		}
		return nil
	}))
	secondaryFS := &walCountingFS{FS: vfs.NewMem()}

	st := cluster.MakeTestingClusterSettings()
	// Disable delete pacing so that obsolete WALs are removed promptly and the
	// measured watermark reflects the flushable queue, not the deletion pacer.
	baselineDeletionRate.Override(ctx, &st.SV, 0)
	// This setting defaults to true but is randomized in test builds; the test is
	// about flushable ingests, so pin it.
	IngestAsFlushable.Override(ctx, &st.SV, true)

	env := mustInitTestEnv(t, primaryFS, "/primary")
	walCfg := storageconfig.WALFailover{
		Mode: storageconfig.WALFailoverToExplicitPath,
		Path: storageconfig.ExternalPath{Path: "/secondary"},
	}
	eng, err := Open(ctx, env, st, WALFailover(walCfg, fs.Envs{env}, secondaryFS, nil))
	require.NoError(t, err)
	defer eng.Close()

	// Generate all the sstables up-front, while the filesystem is still fast.
	// Each sstable spans ["a", "z"] so that it overlaps memtableKey (which we
	// write to the memtable before every ingestion) and is thus eligible to be
	// added to the flushable queue.
	memtableKey := roachpb.Key("m")
	value := MVCCValue{Value: roachpb.MakeValueFromString("v")}
	// Monotonically increasing wall time used for the memtable writes below; the
	// sstables use the [1, numIngests] range.
	wallTime := int64(numIngests)
	putMemtableKey := func() {
		wallTime++
		require.NoError(t, eng.PutMVCC(
			MVCCKey{Key: memtableKey, Timestamp: hlc.Timestamp{WallTime: wallTime}}, value))
	}
	sstPaths := make([]string, numIngests)
	for i := range sstPaths {
		sstPaths[i] = fmt.Sprintf("ingest-%04d.sst", i)
		f, err := env.Create(sstPaths[i], fs.UnspecifiedWriteCategory)
		require.NoError(t, err)
		w := MakeIngestionSSTWriter(ctx, st, objstorageprovider.NewFileWritable(f))
		ts := hlc.Timestamp{WallTime: int64(i + 1)}
		require.NoError(t, w.PutMVCC(MVCCKey{Key: roachpb.Key("a"), Timestamp: ts}, value))
		require.NoError(t, w.PutMVCC(MVCCKey{Key: roachpb.Key("z"), Timestamp: ts}, value))
		require.NoError(t, w.Finish())
		w.Close()
	}

	delaysEnabled.Store(true)

	// Write to the engine until we observe that a WAL has been created in the
	// secondary location, i.e. that we have failed over.
	failoverDeadline := timeutil.Now().Add(30 * time.Second)
	for {
		if _, _, created := secondaryFS.counts(); created > 0 {
			break
		}
		if timeutil.Now().After(failoverDeadline) {
			t.Fatal("timed out waiting for WAL failover to the secondary location")
		}
		putMemtableKey()
	}
	_, _, createdBeforeIngests := secondaryFS.counts()

	// Ingest the sstables one by one. Before each ingestion we write a key to
	// the memtable so that the ingestion always overlaps the flushable queue and
	// is thus eligible to be added to it, even if the queue was just drained.
	//
	// The deadline below doesn't interrupt a hung ingestion (the test timeout
	// does that); it catches the case where the injected delays make the
	// ingestions steadily too slow, and reports how far we got.
	ingestDeadline := timeutil.Now().Add(3 * time.Minute)
	for i, path := range sstPaths {
		putMemtableKey()
		require.NoError(t, eng.IngestLocalFiles(ctx, []string{path}))
		if timeutil.Now().After(ingestDeadline) {
			t.Fatalf("ingestions are too slow; completed %d out of %d", i+1, numIngests)
		}
	}

	live, highWatermark, created := secondaryFS.counts()
	m := eng.GetMetrics()
	t.Logf("secondary WAL files: high watermark %d, live at end %d, created %d",
		highWatermark, live, created)
	t.Logf("flushes: %d (as ingest: %d); ingestions: %d",
		m.Flush.Count, m.Flush.AsIngestCount, m.Ingest.Count)
	// Sanity checks; without these, the bound below could pass vacuously (e.g. if
	// we failed back to the primary, or if the ingestions never made it onto the
	// flushable queue in the first place).
	require.Greater(t, created-createdBeforeIngests, numIngests/2,
		"expected WALs to keep being created in the secondary location")
	require.Greater(t, int(m.Flush.AsIngestCount), numIngests/2,
		"expected ingestions to be added to the flushable queue")

	require.LessOrEqual(t, highWatermark, maxSecondaryWALs)
}
