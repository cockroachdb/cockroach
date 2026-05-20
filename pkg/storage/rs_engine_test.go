// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// mockManifestChangeCommitter implements ManifestChangeCommitter for testing.
// It logs all calls to a strings.Builder for verification.
type mockManifestChangeCommitter struct {
	mu struct {
		syncutil.Mutex
		log                   strings.Builder
		nextFileNum           DiskFileNum
		lastInstalledManifest DiskFileNum
	}
}

func newMockManifestChangeCommitter(startFileNum DiskFileNum) *mockManifestChangeCommitter {
	m := &mockManifestChangeCommitter{}
	m.mu.nextFileNum = startFileNum
	return m
}

func (m *mockManifestChangeCommitter) GetFileNums(count int) ([]DiskFileNum, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start := m.mu.nextFileNum
	result := make([]DiskFileNum, count)
	for i := range count {
		result[i] = m.mu.nextFileNum
		m.mu.nextFileNum++
	}
	fmt.Fprintf(&m.mu.log, "GetFileNums(%d) => [%d, %d)\n", count, start, m.mu.nextFileNum)
	return result, nil
}

func (m *mockManifestChangeCommitter) InstallNewManifest(
	currentManifestNum DiskFileNum, manifestInfo ManifestInfo, ingestHandle interface{},
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.lastInstalledManifest = manifestInfo.Manifest.Num
	fmt.Fprintf(&m.mu.log, "InstallNewManifest(current=%d, new=%s/%d, files=%v",
		currentManifestNum, manifestInfo.Manifest.Name, manifestInfo.Manifest.Num, manifestInfo.Files)
	if ingestHandle != nil {
		flushCommit := ingestHandle.(*FlushCommitInfo)
		if flushCommit != nil {
			fmt.Fprintf(&m.mu.log, ", flushCommit={ExpectedFlushGeneration:%d, ActivateSpans:%v}",
				flushCommit.ExpectedFlushGeneration, flushCommit.ActivateSpans)
		}
	}
	fmt.Fprintf(&m.mu.log, ")\n")
	return nil
}

func (m *mockManifestChangeCommitter) getLog() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.log.String()
}

func (m *mockManifestChangeCommitter) clearLog() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.log.Reset()
}

func (m *mockManifestChangeCommitter) getLastInstalledManifest() DiskFileNum {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.lastInstalledManifest
}

// TestTestingRSEngineOpenClose verifies basic TestingRSEngine functionality.
func TestTestingRSEngineOpenClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	committer := newMockManifestChangeCommitter(100)
	opts := InnerRSEngineOptions{
		manifestChangeCommitter: committer,
		basaltFS:                fs,
		basaltDir:               "/basalt",
		basaltScratchPathDir:    "/scratch",
	}
	// Open fresh engine (manifest=0).
	rsEngine, err := OpenTestingRSEngine(0, opts)
	require.NoError(t, err)
	engine := rsEngine.(*TestingRSEngine)
	require.NotNil(t, engine)
	require.Equal(t, NoManifestNum, engine.currentManifestNum())
	// Verify directories created.
	stat, err := fs.Stat("/basalt")
	require.NoError(t, err)
	require.True(t, stat.IsDir())
	stat, err = fs.Stat("/scratch")
	require.NoError(t, err)
	require.True(t, stat.IsDir())
	engine.quiesce()
	engine.ref()
	engine.unref()
	snap := engine.newSnapshot()
	snap.Close()
	engine.closeInner()
}

// TestTestingRSEngineRefBlocksClose verifies that closeInner blocks while a ref
// is held and completes after unref.
func TestTestingRSEngineRefBlocksClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	committer := newMockManifestChangeCommitter(100)
	opts := InnerRSEngineOptions{
		manifestChangeCommitter: committer,
		basaltFS:                fs,
		basaltDir:               "/basalt",
		basaltScratchPathDir:    "/scratch",
	}
	rsEngine, err := OpenTestingRSEngine(0, opts)
	require.NoError(t, err)
	engine := rsEngine.(*TestingRSEngine)

	// Take an external ref.
	engine.ref()

	// Start async close — it should block.
	engine.closeWaitingCh = make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		engine.closeInner()
		close(doneCh)
	}()
	<-engine.closeWaitingCh
	// Verify closeInner is blocked.
	time.Sleep(time.Millisecond)
	select {
	case <-doneCh:
		t.Fatal("closeInner completed while Ref is held")
	default:
	}

	// Release the ref — Close should complete.
	engine.unref()
	<-doneCh
}

// testingRSEngineState holds state for the datadriven test.
type testingRSEngineState struct {
	t  testing.TB
	fs vfs.FS
	// For convenience, we share a ManifestChangeCommitter across different
	// ranges, even though in reality each will have its own, driving operations
	// through its raft group.
	committer *mockManifestChangeCommitter
	// engines maps rangeID name (e.g. "r1") to engine.
	engines map[string]*TestingRSEngine
	// snapshots maps snapshot name (e.g. "r1-159") to snapshot.
	snapshots map[string]*TestingRSEngineSnapshot
	// asyncCloses tracks async close operations by rangeID.
	asyncCloses map[string]chan struct{}
}

func newTestingRSEngineState(t testing.TB, startFileNum DiskFileNum) *testingRSEngineState {
	return &testingRSEngineState{
		t:           t,
		fs:          vfs.NewMem(),
		committer:   newMockManifestChangeCommitter(startFileNum),
		engines:     make(map[string]*TestingRSEngine),
		snapshots:   make(map[string]*TestingRSEngineSnapshot),
		asyncCloses: make(map[string]chan struct{}),
	}
}

func (s *testingRSEngineState) basaltDir(rangeID string) string {
	return fmt.Sprintf("/%s", rangeID)
}

func (s *testingRSEngineState) scratchDir(rangeID string) string {
	return fmt.Sprintf("/%s-scratch", rangeID)
}

func (s *testingRSEngineState) snapshotName(rangeID string, manifestNum DiskFileNum) string {
	return fmt.Sprintf("%s-%d", rangeID, manifestNum)
}

// openEngine is a wrapper that calls OpenTestingRSEngine.
func (s *testingRSEngineState) openEngine(
	rangeID string, manifestNum DiskFileNum,
) *TestingRSEngine {
	opts := InnerRSEngineOptions{
		manifestChangeCommitter: s.committer,
		basaltFS:                s.fs,
		basaltDir:               s.basaltDir(rangeID),
		basaltScratchPathDir:    s.scratchDir(rangeID),
	}
	rsEngine, err := OpenTestingRSEngine(manifestNum, opts)
	require.NoError(s.t, err)
	return rsEngine.(*TestingRSEngine)
}

// TestTestingRSEngineDatadriven runs datadriven tests for TestingRSEngine.
func TestTestingRSEngineDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var state *testingRSEngineState
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "testing_rs_engine"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				// Initialize fresh test state.
				startFileNum := DiskFileNum(dd.ScanArg[int](t, td, "start-file-num"))
				state = newTestingRSEngineState(t, startFileNum)
				return ""

			case "open":
				require.NotNil(t, state)
				rangeID := dd.ScanArg[string](t, td, "range")
				manifestNum := DiskFileNum(dd.ScanArg[int](t, td, "manifest"))
				engine := state.openEngine(rangeID, manifestNum)
				state.engines[rangeID] = engine
				return fmt.Sprintf("opened %s: manifest=%d\n", rangeID, engine.currentManifestNum())

			case "flush":
				require.NotNil(t, state)
				rangeID := dd.ScanArg[string](t, td, "range")
				fileName := dd.ScanArg[string](t, td, "file")
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				state.committer.clearLog()
				err := engine.TestFlushSSTables(fileName)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				var buf strings.Builder
				buf.WriteString(state.committer.getLog())
				// Close and reopen with new manifest from committer.
				engine.closeInner()
				newManifestNum := state.committer.getLastInstalledManifest()
				state.engines[rangeID] = state.openEngine(rangeID, newManifestNum)
				fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", rangeID, newManifestNum)
				return buf.String()

			case "snapshot":
				require.NotNil(t, state)
				rangeID := dd.ScanArg[string](t, td, "range")
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				snap := engine.newSnapshot().(*TestingRSEngineSnapshot)
				snapName := state.snapshotName(rangeID, snap.ManifestNum())
				state.snapshots[snapName] = snap
				return fmt.Sprintf("snapshot %s\n", snapName)

			case "close-snapshot":
				require.NotNil(t, state)
				snapName := dd.ScanArg[string](t, td, "name")
				snap, ok := state.snapshots[snapName]
				if !ok {
					return fmt.Sprintf("error: snapshot %s not found", snapName)
				}
				snap.Close()
				delete(state.snapshots, snapName)
				return fmt.Sprintf("closed: %s\n", snapName)

			case "split":
				require.NotNil(t, state)
				snapName := dd.ScanArg[string](t, td, "snapshot")
				splitKey := dd.ScanArg[string](t, td, "key")
				rhsRangeID := dd.ScanArg[string](t, td, "rhs-range")
				var lhsRangeID string
				snap, ok := state.snapshots[snapName]
				if !ok {
					return fmt.Sprintf("error: snapshot %s not found", snapName)
				}
				// Find LHS range from snapshot, by parsing from snapshot name:
				// r1-159->r1.
				parts := strings.Split(snapName, "-")
				if len(parts) >= 1 {
					lhsRangeID = parts[0]
				}
				rhsDir := state.basaltDir(rhsRangeID)
				state.committer.clearLog()
				lhsManifest, rhsInfo, err := snap.Split(context.Background(),
					roachpb.Key(splitKey), rhsDir)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				var buf strings.Builder
				buf.WriteString(state.committer.getLog())
				fmt.Fprintf(&buf, "lhs-manifest: %s/%d\n", lhsManifest.Name, lhsManifest.Num)
				fmt.Fprintf(&buf, "rhs-manifest: %s/%d\n", rhsInfo.Manifest.Name, rhsInfo.Manifest.Num)
				fmt.Fprintf(&buf, "rhs-files: %v\n", rhsInfo.Files)
				// Close LHS and reopen with new manifest.
				lhsEngine := state.engines[lhsRangeID]
				// First close snapshot.
				snap.Close()
				delete(state.snapshots, snapName)
				lhsEngine.closeInner()
				state.engines[lhsRangeID] = state.openEngine(lhsRangeID, lhsManifest.Num)
				// Open RHS with new manifest.
				state.engines[rhsRangeID] = state.openEngine(rhsRangeID, rhsInfo.Manifest.Num)
				fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", lhsRangeID, lhsManifest.Num)
				fmt.Fprintf(&buf, "opened %s: manifest=%d\n", rhsRangeID, rhsInfo.Manifest.Num)
				return buf.String()

			case "merge":
				require.NotNil(t, state)
				lhsSnapName := dd.ScanArg[string](t, td, "lhs-snapshot")
				rhsSnapName := dd.ScanArg[string](t, td, "rhs-snapshot")
				lhsSnap, ok := state.snapshots[lhsSnapName]
				if !ok {
					return fmt.Sprintf("error: lhs snapshot %s not found", lhsSnapName)
				}
				rhsSnap, ok := state.snapshots[rhsSnapName]
				if !ok {
					return fmt.Sprintf("error: rhs snapshot %s not found", rhsSnapName)
				}
				// Parse range IDs from snapshot names.
				lhsParts := strings.Split(lhsSnapName, "-")
				rhsParts := strings.Split(rhsSnapName, "-")
				lhsRangeID := lhsParts[0]
				rhsRangeID := rhsParts[0]
				state.committer.clearLog()
				merged, err := lhsSnap.Merge(context.Background(), rhsSnap)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				var buf strings.Builder
				buf.WriteString(state.committer.getLog())
				fmt.Fprintf(&buf, "merged-manifest: %s/%d\n", merged.Manifest.Name, merged.Manifest.Num)
				fmt.Fprintf(&buf, "merged-files: %v\n", merged.Files)
				// Close both snapshots.
				lhsSnap.Close()
				delete(state.snapshots, lhsSnapName)
				rhsSnap.Close()
				delete(state.snapshots, rhsSnapName)
				// Close RHS engine.
				rhsEngine := state.engines[rhsRangeID]
				rhsEngine.closeInner()
				delete(state.engines, rhsRangeID)
				// Close LHS and reopen with merged manifest.
				lhsEngine := state.engines[lhsRangeID]
				lhsEngine.closeInner()
				state.engines[lhsRangeID] = state.openEngine(lhsRangeID, merged.Manifest.Num)
				fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", lhsRangeID, merged.Manifest.Num)
				return buf.String()

			case "state":
				require.NotNil(t, state)
				rangeID := dd.ScanArg[string](t, td, "range")
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				return PrintTestingRSEngineState(engine)

			case "files":
				require.NotNil(t, state)
				dir := dd.ScanArg[string](t, td, "dir")
				return printFilesystem(state.fs, dir, nil)

			case "close":
				require.NotNil(t, state)
				rangeID := dd.ScanArg[string](t, td, "range")
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				engine.closeInner()
				delete(state.engines, rangeID)
				return fmt.Sprintf("closed: %s\n", rangeID)

			case "async-close":
				// Starts Close() in a goroutine and waits until it's blocked
				// waiting for snapshots to drain. Use wait-close to complete
				require.NotNil(t, state)
				rangeID := dd.ScanArg[string](t, td, "range")
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				if _, ok := state.asyncCloses[rangeID]; ok {
					return fmt.Sprintf("error: async close already in progress for %s", rangeID)
				}
				// Set up hook channel and done channel.
				engine.closeWaitingCh = make(chan struct{})
				doneCh := make(chan struct{})
				state.asyncCloses[rangeID] = doneCh
				go func() {
					engine.closeInner()
					close(doneCh)
				}()
				// Wait for Close() to signal it reached the wait point.
				<-engine.closeWaitingCh
				// Verify Close() is actually blocked by sleeping and checking
				// that doneCh is still not signaled. If Close() completes during
				// this window, it means it didn't actually block.
				time.Sleep(time.Millisecond)
				select {
				case <-doneCh:
					return fmt.Sprintf("error: close for %s completed without blocking", rangeID)
				default:
				}
				return fmt.Sprintf("async-close started: %s (blocked waiting for snapshots)\n", rangeID)

			case "wait-close":
				require.NotNil(t, state)
				rangeID := dd.ScanArg[string](t, td, "range")
				doneCh, ok := state.asyncCloses[rangeID]
				if !ok {
					return fmt.Sprintf("error: no async close in progress for %s", rangeID)
				}
				<-doneCh
				delete(state.asyncCloses, rangeID)
				delete(state.engines, rangeID)
				return fmt.Sprintf("async-close completed: %s\n", rangeID)

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}
