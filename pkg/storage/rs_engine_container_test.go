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

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// containerTestState holds state for the rsEngineContainer datadriven test.
type containerTestState struct {
	t          testing.TB
	fs         vfs.FS
	committer  *mockManifestChangeCommitter
	container  *rsEngineContainer
	stopper    *stop.Stopper
	scratchDir string
	// heldInner tracks inner engines that have been ref'd via ref-inner. They
	// are released in LIFO order by unref-inner.
	heldInner []innerRSEngine
	// snapshots tracks snapshots created via the snapshot command.
	snapshots  map[string]RSEngineSnapshot
	nextSnapID int
	// asyncCloseDone is non-nil when an async-close is in progress.
	asyncCloseDone chan struct{}
}

func newContainerTestState(t testing.TB, startFileNum DiskFileNum) *containerTestState {
	fs := vfs.NewMem()
	committer := newMockManifestChangeCommitter(startFileNum)
	stopper := stop.NewStopper()
	basaltDir := "/basalt"
	scratchDir := basaltDir + "/scratch"
	opts := RSEngineOptions{
		ManifestChangeCommitter: committer,
		BasaltFS:                fs,
		BasaltDir:               basaltDir,
		BasaltScratchPathDir:    scratchDir,
		LogCtx:                  context.Background(),
		TestingOpenRSEngineFunc: OpenTestingRSEngine,
		Stopper:                 stopper,
	}
	c, err := OpenRSEngine(0, opts)
	require.NoError(t, err)
	return &containerTestState{
		t:          t,
		fs:         fs,
		committer:  committer,
		container:  c.(*rsEngineContainer),
		stopper:    stopper,
		scratchDir: scratchDir,
		snapshots:  make(map[string]RSEngineSnapshot),
	}
}

func (s *containerTestState) cleanup() {
	for i := len(s.heldInner) - 1; i >= 0; i-- {
		s.heldInner[i].unref()
	}
	s.heldInner = nil
	for name, snap := range s.snapshots {
		snap.Close()
		delete(s.snapshots, name)
	}
	if s.asyncCloseDone != nil {
		<-s.asyncCloseDone
		s.asyncCloseDone = nil
	} else if s.container != nil {
		s.container.Close()
	}
	s.container = nil
	s.stopper.Stop(context.Background())
}

// recoverPanic runs fn and returns the panic message if fn panics, or "" if it
// does not.
func recoverPanic(fn func()) (panicMsg string) {
	defer func() {
		if r := recover(); r != nil {
			panicMsg = fmt.Sprintf("%v", r)
		}
	}()
	fn()
	return ""
}

// TestRSEngineContainerDatadriven tests rsEngineContainer using datadriven.
func TestRSEngineContainerDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var state *containerTestState
	defer func() {
		if state != nil {
			state.cleanup()
		}
	}()
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "rs_engine_container"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				if state != nil {
					state.cleanup()
				}
				startFileNum := DiskFileNum(dd.ScanArg[int](t, td, "start-file-num"))
				state = newContainerTestState(t, startFileNum)
				return ""

			case "state":
				require.NotNil(t, state)
				return state.container.String()

			case "flush":
				require.NotNil(t, state)
				fileName := dd.ScanArg[string](t, td, "file")
				// Create a scratch file with content.
				inner := state.container.TestingInnerEngine().(*TestingRSEngine)
				scratchPath := state.fs.PathJoin(state.scratchDir, fileName)
				content := fmt.Sprintf("previous-manifest:%d", inner.currentManifestNum())
				f, err := state.fs.Create(scratchPath, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				_, err = f.Write([]byte(content))
				require.NoError(t, err)
				require.NoError(t, f.Close())
				state.committer.clearLog()
				err = state.container.FlushSSTables([]string{fileName}, nil)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				var buf strings.Builder
				buf.WriteString(state.committer.getLog())
				return buf.String()

			case "prepare":
				require.NotNil(t, state)
				manifestNum := DiskFileNum(dd.ScanArg[int](t, td, "manifest-num"))
				var errResult error
				panicMsg := recoverPanic(func() {
					errResult = state.container.PrepareExternalManifest(manifestNum)
				})
				if panicMsg != "" {
					return fmt.Sprintf("panic: %s", panicMsg)
				}
				if errResult != nil {
					return fmt.Sprintf("error: %v", errResult)
				}
				return "ok"

			case "install":
				require.NotNil(t, state)
				manifestNum := DiskFileNum(dd.ScanArg[int](t, td, "manifest-num"))
				panicMsg := recoverPanic(func() {
					state.container.InstallPreparedManifest(manifestNum)
				})
				if panicMsg != "" {
					return fmt.Sprintf("panic: %s", panicMsg)
				}
				return "ok"

			case "ref":
				require.NotNil(t, state)
				panicMsg := recoverPanic(func() {
					state.container.Ref()
				})
				if panicMsg != "" {
					return fmt.Sprintf("panic: %s", panicMsg)
				}
				return "ok"

			case "unref":
				require.NotNil(t, state)
				panicMsg := recoverPanic(func() {
					state.container.Unref()
				})
				if panicMsg != "" {
					return fmt.Sprintf("panic: %s", panicMsg)
				}
				return "ok"

			case "ref-inner":
				require.NotNil(t, state)
				inner := state.container.TestingInnerEngine()
				inner.ref()
				state.heldInner = append(state.heldInner, inner)
				return "ok"

			case "unref-inner":
				require.NotNil(t, state)
				n := len(state.heldInner)
				if n == 0 {
					return "error: no inner engine refs held"
				}
				inner := state.heldInner[n-1]
				state.heldInner = state.heldInner[:n-1]
				inner.unref()
				return "ok"

			case "snapshot":
				require.NotNil(t, state)
				snap := state.container.NewSnapshot()
				name := fmt.Sprintf("snap-%d", state.nextSnapID)
				state.nextSnapID++
				state.snapshots[name] = snap
				return fmt.Sprintf("%s manifest-num=%d", name, snap.ManifestNum())

			case "close-snapshot":
				require.NotNil(t, state)
				name := dd.ScanArg[string](t, td, "name")
				snap, ok := state.snapshots[name]
				if !ok {
					return fmt.Sprintf("error: snapshot %s not found", name)
				}
				snap.Close()
				delete(state.snapshots, name)
				return "ok"

			case "close":
				require.NotNil(t, state)
				state.container.Close()
				state.container = nil
				return "ok"

			case "async-close":
				require.NotNil(t, state)
				if state.asyncCloseDone != nil {
					return "error: async close already in progress"
				}
				doneCh := make(chan struct{})
				state.asyncCloseDone = doneCh
				c := state.container
				go func() {
					c.Close()
					close(doneCh)
				}()
				time.Sleep(10 * time.Millisecond)
				select {
				case <-doneCh:
					return "error: close completed without blocking"
				default:
				}
				return "blocked"

			case "wait-close":
				require.NotNil(t, state)
				if state.asyncCloseDone == nil {
					return "error: no async close in progress"
				}
				select {
				case <-state.asyncCloseDone:
				case <-time.After(5 * time.Second):
					t.Fatal("async close did not complete within 5s")
				}
				state.asyncCloseDone = nil
				return "ok"

			case "wait-quiesced":
				require.NotNil(t, state)
				state.container.closeWG.Wait()
				return "ok"

			case "op-log":
				require.NotNil(t, state)
				inner := state.container.TestingInnerEngine().(*TestingRSEngine)
				log := inner.getAndClearOpLog()
				if log == "" {
					return "<empty>"
				}
				return log

			case "compaction-toggle":
				require.NotNil(t, state)
				enable := dd.ScanArg[string](t, td, "enable") == "true"
				state.container.CompactionToggle(enable)
				return "ok"

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}
