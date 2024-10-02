// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// blockingSyncBuffer is wraps a syncBuffer with some sugar to allow the buffer
// to simulate a misbehaving filesystem where new files cannot be created.
type blockingSyncBuffer struct {
	*syncBuffer

	shouldRotate  bool
	didRotate     bool
	blockStartedC chan struct{}
	doneC         chan struct{}
}

func (sb *blockingSyncBuffer) rotateFileLocked(now time.Time) (err error) {
	// Block here until we get told to move on. This simulates the blocking
	// behavior.
	sb.blockStartedC <- struct{}{}
	<-sb.doneC
	sb.didRotate = true
	return sb.syncBuffer.rotateFileLocked(now)
}

func (sb *blockingSyncBuffer) Write(p []byte) (n int, err error) {
	// This mimics the original code, but we're intercepting the call here in
	// order to do the blocking on the file rotation.
	if sb.shouldRotate {
		if err := sb.rotateFileLocked(timeutil.Now()); err != nil {
			return 0, err
		}
		sb.shouldRotate = false // only need to rotate once.
	}
	// Pass the call down to the underlying writer.
	n, err = sb.Writer.Write(p)
	sb.nbytes += int64(n)
	return n, err
}

var exitCode = struct {
	mu struct {
		syncutil.Mutex
		observedExitCode *exit.Code
	}
}{}

// TestRepro81025 reproduces the issue documented in #81025.
//
// Set up a log sink that can block when it is time to rotate a file, simulating
// issues on the filesystem to which the logs are written.
//
// Once the sink becomes blocking, attempting to write a fatal log event will
// not cause the process to crash immediately. Rather, the fatal log event is
// only emitted once the sink is unblocked.
func TestRepro81025(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)

	// Don't bother sending crash events.
	MaybeSendCrashReport = func(ctx context.Context, err error) {}
	ExitTimeoutOnFatalLog = time.Second

	// Create a new file-backed sink.
	dir := t.TempDir()
	s := newFileSink(
		dir, "test-group",
		false,  /* unbuffered */
		1<<20,  /* single file max: 10 MB */
		10<<20, /* group max: 20 MB */
		nil,    /* getStartLines */
		0777,   /* file mode */
		nil,    /* logBytesWritten */
	)
	defer func() { _ = s.closeFileLocked() }()

	// Update the global stderr sink. This ensures that we pipe fatal errors into
	// our blocking sink.
	logging.rmu.Lock()
	logging.rmu.currentStderrSinkInfo.sink = s
	logging.rmu.Unlock()

	// Hijack the exit func to ensure we don't exit during the test when seeing a
	// fatal event.
	logging.mu.Lock()
	logging.mu.exitOverride.f = func(code exit.Code, err error) {
		exitCode.mu.Lock()
		defer exitCode.mu.Unlock()
		exitCode.mu.observedExitCode = &code
	}
	logging.mu.Unlock()

	// Force the first log file into existence so that we can wrap its buffer in
	// our own blocking buffer.
	err := s.createFileLocked()
	require.NoError(t, err)

	s.mu.Lock()
	sb := &blockingSyncBuffer{
		syncBuffer:    s.mu.file.(*syncBuffer),
		blockStartedC: make(chan struct{}),
		doneC:         make(chan struct{}),
	}
	s.mu.file = sb
	s.mu.Unlock()

	listFiles := func() ([]string, error) {
		var paths []string
		err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			paths = append(paths, path)
			return nil
		})
		return paths, err
	}

	// One file plus a symlink in the log dir.
	files, err := listFiles()
	require.NoError(t, err)
	require.Len(t, files, 2)

	// Allow the first log line through into the first file.
	err = s.output([]byte("foo"), sinkOutputOptions{})
	require.NoError(t, err)
	require.False(t, sb.didRotate)

	// Still just one file plus a symlink.
	files, err = listFiles()
	require.NoError(t, err)
	require.Len(t, files, 2)

	// Indicate that file rotation should block on the next log line.
	sb.shouldRotate = true

	// Writing the next log line should trigger a file rotation, which will hang.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := s.output([]byte("bar"), sinkOutputOptions{})
		require.NoError(t, err)
		wg.Done()
	}()

	// Wait to be told that we're blocking.
	<-sb.blockStartedC

	// At this point, we're blocked waiting for the file to rotate. Emit a fatal
	// log line.
	wg.Add(1)
	go func() {
		Fatalf(context.Background(), "uh oh")
		wg.Done()
	}()

	// Wait a few seconds to show that even though we logged a fatal event
	// (above), we haven't seen it. This is a compressed version of what happened
	// in the original issue, where it took 7 mins for the file rotation to
	// complete.
	require.Eventuallyf(t, func() bool {
		exitCode.mu.Lock()
		defer exitCode.mu.Unlock()
		return exitCode.mu.observedExitCode != nil && *exitCode.mu.observedExitCode == exit.TimeoutAfterFatalError()
	}, 5*time.Second, 100*time.Millisecond, "eventually exit")

	// Unblock the file rotation.
	sb.doneC <- struct{}{}

	// Wait for the sink to uncork, and the fatal log line error to be captured.
	wg.Wait()
	require.True(t, sb.didRotate)

	// The fatal error eventually came through after the rotation was unblocked.
	exitCode.mu.Lock()
	require.Equal(t, exit.FatalError(), *exitCode.mu.observedExitCode)
	exitCode.mu.Unlock()

	// Three files now, as the first one was rotated.
	files, err = listFiles()
	require.NoError(t, err)
	require.Len(t, files, 3)
}
