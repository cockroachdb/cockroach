// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	testLogGC(t, debugLog, Infof)
}

func TestSecondaryGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a standalone secondary logger.
	m := logconfig.ByteSize(math.MaxInt64)
	f := logconfig.DefaultFileFormat
	common := logconfig.DefaultConfig().FileDefaults.CommonSinkConfig
	common.Format = &f
	bf := false
	fc := logconfig.FileSinkConfig{
		FileDefaults: logconfig.FileDefaults{
			CommonSinkConfig: common,
			Dir:              &s.logDir,
			MaxFileSize:      &m,
			MaxGroupSize:     &m,
			BufferedWrites:   &bf,
		},
	}
	logger := &loggerT{}
	si, fileSink, err := newFileSinkInfo("gctest", fc)
	if err != nil {
		t.Fatal(err)
	}
	logger.sinkInfos = []*sinkInfo{si}

	// Enable the garbage collector.
	go fileSink.gcDaemon(ctx)

	testLogGC(t, logger,
		func(ctx context.Context, format string, args ...interface{}) {
			entry := makeUnstructuredEntry(ctx, severity.INFO, channel.DEV, 1,
				true,   /* redactable */
				format, /* nolint:fmtsafe */
				args...)
			logger.outputLogEntry(entry)
		})
}

func testLogGC(
	t *testing.T,
	logger *loggerT,
	logFn func(ctx context.Context, format string, args ...interface{}),
) {
	logging.mu.Lock()
	logging.mu.disableDaemons = true
	defer func(previous bool) {
		logging.mu.Lock()
		logging.mu.disableDaemons = previous
		logging.mu.Unlock()
	}(logging.mu.disableDaemons)
	logging.mu.Unlock()

	// Make an entry in the target logger. This ensures That there is at
	// least one file in the target directory for the logger being
	// tested. This serves two
	// purposes. One is to serve as baseline for the file size. The
	// other is to ensure that the TestLogScope does not erase the
	// temporary directory, preventing further investigation.
	logFn(context.Background(), "0")

	fileSink := logger.getFileSink()
	if fileSink == nil {
		t.Fatal("no file sink")
	}

	// Check that the file was created.
	origDir, allFilesOriginal, err := fileSink.listLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := 1, len(allFilesOriginal); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}

	// Check that the file exists, and also measure its size.
	// We'll use this as base value for the maximum combined size
	// below, to force GC.
	dir := fileSink.mu.logDir
	if dir == "" {
		t.Fatal(errDirectoryNotSet)
	}
	if origDir != dir {
		t.Fatalf("dir expected %q, got %q", dir, origDir)
	}
	stat, err := os.Stat(filepath.Join(dir, allFilesOriginal[0].Name))
	if err != nil {
		t.Fatal(err)
	}
	// logFileSize is the size of the first log file we wrote to.
	logFileSize := stat.Size()
	const expectedFilesAfterGC = 2
	// Pick a max total size that's between 2 and 3 log files in size.
	maxTotalLogFileSize := logFileSize*expectedFilesAfterGC + logFileSize // 2

	t.Logf("new max total log file size: %d", maxTotalLogFileSize)

	// We want to create multiple log files below. For this we need to
	// override the size/number limits first to the values suitable for
	// the test.
	defer func(previous int64) { fileSink.logFileMaxSize = previous }(fileSink.logFileMaxSize)
	fileSink.logFileMaxSize = 1 // ensure rotation on every log write
	defer func(previous int64) {
		atomic.StoreInt64(&fileSink.logFilesCombinedMaxSize, previous)
	}(fileSink.logFilesCombinedMaxSize)
	atomic.StoreInt64(&fileSink.logFilesCombinedMaxSize, maxTotalLogFileSize)

	// Create the number of expected log files.
	const newLogFiles = 20

	for i := 1; i < newLogFiles; i++ {
		logFn(context.Background(), "%d", i)
		Flush()
	}

	_, allFilesBefore, err := fileSink.listLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := newLogFiles, len(allFilesBefore); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}

	// Re-enable GC, so that the GC daemon can pick up the files.
	logging.mu.Lock()
	logging.mu.disableDaemons = false
	logging.mu.Unlock()
	// Start the GC daemon, using a context that will terminate it
	// at the end of the test.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		fileSink.gcDaemon(ctx)
		close(done)
	}()

	// Emit a log line which will rotate the files and trigger GC.
	logFn(context.Background(), "final")
	Flush()

	succeedsSoon(t, func() error {
		_, allFilesAfter, err := fileSink.listLogFiles()
		if err != nil {
			return err
		}
		if e, a := expectedFilesAfterGC, len(allFilesAfter); e != a {
			return fmt.Errorf("expected %d files, but found %d", e, a)
		}
		return nil
	})
	cancel()
	<-done
}

// succeedsSoon is a simplified version of testutils.SucceedsSoon.
// The main implementation cannot be used here because of
// an import cycle.
func succeedsSoon(t *testing.T, fn func() error) {
	t.Helper()
	tBegin := timeutil.Now()
	deadline := tBegin.Add(5 * time.Second)
	var lastErr error
	for wait := time.Duration(1); timeutil.Now().Before(deadline); wait *= 2 {
		lastErr = fn()
		if lastErr == nil {
			return
		}
		if wait > time.Second {
			wait = time.Second
		}
		if timeutil.Since(tBegin) > 3*time.Second {
			InfofDepth(context.Background(), 2, "SucceedsSoon: %+v", lastErr)
		}
		time.Sleep(wait)
	}
	if lastErr != nil {
		t.Fatalf("condition failed to evalute: %+v", lastErr)
	}
}
