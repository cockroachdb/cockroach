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
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	fs := debugLog.getFileSink()
	if fs == nil {
		t.Fatal("no file sink")
	}

	testLogGC(t, fs, Info)
}

func TestSecondaryGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	// Make a config including a "gctest" file sink on the OPS channel.
	m := logconfig.ByteSize(math.MaxInt64)
	bf := false
	config := logconfig.DefaultConfig()
	if config.Sinks.FileGroups == nil {
		config.Sinks.FileGroups = make(map[string]*logconfig.FileSinkConfig)
	}
	config.Sinks.FileGroups["gctest"] = &logconfig.FileSinkConfig{
		FileDefaults: logconfig.FileDefaults{
			Dir:            &s.logDir,
			MaxFileSize:    &m,
			MaxGroupSize:   &m,
			BufferedWrites: &bf,
		},
		Channels: logconfig.SelectChannels(channel.OPS),
	}

	// Validate and apply the config.
	require.NoError(t, config.Validate(&s.logDir))
	TestingResetActive()
	cleanupFn, err := ApplyConfig(config)
	require.NoError(t, err)
	defer cleanupFn()

	// Find our "gctest" file sink
	var fs *fileSink
	require.NoError(t, logging.allSinkInfos.iterFileSinks(
		func(p *fileSink) error {
			if p.nameGenerator.ownsFileByPrefix(fileNameConstants.program + "-gctest") {
				fs = p
			}
			return nil
		}))
	if fs == nil {
		t.Fatal("fileSink 'gctest' not found")
	}

	testLogGC(t, fs, Ops.Info)
}

func testLogGC(t *testing.T, fileSink *fileSink, logFn func(ctx context.Context, msg string)) {
	// Set to the provided value, return the original value.
	setDisableDaemons := func(val bool) bool {
		logging.mu.Lock()
		ret := logging.mu.disableDaemons
		logging.mu.disableDaemons = val
		logging.mu.Unlock()
		return ret
	}

	// Immediately disable GC daemons;
	// defer restoring their original value.
	defer setDisableDaemons(setDisableDaemons(true))

	// Make an entry in the target logger. This ensures That there is at
	// least one file in the target directory for the logger being
	// tested. This serves two
	// purposes. One is to serve as baseline for the file size. The
	// other is to ensure that the TestLogScope does not erase the
	// temporary directory, preventing further investigation.
	logFn(context.Background(), "0")

	dir := fileSink.mu.logDir
	expectFileCount := func(e int) ([]logpb.FileInfo, error) {
		listDir, files, err := fileSink.listLogFiles()
		if err != nil {
			return nil, err
		}
		if a := len(files); a != e {
			return nil, errors.Errorf("expect %d files, but found %d", e, a)
		}
		if listDir != dir {
			return nil, errors.Errorf("dir expected %q, got %q", dir, listDir)
		}
		return files, nil
	}

	// Check that the file was created.
	files, err := expectFileCount(1)
	if err != nil {
		t.Fatal(err)
	}

	// Check that the file exists, and also measure its size.
	// We'll use this as base value for the maximum combined size
	// below, to force GC.
	stat, err := os.Stat(filepath.Join(dir, files[0].Name))
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
		logFn(context.Background(), fmt.Sprint(i))
		FlushFileSinks()
	}
	if _, err := expectFileCount(newLogFiles); err != nil {
		t.Fatal(err)
	}

	// Re-enable GC, so that the GC daemon can pick up the files.
	setDisableDaemons(false)

	// Emit a log line which will rotate the files and trigger GC.
	logFn(context.Background(), "final")
	FlushFileSinks()

	succeedsSoon(t, func() error {
		_, err := expectFileCount(expectedFilesAfterGC)
		return err
	})
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
