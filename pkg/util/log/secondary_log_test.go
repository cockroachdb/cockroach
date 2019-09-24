// Copyright 2018 The Cockroach Authors.
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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/logtags"
)

func TestSecondaryLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Make a new logger, in the same directory.
	l := NewSecondaryLogger(ctx, &logging.logDir, "woo", true, false, true)

	// Interleave some messages.
	Infof(context.Background(), "test1")
	ctx = logtags.AddTag(ctx, "hello", "world")
	l.Logf(ctx, "story time")
	Infof(context.Background(), "test2")

	// Make sure the content made it to disk.
	Flush()

	// Check that the messages indeed made it to different files.

	contents, err := ioutil.ReadFile(logging.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "test1") || !strings.Contains(string(contents), "test2") {
		t.Errorf("log does not contain error text\n%s", contents)
	}
	if strings.Contains(string(contents), "world") {
		t.Errorf("secondary log spilled into primary\n%s", contents)
	}

	contents, err = ioutil.ReadFile(l.logger.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "hello") ||
		!strings.Contains(string(contents), "world") ||
		!strings.Contains(string(contents), "story time") {
		t.Errorf("secondary log does not contain text\n%s", contents)
	}
	if strings.Contains(string(contents), "test1") {
		t.Errorf("primary log spilled into secondary\n%s", contents)
	}

}

func TestRedirectStderrWithSecondaryLoggersActive(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	setFlags()
	logging.stderrThreshold = Severity_NONE

	// Ensure that the main log is initialized. This should take over
	// stderr.
	Infof(context.Background(), "test123")

	// Now create a secondary logger in the same directory.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := NewSecondaryLogger(ctx, &logging.logDir, "woo", true, false, true)

	// Log something on the secondary logger.
	l.Logf(context.Background(), "test456")

	// Send something on stderr.
	const stderrText = "hello stderr"
	fmt.Fprint(os.Stderr, stderrText)

	// Check the main log file: we want our stderr text there.
	contents, err := ioutil.ReadFile(logging.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), stderrText) {
		t.Errorf("log does not contain stderr text\n%s", contents)
	}

	// Check the secondary log file: we don't want our stderr text there.
	contents2, err := ioutil.ReadFile(l.logger.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents2), stderrText) {
		t.Errorf("secondary log erronously contains stderr text\n%s", contents2)
	}
}

func TestSecondaryGC(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	logging.mu.Lock()
	logging.disableDaemons = true
	defer func(previous bool) {
		logging.mu.Lock()
		logging.disableDaemons = previous
		logging.mu.Unlock()
	}(logging.disableDaemons)
	logging.mu.Unlock()

	setFlags()

	const newLogFiles = 20

	// Prevent writes to stderr from being sent to log files which would screw up
	// the expected number of log file calculation below.
	logging.noStderrRedirect = true

	// Ensure the main log has at least one entry. Otherwise, the test
	// log scope will erase the temporary directory, preventing further
	// investigation.
	Infof(context.Background(), "hello world")

	tmpDir, err := ioutil.TempDir(logging.logDir.String(), "gctest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !t.Failed() {
			// If the test failed, we'll want to keep the artifacts for
			// troubleshooting.
			_ = os.RemoveAll(tmpDir)
		}
	}()
	tmpDirName := DirName{name: tmpDir}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := NewSecondaryLogger(ctx, &tmpDirName, "woo", true /*enableGc*/, false /*syncWrites*/, true /*msgCount*/)

	// Create 1 log file to figure out its size.
	l.Logf(context.Background(), "0")
	Flush()

	allFilesOriginal, err := l.logger.listLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := 1, len(allFilesOriginal); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}
	dir, err := l.logger.logDir.get()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, allFilesOriginal[0].Name))
	if err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// logFileSize is the size of the first log file we wrote to.
	logFileSize := stat.Size()
	const expectedFilesAfterGC = 2
	// Pick a max total size that's between 2 and 3 log files in size.
	maxTotalLogFileSize := logFileSize*expectedFilesAfterGC + logFileSize // 2

	defer func(previous int64) { LogFileMaxSize = previous }(LogFileMaxSize)
	LogFileMaxSize = 1 // ensure rotation on every log write
	defer func(previous int64) {
		atomic.StoreInt64(&LogFilesCombinedMaxSize, previous)
	}(LogFilesCombinedMaxSize)
	atomic.StoreInt64(&LogFilesCombinedMaxSize, maxTotalLogFileSize)

	for i := 1; i < newLogFiles; i++ {
		l.Logf(context.Background(), "%d", i)
		Flush()
	}

	allFilesBefore, err := l.logger.listLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := newLogFiles, len(allFilesBefore); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}

	l.logger.gcOldFiles()

	allFilesAfter, err := l.logger.listLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := expectedFilesAfterGC, len(allFilesAfter); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}
}
