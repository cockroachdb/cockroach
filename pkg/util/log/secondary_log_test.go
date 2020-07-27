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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
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
	l := newSecondaryLogger(ctx, &logging.logDir, "woo", true, false, true)
	defer l.Close()

	// Interleave some messages.
	Infof(context.Background(), "test1")
	ctx = logtags.AddTag(ctx, "hello", "world")
	l.output(ctx, 0, severity.INFO, channel.DEV, "story time")
	Infof(context.Background(), "test2")

	// Make sure the content made it to disk.
	Flush()

	// Check that the messages indeed made it to different files.

	contents, err := ioutil.ReadFile(debugLog.fileSink.mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "test1") || !strings.Contains(string(contents), "test2") {
		t.Errorf("log does not contain error text\n%s", contents)
	}
	if strings.Contains(string(contents), "world") {
		t.Errorf("secondary log spilled into debug log\n%s", contents)
	}

	contents, err = ioutil.ReadFile(l.logger.fileSink.mu.file.(*syncBuffer).file.Name())
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
	logging.stderrThreshold = severity.NONE

	// Take over stderr.
	TestingResetActive()
	cleanup, err := SetupRedactionAndLoggingChannels(DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// Now create a secondary logger in the same directory.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := newSecondaryLogger(ctx, &logging.logDir, "woo", true, false, true)
	defer l.Close()

	// Log something on the secondary logger.
	l.output(context.Background(), 0, severity.INFO, channel.DEV, "test456")

	// Send something on stderr.
	const stderrText = "hello stderr"
	fmt.Fprint(os.Stderr, stderrText)

	// Check the stderr log file: we want our stderr text there.
	contents, err := ioutil.ReadFile(stderrLog.fileSink.mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), stderrText) {
		t.Errorf("log does not contain stderr text\n%s", contents)
	}

	// Check the secondary log file: we don't want our stderr text there.
	contents2, err := ioutil.ReadFile(l.logger.fileSink.mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents2), stderrText) {
		t.Errorf("secondary log erronously contains stderr text\n%s", contents2)
	}
}

func TestListLogFilesIncludeSecondaryLogs(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Make a new logger, in the same directory.
	l := newSecondaryLogger(ctx, &logging.logDir, "woo", true, false, true)
	defer l.Close()

	// Emit some logging and ensure the files gets created.
	l.output(ctx, 0, severity.INFO, channel.DEV, "story time")
	Flush()

	results, err := ListLogFiles()
	if err != nil {
		t.Fatalf("error in ListLogFiles: %v", err)
	}

	expectedName := filepath.Base(l.logger.fileSink.mu.file.(*syncBuffer).file.Name())
	foundExpected := false
	for i := range results {
		if results[i].Name == expectedName {
			foundExpected = true
			break
		}
	}
	if !foundExpected {
		t.Fatalf("unexpected results; expected file %q, got: %+v", expectedName, results)
	}
}
