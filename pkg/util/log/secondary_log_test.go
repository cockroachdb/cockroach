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
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/logtags"
)

// installSessionsFileSink configures the SESSIONS channel to have a file sink.
func installSessionsFileSink(sc *TestLogScope, t *testing.T) func() {
	t.Helper()

	// Make a configuration with a file sink for SESSIONS, which numbers the
	// output entries.
	cfg := logconfig.DefaultConfig()
	bt := true
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"sessions": {
			FileDefaults: logconfig.FileDefaults{
				CommonSinkConfig: logconfig.CommonSinkConfig{Auditable: &bt},
			},
			Channels: logconfig.ChannelList{Channels: []Channel{channel.SESSIONS}}},
	}

	// Derive a full config using the same directory as the
	// TestLogScope.
	if err := cfg.Validate(&sc.logDir); err != nil {
		t.Fatal(err)
	}

	// Apply the configuration.
	TestingResetActive()
	cleanup, err := ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cleanup
}

func TestSecondaryLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	defer installSessionsFileSink(s, t)()

	ctx := context.Background()

	// Interleave some messages.
	Infof(context.Background(), "test1")
	ctx = logtags.AddTag(ctx, "hello", "world")
	Sessions.Infof(ctx, "story time")
	Infof(context.Background(), "test2")

	// Make sure the content made it to disk.
	Flush()

	// Check that the messages indeed made it to different files.

	bcontents, err := ioutil.ReadFile(debugLog.getFileSink().mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	contents := string(bcontents)
	if !strings.Contains(contents, "test1") || !strings.Contains(contents, "test2") {
		t.Errorf("log does not contain error text\n%s", contents)
	}
	if strings.Contains(contents, "world") {
		t.Errorf("secondary log spilled into debug log\n%s", contents)
	}

	l := logging.getLogger(channel.SESSIONS)
	bcontents, err = ioutil.ReadFile(l.getFileSink().mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	contents = string(bcontents)
	if !strings.Contains(contents, "hello") ||
		!strings.Contains(contents, "world") ||
		!strings.Contains(contents, "1  " /* entry counter */ +"story time") {
		t.Errorf("secondary log does not contain text or counter\n%s", contents)
	}
	if strings.Contains(contents, "test1") {
		t.Errorf("primary log spilled into secondary\n%s", contents)
	}
}

func TestRedirectStderrWithSecondaryLoggersActive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	defer installSessionsFileSink(s, t)()

	// Log something on the secondary logger.
	ctx := context.Background()

	Sessions.Infof(ctx, "test456")

	// Send something on stderr.
	const stderrText = "hello stderr"
	fmt.Fprint(os.Stderr, stderrText)

	// Check the stderr log file: we want our stderr text there.
	stderrLog := logging.testingFd2CaptureLogger
	contents, err := ioutil.ReadFile(stderrLog.getFileSink().mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), stderrText) {
		t.Errorf("log does not contain stderr text\n%s", contents)
	}

	// Check the secondary log file: we don't want our stderr text there.
	l := logging.getLogger(channel.SESSIONS)
	contents2, err := ioutil.ReadFile(l.getFileSink().mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents2), stderrText) {
		t.Errorf("secondary log erronously contains stderr text\n%s", contents2)
	}
}

func TestListLogFilesIncludeSecondaryLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	defer installSessionsFileSink(s, t)()

	// Emit some logging and ensure the files gets created.
	ctx := context.Background()
	Sessions.Infof(ctx, "story time")
	Flush()

	results, err := ListLogFiles()
	if err != nil {
		t.Fatalf("error in ListLogFiles: %v", err)
	}

	l := logging.getLogger(channel.SESSIONS)
	expectedName := filepath.Base(l.getFileSink().mu.file.(*syncBuffer).file.Name())
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
