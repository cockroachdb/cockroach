// Copyright 2013 Google Inc. All Rights Reserved.
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code originated in the github.com/golang/glog package.

package log

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	stdLog "log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

// Test that shortHostname works as advertised.
func TestShortHostname(t *testing.T) {
	for hostname, expect := range map[string]string{
		"":                "",
		"host":            "host",
		"host.google.com": "host",
	} {
		if got := shortHostname(hostname); expect != got {
			t.Errorf("shortHostname(%q): expected %q, got %q", hostname, expect, got)
		}
	}
}

// flushBuffer wraps a bytes.Buffer to satisfy flushSyncWriter.
type flushBuffer struct {
	bytes.Buffer
}

func (f *flushBuffer) Flush() error {
	return nil
}

func (f *flushBuffer) Sync() error {
	return nil
}

// capture changes the debugLog to output to a flushBuffer (see
// above), so that the original output sink is restored upon calling
// the returned fn.
//
// While the output is captured, a test can use contents() below
// to retrieve the captured output so far.
func capture() func() {
	fileSink := debugLog.getFileSink()
	fileSink.mu.Lock()
	oldFile := fileSink.mu.file
	fileSink.mu.file = new(flushBuffer)
	fileSink.mu.Unlock()
	return func() {
		fileSink.mu.Lock()
		fileSink.mu.file = oldFile
		fileSink.mu.Unlock()
	}
}

// resetCaptured erases the logging output captured so far.
func resetCaptured() {
	fs := debugLog.getFileSink()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.mu.file.(*flushBuffer).Buffer.Reset()
}

// contents returns the specified log value as a string.
func contents() string {
	fs := debugLog.getFileSink()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.mu.file.(*flushBuffer).Buffer.String()
}

func (l *fileSink) getDir() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.logDir
}

func getDebugLogFileName(t *testing.T) string {
	fs := debugLog.getFileSink()
	return fs.getFileName(t)
}

func (l *fileSink) getFileName(t *testing.T) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	sb, ok := l.mu.file.(*syncBuffer)
	if !ok {
		t.Fatalf("buffer wasn't created")
	}
	return sb.file.Name()
}

// contains reports whether the string is contained in the log.
func contains(str string, t *testing.T) bool {
	c := contents()
	return strings.Contains(c, str)
}

// Test that Info works as advertised.
func TestInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()
	Info(context.Background(), "test")
	if !contains("I", t) {
		t.Errorf("Info has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Info failed")
	}
}

// Test that copyStandardLogTo panics on bad input.
func TestCopyStandardLogToPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer func() {
		if s, ok := recover().(string); !ok || !strings.Contains(s, "LOG") {
			t.Errorf(`copyStandardLogTo("LOG") should have panicked: %v`, s)
		}
	}()
	copyStandardLogTo("LOG")
}

// Test that using the standard log package logs to INFO.
func TestStandardLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()
	stdLog.Print("test")
	if !contains("I", t) {
		t.Errorf("Info has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Info failed")
	}
}

// Test that an Error log goes to Warning and Info.
// Even in the Info log, the source character will be E, so the data should
// all be identical.
func TestError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()

	Error(context.Background(), "test")
	if !contains("E", t) {
		t.Errorf("Error has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Error failed")
	}
}

// Test that a Warning log goes to Info.
// Even in the Info log, the source character will be W, so the data should
// all be identical.
func TestWarning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()

	Warning(context.Background(), "test")
	if !contains("W", t) {
		t.Errorf("Warning has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Warning failed")
	}
}

// Test that a V log goes to Info.
func TestV(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()

	_ = logging.vmoduleConfig.verbosity.Set("2")
	defer func() { _ = logging.vmoduleConfig.verbosity.Set("0") }()
	if V(2) {
		logfDepth(context.Background(), 1, severity.INFO, channel.DEV, "test")
	}
	if !contains("I", t) {
		t.Errorf("Info has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Info failed")
	}
}

// Test that a vmodule enables a log in this file.
func TestVmoduleOn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()

	_ = SetVModule("clog_test=2")
	defer func() { _ = SetVModule("") }()
	if !V(1) {
		t.Error("V not enabled for 1")
	}
	if !V(2) {
		t.Error("V not enabled for 2")
	}
	if V(3) {
		t.Error("V enabled for 3")
	}
	if V(2) {
		logfDepth(context.Background(), 1, severity.INFO, channel.DEV, "test")
	}
	if !contains("I", t) {
		t.Errorf("Info has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Info failed")
	}
}

// Test that a vmodule of another file does not enable a log in this file.
func TestVmoduleOff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()

	_ = SetVModule("notthisfile=2")
	defer func() { _ = SetVModule("") }()
	for i := 1; i <= 3; i++ {
		if V(Level(i)) {
			t.Errorf("V enabled for %d", i)
		}
	}
	if V(2) {
		logfDepth(context.Background(), 1, severity.INFO, channel.DEV, "test")
	}
	if contents() != "" {
		t.Error("V logged incorrectly")
	}
}

// vGlobs are patterns that match/don't match this file at V=2.
var vGlobs = map[string]bool{
	// Easy to test the numeric match here.
	"clog_test=1": false, // If --vmodule sets V to 1, v(2) will fail.
	"clog_test=2": true,
	"clog_test=3": true, // If --vmodule sets V to 1, v(3) will succeed.
	// These all use 2 and check the patterns. All are true.
	"*=2":           true,
	"?l*=2":         true,
	"????_*=2":      true,
	"??[mno]?_*t=2": true,
	// These all use 2 and check the patterns. All are false.
	"*x=2":         false,
	"m*=2":         false,
	"??_*=2":       false,
	"?[abc]?_*t=2": false,
}

// Test that vmodule globbing works as advertised.
func testVmoduleGlob(pat string, match bool, t *testing.T) {
	defer func() { _ = SetVModule("") }()
	_ = SetVModule(pat)
	if V(2) != match {
		t.Errorf("incorrect match for %q: got %t expected %t", pat, V(2), match)
	}
}

// Test that a vmodule globbing works as advertised.
func TestVmoduleGlob(t *testing.T) {
	for glob, match := range vGlobs {
		testVmoduleGlob(glob, match, t)
	}
}

func TestListLogFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	Info(context.Background(), "x")

	fileName := getDebugLogFileName(t)

	results, err := ListLogFiles()
	if err != nil {
		t.Fatalf("error in ListLogFiles: %v", err)
	}

	expectedName := filepath.Base(fileName)
	foundExpected := false
	for i := range results {
		if results[i].Name == expectedName {
			foundExpected = true
			break
		}
	}
	if !foundExpected {
		t.Fatalf("unexpected results: %q", results)
	}
}

func TestFilePermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	fileMode := os.FileMode(0o400) // not the default 0o644

	fs := debugLog.getFileSink()
	defer func(p os.FileMode) { fs.filePermissions = p }(fs.filePermissions)
	fs.filePermissions = fileMode

	Info(context.Background(), "x")

	fileName := fs.getFileName(t)

	results, err := ListLogFiles()
	if err != nil {
		t.Fatalf("error in ListLogFiles: %v", err)
	}

	expectedName := filepath.Base(fileName)
	foundExpected := false
	for _, r := range results {
		if r.Name != expectedName {
			continue
		}
		foundExpected = true
		if os.FileMode(r.FileMode) != fileMode {
			t.Errorf("Logfile %v has file mode %v, expected %v",
				expectedName, os.FileMode(r.FileMode), fileMode)
		}
	}
	if !foundExpected {
		t.Fatalf("unexpected results: %q", results)
	}
}

func TestGetLogReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	// Create two log directories.
	dir1 := filepath.Join(sc.logDir, "dir1")
	require.NoError(t, os.MkdirAll(dir1, 0755))

	// Create a config with two groups in separate log directories.
	config := logconfig.DefaultConfig()
	config.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"g1": {
			FileDefaults: logconfig.FileDefaults{Dir: &dir1},
			Channels:     logconfig.SelectChannels(channel.OPS),
		},
	}

	// Validate and apply the config.
	require.NoError(t, config.Validate(&sc.logDir))
	TestingResetActive()
	cleanupFn, err := ApplyConfig(config)
	require.NoError(t, err)
	defer cleanupFn()

	t.Logf("applied logging configuration:\n  %s\n",
		strings.TrimSpace(strings.ReplaceAll(DescribeAppliedConfig(), "\n", "\n  ")))

	// Force creation of a file on the default sink.
	Info(context.Background(), "x")
	fileName := getDebugLogFileName(t)
	infoName := filepath.Base(fileName)

	// Create some relative path. We'll check below these cannot be
	// accessed.
	curDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relPathFromCurDir, err := filepath.Rel(curDir, fileName)
	if err != nil {
		t.Fatal(err)
	}

	// Directory for the default sink.
	dir := debugLog.getFileSink().getDir()
	if dir == "" {
		t.Fatal(errDirectoryNotSet)
	}

	// Some arbitrary non-log file.
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "other.txt"), nil, 0644))
	relPathFromLogDir := strings.Join([]string{"..", filepath.Base(dir), infoName}, string(os.PathSeparator))

	cntr := 0
	genFileName := func(prefix string) string {
		cntr++
		return fileNameConstants.program + prefix + ".roach0.root.2015-09-25T19_24_19Z." + fmt.Sprintf("%05d", cntr) + ".log"
	}

	// A log file in a non-default directory.
	fname1 := genFileName("-g1")
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir1, fname1), nil, 0644))

	// A log file that matches the file pattern for the default sink,
	// in a non-default directory.
	fname2 := genFileName("")
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir1, fname2), nil, 0644))
	fname3 := genFileName("-g1")
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir, fname3), nil, 0644))

	// Fake symlink to check the symlink error below.
	fname4 := genFileName("")
	createSymlink("bogus.log", filepath.Join(dir, fname4))

	testCases := []struct {
		filename         string
		expErrRestricted string
	}{
		// Base filename is specified.
		{infoName, ""},
		{fname1, ""},
		// File exists but in a different directory than what the sink
		// configuration indicates. It is invisible to the API.
		{fname2, "no such file"},
		{fname3, "no such file"},
		// File is not specified (trying to open a directory instead).
		{dir, "pathnames must be basenames"},
		// Absolute filename is specified.
		{fileName, "pathnames must be basenames"},
		// Symlink to a log file.
		{filepath.Join(dir, fileNameConstants.program+".log"), "pathnames must be basenames"},
		// Symlink relative to logDir.
		{fname4, "symlinks are not allowed"},
		// Non-log file.
		{"other.txt", "malformed log filename"},
		// Non-existent file matching RE.
		{fileNameConstants.program + ".roach0.root.2015-09-25T19_24_19Z.00000.log", "no such file"},
		// Relative path with directory components.
		{relPathFromCurDir, "pathnames must be basenames"},
		// Relative path within the logs directory.
		{relPathFromLogDir, "pathnames must be basenames"},
	}

	for _, test := range testCases {
		t.Run(test.filename, func(t *testing.T) {
			expErr := test.expErrRestricted
			reader, err := GetLogReader(test.filename)
			if expErr == "" {
				if err != nil {
					t.Errorf("expected ok, got %s", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error %s; got nil", expErr)
				} else if matched, matchErr := regexp.MatchString(expErr, err.Error()); matchErr != nil || !matched {
					t.Errorf("expected error %s; got %v", expErr, err)
				}
			}
			if reader != nil {
				reader.Close()
			}
		})
	}
}

func TestRollover(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	var err error
	setExitErrFunc(false /* hideStack */, func(_ exit.Code, e error) {
		err = e
	})

	debugFileSink := debugLog.getFileSink()
	defer func(previous int64) { debugFileSink.logFileMaxSize = previous }(debugFileSink.logFileMaxSize)
	debugFileSink.logFileMaxSize = 2048

	Info(context.Background(), "x") // Be sure we have a file.
	if err != nil {
		t.Fatalf("info has initial error: %v", err)
	}
	fname0 := debugFileSink.getFileName(t)
	Infof(context.Background(), "%s", strings.Repeat("x", int(debugFileSink.logFileMaxSize))) // force a rollover
	if err != nil {
		t.Fatalf("info has error after big write: %v", err)
	}

	// Make sure the next log file gets a file name with a different
	// time stamp.

	Info(context.Background(), "x") // create a new file
	if err != nil {
		t.Fatalf("error after rotation: %v", err)
	}

	fs := debugLog.getFileSink()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	info := fs.mu.file.(*syncBuffer)

	fname1 := info.file.Name()
	if fname0 == fname1 {
		t.Errorf("info.f.Name did not change: %v", fname0)
	}
	if info.nbytes >= debugFileSink.logFileMaxSize {
		t.Errorf("file size was not reset: %d", info.nbytes)
	}
}

// TestFatalStacktraceStderr verifies that a full stacktrace is output.
// This test would be more interesting if -logtostderr could actually
// be tested. Well, it wasn't, and it looked like stack trace dumping
// was broken when that option was used. This is fixed now, and perhaps
// in the future clog and this test can be adapted to actually test that;
// right now clog writes straight to os.StdErr.
func TestFatalStacktraceStderr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, level := range []int{tracebackNone, tracebackSingle, tracebackAll} {
		t.Run(fmt.Sprintf("%d", level), func(t *testing.T) {
			defer ScopeWithoutShowLogs(t).Close(t)

			SetExitFunc(false /* hideStack */, func(exit.Code) {})

			defer capture()()

			traceback = level
			Fatalf(context.Background(), "cinap")
			cont := contents()
			if !strings.Contains(cont, " cinap") {
				t.Fatalf("panic output does not contain cinap:\n%s", cont)
			}
			if !strings.Contains(cont, "clog_test") {
				t.Fatalf("stack trace does not contain file name: %s", cont)
			}
			switch traceback {
			case tracebackNone:
				if strings.Count(cont, "goroutine ") > 0 {
					t.Fatalf("unexpected stack trace:\n%s", cont)
				}
			case tracebackSingle:
				if strings.Count(cont, "goroutine ") != 1 {
					t.Fatalf("stack trace contains too many goroutines: %s", cont)
				}
			case tracebackAll:
				if strings.Count(cont, "goroutine ") < 2 {
					t.Fatalf("stack trace contains less than two goroutines: %s", cont)
				}
			}
		})
	}
}

func TestFd2Capture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	// Create a fresh configuration; this automatically sets up fd 2
	// redirection.
	cfg := logconfig.DefaultConfig()
	if err := cfg.Validate(&s.logDir); err != nil {
		t.Fatal(err)
	}
	TestingResetActive()
	cleanupFn, err := ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupFn()

	Infof(context.Background(), "test")

	const stderrText = "hello stderr"
	fmt.Fprint(os.Stderr, stderrText)

	contents, err := ioutil.ReadFile(logging.testingFd2CaptureLogger.getFileSink().getFileName(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), stderrText) {
		t.Fatalf("log does not contain stderr text\n%s", contents)
	}
}

func TestFileSeverityFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	var debugFileSinkInfo *sinkInfo
	for _, si := range debugLog.sinkInfos {
		si.threshold.set(channel.DEV, severity.ERROR)
		if _, ok := si.sink.(*fileSink); ok {
			debugFileSinkInfo = si
		}
	}

	Infof(context.Background(), "test1")
	Errorf(context.Background(), "test2")

	Flush()

	debugFileSink := debugFileSinkInfo.sink.(*fileSink)
	contents, err := ioutil.ReadFile(debugFileSink.getFileName(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "test2") {
		t.Errorf("log does not contain error text\n%s", contents)
	}
	if strings.Contains(string(contents), "test1") {
		t.Errorf("info text was not filtered out of log\n%s", contents)
	}
}

type outOfSpaceWriter struct{}

func (w *outOfSpaceWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("no space left on device")
}

func TestExitOnFullDisk(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	var exited sync.WaitGroup
	exited.Add(1)

	SetExitFunc(false, func(exit.Code) {
		exited.Done()
	})

	fs := &fileSink{}
	l := &loggerT{sinkInfos: []*sinkInfo{{
		sink:        fs,
		editor:      getEditor(SelectEditMode(false /* redact */, true /* redactable */)),
		criticality: true,
	}}}
	fs.mu.file = &syncBuffer{
		fileSink: fs,
		Writer:   bufio.NewWriterSize(&outOfSpaceWriter{}, 1),
	}

	l.outputMu.Lock()
	l.exitLocked(fmt.Errorf("out of space"), exit.UnspecifiedError())
	l.outputMu.Unlock()

	exited.Wait()
}

func BenchmarkHeader(b *testing.B) {
	entry := logpb.Entry{
		Severity:  severity.INFO,
		Time:      timeutil.Now().UnixNano(),
		Goroutine: 200,
		File:      "file.go",
		Line:      100,
	}
	for i := 0; i < b.N; i++ {
		var w bytes.Buffer
		_ = FormatLegacyEntry(entry, &w)
	}
}

func BenchmarkVDepthWithVModule(b *testing.B) {
	if err := SetVModule("craigthecockroach=5"); err != nil {
		b.Fatal(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = VDepth(1, 1)
		}
	})
}

// TestLogEntryPropagation ensures that a log entry is written
// to file even when stderr is not available.
func TestLogEntryPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	defer capture()()

	tmpDir := s.logDir

	// Make stderr read-only so that writes to it reliably fail.
	f, err := os.OpenFile(filepath.Join(tmpDir, "test"), os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	defer func(prevStderr *os.File) { OrigStderr = prevStderr }(OrigStderr)
	OrigStderr = f

	const specialMessage = `CAPTAIN KIRK`

	// Enable output to stderr (the Scope disabled it).
	l := logging.getLogger(channel.DEV)
	for _, si := range l.sinkInfos {
		if si.sink == &logging.stderrSink {
			si.threshold.set(channel.DEV, severity.INFO)

			// Make stderr non-critical.
			defer func(prevCriticality bool, si *sinkInfo) { si.criticality = prevCriticality }(si.criticality, si)
			si.criticality = false
			break
		}
	}

	// Now emit the log message. If criticality is respected, the
	// failure to write on stderr is graceful and the message gets
	// printed on the file output (and can be picked up by the contains
	// function). If it is not, the test runner will stop abruptly.
	Error(context.Background(), specialMessage)

	if !contains(specialMessage, t) {
		t.Fatalf("expected special message in file, got:\n%s", contents())
	}
}

func BenchmarkLogEntry_String(b *testing.B) {
	ctxtags := logtags.AddTag(context.Background(), "foo", "bar")
	entry := &logEntry{
		idPayload: idPayload{
			clusterID:     "fooo",
			nodeID:        "10",
			tenantID:      "12",
			sqlInstanceID: "9",
		},
		ts:         timeutil.Now().UnixNano(),
		header:     false,
		sev:        logpb.Severity_INFO,
		ch:         1,
		gid:        2,
		file:       "foo.go",
		line:       192,
		counter:    12,
		stacks:     nil,
		structured: false,
		payload: entryPayload{
			tags:       makeFormattableTags(ctxtags, false),
			redactable: false,
			message:    "hello there",
		},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = entry.String()
	}
}

func BenchmarkEventf_WithVerboseTraceSpan(b *testing.B) {
	for _, redactable := range []bool{false, true} {
		b.Run(fmt.Sprintf("redactable=%t", redactable), func(b *testing.B) {
			b.ReportAllocs()
			tagbuf := logtags.SingleTagBuffer("hello", "there")
			ctx := logtags.WithTags(context.Background(), tagbuf)
			tracer := tracing.NewTracer()
			tracer.SetRedactable(redactable)
			ctx, sp := tracer.StartSpanCtx(ctx, "benchspan", tracing.WithForceRealSpan())
			defer sp.Finish()
			sp.SetRecordingType(tracing.RecordingVerbose)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Eventf(ctx, "%s %s %s", "foo", "bar", "baz")
			}
		})
	}
}
