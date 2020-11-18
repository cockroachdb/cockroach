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
	"io"
	"io/ioutil"
	stdLog "log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
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
	debugLog.getFileSink().mu.file.(*flushBuffer).Buffer.Reset()
}

// contents returns the specified log value as a string.
func contents() string {
	return debugLog.getFileSink().mu.file.(*flushBuffer).Buffer.String()
}

// contains reports whether the string is contained in the log.
func contains(str string, t *testing.T) bool {
	c := contents()
	return strings.Contains(c, str)
}

// setFlags resets the logging flags and exit function to what tests expect.
func setFlags() {
	ResetExitFunc()
	// Make all logged errors go to the external stderr, in addition to
	// the log file.
	logging.stderrSink.threshold = severity.ERROR
}

// Test that Info works as advertised.
func TestInfo(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
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
	setFlags()
	defer func() {
		if s, ok := recover().(string); !ok || !strings.Contains(s, "LOG") {
			t.Errorf(`copyStandardLogTo("LOG") should have panicked: %v`, s)
		}
	}()
	copyStandardLogTo("LOG")
}

// Test that using the standard log package logs to INFO.
func TestStandardLog(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
	defer capture()()
	stdLog.Print("test")
	if !contains("I", t) {
		t.Errorf("Info has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Info failed")
	}
}

func TestEntryDecoder(t *testing.T) {
	formatEntry := func(s Severity, now time.Time, gid int, file string, line int, msg string) string {
		entry := logpb.Entry{
			Severity:  s,
			Time:      now.UnixNano(),
			Goroutine: int64(gid),
			File:      file,
			Line:      int64(line),
			Message:   msg,
		}
		var f formatCrdbV1
		buf := f.formatEntry(entry, nil /* stacks */)
		defer putBuffer(buf)
		return buf.String()
	}

	t1 := timeutil.Now().Round(time.Microsecond)
	t2 := t1.Add(time.Microsecond)
	t3 := t2.Add(time.Microsecond)
	t4 := t3.Add(time.Microsecond)
	t5 := t4.Add(time.Microsecond)
	t6 := t5.Add(time.Microsecond)
	t7 := t6.Add(time.Microsecond)
	t8 := t7.Add(time.Microsecond)

	// Verify the truncation logic for reading logs that are longer than the
	// default scanner can handle.
	preambleLength := len(formatEntry(severity.INFO, t1, 0, "clog_test.go", 136, ""))
	maxMessageLength := bufio.MaxScanTokenSize - preambleLength - 1
	reallyLongEntry := string(bytes.Repeat([]byte("a"), maxMessageLength))
	tooLongEntry := reallyLongEntry + "a"

	contents := formatEntry(severity.INFO, t1, 0, "clog_test.go", 136, "info")
	contents += formatEntry(severity.INFO, t2, 1, "clog_test.go", 137, "multi-\nline")
	contents += formatEntry(severity.INFO, t3, 2, "clog_test.go", 138, reallyLongEntry)
	contents += formatEntry(severity.INFO, t4, 3, "clog_test.go", 139, tooLongEntry)
	contents += formatEntry(severity.WARNING, t5, 4, "clog_test.go", 140, "warning")
	contents += formatEntry(severity.ERROR, t6, 5, "clog_test.go", 141, "error")
	contents += formatEntry(severity.FATAL, t7, 6, "clog_test.go", 142, "fatal\nstack\ntrace")
	contents += formatEntry(severity.INFO, t8, 7, "clog_test.go", 143, tooLongEntry)

	readAllEntries := func(contents string) []logpb.Entry {
		decoder := NewEntryDecoder(strings.NewReader(contents), WithFlattenedSensitiveData)
		var entries []logpb.Entry
		var entry logpb.Entry
		for {
			if err := decoder.Decode(&entry); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatal(err)
			}
			entries = append(entries, entry)
		}
		return entries
	}

	entries := readAllEntries(contents)
	expected := []logpb.Entry{
		{
			Severity:  severity.INFO,
			Time:      t1.UnixNano(),
			Goroutine: 0,
			File:      `clog_test.go`,
			Line:      136,
			Message:   `info`,
		},
		{
			Severity:  severity.INFO,
			Time:      t2.UnixNano(),
			Goroutine: 1,
			File:      `clog_test.go`,
			Line:      137,
			Message: `multi-
line`,
		},
		{
			Severity:  severity.INFO,
			Time:      t3.UnixNano(),
			Goroutine: 2,
			File:      `clog_test.go`,
			Line:      138,
			Message:   reallyLongEntry,
		},
		{
			Severity:  severity.INFO,
			Time:      t4.UnixNano(),
			Goroutine: 3,
			File:      `clog_test.go`,
			Line:      139,
			Message:   tooLongEntry[:maxMessageLength],
		},
		{
			Severity:  severity.WARNING,
			Time:      t5.UnixNano(),
			Goroutine: 4,
			File:      `clog_test.go`,
			Line:      140,
			Message:   `warning`,
		},
		{
			Severity:  severity.ERROR,
			Time:      t6.UnixNano(),
			Goroutine: 5,
			File:      `clog_test.go`,
			Line:      141,
			Message:   `error`,
		},
		{
			Severity:  severity.FATAL,
			Time:      t7.UnixNano(),
			Goroutine: 6,
			File:      `clog_test.go`,
			Line:      142,
			Message: `fatal
stack
trace`,
		},
		{
			Severity:  severity.INFO,
			Time:      t8.UnixNano(),
			Goroutine: 7,
			File:      `clog_test.go`,
			Line:      143,
			Message:   tooLongEntry[:maxMessageLength],
		},
	}
	if !reflect.DeepEqual(expected, entries) {
		t.Fatalf("%s\n", strings.Join(pretty.Diff(expected, entries), "\n"))
	}

	entries = readAllEntries("file header\n\n\n" + contents)
	if !reflect.DeepEqual(expected, entries) {
		t.Fatalf("%s\n", strings.Join(pretty.Diff(expected, entries), "\n"))
	}
}

// Test that an Error log goes to Warning and Info.
// Even in the Info log, the source character will be E, so the data should
// all be identical.
func TestError(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
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
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
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
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
	defer capture()()

	_ = logging.vmoduleConfig.verbosity.Set("2")
	defer func() { _ = logging.vmoduleConfig.verbosity.Set("0") }()
	if V(2) {
		addStructured(context.Background(), severity.INFO, 1, "", []interface{}{"test"})
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
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
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
		addStructured(context.Background(), severity.INFO, 1, "", []interface{}{"test"})
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
	setFlags()
	defer capture()()

	_ = SetVModule("notthisfile=2")
	defer func() { _ = SetVModule("") }()
	for i := 1; i <= 3; i++ {
		if V(Level(i)) {
			t.Errorf("V enabled for %d", i)
		}
	}
	if V(2) {
		addStructured(context.Background(), severity.INFO, 1, "", []interface{}{"test"})
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
	setFlags()
	defer capture()()

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
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()

	Info(context.Background(), "x")

	sb, ok := debugLog.getFileSink().mu.file.(*syncBuffer)
	if !ok {
		t.Fatalf("buffer wasn't created")
	}

	results, err := ListLogFiles()
	if err != nil {
		t.Fatalf("error in ListLogFiles: %v", err)
	}

	expectedName := filepath.Base(sb.file.Name())
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

func TestGetLogReader(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
	Info(context.Background(), "x")
	info, ok := debugLog.getFileSink().mu.file.(*syncBuffer)
	if !ok {
		t.Fatalf("buffer wasn't created")
	}
	infoName := filepath.Base(info.file.Name())

	curDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relPathFromCurDir, err := filepath.Rel(curDir, info.file.Name())
	if err != nil {
		t.Fatal(err)
	}

	dir := debugLog.getFileSink().mu.logDir
	if dir == "" {
		t.Fatal(errDirectoryNotSet)
	}
	otherFile, err := os.Create(filepath.Join(dir, "other.txt"))
	if err != nil {
		t.Fatal(err)
	}
	otherFile.Close()
	relPathFromLogDir := strings.Join([]string{"..", filepath.Base(dir), infoName}, string(os.PathSeparator))

	testCases := []struct {
		filename           string
		expErrRestricted   string
		expErrUnrestricted string
	}{
		// File is not specified (trying to open a directory instead).
		{dir, "pathnames must be basenames", "not a regular file"},
		// Absolute filename is specified.
		{info.file.Name(), "pathnames must be basenames", ""},
		// Symlink to a log file.
		{filepath.Join(dir, removePeriods(program)+".log"), "pathnames must be basenames", ""},
		// Symlink relative to logDir.
		{removePeriods(program) + ".log", "symlinks are not allowed", ""},
		// Non-log file.
		{"other.txt", "malformed log filename", "malformed log filename"},
		// Non-existent file matching RE.
		{"cockroach.roach0.root.2015-09-25T19_24_19Z.00000.log", "no such file", "no such file"},
		// Base filename is specified.
		{infoName, "", ""},
		// Relative path with directory components.
		{relPathFromCurDir, "pathnames must be basenames", ""},
		// Relative path within the logs directory.
		{relPathFromLogDir, "pathnames must be basenames", ""},
	}

	for _, test := range testCases {
		t.Run(test.filename, func(t *testing.T) {
			for _, restricted := range []bool{true, false} {
				t.Run(fmt.Sprintf("restricted=%t", restricted), func(t *testing.T) {
					var expErr string
					if restricted {
						expErr = test.expErrRestricted
					} else {
						expErr = test.expErrUnrestricted
					}
					reader, err := GetLogReader(test.filename, restricted)
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
		})
	}
}

func TestRollover(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	setFlags()
	var err error
	setExitErrFunc(false /* hideStack */, func(_ exit.Code, e error) {
		err = e
	})

	debugFileSink := debugLog.getFileSink()
	defer func(previous int64) { debugFileSink.logFileMaxSize = previous }(debugFileSink.logFileMaxSize)
	debugFileSink.logFileMaxSize = 2048

	Info(context.Background(), "x") // Be sure we have a file.
	info, ok := debugFileSink.mu.file.(*syncBuffer)
	if !ok {
		t.Fatal("info wasn't created")
	}
	if err != nil {
		t.Fatalf("info has initial error: %v", err)
	}
	fname0 := info.file.Name()
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
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	setFlags()
	logging.stderrSink.threshold = severity.NONE
	SetExitFunc(false /* hideStack */, func(exit.Code) {})

	defer setFlags()
	defer capture()()

	for _, level := range []int{tracebackNone, tracebackSingle, tracebackAll} {
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
	}
}

func TestRedirectStderr(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	setFlags()
	logging.stderrSink.threshold = severity.NONE

	Infof(context.Background(), "test")

	TestingResetActive()
	cleanup, err := SetupRedactionAndStderrRedirects()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	const stderrText = "hello stderr"
	fmt.Fprint(os.Stderr, stderrText)

	contents, err := ioutil.ReadFile(stderrLog.getFileSink().mu.file.(*syncBuffer).file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), stderrText) {
		t.Fatalf("log does not contain stderr text\n%s", contents)
	}
}

func TestFileSeverityFilter(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	setFlags()
	debugFileSink := debugLog.getFileSink()
	defer func(save Severity) { debugFileSink.threshold = save }(debugFileSink.threshold)
	debugFileSink.threshold = severity.ERROR

	Infof(context.Background(), "test1")
	Errorf(context.Background(), "test2")

	Flush()

	contents, err := ioutil.ReadFile(debugFileSink.mu.file.(*syncBuffer).file.Name())
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
	setFlags()

	var exited sync.WaitGroup
	exited.Add(1)

	SetExitFunc(false, func(exit.Code) {
		exited.Done()
	})

	fs := &fileSink{}
	l := &loggerT{sinkInfos: []sinkInfo{{
		sink:        fs,
		editor:      func(r redactablePackage) redactablePackage { return r },
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
	var f formatCrdbV1
	for i := 0; i < b.N; i++ {
		buf := f.formatEntry(entry, nil /* stacks */)
		putBuffer(buf)
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
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
	defer capture()()

	tmpDir, err := ioutil.TempDir("", fileutil.EscapeFilename(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !t.Failed() {
			_ = os.RemoveAll(tmpDir)
		}
	}()

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
	logging.stderrSink.threshold.SetValue(severity.INFO)

	// Make stderr non-critical.
	// We assume that the stderr sink is the first one.
	defer func(prevCriticality bool) { debugLog.sinkInfos[0].criticality = prevCriticality }(debugLog.sinkInfos[0].criticality)
	debugLog.sinkInfos[0].criticality = false

	// Now emit the log message. If criticality is respected, the
	// failure to write on stderr is graceful and the message gets
	// printed on the file output (and can be picked up by the contains
	// function). If it is not, the test runner will stop abruptly.
	Error(context.Background(), specialMessage)

	if !contains(specialMessage, t) {
		t.Fatalf("expected special message in file, got:\n%s", contents())
	}
}
