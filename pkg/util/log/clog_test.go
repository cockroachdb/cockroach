// Copyright 2013 Google Inc. All Rights Reserved.
// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// swap sets the log writer and returns the old writer.
func (l *loggingT) swap(writer flushSyncWriter) (old flushSyncWriter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	old = l.file
	l.file = writer
	return old
}

// newBuffers sets the log writers to all new byte buffers and returns the old array.
func (l *loggingT) newBuffers() flushSyncWriter {
	return l.swap(new(flushBuffer))
}

// contents returns the specified log value as a string.
func contents() string {
	return logging.file.(*flushBuffer).Buffer.String()
}

// contains reports whether the string is contained in the log.
func contains(str string, t *testing.T) bool {
	c := contents()
	return strings.Contains(c, str)
}

// setFlags configures the logging flags and exitFunc how the test expects
// them.
func setFlags() {
	SetExitFunc(os.Exit)
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.noStderrRedirect = false
	logging.stderrThreshold = Severity_ERROR
}

// Test that Info works as advertised.
func TestInfo(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
	defer logging.swap(logging.newBuffers())
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
	defer logging.swap(logging.newBuffers())
	stdLog.Print("test")
	if !contains("I", t) {
		t.Errorf("Info has wrong character: %q", contents())
	}
	if !contains("test", t) {
		t.Error("Info failed")
	}
}

// Verify that a log can be fetched in JSON format.
func TestEntryDecoder(t *testing.T) {
	formatEntry := func(s Severity, now time.Time, gid int, file string, line int, msg string) string {
		buf := formatHeader(s, now, gid, file, line, nil)
		buf.WriteString(msg)
		buf.WriteString("\n")
		defer logging.putBuffer(buf)
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
	preambleLength := len(formatEntry(Severity_INFO, t1, 0, "clog_test.go", 136, ""))
	maxMessageLength := bufio.MaxScanTokenSize - preambleLength - 1
	reallyLongEntry := string(bytes.Repeat([]byte("a"), maxMessageLength))
	tooLongEntry := reallyLongEntry + "a"

	contents := formatEntry(Severity_INFO, t1, 0, "clog_test.go", 136, "info")
	contents += formatEntry(Severity_INFO, t2, 1, "clog_test.go", 137, "multi-\nline")
	contents += formatEntry(Severity_INFO, t3, 2, "clog_test.go", 138, reallyLongEntry)
	contents += formatEntry(Severity_INFO, t4, 3, "clog_test.go", 139, tooLongEntry)
	contents += formatEntry(Severity_WARNING, t5, 4, "clog_test.go", 140, "warning")
	contents += formatEntry(Severity_ERROR, t6, 5, "clog_test.go", 141, "error")
	contents += formatEntry(Severity_FATAL, t7, 6, "clog_test.go", 142, "fatal\nstack\ntrace")
	contents += formatEntry(Severity_INFO, t8, 7, "clog_test.go", 143, tooLongEntry)

	readAllEntries := func(contents string) []Entry {
		decoder := NewEntryDecoder(strings.NewReader(contents))
		var entries []Entry
		var entry Entry
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
	expected := []Entry{
		{
			Severity:  Severity_INFO,
			Time:      t1.UnixNano(),
			Goroutine: 0,
			File:      `clog_test.go`,
			Line:      136,
			Message:   `info`,
		},
		{
			Severity:  Severity_INFO,
			Time:      t2.UnixNano(),
			Goroutine: 1,
			File:      `clog_test.go`,
			Line:      137,
			Message: `multi-
line`,
		},
		{
			Severity:  Severity_INFO,
			Time:      t3.UnixNano(),
			Goroutine: 2,
			File:      `clog_test.go`,
			Line:      138,
			Message:   reallyLongEntry,
		},
		{
			Severity:  Severity_INFO,
			Time:      t4.UnixNano(),
			Goroutine: 3,
			File:      `clog_test.go`,
			Line:      139,
			Message:   tooLongEntry[:maxMessageLength],
		},
		{
			Severity:  Severity_WARNING,
			Time:      t5.UnixNano(),
			Goroutine: 4,
			File:      `clog_test.go`,
			Line:      140,
			Message:   `warning`,
		},
		{
			Severity:  Severity_ERROR,
			Time:      t6.UnixNano(),
			Goroutine: 5,
			File:      `clog_test.go`,
			Line:      141,
			Message:   `error`,
		},
		{
			Severity:  Severity_FATAL,
			Time:      t7.UnixNano(),
			Goroutine: 6,
			File:      `clog_test.go`,
			Line:      142,
			Message: `fatal
stack
trace`,
		},
		{
			Severity:  Severity_INFO,
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
	defer logging.swap(logging.newBuffers())
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
	defer logging.swap(logging.newBuffers())
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
	defer logging.swap(logging.newBuffers())
	_ = logging.verbosity.Set("2")
	defer func() { _ = logging.verbosity.Set("0") }()
	if v(2) {
		addStructured(context.Background(), Severity_INFO, 1, "", []interface{}{"test"})
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
	defer logging.swap(logging.newBuffers())
	_ = logging.vmodule.Set("clog_test=2")
	defer func() { _ = logging.vmodule.Set("") }()
	if !v(1) {
		t.Error("V not enabled for 1")
	}
	if !v(2) {
		t.Error("V not enabled for 2")
	}
	if v(3) {
		t.Error("V enabled for 3")
	}
	if v(2) {
		addStructured(context.Background(), Severity_INFO, 1, "", []interface{}{"test"})
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
	defer logging.swap(logging.newBuffers())
	_ = logging.vmodule.Set("notthisfile=2")
	defer func() { _ = logging.vmodule.Set("") }()
	for i := 1; i <= 3; i++ {
		if v(level(i)) {
			t.Errorf("V enabled for %d", i)
		}
	}
	if v(2) {
		addStructured(context.Background(), Severity_INFO, 1, "", []interface{}{"test"})
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
	defer logging.swap(logging.newBuffers())
	defer func() { _ = logging.vmodule.Set("") }()
	_ = logging.vmodule.Set(pat)
	if v(2) != match {
		t.Errorf("incorrect match for %q: got %t expected %t", pat, v(2), match)
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

	sb, ok := logging.file.(*syncBuffer)
	if !ok {
		t.Fatalf("buffer wasn't created")
	}

	expectedName := filepath.Base(sb.file.Name())

	results, err := ListLogFiles()
	if err != nil {
		t.Fatalf("error in ListLogFiles: %v", err)
	}

	if len(results) != 1 || results[0].Name != expectedName {
		t.Fatalf("unexpected results: %q", results)
	}
}

func TestGetLogReader(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
	Info(context.Background(), "x")
	info, ok := logging.file.(*syncBuffer)
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

	dir, err := logging.logDir.get()
	if err != nil {
		t.Fatal(err)
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
	defer func(previous func(error)) { logExitFunc = previous }(logExitFunc)
	logExitFunc = func(e error) {
		err = e
	}
	defer func(previous int64) { LogFileMaxSize = previous }(LogFileMaxSize)
	LogFileMaxSize = 2048

	Info(context.Background(), "x") // Be sure we have a file.
	info, ok := logging.file.(*syncBuffer)
	if !ok {
		t.Fatal("info wasn't created")
	}
	if err != nil {
		t.Fatalf("info has initial error: %v", err)
	}
	fname0 := info.file.Name()
	Info(context.Background(), strings.Repeat("x", int(LogFileMaxSize))) // force a rollover
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
	if info.nbytes >= LogFileMaxSize {
		t.Errorf("file size was not reset: %d", info.nbytes)
	}
}

func TestGC(t *testing.T) {
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

	// Create 1 log file to figure out its size.
	Infof(context.Background(), "0")

	allFilesOriginal, err := ListLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := 1, len(allFilesOriginal); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}
	dir, err := logging.logDir.get()
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
		Infof(context.Background(), "%d", i)
		Flush()
	}

	allFilesBefore, err := ListLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := newLogFiles, len(allFilesBefore); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}

	logging.gcOldFiles()

	allFilesAfter, err := ListLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	if e, a := expectedFilesAfterGC, len(allFilesAfter); e != a {
		t.Fatalf("expected %d files, but found %d", e, a)
	}
}

func TestLogBacktraceAt(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)

	setFlags()
	defer logging.swap(logging.newBuffers())
	// The peculiar style of this code simplifies line counting and maintenance of the
	// tracing block below.
	var infoLine string
	setTraceLocation := func(file string, line int, delta int) {
		_, file = filepath.Split(file)
		infoLine = fmt.Sprintf("%s:%d", file, line+delta)
		err := logging.traceLocation.Set(infoLine)
		if err != nil {
			t.Fatal("error setting log_backtrace_at: ", err)
		}
	}
	{
		// Start of tracing block. These lines know about each other's relative position.
		file, line, _ := caller.Lookup(0)
		setTraceLocation(file, line, +2) // Two lines between Caller and Info calls.
		Info(context.Background(), "we want a stack trace here")
		if err := logging.traceLocation.Set(""); err != nil {
			t.Fatal(err)
		}
	}
	numAppearances := strings.Count(contents(), infoLine)
	if numAppearances < 2 {
		// Need 2 appearances, one in the log header and one in the trace:
		//   log_test.go:281: I0511 16:36:06.952398 02238 log_test.go:280] we want a stack trace here
		//   ...
		//   github.com/clog/glog_test.go:280 (0x41ba91)
		//   ...
		// We could be more precise but that would require knowing the details
		// of the traceback format, which may not be dependable.
		t.Fatal("got no trace back; log is ", contents())
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
	logging.stderrThreshold = Severity_NONE
	SetExitFunc(func(int) {})

	defer setFlags()
	defer logging.swap(logging.newBuffers())

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
	logging.stderrThreshold = Severity_NONE

	Infof(context.Background(), "test")

	const stderrText = "hello stderr"
	fmt.Fprintf(os.Stderr, stderrText)

	contents, err := ioutil.ReadFile(logging.file.(*syncBuffer).file.Name())
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
	defer func(save Severity) { logging.fileThreshold = save }(logging.fileThreshold)
	logging.fileThreshold = Severity_ERROR

	Infof(context.Background(), "test1")
	Errorf(context.Background(), "test2")

	Flush()

	contents, err := ioutil.ReadFile(logging.file.(*syncBuffer).file.Name())
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
	oldLogExitFunc := logExitFunc
	logExitFunc = nil
	defer func() { logExitFunc = oldLogExitFunc }()

	var exited sync.WaitGroup
	exited.Add(1)
	l := &loggingT{
		exitFunc: func(int) {
			exited.Done()
		},
	}
	l.file = &syncBuffer{
		logger: l,
		Writer: bufio.NewWriterSize(&outOfSpaceWriter{}, 1),
	}

	l.mu.Lock()
	l.exitLocked(fmt.Errorf("out of space"))
	l.mu.Unlock()

	exited.Wait()
}

func BenchmarkHeader(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := formatHeader(Severity_INFO, timeutil.Now(), 200, "file.go", 100, nil)
		logging.putBuffer(buf)
	}
}
