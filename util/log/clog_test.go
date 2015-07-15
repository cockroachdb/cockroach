// Go support for leveled logs, analogous to https://code.google.com/p/google-clog/
//
// Copyright 2013 Google Inc. All Rights Reserved.
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

package log

import (
	"bytes"
	"fmt"
	"io/ioutil"
	stdLog "log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/caller"
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

// swap sets the log writers and returns the old array.
func (l *loggingT) swap(writers [NumSeverity]flushSyncWriter) (old [NumSeverity]flushSyncWriter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	old = l.file
	for i, w := range writers {
		logging.file[i] = w
	}
	return
}

// newBuffers sets the log writers to all new byte buffers and returns the old array.
func (l *loggingT) newBuffers() [NumSeverity]flushSyncWriter {
	return l.swap([NumSeverity]flushSyncWriter{new(flushBuffer), new(flushBuffer), new(flushBuffer), new(flushBuffer)})
}

// contents returns the specified log value as a string.
func contents(s Severity) string {
	buffer := bytes.NewBuffer(logging.file[s].(*flushBuffer).Buffer.Bytes())
	hr := NewTermEntryReader(buffer)
	bytes, err := ioutil.ReadAll(hr)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

// jsonContents returns the specified log JSON-encoded.
func jsonContents(s Severity) []byte {
	buffer := bytes.NewBuffer(logging.file[s].(*flushBuffer).Buffer.Bytes())
	hr := NewJSONEntryReader(buffer)
	bytes, err := ioutil.ReadAll(hr)
	if err != nil {
		panic(err)
	}
	return bytes
}

// contains reports whether the string is contained in the log.
func contains(s Severity, str string, t *testing.T) bool {
	c := contents(s)
	return strings.Contains(c, str)
}

// setFlags configures the logging flags and osExitFunc how the test expects
// them.
func setFlags() {
	osExitFunc = os.Exit
	logging.toStderr = false
}

// Test that Info works as advertised.
func TestInfo(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	Info("test")
	if !contains(InfoLog, "I", t) {
		t.Errorf("Info has wrong character: %q", contents(InfoLog))
	}
	if !contains(InfoLog, "test", t) {
		t.Error("Info failed")
	}
}

func init() {
	CopyStandardLogTo("INFO")
}

// Test that CopyStandardLogTo panics on bad input.
func TestCopyStandardLogToPanic(t *testing.T) {
	setFlags()
	defer func() {
		if s, ok := recover().(string); !ok || !strings.Contains(s, "LOG") {
			t.Errorf(`CopyStandardLogTo("LOG") should have panicked: %v`, s)
		}
	}()
	CopyStandardLogTo("LOG")
}

// Test that using the standard log package logs to INFO.
func TestStandardLog(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	stdLog.Print("test")
	if !contains(InfoLog, "I", t) {
		t.Errorf("Info has wrong character: %q", contents(InfoLog))
	}
	if !contains(InfoLog, "test", t) {
		t.Error("Info failed")
	}
}

// Verify that a log can be fetched in JSON format.
func TestJSONLogFormat(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	stdLog.Print("test")
	json := jsonContents(InfoLog)
	expPat := `{
  "severity": 0,
  "time": [\d]+,
  "thread_id": [\d]+,
  "file": "clog_test.go",
  "line": [\d]+,
  "format": "test",
  "args": null
}`
	if ok, _ := regexp.Match(expPat, json); !ok {
		t.Errorf("expected json match; got %s", json)
	}
}

// Test that an Error log goes to Warning and Info.
// Even in the Info log, the source character will be E, so the data should
// all be identical.
func TestError(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	Error("test")
	if !contains(ErrorLog, "E", t) {
		t.Errorf("Error has wrong character: %q", contents(ErrorLog))
	}
	if !contains(ErrorLog, "test", t) {
		t.Error("Error failed")
	}
	str := contents(ErrorLog)
	if !contains(WarningLog, str, t) {
		t.Error("Warning failed")
	}
	if !contains(InfoLog, str, t) {
		t.Error("Info failed")
	}
}

// Test that a Warning log goes to Info.
// Even in the Info log, the source character will be W, so the data should
// all be identical.
func TestWarning(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	Warning("test")
	if !contains(WarningLog, "W", t) {
		t.Errorf("Warning has wrong character: %q", contents(WarningLog))
	}
	if !contains(WarningLog, "test", t) {
		t.Error("Warning failed")
	}
	str := contents(WarningLog)
	if !contains(InfoLog, str, t) {
		t.Error("Info failed")
	}
}

// Test that a V log goes to Info.
func TestV(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	_ = logging.verbosity.Set("2")
	defer func() { _ = logging.verbosity.Set("0") }()
	if v(2) {
		logging.print(InfoLog, "test")
	}
	if !contains(InfoLog, "I", t) {
		t.Errorf("Info has wrong character: %q", contents(InfoLog))
	}
	if !contains(InfoLog, "test", t) {
		t.Error("Info failed")
	}
}

// Test that a vmodule enables a log in this file.
func TestVmoduleOn(t *testing.T) {
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
		logging.print(InfoLog, "test")
	}
	if !contains(InfoLog, "I", t) {
		t.Errorf("Info has wrong character: %q", contents(InfoLog))
	}
	if !contains(InfoLog, "test", t) {
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
		logging.print(InfoLog, "test")
	}
	if contents(InfoLog) != "" {
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
	setFlags()
	*logDir = os.TempDir()

	Info("x")    // Be sure we have a file.
	Warning("x") // Be sure we have a file.
	var info, warn *syncBuffer
	var ok bool
	info, ok = logging.file[InfoLog].(*syncBuffer)
	if !ok {
		t.Fatal("info wasn't created")
	}
	infoName := filepath.Base(info.file.Name())
	warn, ok = logging.file[WarningLog].(*syncBuffer)
	if !ok {
		t.Fatal("warning wasn't created")
	}
	warnName := filepath.Base(warn.file.Name())
	results, err := ListLogFiles()
	if err != nil {
		t.Fatal(err)
	}
	var foundInfo, foundWarn bool
	for _, r := range results {
		fmt.Printf("Results: Name:%v\n", r.Name)
		if r.Name == infoName {
			foundInfo = true
		}
		if r.Name == warnName {
			foundWarn = true
		}
	}
	if !foundInfo || !foundWarn {
		t.Errorf("expected to find %s, %s; got %d results", infoName, warnName, len(results))
	}
}

func TestGetLogReader(t *testing.T) {
	setFlags()
	*logDir = os.TempDir()
	Warning("x")
	warn, ok := logging.file[WarningLog].(*syncBuffer)
	if !ok {
		t.Fatal("warning wasn't created")
	}
	warnName := filepath.Base(warn.file.Name())

	testCases := []struct {
		filename string
		allowAbs bool
		expErr   bool
	}{
		// File is not specified (trying to open a directory instead).
		{*logDir, false, true},
		{*logDir, true, true},
		// Absolute filename is specified.
		{warn.file.Name(), false, true},
		{warn.file.Name(), true, false},
		// File not matching log RE.
		{"cockroach.WARNING", false, true},
		{"cockroach.WARNING", true, true},
		{filepath.Join(*logDir, "cockroach.WARNING"), false, true},
		{filepath.Join(*logDir, "cockroach.WARNING"), true, true},
		// Relative filename is specified.
		{warnName, true, false},
		{warnName, false, false},
	}

	for i, test := range testCases {
		reader, err := GetLogReader(test.filename, test.allowAbs)
		if (err != nil) != test.expErr {
			t.Errorf("%d: expected error %t; got %t: %s", i, test.expErr, err != nil, err)
		}
		if reader != nil {
			reader.Close()
		}
	}
}

func TestRollover(t *testing.T) {
	setFlags()
	*logDir = os.TempDir()
	var err error
	defer func(previous func(error)) { logExitFunc = previous }(logExitFunc)
	logExitFunc = func(e error) {
		err = e
	}
	defer func(previous uint64) { MaxSize = previous }(MaxSize)
	MaxSize = 512

	Info("x") // Be sure we have a file.
	info, ok := logging.file[InfoLog].(*syncBuffer)
	if !ok {
		t.Fatal("info wasn't created")
	}
	if err != nil {
		t.Fatalf("info has initial error: %v", err)
	}
	fname0 := info.file.Name()
	Info(strings.Repeat("x", int(MaxSize))) // force a rollover
	if err != nil {
		t.Fatalf("info has error after big write: %v", err)
	}

	// Make sure the next log file gets a file name with a different
	// time stamp.
	//
	// TODO: determine whether we need to support subsecond log
	// rotation.  C++ does not appear to handle this case (nor does it
	// handle Daylight Savings Time properly).
	time.Sleep(1 * time.Second)

	Info("x") // create a new file
	if err != nil {
		t.Fatalf("error after rotation: %v", err)
	}
	fname1 := info.file.Name()
	if fname0 == fname1 {
		t.Errorf("info.f.Name did not change: %v", fname0)
	}
	if info.nbytes >= MaxSize {
		t.Errorf("file size was not reset: %d", info.nbytes)
	}
}

func TestLogBacktraceAt(t *testing.T) {
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
		Info("we want a stack trace here")
		if err := logging.traceLocation.Set(""); err != nil {
			t.Fatal(err)
		}
	}
	numAppearances := strings.Count(contents(InfoLog), infoLine)
	if numAppearances < 2 {
		// Need 2 appearances, one in the log header and one in the trace:
		//   log_test.go:281: I0511 16:36:06.952398 02238 log_test.go:280] we want a stack trace here
		//   ...
		//   github.com/clog/glog_test.go:280 (0x41ba91)
		//   ...
		// We could be more precise but that would require knowing the details
		// of the traceback format, which may not be dependable.
		t.Fatal("got no trace back; log is ", contents(InfoLog))
	}
}

// TestFatalStacktraceStderr verifies that a full stacktrace is output.
// This test would be more interesting if -logtostderr could actually
// be tested. Well, it wasn't, and it looked like stack trace dumping
// was broken when that option was used. This is fixed now, and perhaps
// in the future clog and this test can be adapted to actually test that;
// right now clog writes straight to os.StdErr.
func TestFatalStacktraceStderr(t *testing.T) {
	setFlags()
	logging.toStderr = false // TODO
	osExitFunc = func(int) {}

	defer setFlags()
	defer logging.swap(logging.newBuffers())

	Fatalf("cinap")
	cont := contents(FatalLog)
	msg := ""
	if !strings.Contains(cont, " cinap") {
		msg = "panic output does not contain cinap"
	} else if strings.Count(cont, "goroutine ") < 2 {
		msg = "stack trace contains less than two goroutines"
	} else if !strings.Contains(cont, "clog_test") {
		msg = "stack trace does not contain file name"
	}

	if msg != "" {
		t.Fatalf("%s: %s", msg, contents(FatalLog))
	}

}

func BenchmarkHeader(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := formatHeader(InfoLog, time.Now(), 1, "file.go", 100, nil)
		logging.putBuffer(buf)
	}
}
