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
//
// Original version (c) Google.
// Author (fork from https://github.com/golang/glog): Tobias Schottdorf

package log

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	stdLog "log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Severity identifies the sort of log: info, warning etc. It also implements
// the flag.Value interface. The -stderrthreshold flag is of type Severity and
// should be modified only through the flag.Value interface. The values match
// the corresponding constants in C++.
type Severity int32 // sync/atomic int32

// These constants identify the log levels in order of increasing Severity.
// A message written to a high-Severity log file is also written to each
// lower-Severity log file.
const (
	InfoLog Severity = iota
	WarningLog
	ErrorLog
	FatalLog
	NumSeverity = 4
)

const severityChar = "IWEF"

// severityName provides a mapping from Severity level to a string.
var severityName = []string{
	InfoLog:     "INFO",
	WarningLog:  "WARNING",
	ErrorLog:    "ERROR",
	FatalLog:    "FATAL",
	NumSeverity: "NONE",
}

const (
	tracebackNone = iota
	tracebackSingle
	tracebackAll
)

// Obey the GOTRACEBACK environment variable for determining which stacks to
// output during a log.Fatal.
var traceback = func() int {
	switch os.Getenv("GOTRACEBACK") {
	case "none":
		return tracebackNone
	case "single", "":
		return tracebackSingle
	default: // "all", "system", "crash"
		return tracebackAll
	}
}()

// get returns the value of the Severity.
func (s *Severity) get() Severity {
	return Severity(atomic.LoadInt32((*int32)(s)))
}

// set sets the value of the Severity.
func (s *Severity) set(val Severity) {
	atomic.StoreInt32((*int32)(s), int32(val))
}

// String is part of the flag.Value interface.
func (s *Severity) String() string {
	if i := int(*s); i >= 0 && i < len(severityName) {
		return severityName[i]
	}
	return strconv.FormatInt(int64(*s), 10)
}

// Set is part of the flag.Value interface.
func (s *Severity) Set(value string) error {
	var threshold Severity
	// Is it a known name?
	if v, ok := SeverityByName(value); ok {
		threshold = v
	} else {
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		threshold = Severity(v)
	}
	logging.stderrThreshold.set(threshold)
	return nil
}

// Name returns the string representation of the severity (i.e. ERROR, INFO).
func (s *Severity) Name() string {
	return severityName[s.get()]
}

// SeverityByName attempts to parse the passed in string into a severity. (i.e.
// ERROR, INFO). If it succeeds, the returned bool is set to true.
func SeverityByName(s string) (Severity, bool) {
	s = strings.ToUpper(s)
	for i, name := range severityName {
		if name == s {
			return Severity(i), true
		}
	}
	switch s {
	case "TRUE":
		return InfoLog, true
	case "FALSE":
		return NumSeverity, true
	}
	return 0, false
}

// colorProfile defines escape sequences which provide color in
// terminals. Some terminals support 8 colors, some 256, others
// none at all.
type colorProfile struct {
	infoPrefix  []byte
	warnPrefix  []byte
	errorPrefix []byte
	timePrefix  []byte
}

var colorReset = []byte("\033[0m")

// For terms with 8-color support.
var colorProfile8 = &colorProfile{
	infoPrefix:  []byte("\033[0;36;49m"),
	warnPrefix:  []byte("\033[0;33;49m"),
	errorPrefix: []byte("\033[0;31;49m"),
	timePrefix:  []byte("\033[2;37;49m"),
}

// For terms with 256-color support.
var colorProfile256 = &colorProfile{
	infoPrefix:  []byte("\033[38;5;33m"),
	warnPrefix:  []byte("\033[38;5;214m"),
	errorPrefix: []byte("\033[38;5;160m"),
	timePrefix:  []byte("\033[38;5;246m"),
}

// OutputStats tracks the number of output lines and bytes written.
type outputStats struct {
	lines int64
	bytes int64
}

// Stats tracks the number of lines of output and number of bytes
// per severity level. Values must be read with atomic.LoadInt64.
var Stats struct {
	Info, Warning, Error outputStats
}

var severityStats = [NumSeverity]*outputStats{
	InfoLog:    &Stats.Info,
	WarningLog: &Stats.Warning,
	ErrorLog:   &Stats.Error,
}

// Level is exported because it appears in the arguments to V and is
// the type of the v flag, which can be set programmatically.
// It's a distinct type because we want to discriminate it from logType.
// Variables of type level are only changed under logging.mu.
// The --verbosity flag is read only with atomic ops, so the state of the logging
// module is consistent.

// Level is treated as a sync/atomic int32.

// Level specifies a level of verbosity for V logs. *Level implements
// flag.Value; the --verbosity flag is of type Level and should be modified
// only through the flag.Value interface.
type level int32

// get returns the value of the Level.
func (l *level) get() level {
	return level(atomic.LoadInt32((*int32)(l)))
}

// set sets the value of the Level.
func (l *level) set(val level) {
	atomic.StoreInt32((*int32)(l), int32(val))
}

// String is part of the flag.Value interface.
func (l *level) String() string {
	return strconv.FormatInt(int64(*l), 10)
}

// Set is part of the flag.Value interface.
func (l *level) Set(value string) error {
	v, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.setVState(level(v), logging.vmodule.filter, false)
	return nil
}

// moduleSpec represents the setting of the --vmodule flag.
type moduleSpec struct {
	filter []modulePat
}

// modulePat contains a filter for the --vmodule flag.
// It holds a verbosity level and a file pattern to match.
type modulePat struct {
	pattern string
	literal bool // The pattern is a literal string
	level   level
}

// match reports whether the file matches the pattern. It uses a string
// comparison if the pattern contains no metacharacters.
func (m *modulePat) match(file string) bool {
	if m.literal {
		return file == m.pattern
	}
	match, _ := filepath.Match(m.pattern, file)
	return match
}

func (m *moduleSpec) String() string {
	// Lock because the type is not atomic. TODO: clean this up.
	logging.mu.Lock()
	defer logging.mu.Unlock()
	var b bytes.Buffer
	for i, f := range m.filter {
		if i > 0 {
			b.WriteRune(',')
		}
		fmt.Fprintf(&b, "%s=%d", f.pattern, f.level)
	}
	return b.String()
}

var errVmoduleSyntax = errors.New("syntax error: expect comma-separated list of filename=N")

// Syntax: --vmodule=recordio=2,file=1,gfs*=3
func (m *moduleSpec) Set(value string) error {
	var filter []modulePat
	for _, pat := range strings.Split(value, ",") {
		if len(pat) == 0 {
			// Empty strings such as from a trailing comma can be ignored.
			continue
		}
		patLev := strings.Split(pat, "=")
		if len(patLev) != 2 || len(patLev[0]) == 0 || len(patLev[1]) == 0 {
			return errVmoduleSyntax
		}
		pattern := patLev[0]
		v, err := strconv.Atoi(patLev[1])
		if err != nil {
			return errors.New("syntax error: expect comma-separated list of filename=N")
		}
		if v < 0 {
			return errors.New("negative value for vmodule level")
		}
		if v == 0 {
			continue // Ignore. It's harmless but no point in paying the overhead.
		}
		// TODO: check syntax of filter?
		filter = append(filter, modulePat{pattern, isLiteral(pattern), level(v)})
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.setVState(logging.verbosity, filter, true)
	return nil
}

// isLiteral reports whether the pattern is a literal string, that is, has no metacharacters
// that require filepath.Match to be called to match the pattern.
func isLiteral(pattern string) bool {
	return !strings.ContainsAny(pattern, `\*?[]`)
}

// traceLocation represents the setting of the -log_backtrace_at flag.
type traceLocation struct {
	file string
	line int
}

// isSet reports whether the trace location has been specified.
// logging.mu is held.
func (t *traceLocation) isSet() bool {
	return t.line > 0
}

// match reports whether the specified file and line matches the trace location.
// The argument file name is the full path, not the basename specified in the flag.
// logging.mu is held.
func (t *traceLocation) match(file string, line int) bool {
	if t.line != line {
		return false
	}
	if i := strings.LastIndex(file, "/"); i >= 0 {
		file = file[i+1:]
	}
	return t.file == file
}

func (t *traceLocation) String() string {
	// Lock because the type is not atomic. TODO: clean this up.
	logging.mu.Lock()
	defer logging.mu.Unlock()
	return fmt.Sprintf("%s:%d", t.file, t.line)
}

var errTraceSyntax = errors.New("syntax error: expect file.go:234")

// Syntax: -log_backtrace_at=gopherflakes.go:234
// Note that unlike vmodule the file extension is included here.
func (t *traceLocation) Set(value string) error {
	if value == "" {
		// Unset.
		logging.mu.Lock()
		defer logging.mu.Unlock()
		t.line = 0
		t.file = ""
		return nil
	}
	fields := strings.Split(value, ":")
	if len(fields) != 2 {
		return errTraceSyntax
	}
	file, line := fields[0], fields[1]
	if !strings.Contains(file, ".") {
		return errTraceSyntax
	}
	v, err := strconv.Atoi(line)
	if err != nil {
		return errTraceSyntax
	}
	if v <= 0 {
		return errors.New("negative or zero value for level")
	}
	logging.mu.Lock()
	defer logging.mu.Unlock()
	t.line = v
	t.file = file
	return nil
}

var entryRE = regexp.MustCompile(
	`(?m)^([IWEF])(\d{6} \d{2}:\d{2}:\d{2}.\d{6}) ([^:]+):(\d+)  (.*)`)

// EntryDecoder reads successive encoded log entries from the input
// buffer. Each entry is preceded by a single big-ending uint32
// describing the next entry's length.
type EntryDecoder struct {
	scanner *bufio.Scanner
}

// NewEntryDecoder creates a new instance of EntryDecoder.
func NewEntryDecoder(in io.Reader) *EntryDecoder {
	d := &EntryDecoder{scanner: bufio.NewScanner(in)}
	d.scanner.Split(d.split)
	return d
}

// Decode decodes the next log entry into the provided protobuf message.
func (d *EntryDecoder) Decode(entry *Entry) error {
	for {
		if !d.scanner.Scan() {
			if err := d.scanner.Err(); err != nil {
				return err
			}
			return io.EOF
		}
		b := d.scanner.Bytes()
		m := entryRE.FindSubmatch(b)
		if m == nil {
			continue
		}
		entry.Severity = strings.IndexByte(severityChar, m[1][0])
		t, err := time.ParseInLocation("060102 15:04:05.999999", string(m[2]), time.Local)
		if err != nil {
			return err
		}
		entry.Time = t.UnixNano()
		entry.File = string(m[3])
		entry.Line, err = strconv.Atoi(string(m[4]))
		if err != nil {
			return err
		}
		entry.Message = string(m[5])
		return nil
	}
}

func (d *EntryDecoder) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	// We assume we're currently positioned at a log entry. We want to find the
	// next one so we start our search at data[1].
	i := entryRE.FindIndex(data[1:])
	if i == nil {
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
	// i[0] is the start of the next log entry, but we need to adjust the value
	// to account for using data[1:] above.
	i[0]++
	return i[0], data[:i[0]], nil
}

// flushSyncWriter is the interface satisfied by logging destinations.
type flushSyncWriter interface {
	Flush() error
	Sync() error
	io.Writer
}

// formatHeader formats a log header using the provided file name and
// line number. Log lines are colorized depending on severity.
//
// Log lines have this form:
// 	Lyymmdd hh:mm:ss.uuuuuu file:line] msg...
// where the fields are defined as follows:
// 	L                A single character, representing the log level (eg 'I' for INFO)
// 	yy               The year (zero padded; ie 2016 is '16')
// 	mm               The month (zero padded; ie May is '05')
// 	dd               The day (zero padded)
// 	hh:mm:ss.uuuuuu  Time in hours, minutes and fractional seconds
// 	file             The file name
// 	line             The line number
// 	msg              The user-supplied message
func formatHeader(s Severity, now time.Time, file string, line int, colors *colorProfile) *buffer {
	buf := logging.getBuffer()
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
	}
	if s > FatalLog {
		s = InfoLog // for safety.
	}

	tmp := buf.tmp[:len(buf.tmp)]
	var n int
	if colors != nil {
		var prefix []byte
		switch s {
		case InfoLog:
			prefix = colors.infoPrefix
		case WarningLog:
			prefix = colors.warnPrefix
		case ErrorLog, FatalLog:
			prefix = colors.errorPrefix
		}
		n += copy(tmp, prefix)
	}
	// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
	// It's worth about 3X. Fprintf is hard.
	year, month, day := now.Date()
	hour, minute, second := now.Clock()
	// Lyymmdd hh:mm:ss.uuuuuu file:line
	tmp[n] = severityChar[s]
	n++
	n += buf.twoDigits(n, int(year)-2000)
	n += buf.twoDigits(n, int(month))
	n += buf.twoDigits(n, day)
	if colors != nil {
		n += copy(tmp[n:], colors.timePrefix) // gray for time, file & line
	}
	tmp[n] = ' '
	n++
	n += buf.twoDigits(n, hour)
	tmp[n] = ':'
	n++
	n += buf.twoDigits(n, minute)
	tmp[n] = ':'
	n++
	n += buf.twoDigits(n, second)
	tmp[n] = '.'
	n++
	n += buf.nDigits(6, n, now.Nanosecond()/1000, '0')
	tmp[n] = ' '
	n++
	buf.Write(tmp[:n])
	buf.WriteString(file)
	tmp[0] = ':'
	n = buf.someDigits(1, int(line))
	n++
	// Extra space between the header and the actual message for scannability.
	tmp[n] = ' '
	n++
	if colors != nil {
		n += copy(tmp[n:], colorReset)
	}
	tmp[n] = ' '
	n++
	buf.Write(tmp[:n])
	return buf
}

// Some custom tiny helper functions to print the log header efficiently.

const digits = "0123456789"

// twoDigits formats a zero-prefixed two-digit integer at buf.tmp[i].
// Returns two.
func (buf *buffer) twoDigits(i, d int) int {
	buf.tmp[i+1] = digits[d%10]
	d /= 10
	buf.tmp[i] = digits[d%10]
	return 2
}

// nDigits formats an n-digit integer at buf.tmp[i],
// padding with pad on the left.
// It assumes d >= 0. Returns n.
func (buf *buffer) nDigits(n, i, d int, pad byte) int {
	j := n - 1
	for ; j >= 0 && d > 0; j-- {
		buf.tmp[i+j] = digits[d%10]
		d /= 10
	}
	for ; j >= 0; j-- {
		buf.tmp[i+j] = pad
	}
	return n
}

// someDigits formats a zero-prefixed variable-width integer at buf.tmp[i].
func (buf *buffer) someDigits(i, d int) int {
	// Print into the top, then copy down. We know there's space for at least
	// a 10-digit number.
	j := len(buf.tmp)
	for {
		j--
		buf.tmp[j] = digits[d%10]
		d /= 10
		if d == 0 {
			break
		}
	}
	return copy(buf.tmp[i:], buf.tmp[j:])
}

func formatLogEntry(entry Entry, stacks []byte, colors *colorProfile) *buffer {
	buf := formatHeader(Severity(entry.Severity), time.Unix(0, entry.Time),
		entry.File, entry.Line, colors)
	_, _ = buf.WriteString(entry.Message)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		_ = buf.WriteByte('\n')
	}
	if len(stacks) > 0 {
		buf.Write(stacks)
	}
	return buf
}

func init() {
	// Default stderrThreshold to log nothing.
	logging.stderrThreshold = NumSeverity

	logging.setVState(0, nil, false)
	osExitFunc = os.Exit

	go logging.flushDaemon()
}

// Flush flushes all pending log I/O.
func Flush() {
	logging.lockAndFlushAll()
}

// loggingT collects all the global state of the logging setup.
type loggingT struct {
	nocolor         bool          // The -nocolor flag.
	hasColorProfile bool          // True if the color profile has been determined
	colorProfile    *colorProfile // Set via call to getTermColorProfile

	// Level flag. Handled atomically.
	stderrThreshold Severity // The -alsologtostderr flag.

	// freeList is a list of byte buffers, maintained under freeListMu.
	freeList *buffer
	// freeListMu maintains the free list. It is separate from the main mutex
	// so buffers can be grabbed and printed to without holding the main lock,
	// for better parallelization.
	freeListMu sync.Mutex

	// mu protects the remaining elements of this structure and is
	// used to synchronize logging.

	// Boolean flags. Also protected by mu (see flags.go).
	toStderr bool // The -logtostderr flag.

	mu sync.Mutex
	// file holds writer for each of the log types.
	file [NumSeverity]flushSyncWriter
	// pcs is used in V to avoid an allocation when computing the caller's PC.
	pcs [1]uintptr
	// vmap is a cache of the V Level for each V() call site, identified by PC.
	// It is wiped whenever the vmodule flag changes state.
	vmap map[uintptr]level
	// filterLength stores the length of the vmodule filter chain. If greater
	// than zero, it means vmodule is enabled. It may be read safely
	// using sync.LoadInt32, but is only modified under mu.
	filterLength int32
	// traceLocation is the state of the -log_backtrace_at flag.
	traceLocation traceLocation
	// These flags are modified only under lock, although verbosity may be fetched
	// safely using atomic.LoadInt32.
	vmodule   moduleSpec // The state of the --vmodule flag.
	verbosity level      // V logging level, the value of the --verbosity flag/
}

// buffer holds a byte Buffer for reuse. The zero value is ready for use.
type buffer struct {
	bytes.Buffer
	tmp  [64]byte // temporary byte array for creating headers.
	next *buffer
}

var logging loggingT

// setVState sets a consistent state for V logging.
// l.mu is held.
func (l *loggingT) setVState(verbosity level, filter []modulePat, setFilter bool) {
	// Turn verbosity off so V will not fire while we are in transition.
	logging.verbosity.set(0)
	// Ditto for filter length.
	atomic.StoreInt32(&logging.filterLength, 0)

	// Set the new filters and wipe the pc->Level map if the filter has changed.
	if setFilter {
		logging.vmodule.filter = filter
		logging.vmap = make(map[uintptr]level)
	}

	// Things are consistent now, so enable filtering and verbosity.
	// They are enabled in order opposite to that in V.
	atomic.StoreInt32(&logging.filterLength, int32(len(filter)))
	logging.verbosity.set(verbosity)
}

// getBuffer returns a new, ready-to-use buffer.
func (l *loggingT) getBuffer() *buffer {
	l.freeListMu.Lock()
	b := l.freeList
	if b != nil {
		l.freeList = b.next
	}
	l.freeListMu.Unlock()
	if b == nil {
		b = new(buffer)
	} else {
		b.next = nil
		b.Reset()
	}
	return b
}

// putBuffer returns a buffer to the free list.
func (l *loggingT) putBuffer(b *buffer) {
	if b.Len() >= 256 {
		// Let big buffers die a natural death.
		return
	}
	l.freeListMu.Lock()
	b.next = l.freeList
	l.freeList = b
	l.freeListMu.Unlock()
}

// outputLogEntry marshals a log entry proto into bytes, and writes
// the data to the log files. If a trace location is set, stack traces
// are added to the entry before marshaling.
func (l *loggingT) outputLogEntry(s Severity, file string, line int, msg string) {
	l.mu.Lock()

	// Set additional details in log entry.
	now := time.Now()
	entry := Entry{
		Severity: int(s),
		Time:     now.UnixNano(),
		File:     file,
		Line:     line,
		Message:  msg,
	}
	// On fatal log, set all stacks.
	var stacks []byte
	if s == FatalLog {
		switch traceback {
		case tracebackSingle:
			stacks = getStacks(false)
		case tracebackAll:
			stacks = getStacks(true)
		}
		logExitFunc = func(error) {} // If we get a write error, we'll still exit.
	} else if l.traceLocation.isSet() {
		if l.traceLocation.match(file, line) {
			stacks = getStacks(false)
		}
	}

	if l.toStderr {
		l.outputToStderr(entry, stacks)
	} else {
		if s >= l.stderrThreshold.get() {
			l.outputToStderr(entry, stacks)
		}
		if l.file[s] == nil {
			if err := l.createFiles(s); err != nil {
				// Make sure the message appears somewhere.
				l.outputToStderr(entry, stacks)
				l.exit(err)
			}
		}

		buf := l.processForFile(entry, stacks)
		data := buf.Bytes()

		switch s {
		case FatalLog:
			if _, err := l.file[FatalLog].Write(data); err != nil {
				panic(err)
			}
			fallthrough
		case ErrorLog:
			if _, err := l.file[ErrorLog].Write(data); err != nil {
				panic(err)
			}
			fallthrough
		case WarningLog:
			if _, err := l.file[WarningLog].Write(data); err != nil {
				panic(err)
			}
			fallthrough
		case InfoLog:
			if _, err := l.file[InfoLog].Write(data); err != nil {
				panic(err)
			}
		}

		l.putBuffer(buf)

		if stats := severityStats[s]; stats != nil {
			atomic.AddInt64(&stats.lines, 1)
			atomic.AddInt64(&stats.bytes, int64(len(data)))
		}
	}
	l.mu.Unlock()
	// Flush and exit on fatal logging.
	if s == FatalLog {
		// If we got here via Exit rather than Fatal, print no stacks.
		timeoutFlush(10 * time.Second)
		if atomic.LoadUint32(&fatalNoStacks) > 0 {
			osExitFunc(1)
		} else {
			osExitFunc(255) // C++ uses -1, which is silly because it's anded with 255 anyway.
		}
	}
}

func (l *loggingT) outputToStderr(entry Entry, stacks []byte) {
	buf := l.processForStderr(entry, stacks)
	if _, err := os.Stderr.Write(buf.Bytes()); err != nil {
		panic(err)
	}
	l.putBuffer(buf)
}

// processForStderr formats a log entry for output to standard error.
func (l *loggingT) processForStderr(entry Entry, stacks []byte) *buffer {
	return formatLogEntry(entry, stacks, l.getTermColorProfile())
}

// processForFile formats a log entry for output to a file.
func (l *loggingT) processForFile(entry Entry, stacks []byte) *buffer {
	return formatLogEntry(entry, stacks, nil)
}

// checkForColorTerm attempts to verify that stderr is a character
// device and if so, that the terminal supports color output.
func (l *loggingT) getTermColorProfile() *colorProfile {
	if !l.hasColorProfile {
		l.hasColorProfile = true
		if !l.nocolor {
			fi, _ := os.Stderr.Stat() // get the FileInfo struct describing the standard input.
			if (fi.Mode() & os.ModeCharDevice) != 0 {
				term := os.Getenv("TERM")
				switch term {
				case "ansi", "xterm-color":
					l.colorProfile = colorProfile8
				case "xterm-256color", "screen-256color":
					l.colorProfile = colorProfile256
				}
			}
		}
	}
	return l.colorProfile
}

// timeoutFlush calls Flush and returns when it completes or after timeout
// elapses, whichever happens first.  This is needed because the hooks invoked
// by Flush may deadlock when clog.Fatal is called from a hook that holds
// a lock.
func timeoutFlush(timeout time.Duration) {
	done := make(chan bool, 1)
	go func() {
		Flush() // calls logging.lockAndFlushAll()
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		fmt.Fprintln(os.Stderr, "clog: Flush took longer than", timeout)
	}
}

// getStacks is a wrapper for runtime.Stack that attempts to recover the data for all goroutines.
func getStacks(all bool) []byte {
	// We don't know how big the traces are, so grow a few times if they don't fit. Start large, though.
	n := 10000
	if all {
		n = 100000
	}
	var trace []byte
	for i := 0; i < 5; i++ {
		trace = make([]byte, n)
		nbytes := runtime.Stack(trace, all)
		if nbytes < len(trace) {
			return trace[:nbytes]
		}
		n *= 2
	}
	return trace
}

// logExitFunc provides a simple mechanism to override the default behavior
// of exiting on error. Used in testing and to guarantee we reach a required exit
// for fatal logs. Instead, exit could be a function rather than a method but that
// would make its use clumsier.
var logExitFunc func(error)
var osExitFunc func(int)

// exit is called if there is trouble creating or writing log files.
// It flushes the logs and exits the program; there's no point in hanging around.
// l.mu is held.
func (l *loggingT) exit(err error) {
	fmt.Fprintf(os.Stderr, "log: exiting because of error: %s\n", err)
	// If logExitFunc is set, we do that instead of exiting.
	if logExitFunc != nil {
		logExitFunc(err)
		return
	}
	l.flushAll()
	osExitFunc(2)
}

// syncBuffer joins a bufio.Writer to its underlying file, providing access to the
// file's Sync method and providing a wrapper for the Write method that provides log
// file rotation. There are conflicting methods, so the file cannot be embedded.
// l.mu is held for all its methods.
type syncBuffer struct {
	logger *loggingT
	*bufio.Writer
	file   *os.File
	sev    Severity
	nbytes uint64 // The number of bytes written to this file
}

func (sb *syncBuffer) Sync() error {
	return sb.file.Sync()
}

func (sb *syncBuffer) Write(p []byte) (n int, err error) {
	if sb.nbytes+uint64(len(p)) >= MaxSize {
		if err := sb.rotateFile(time.Now()); err != nil {
			sb.logger.exit(err)
		}
	}
	n, err = sb.Writer.Write(p)
	sb.nbytes += uint64(n)
	if err != nil {
		sb.logger.exit(err)
	}
	return
}

// rotateFile closes the syncBuffer's file and starts a new one.
func (sb *syncBuffer) rotateFile(now time.Time) error {
	if sb.file != nil {
		if err := sb.Flush(); err != nil {
			return err
		}
		if err := sb.file.Close(); err != nil {
			return err
		}
	}
	var err error
	sb.file, _, err = create(sb.sev, now)
	sb.nbytes = 0
	if err != nil {
		return err
	}

	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	// Write header.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Log file created at: %s\n", now.Format("2006/01/02 15:04:05"))
	fmt.Fprintf(&buf, "Running on machine: %s\n", host)
	fmt.Fprintf(&buf, "Binary: Built with %s %s for %s/%s\n", runtime.Compiler, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&buf, "Log line format: [IWEF]yymmdd hh:mm:ss.uuuuuu file:line msg\n")
	var n int
	n, err = sb.file.Write(buf.Bytes())
	sb.nbytes += uint64(n)
	return err
}

// bufferSize sizes the buffer associated with each log file. It's large
// so that log records can accumulate without the logging thread blocking
// on disk I/O. The flushDaemon will block instead.
const bufferSize = 256 * 1024

func (l *loggingT) removeFilesLocked() error {
	for s := FatalLog; s >= InfoLog; s-- {
		if sb, ok := l.file[s].(*syncBuffer); ok {
			if err := sb.file.Close(); err != nil {
				return err
			}
			if err := os.Remove(sb.file.Name()); err != nil {
				return err
			}
		}
		l.file[s] = nil
	}
	return nil
}

// createFiles creates all the log files for severity from sev down to InfoLog.
// l.mu is held.
func (l *loggingT) createFiles(sev Severity) error {
	now := time.Now()
	// Files are created in decreasing severity order, so as soon as we find one
	// has already been created, we can stop.
	for s := sev; s >= InfoLog && l.file[s] == nil; s-- {
		sb := &syncBuffer{
			logger: l,
			sev:    s,
		}
		if err := sb.rotateFile(now); err != nil {
			return err
		}
		l.file[s] = sb
	}
	return nil
}

const flushInterval = 30 * time.Second

// flushDaemon periodically flushes the log file buffers.
func (l *loggingT) flushDaemon() {
	// doesn't need to be Stop()'d as the loop never escapes
	for range time.Tick(flushInterval) {
		l.lockAndFlushAll()
	}
}

// lockAndFlushAll is like flushAll but locks l.mu first.
func (l *loggingT) lockAndFlushAll() {
	l.mu.Lock()
	l.flushAll()
	l.mu.Unlock()
}

// flushAll flushes all the logs and attempts to "sync" their data to disk.
// l.mu is held.
func (l *loggingT) flushAll() {
	// Flush from fatal down, in case there's trouble flushing.
	for s := FatalLog; s >= InfoLog; s-- {
		file := l.file[s]
		if file != nil {
			_ = file.Flush() // ignore error
			_ = file.Sync()  // ignore error
		}
	}
}

// copyStandardLogTo arranges for messages written to the Go "log" package's
// default logs to also appear in the Google logs for the named and lower
// severities.  Subsequent changes to the standard log's default output location
// or format may break this behavior.
//
// Valid names are "INFO", "WARNING", "ERROR", and "FATAL".  If the name is not
// recognized, copyStandardLogTo panics.
func copyStandardLogTo(name string) {
	sev, ok := SeverityByName(name)
	if !ok {
		panic(fmt.Sprintf("copyStandardLogTo(%q): unrecognized Severity name", name))
	}
	// Set a log format that captures the user's file and line:
	//   d.go:23: message
	stdLog.SetFlags(stdLog.Lshortfile)
	stdLog.SetOutput(logBridge(sev))
}

// logBridge provides the Write method that enables copyStandardLogTo to connect
// Go's standard logs to the logs provided by this package.
type logBridge Severity

// Write parses the standard logging line and passes its components to the
// logger for Severity(lb).
func (lb logBridge) Write(b []byte) (n int, err error) {
	var (
		file = "???"
		line = 1
		text string
	)
	// Split "d.go:23: message" into "d.go", "23", and "message".
	if parts := bytes.SplitN(b, []byte{':'}, 3); len(parts) != 3 || len(parts[0]) < 1 || len(parts[2]) < 1 {
		text = fmt.Sprintf("bad log format: %s", b)
	} else {
		file = string(parts[0])
		text = string(parts[2][1 : len(parts[2])-1]) // skip leading space and trailing newline
		line, err = strconv.Atoi(string(parts[1]))
		if err != nil {
			text = fmt.Sprintf("bad line number: %s", b)
			line = 1
		}
	}
	logging.outputLogEntry(Severity(lb), file, line, text)
	return len(b), nil
}

// NewStdLogger creates a *stdLog.Logger that forwards messages to the Google
// logs for the specified severity.
func NewStdLogger(severity Severity) *stdLog.Logger {
	return stdLog.New(logBridge(severity), "", stdLog.Lshortfile)
}

// setV computes and remembers the V level for a given PC
// when vmodule is enabled.
// File pattern matching takes the basename of the file, stripped
// of its .go suffix, and uses filepath.Match, which is a little more
// general than the *? matching used in C++.
// l.mu is held.
func (l *loggingT) setV(pc uintptr) level {
	fn := runtime.FuncForPC(pc)
	file, _ := fn.FileLine(pc)
	// The file is something like /a/b/c/d.go. We want just the d.
	if strings.HasSuffix(file, ".go") {
		file = file[:len(file)-3]
	}
	if slash := strings.LastIndex(file, "/"); slash >= 0 {
		file = file[slash+1:]
	}
	for _, filter := range l.vmodule.filter {
		if filter.match(file) {
			l.vmap[pc] = filter.level
			return filter.level
		}
	}
	l.vmap[pc] = 0
	return 0
}

func v(level level) bool {
	return VDepth(level, 1)
}

// VDepth reports whether verbosity at the call site is at least the requested
// level.
func VDepth(level level, depth int) bool {
	// This function tries hard to be cheap unless there's work to do.
	// The fast path is two atomic loads and compares.

	// Here is a cheap but safe test to see if V logging is enabled globally.
	if logging.verbosity.get() >= level {
		return true
	}

	// It's off globally but it vmodule may still be set.
	// Here is another cheap but safe test to see if vmodule is enabled.
	if atomic.LoadInt32(&logging.filterLength) > 0 {
		// Now we need a proper lock to use the logging structure. The pcs field
		// is shared so we must lock before accessing it. This is fairly expensive,
		// but if V logging is enabled we're slow anyway.
		logging.mu.Lock()
		defer logging.mu.Unlock()
		if runtime.Callers(2+depth, logging.pcs[:]) == 0 {
			return false
		}
		v, ok := logging.vmap[logging.pcs[0]]
		if !ok {
			v = logging.setV(logging.pcs[0])
		}
		return v >= level
	}
	return false
}

// fatalNoStacks is non-zero if we are to exit without dumping goroutine stacks.
// It allows Exit and relatives to use the Fatal logs.
var fatalNoStacks uint32
