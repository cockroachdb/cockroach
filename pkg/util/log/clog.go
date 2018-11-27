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
	"errors"
	"fmt"
	"io"
	stdLog "log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/ttycolor"
	"github.com/petermattis/goid"
)

const fatalErrorPostamble = `

****************************************************************************

This node experienced a fatal error (printed above), and as a result the
process is terminating.

Fatal errors can occur due to faulty hardware (disks, memory, clocks) or a
problem in CockroachDB. With your help, the support team at Cockroach Labs
will try to determine the root cause, recommend next steps, and we can
improve CockroachDB based on your report.

Please submit a crash report by following the instructions here:

    https://github.com/cockroachdb/cockroach/issues/new/choose

If you would rather not post publicly, please contact us directly at:

    support@cockroachlabs.com

The Cockroach Labs team appreciates your feedback.
`

// FatalChan is closed when Fatal is called. This can be used to make
// the process stop handling requests while the final log messages and
// crash report are being written.
func FatalChan() <-chan struct{} {
	return logging.fatalCh
}

const severityChar = "IWEF"

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

// DisableTracebacks turns off tracebacks for log.Fatals. Returns a function
// that sets the traceback settings back to where they were.
// Only intended for use by tests.
func DisableTracebacks() func() {
	oldVal := traceback
	traceback = tracebackNone
	return func() { traceback = oldVal }
}

// get returns the value of the Severity.
func (s *Severity) get() Severity {
	return Severity(atomic.LoadInt32((*int32)(s)))
}

// set sets the value of the Severity.
func (s *Severity) set(val Severity) {
	atomic.StoreInt32((*int32)(s), int32(val))
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
	s.set(threshold)
	return nil
}

// Name returns the string representation of the severity (i.e. ERROR, INFO).
func (s *Severity) Name() string {
	return s.String()
}

// SeverityByName attempts to parse the passed in string into a severity. (i.e.
// ERROR, INFO). If it succeeds, the returned bool is set to true.
func SeverityByName(s string) (Severity, bool) {
	s = strings.ToUpper(s)
	if i, ok := Severity_value[s]; ok {
		return Severity(i), true
	}
	switch s {
	case "TRUE":
		return Severity_INFO, true
	case "FALSE":
		return Severity_NONE, true
	}
	return 0, false
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
	if i := strings.LastIndexByte(file, '/'); i >= 0 {
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

// We don't include a capture group for the log message here, just for the
// preamble, because a capture group that handles multiline messages is very
// slow when running on the large buffers passed to EntryDecoder.split.
var entryRE = regexp.MustCompile(
	`(?m)^([IWEF])(\d{6} \d{2}:\d{2}:\d{2}.\d{6}) (?:(\d+) )?([^:]+):(\d+)`)

// EntryDecoder reads successive encoded log entries from the input
// buffer. Each entry is preceded by a single big-ending uint32
// describing the next entry's length.
type EntryDecoder struct {
	scanner            *bufio.Scanner
	truncatedLastEntry bool
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
		entry.Severity = Severity(strings.IndexByte(severityChar, m[1][0]) + 1)
		t, err := time.Parse("060102 15:04:05.999999", string(m[2]))
		if err != nil {
			return err
		}
		entry.Time = t.UnixNano()
		if len(m[3]) > 0 {
			goroutine, err := strconv.Atoi(string(m[3]))
			if err != nil {
				return err
			}
			entry.Goroutine = int64(goroutine)
		}
		entry.File = string(m[4])
		line, err := strconv.Atoi(string(m[5]))
		if err != nil {
			return err
		}
		entry.Line = int64(line)
		entry.Message = strings.TrimSpace(string(b[len(m[0]):]))
		return nil
	}
}

func (d *EntryDecoder) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if d.truncatedLastEntry {
		i := entryRE.FindIndex(data)
		if i == nil {
			// If there's no entry that starts in this chunk, advance past it, since
			// we've truncated the entry it was originally part of.
			return len(data), nil, nil
		}
		d.truncatedLastEntry = false
		if i[0] > 0 {
			// If an entry starts anywhere other than the first index, advance to it
			// to maintain the invariant that entries start at the beginning of data.
			// This isn't necessary, but simplifies the code below.
			return i[0], nil, nil
		}
		// If i[0] == 0, then a new entry starts at the beginning of data, so fall
		// through to the normal logic.
	}
	// From this point on, we assume we're currently positioned at a log entry.
	// We want to find the next one so we start our search at data[1].
	i := entryRE.FindIndex(data[1:])
	if i == nil {
		if atEOF {
			return len(data), data, nil
		}
		if len(data) >= bufio.MaxScanTokenSize {
			// If there's no room left in the buffer, return the current truncated
			// entry.
			d.truncatedLastEntry = true
			return len(data), data, nil
		}
		// If there is still room to read more, ask for more before deciding whether
		// to truncate the entry.
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

// the --no-color flag.
var noColor bool

// formatHeader formats a log header using the provided file name and
// line number. Log lines are colorized depending on severity.
//
// Log lines have this form:
// 	Lyymmdd hh:mm:ss.uuuuuu goid file:line msg...
// where the fields are defined as follows:
// 	L                A single character, representing the log level (eg 'I' for INFO)
// 	yy               The year (zero padded; ie 2016 is '16')
// 	mm               The month (zero padded; ie May is '05')
// 	dd               The day (zero padded)
// 	hh:mm:ss.uuuuuu  Time in hours, minutes and fractional seconds
// 	goid             The goroutine id (omitted if zero for use by tests)
// 	file             The file name
// 	line             The line number
// 	msg              The user-supplied message
func formatHeader(
	s Severity, now time.Time, gid int, file string, line int, cp ttycolor.Profile,
) *buffer {
	if noColor {
		cp = nil
	}

	buf := logging.getBuffer()
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
	}
	if s > Severity_FATAL || s <= Severity_UNKNOWN {
		s = Severity_INFO // for safety.
	}

	tmp := buf.tmp[:len(buf.tmp)]
	var n int
	var prefix []byte
	switch s {
	case Severity_INFO:
		prefix = cp[ttycolor.Cyan]
	case Severity_WARNING:
		prefix = cp[ttycolor.Yellow]
	case Severity_ERROR, Severity_FATAL:
		prefix = cp[ttycolor.Red]
	}
	n += copy(tmp, prefix)
	// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
	// It's worth about 3X. Fprintf is hard.
	year, month, day := now.Date()
	hour, minute, second := now.Clock()
	// Lyymmdd hh:mm:ss.uuuuuu file:line
	tmp[n] = severityChar[s-1]
	n++
	if year < 2000 {
		year = 2000
	}
	n += buf.twoDigits(n, year-2000)
	n += buf.twoDigits(n, int(month))
	n += buf.twoDigits(n, day)
	n += copy(tmp[n:], cp[ttycolor.Gray]) // gray for time, file & line
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
	if gid > 0 {
		n += buf.someDigits(n, gid)
		tmp[n] = ' '
		n++
	}
	buf.Write(tmp[:n])
	buf.WriteString(file)
	tmp[0] = ':'
	n = buf.someDigits(1, line)
	n++
	// Extra space between the header and the actual message for scannability.
	tmp[n] = ' '
	n++
	n += copy(tmp[n:], cp[ttycolor.Reset])
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

func formatLogEntry(entry Entry, stacks []byte, cp ttycolor.Profile) *buffer {
	buf := formatHeader(entry.Severity, timeutil.Unix(0, entry.Time),
		int(entry.Goroutine), entry.File, int(entry.Line), cp)
	msg := entry.Message
	if n := len(msg); n != 0 && msg[n-1] == '\n' {
		msg = msg[:n-1]
	}
	msg = strings.Replace(msg, "\n", "\n"+buf.String(), -1)
	_, _ = buf.WriteString(msg)
	_ = buf.WriteByte('\n')
	if len(stacks) > 0 {
		buf.Write(stacks)
	}
	return buf
}

func init() {
	// Default stderrThreshold and fileThreshold to log everything.
	// This will be the default in tests unless overridden; the CLI
	// commands set their default separately in cli/flags.go
	logging.stderrThreshold = Severity_INFO
	logging.fileThreshold = Severity_INFO

	logging.pcsPool = sync.Pool{
		New: func() interface{} {
			return [1]uintptr{}
		},
	}
	logging.prefix = program
	logging.setVState(0, nil, false)
	logging.gcNotify = make(chan struct{}, 1)
	logging.fatalCh = make(chan struct{})

	go flushDaemon()
	go signalFlusher()
}

// signalFlusher flushes the log(s) every time SIGHUP is received.
func signalFlusher() {
	ch := sysutil.RefreshSignaledChan()
	for sig := range ch {
		Infof(context.Background(), "%s received, flushing logs", sig)
		Flush()
	}
}

// LoggingToStderr returns true if log messages of the given severity
// are visible on stderr.
func LoggingToStderr(s Severity) bool {
	return s >= logging.stderrThreshold.get()
}

// StartGCDaemon starts the log file GC -- this must be called after
// command-line parsing has completed so that no data is lost when the
// user configures larger max sizes than the defaults.
func StartGCDaemon() {
	go logging.gcDaemon()
}

// Flush flushes all pending log I/O.
func Flush() {
	logging.lockAndFlushAll()
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for _, l := range secondaryLogRegistry.mu.loggers {
		// Some loggers (e.g. the audit log) want to keep all the files.
		l.logger.lockAndFlushAll()
	}
}

// SetSync configures whether logging synchronizes all writes.
func SetSync(sync bool) {
	logging.lockAndSetSync(sync)
	func() {
		secondaryLogRegistry.mu.Lock()
		defer secondaryLogRegistry.mu.Unlock()
		for _, l := range secondaryLogRegistry.mu.loggers {
			if !sync && l.forceSyncWrites {
				// We're not changing this.
				continue
			}
			l.logger.lockAndSetSync(sync)
		}
	}()
	if sync {
		// There may be something in the buffers already; flush it.
		Flush()
	}
}

// loggingT collects all the global state of the logging setup.
type loggingT struct {
	noStderrRedirect bool

	// Directory prefix where to store this logger's files.
	logDir DirName

	// Name prefix for log files.
	prefix string

	// Level flag for output to stderr. Handled atomically.
	stderrThreshold Severity
	// Level flag for output to files.
	fileThreshold Severity

	// freeList is a list of byte buffers, maintained under freeListMu.
	freeList *buffer
	// freeListMu maintains the free list. It is separate from the main mutex
	// so buffers can be grabbed and printed to without holding the main lock,
	// for better parallelization.
	freeListMu syncutil.Mutex

	// mu protects the remaining elements of this structure and is
	// used to synchronize logging.
	mu syncutil.Mutex
	// file holds the log file writer.
	file flushSyncWriter
	// syncWrites if true calls file.Flush and file.Sync on every log write.
	syncWrites bool
	// pcsPool maintains a set of [1]uintptr buffers to be used in V to avoid
	// allocating every time we compute the caller's PC.
	pcsPool sync.Pool
	// vmap is a cache of the V Level for each V() call site, identified by PC.
	// It is wiped whenever the vmodule flag changes state.
	vmap map[uintptr]level
	// filterLength stores the length of the vmodule filter chain. If greater
	// than zero, it means vmodule is enabled. It may be read safely
	// using sync.LoadInt32, but is only modified under mu.
	filterLength int32
	// traceLocation is the state of the -log_backtrace_at flag.
	traceLocation traceLocation
	// disableDaemons can be used to turn off both the GC and flush deamons.
	disableDaemons bool
	// These flags are modified only under lock, although verbosity may be fetched
	// safely using atomic.LoadInt32.
	vmodule      moduleSpec // The state of the --vmodule flag.
	verbosity    level      // V logging level, the value of the --verbosity flag/
	exitOverride struct {
		f         func(int) // overrides os.Exit when non-nil; testing only
		hideStack bool      // hides stack trace; only in effect when f is not nil
	}
	gcNotify chan struct{} // notify GC daemon that a new log file was created
	fatalCh  chan struct{} // closed on fatal error

	interceptor atomic.Value // InterceptorFn

	// The Cluster ID is reported on every new log file so as to ease the correlation
	// of panic reports with self-reported log files.
	clusterID string
}

// buffer holds a byte Buffer for reuse. The zero value is ready for use.
type buffer struct {
	bytes.Buffer
	tmp  [64]byte // temporary byte array for creating headers.
	next *buffer
}

var logging loggingT

// SetClusterID stores the Cluster ID for further reference.
func SetClusterID(clusterID string) {
	// Ensure that the clusterID is logged with the same format as for
	// new log files, even on the first log file. This ensures that grep
	// will always find it.
	file, line, _ := caller.Lookup(1)
	logging.outputLogEntry(Severity_INFO, file, line,
		fmt.Sprintf("[config] clusterID: %s", clusterID))

	// Perform the change proper.
	logging.mu.Lock()
	defer logging.mu.Unlock()

	if logging.clusterID != "" {
		panic("clusterID already set")
	}

	logging.clusterID = clusterID
}

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

// ensureFile ensures that l.file is set and valid.
func (l *loggingT) ensureFile() error {
	if l.file == nil {
		return l.createFile()
	}
	return nil
}

// writeToFile writes to the file and applies the synchronization policy.
func (l *loggingT) writeToFile(data []byte) error {
	if _, err := l.file.Write(data); err != nil {
		return err
	}
	if l.syncWrites {
		_ = l.file.Flush()
		_ = l.file.Sync()
	}
	return nil
}

// outputLogEntry marshals a log entry proto into bytes, and writes
// the data to the log files. If a trace location is set, stack traces
// are added to the entry before marshaling.
func (l *loggingT) outputLogEntry(s Severity, file string, line int, msg string) {
	// Set additional details in log entry.
	now := timeutil.Now()
	entry := MakeEntry(s, now.UnixNano(), file, line, msg)

	if f, ok := l.interceptor.Load().(InterceptorFn); ok && f != nil {
		f(entry)
		return
	}

	// TODO(tschottdorf): this is a pretty horrible critical section.
	l.mu.Lock()

	var stacks []byte
	var fatalTrigger chan struct{}
	if s == Severity_FATAL {
		// Close l.fatalCh if it is not already closed (note that we're
		// holding l.mu to guard against concurrent closes).
		select {
		case <-l.fatalCh:
		default:
			close(l.fatalCh)
		}

		switch traceback {
		case tracebackSingle:
			stacks = getStacks(false)
		case tracebackAll:
			stacks = getStacks(true)
		}
		stacks = append(stacks, []byte(fatalErrorPostamble)...)

		logExitFunc = func(error) {} // If we get a write error, we'll still exit.

		// We don't want to hang forever writing our final log message. If
		// things are broken (for example, if the disk fills up and there
		// are cascading errors and our process manager has stopped
		// reading from its side of a stderr pipe), it's more important to
		// let the process exit than limp along.
		//
		// Note that we do not use os.File.SetWriteDeadline because not
		// all files support this (for example, plain files on a network
		// file system do not support deadlines but can block
		// indefinitely).
		//
		// https://github.com/cockroachdb/cockroach/issues/23119
		fatalTrigger = make(chan struct{})
		exitFunc := os.Exit
		if l.exitOverride.f != nil {
			if l.exitOverride.hideStack {
				stacks = []byte("stack trace omitted via SetExitFunc)\n")
			}
			exitFunc = l.exitOverride.f
		}
		exitCalled := make(chan struct{})

		defer func() {
			<-exitCalled
		}()
		go func() {
			select {
			case <-time.After(10 * time.Second):
			case <-fatalTrigger:
			}
			exitFunc(255) // C++ uses -1, which is silly because it's anded with 255 anyway.
			close(exitCalled)
		}()
	} else if l.traceLocation.isSet() {
		if l.traceLocation.match(file, line) {
			stacks = getStacks(false)
		}
	}

	if s >= l.stderrThreshold.get() || (s == Severity_FATAL && stderrRedirected) {
		// We force-copy FATAL messages to stderr, because the process is bound
		// to terminate and the user will want to know why.
		l.outputToStderr(entry, stacks)
	}
	if l.logDir.IsSet() && s >= l.fileThreshold.get() {
		if err := l.ensureFile(); err != nil {
			// Make sure the message appears somewhere.
			l.outputToStderr(entry, stacks)
			l.exitLocked(err)
			l.mu.Unlock()
			return
		}

		buf := l.processForFile(entry, stacks)
		data := buf.Bytes()

		if err := l.writeToFile(data); err != nil {
			l.exitLocked(err)
			l.mu.Unlock()
			return
		}

		l.putBuffer(buf)
	}
	// Flush and exit on fatal logging.
	if s == Severity_FATAL {
		l.flushAndSync(true /*doSync*/)
		close(fatalTrigger)
	}
	l.mu.Unlock()
}

// printPanicToFile copies the panic details to the log file. This is
// useful when the standard error is not redirected to the log file
// (!stderrRedirected), as the go runtime will only print panics to
// stderr.
func (l *loggingT) printPanicToFile(r interface{}) {
	if !l.logDir.IsSet() {
		// There's no log file. Nothing to do.
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureFile(); err != nil {
		fmt.Fprintf(OrigStderr, "log: %v", err)
		return
	}

	panicBytes := []byte(fmt.Sprintf("%v\n\n%s\n", r, debug.Stack()))
	if err := l.writeToFile(panicBytes); err != nil {
		fmt.Fprintf(OrigStderr, "log: %v", err)
		return
	}
}

func (l *loggingT) outputToStderr(entry Entry, stacks []byte) {
	buf := l.processForStderr(entry, stacks)
	if _, err := OrigStderr.Write(buf.Bytes()); err != nil {
		l.exitLocked(err)
	}
	l.putBuffer(buf)
}

// processForStderr formats a log entry for output to standard error.
func (l *loggingT) processForStderr(entry Entry, stacks []byte) *buffer {
	return formatLogEntry(entry, stacks, ttycolor.StderrProfile)
}

// processForFile formats a log entry for output to a file.
func (l *loggingT) processForFile(entry Entry, stacks []byte) *buffer {
	return formatLogEntry(entry, stacks, nil)
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

// exitLocked is called if there is trouble creating or writing log files, or
// writing to stderr. It flushes the logs and exits the program; there's no
// point in hanging around. l.mu is held.
func (l *loggingT) exitLocked(err error) {
	l.mu.AssertHeld()

	// Either stderr or our log file is broken. Try writing the error to both
	// streams in the hope that one still works or else the user will have no idea
	// why we crashed.
	outputs := make([]io.Writer, 2)
	outputs[0] = OrigStderr
	if f, ok := l.file.(*syncBuffer); ok {
		// Don't call syncBuffer's Write method, because it can call back into
		// exitLocked. Go directly to syncBuffer's underlying writer.
		outputs[1] = f.Writer
	} else {
		outputs[1] = l.file
	}
	for _, w := range outputs {
		if w == nil {
			continue
		}
		fmt.Fprintf(w, "log: exiting because of error: %s\n", err)
	}
	// If logExitFunc is set, we do that instead of exiting.
	if logExitFunc != nil {
		logExitFunc(err)
		return
	}
	l.flushAndSync(true /*doSync*/)
	if l.exitOverride.f != nil {
		l.exitOverride.f(2)
	} else {
		os.Exit(2)
	}
}

// syncBuffer joins a bufio.Writer to its underlying file, providing access to the
// file's Sync method and providing a wrapper for the Write method that provides log
// file rotation. There are conflicting methods, so the file cannot be embedded.
// l.mu is held for all its methods.
type syncBuffer struct {
	logger *loggingT
	*bufio.Writer
	file         *os.File
	lastRotation int64
	nbytes       int64 // The number of bytes written to this file
}

func (sb *syncBuffer) Sync() error {
	return sb.file.Sync()
}

func (sb *syncBuffer) Write(p []byte) (n int, err error) {
	if sb.nbytes+int64(len(p)) >= atomic.LoadInt64(&LogFileMaxSize) {
		if err := sb.rotateFile(timeutil.Now()); err != nil {
			sb.logger.exitLocked(err)
		}
	}
	n, err = sb.Writer.Write(p)
	sb.nbytes += int64(n)
	if err != nil {
		sb.logger.exitLocked(err)
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
	sb.file, sb.lastRotation, _, err = create(&sb.logger.logDir, sb.logger.prefix, now, sb.lastRotation)
	sb.nbytes = 0
	if err != nil {
		return err
	}

	// Redirect stderr to the current INFO log file in order to capture panic
	// stack traces that are written by the Go runtime to stderr. Note that if
	// --logtostderr is true we'll never enter this code path and panic stack
	// traces will go to the original stderr as you would expect.
	if logging.stderrThreshold > Severity_INFO && !logging.noStderrRedirect {
		// NB: any concurrent output to stderr may straddle the old and new
		// files. This doesn't apply to log messages as we won't reach this code
		// unless we're not logging to stderr.
		if err := hijackStderr(sb.file); err != nil {
			return err
		}
	}

	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	messages := make([]string, 0, 6)
	messages = append(messages,
		fmt.Sprintf("[config] file created at: %s\n", now.Format("2006/01/02 15:04:05")),
		fmt.Sprintf("[config] running on machine: %s\n", host),
		fmt.Sprintf("[config] binary: %s\n", build.GetInfo().Short()),
		fmt.Sprintf("[config] arguments: %s\n", os.Args),
	)
	if sb.logger.clusterID != "" {
		messages = append(messages, fmt.Sprintf("[config] clusterID: %s\n", sb.logger.clusterID))
	}
	// Including a non-ascii character in the first 1024 bytes of the log helps
	// viewers that attempt to guess the character encoding.
	messages = append(messages, fmt.Sprintf("line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid file:line msg utf8=\u2713\n"))

	f, l, _ := caller.Lookup(1)
	for _, msg := range messages {
		buf := formatLogEntry(Entry{
			Severity:  Severity_INFO,
			Time:      now.UnixNano(),
			Goroutine: goid.Get(),
			File:      f,
			Line:      int64(l),
			Message:   msg,
		}, nil, nil)
		var n int
		n, err = sb.file.Write(buf.Bytes())
		sb.nbytes += int64(n)
		if err != nil {
			return err
		}
		logging.putBuffer(buf)
	}

	select {
	case logging.gcNotify <- struct{}{}:
	default:
	}
	return nil
}

// bufferSize sizes the buffer associated with each log file. It's large
// so that log records can accumulate without the logging thread blocking
// on disk I/O. The flushDaemon will block instead.
const bufferSize = 256 * 1024

func (l *loggingT) closeFileLocked() error {
	if l.file != nil {
		if sb, ok := l.file.(*syncBuffer); ok {
			if err := sb.file.Close(); err != nil {
				return err
			}
		}
		l.file = nil
	}
	return restoreStderr()
}

// createFile creates the log file.
// l.mu is held.
func (l *loggingT) createFile() error {
	now := timeutil.Now()
	if l.file == nil {
		sb := &syncBuffer{
			logger: l,
		}
		if err := sb.rotateFile(now); err != nil {
			return err
		}
		l.file = sb
	}
	return nil
}

// flushInterval is the delay between periodic flushes of the buffered log data.
const flushInterval = time.Second

// syncInterval is the multiple of flushInterval where the log is also synced to disk.
const syncInterval = 30

// flushDaemon periodically flushes and syncs the log file buffers.
//
// Flush propagates the in-memory buffer inside CockroachDB to the
// in-memory buffer(s) of the OS. The flush is relatively frequent so
// that a human operator can see "up to date" logging data in the log
// file.
//
// Syncs ensure that the OS commits the data to disk. Syncs are less
// frequent because they can incur more significant I/O costs.
func flushDaemon() {
	syncCounter := 1

	// This doesn't need to be Stop()'d as the loop never escapes.
	for range time.Tick(flushInterval) {
		doSync := syncCounter == syncInterval
		syncCounter = (syncCounter + 1) % syncInterval

		// Flush the main log.
		logging.mu.Lock()
		if !logging.disableDaemons {
			logging.flushAndSync(doSync)
		}
		logging.mu.Unlock()

		// Flush the secondary logs.
		secondaryLogRegistry.mu.Lock()
		for _, l := range secondaryLogRegistry.mu.loggers {
			l.logger.mu.Lock()
			if !l.logger.disableDaemons {
				l.logger.flushAndSync(doSync)
			}
			l.logger.mu.Unlock()
		}
		secondaryLogRegistry.mu.Unlock()
	}
}

// lockAndFlushAll is like flushAll but locks l.mu first.
func (l *loggingT) lockAndFlushAll() {
	l.mu.Lock()
	l.flushAndSync(true /*doSync*/)
	l.mu.Unlock()
}

// lockAndSetSync configures syncWrites
func (l *loggingT) lockAndSetSync(sync bool) {
	l.mu.Lock()
	l.syncWrites = sync
	l.mu.Unlock()
}

// flushAndSync flushes the current log and, if doSync is set,
// attempts to sync its data to disk.
// l.mu is held.
func (l *loggingT) flushAndSync(doSync bool) {
	if l.file != nil {
		_ = l.file.Flush() // ignore error
		if doSync {
			_ = l.file.Sync() // ignore error
		}
	}
}

func (l *loggingT) gcDaemon() {
	l.gcOldFiles()
	for range l.gcNotify {
		l.mu.Lock()
		if !l.disableDaemons {
			l.gcOldFiles()
		}
		l.mu.Unlock()
	}
}

func (l *loggingT) gcOldFiles() {
	dir, err := l.logDir.get()
	if err != nil {
		// No log directory configured. Nothing to do.
		return
	}

	// This only lists the log files for the current logger (sharing the
	// prefix).
	allFiles, err := l.listLogFiles()
	if err != nil {
		fmt.Fprintf(OrigStderr, "unable to GC log files: %s\n", err)
		return
	}

	logFilesCombinedMaxSize := atomic.LoadInt64(&LogFilesCombinedMaxSize)
	files := selectFiles(allFiles, math.MaxInt64)
	if len(files) == 0 {
		return
	}
	// files is sorted with the newest log files first (which we want
	// to keep). Note that we always keep the most recent log file.
	sum := files[0].SizeBytes
	for _, f := range files[1:] {
		sum += f.SizeBytes
		if sum < logFilesCombinedMaxSize {
			continue
		}
		path := filepath.Join(dir, f.Name)
		if err := os.Remove(path); err != nil {
			fmt.Fprintln(OrigStderr, err)
		}
	}
}

// copyStandardLogTo arranges for messages written to the Go "log"
// package's default logs to also appear in the CockroachDB logs with
// the specified severity.  Subsequent changes to the standard log's
// default output location or format may break this behavior.
//
// Valid names are "INFO", "WARNING", "ERROR", and "FATAL".  If the name is not
// recognized, copyStandardLogTo panics.
func copyStandardLogTo(severityName string) {
	sev, ok := SeverityByName(severityName)
	if !ok {
		panic(fmt.Sprintf("copyStandardLogTo(%q): unrecognized Severity name", severityName))
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

// NewStdLogger creates a *stdLog.Logger that forwards messages to the
// CockroachDB logs with the specified severity.
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
	if slash := strings.LastIndexByte(file, '/'); slash >= 0 {
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
	return VDepth(int32(level), 1)
}

// InterceptorFn is the type of function accepted by Intercept().
type InterceptorFn func(entry Entry)

// Intercept diverts log traffic to the given function `f`. When `f` is not nil,
// the logging package begins operating at full verbosity (i.e. `V(n) == true`
// for all `n`) but nothing will be printed to the logs. Instead, `f` is invoked
// for each log entry.
//
// To end log interception, invoke `Intercept()` with `f == nil`. Note that
// interception does not terminate atomically, that is, the originally supplied
// callback may still be invoked after a call to `Intercept` with `f == nil`.
func Intercept(ctx context.Context, f InterceptorFn) {
	logging.Intercept(ctx, f)
}

func (l *loggingT) Intercept(ctx context.Context, f InterceptorFn) {
	// TODO(tschottdorf): restore sanity so that all methods have a *loggingT
	// receiver.
	if f != nil {
		logDepth(ctx, 0, Severity_WARNING, "log traffic is now intercepted; log files will be incomplete", nil)
	}
	l.interceptor.Store(f) // intentionally also when f == nil
	if f == nil {
		logDepth(ctx, 0, Severity_INFO, "log interception is now stopped; normal logging resumes", nil)
	}
}

// VDepth reports whether verbosity at the call site is at least the requested
// level.
func VDepth(l int32, depth int) bool {
	// This function tries hard to be cheap unless there's work to do.
	// The fast path is three atomic loads and compares.

	// Here is a cheap but safe test to see if V logging is enabled globally.
	if logging.verbosity.get() >= level(l) {
		return true
	}

	if f, ok := logging.interceptor.Load().(InterceptorFn); ok && f != nil {
		return true
	}

	// It's off globally but vmodule may still be set.
	// Here is another cheap but safe test to see if vmodule is enabled.
	if atomic.LoadInt32(&logging.filterLength) > 0 {
		// Grab a buffer to use for reading the program counter. Keeping the
		// interface{} version around to Put back into the pool rather than
		// Put-ting the array saves an interface allocation.
		poolObj := logging.pcsPool.Get()
		pcs := poolObj.([1]uintptr)
		// We prefer not to use a defer in this function, which can be used in hot
		// paths, because a defer anywhere in the body of a function causes a call
		// to runtime.deferreturn at the end of that function, which has a
		// measurable performance penalty when in a very hot path.
		// defer logging.pcsPool.Put(pcs)
		if runtime.Callers(2+depth, pcs[:]) == 0 {
			logging.pcsPool.Put(poolObj)
			return false
		}
		logging.mu.Lock()
		v, ok := logging.vmap[pcs[0]]
		if !ok {
			v = logging.setV(pcs[0])
		}
		logging.mu.Unlock()
		logging.pcsPool.Put(poolObj)
		return v >= level(l)
	}
	return false
}
