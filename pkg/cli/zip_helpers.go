// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// zipper is the interface to the zip file stored on disk.
type zipper struct {
	// zipper implements Mutex because it's not possible for multiple
	// goroutines to write concurrently to a zip.Writer.
	syncutil.Mutex

	f *os.File
	z *zip.Writer
}

func newZipper(f *os.File) *zipper {
	return &zipper{
		f: f,
		z: zip.NewWriter(f),
	}
}

func (z *zipper) close() error {
	z.Lock()
	defer z.Unlock()

	err1 := z.z.Close()
	err2 := z.f.Close()
	return errors.CombineErrors(err1, err2)
}

// createLocked opens a new entry in the zip file. The caller is
// responsible for locking the zipper beforehand.
// Unsafe for concurrent use otherwise.
func (z *zipper) createLocked(name string, mtime time.Time) (io.Writer, error) {
	if mtime.IsZero() {
		mtime = timeutil.Now()
	}
	return z.z.CreateHeader(&zip.FileHeader{
		Name:     name,
		Method:   zip.Deflate,
		Modified: mtime,
	})
}

// createRaw creates an entry and writes its contents as a byte slice.
// Safe for concurrent use.
func (z *zipper) createRaw(s *zipReporter, name string, b []byte) error {
	z.Lock()
	defer z.Unlock()

	s.progress("writing binary output: %s", name)
	w, err := z.createLocked(name, time.Time{})
	if err != nil {
		return s.fail(err)
	}
	_, err = w.Write(b)
	return s.result(err)
}

// createJSON creates an entry and writes its contents from a struct payload, converted to JSON.
// Safe for concurrent use.
func (z *zipper) createJSON(s *zipReporter, name string, m interface{}) (err error) {
	if !strings.HasSuffix(name, ".json") {
		return s.fail(errors.Errorf("%s does not have .json suffix", name))
	}
	s.progress("converting to JSON")
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return s.fail(err)
	}
	return z.createRaw(s, name, b)
}

// createError reports an error payload.
// Safe for concurrent use.
func (z *zipper) createError(s *zipReporter, name string, e error) error {
	z.Lock()
	defer z.Unlock()

	s.shout("last request failed: %v", e)
	out := name + ".err.txt"
	s.progress("creating error output: %s", out)
	w, err := z.createLocked(out, time.Time{})
	if err != nil {
		return s.fail(err)
	}
	fmt.Fprintf(w, "%+v\n", e)
	s.done()
	return nil
}

// createJSONOrError calls either createError() or createJSON()
// depending on whether the error argument is nil.
// Safe for concurrent use.
func (z *zipper) createJSONOrError(s *zipReporter, name string, m interface{}, e error) error {
	if e != nil {
		return z.createError(s, name, e)
	}
	return z.createJSON(s, name, m)
}

// createJSONOrError calls either createError() or createRaw()
// depending on whether the error argument is nil.
// Safe for concurrent use.
func (z *zipper) createRawOrError(s *zipReporter, name string, b []byte, e error) error {
	if filepath.Ext(name) == "" {
		return errors.Errorf("%s has no extension", name)
	}
	if e != nil {
		return z.createError(s, name, e)
	}
	return z.createRaw(s, name, b)
}

// fileNameEscaper is used to generate file names when the name of the
// file is derived from a SQL identifier or other stored data. This is
// necessary because not all characters in SQL identifiers and strings
// can be used in file names.
type fileNameEscaper struct {
	counters map[string]int
}

// escape ensures that f is stripped of characters that
// may be invalid in file names. The characters are also lowercased
// to ensure proper normalization in case-insensitive filesystems.
func (fne *fileNameEscaper) escape(f string) string {
	f = strings.ToLower(f)
	var out strings.Builder
	for _, c := range f {
		if c < 127 && (unicode.IsLetter(c) || unicode.IsDigit(c)) {
			out.WriteRune(c)
		} else {
			out.WriteByte('_')
		}
	}
	objName := out.String()
	result := objName

	if fne.counters == nil {
		fne.counters = make(map[string]int)
	}
	cnt := fne.counters[objName]
	if cnt > 0 {
		result += fmt.Sprintf("-%d", cnt)
	}
	cnt++
	fne.counters[objName] = cnt
	return result
}

// nodeSelection is used to define a subset of the nodes on the command line.
type nodeSelection struct {
	inclusive     rangeSelection
	exclusive     rangeSelection
	includedCache map[int]struct{}
	excludedCache map[int]struct{}
}

func (n *nodeSelection) isIncluded(nodeID roachpb.NodeID) bool {
	// Avoid recomputing the maps on every call.
	if n.includedCache == nil {
		n.includedCache = n.inclusive.items()
	}
	if n.excludedCache == nil {
		n.excludedCache = n.exclusive.items()
	}

	// If the included cache is empty, then we're assuming the node is included.
	isIncluded := true
	if len(n.includedCache) > 0 {
		_, isIncluded = n.includedCache[int(nodeID)]
	}
	// Then filter out excluded IDs.
	if _, excluded := n.excludedCache[int(nodeID)]; excluded {
		isIncluded = false
	}
	return isIncluded
}

// rangeSelection enables the selection of multiple ranges of
// consecutive integers. Used in combination with the node selection
// to enable selecting ranges of node IDs.
type rangeSelection struct {
	input  string
	ranges []vrange
}

type vrange struct {
	a, b int
}

func (r *rangeSelection) String() string { return r.input }

func (r *rangeSelection) Type() string {
	return "a-b,c,d-e,..."
}

func (r *rangeSelection) Set(v string) error {
	r.input = v
	for _, rs := range strings.Split(v, ",") {
		var thisRange vrange
		if strings.Contains(rs, "-") {
			ab := strings.SplitN(rs, "-", 2)
			a, err := strconv.Atoi(ab[0])
			if err != nil {
				return err
			}
			b, err := strconv.Atoi(ab[1])
			if err != nil {
				return err
			}
			if b < a {
				return errors.New("invalid range")
			}
			thisRange = vrange{a, b}
		} else {
			a, err := strconv.Atoi(rs)
			if err != nil {
				return err
			}
			thisRange = vrange{a, a}
		}
		r.ranges = append(r.ranges, thisRange)
	}
	return nil
}

// items returns the values selected by the range selection.
func (r *rangeSelection) items() map[int]struct{} {
	s := map[int]struct{}{}
	for _, vr := range r.ranges {
		for i := vr.a; i <= vr.b; i++ {
			s[i] = struct{}{}
		}
	}
	return s
}

// fileSelection is used to define a subset of the files on the command line.
type fileSelection struct {
	includePatterns []string
	excludePatterns []string
	startTimestamp  timestampValue
	endTimestamp    timestampValue
}

// validate checks that all specified patterns are valid.
func (fs *fileSelection) validate() error {
	for _, p := range append(fs.includePatterns, fs.excludePatterns...) {
		if _, err := filepath.Match(p, ""); err != nil {
			return err
		}
	}
	return nil
}

// retrievalPatterns returns the list of glob patterns to send to the
// server, when listing which files are remotely available. We perform
// this filtering server-side so that the inclusion pattern can be
// used to reduce the amount of data retrieved in the "get file list"
// response.
func (fs *fileSelection) retrievalPatterns() []string {
	if len(fs.includePatterns) == 0 {
		// No include pattern defined: retrieve all files.
		return []string{"*"}
	}
	return fs.includePatterns
}

// isIncluded determine whether the given file name is included in the selection.
func (fs *fileSelection) isIncluded(filename string, ctime, mtime time.Time) bool {
	// To be included, a file must be included in at least one of the retrieval patterns.
	included := false
	for _, p := range fs.retrievalPatterns() {
		if matched, _ := filepath.Match(p, filename); matched {
			included = true
			break
		}
	}
	if !included {
		return false
	}
	// Then it must not match any of the exclusion patterns.
	for _, p := range fs.excludePatterns {
		if matched, _ := filepath.Match(p, filename); matched {
			included = false
			break
		}
	}
	if !included {
		return false
	}
	// Then its mtime must not be before the selected "from" time.
	if mtime.Before(time.Time(fs.startTimestamp)) {
		return false
	}
	// And the selected "until" time must not be before the ctime.
	// Note: the inverted call is because `Before` uses strict
	// inequality.
	if (*time.Time)(&fs.endTimestamp).Before(ctime) {
		return false
	}
	return true
}

// to prevent interleaved output.
var zipReportingMu syncutil.Mutex

// zipReporter is a helper struct that is responsible for printing
// progress messages for the zip command.
type zipReporter struct {
	// prefix is the string printed at the start of new lines.
	prefix string

	// flowing when set indicates the reporter should attempt to print
	// progress about a single item of work on the same line of output.
	flowing bool

	// newline is true when flowing is true and a newline has just been
	// printed, so that the next output can avoid emitting an extraneous
	// newline.
	newline bool

	// inItem helps asserting that the API is used in the right order:
	// withPrefix(), start(), info() are only valid while inItem is false,
	// whereas progress(), done() and fail() are only valid while inItem is true.
	inItem bool
}

func (zc *zipContext) newZipReporter(format string, args ...interface{}) *zipReporter {
	return &zipReporter{
		flowing: zc.concurrency == 1,
		prefix:  "[" + fmt.Sprintf(format, args...) + "]",
		newline: true,
		inItem:  false,
	}
}

// withPrefix creates a reported which adds the provided formatted
// message as additional prefix at the start of new lines.
func (z *zipReporter) withPrefix(format string, args ...interface{}) *zipReporter {
	zipReportingMu.Lock()
	defer zipReportingMu.Unlock()

	if z.inItem {
		panic(errors.AssertionFailedf("can't use withPrefix() under start()"))
	}

	z.completeprevLocked()
	return &zipReporter{
		prefix:  z.prefix + " [" + fmt.Sprintf(format, args...) + "]",
		flowing: z.flowing,
		newline: z.newline,
	}
}

// start begins a new unit of work. The returning reporter is
// specific to that unit of work. The caller can call .progress()
// zero or more times, and complete with .done() / .fail() /
// .result().
func (z *zipReporter) start(format string, args ...interface{}) *zipReporter {
	zipReportingMu.Lock()
	defer zipReportingMu.Unlock()

	if z.inItem {
		panic(errors.AssertionFailedf("can't use start() under start()"))
	}

	z.completeprevLocked()
	msg := z.prefix + " " + fmt.Sprintf(format, args...)
	nz := &zipReporter{
		prefix:  msg,
		flowing: z.flowing,
		inItem:  true,
	}
	fmt.Print(msg + "...")
	nz.flowLocked()
	return nz
}

// flowLocked is used internally by the reporter when progress on a
// unit of work can be followed with additional output.
//
// zipReporterMu is held.
func (z *zipReporter) flowLocked() {
	if !z.flowing {
		// Prevent multi-line output.
		fmt.Println()
	} else {
		z.newline = false
	}
}

// resumeLocked is used internally by the reporter when progress
// on a unit of work is resuming.
//
// zipReporterMu is held.
func (z *zipReporter) resumeLocked() {
	if !z.flowing || z.newline {
		fmt.Print(z.prefix + ":")
	}
	if z.flowing {
		z.newline = false
	}
}

// completeprevLocked is used internally by the reporter when a
// message that needs to stand out on its own is about to be printed,
// to complete any ongoing output and start a new line.
//
// zipReporterMu is held.
func (z *zipReporter) completeprevLocked() {
	if z.flowing && !z.newline {
		fmt.Println()
		z.newline = true
	}
}

// endlLocked is used internally by the reported when
// completing a message that needs to stand out on its own.
//
// zipReporterMu is held.
func (z *zipReporter) endlLocked() {
	fmt.Println()
	if z.flowing {
		z.newline = true
	}
}

// info prints a message through the reporter that
// needs to stand on its own.
func (z *zipReporter) info(format string, args ...interface{}) {
	zipReportingMu.Lock()
	defer zipReportingMu.Unlock()

	z.completeprevLocked()
	fmt.Print(z.prefix)
	fmt.Print(" ")
	fmt.Printf(format, args...)
	z.endlLocked()
}

// progress reports a step towards the current unit of work.
// Only valid for reporters generated via start(), before
// done/fail/result have been called.
func (z *zipReporter) progress(format string, args ...interface{}) {
	zipReportingMu.Lock()
	defer zipReportingMu.Unlock()

	if !z.inItem {
		panic(errors.AssertionFailedf("can't use progress() without start()"))
	}

	z.resumeLocked()
	fmt.Print(" ")
	fmt.Printf(format, args...)
	fmt.Print("...")
	z.flowLocked()
}

// shout is a variant of info which prints a colon after the
// prefix. This is intended for use after start().
func (z *zipReporter) shout(format string, args ...interface{}) {
	zipReportingMu.Lock()
	defer zipReportingMu.Unlock()

	z.completeprevLocked()
	fmt.Print(z.prefix + ": ")
	fmt.Printf(format, args...)
	z.endlLocked()
}

// done completes a unit of work started with start().
func (z *zipReporter) done() {
	zipReportingMu.Lock()
	defer zipReportingMu.Unlock()

	if !z.inItem {
		panic(errors.AssertionFailedf("can't use done() without start()"))
	}
	z.resumeLocked()
	fmt.Print(" done")
	z.endlLocked()
	z.inItem = false
}

// done completes a unit of work started with start().
func (z *zipReporter) fail(err error) error {
	zipReportingMu.Lock()
	defer zipReportingMu.Unlock()

	if !z.inItem {
		panic(errors.AssertionFailedf("can't use fail() without start()"))
	}

	z.resumeLocked()
	fmt.Print(" error:", err)
	z.endlLocked()
	z.inItem = false
	return err
}

// done completes a unit of work started with start().
func (z *zipReporter) result(err error) error {
	if err == nil {
		z.done()
		return nil
	}
	return z.fail(err)
}

// timestampValue is a wrapper around time.Time which supports the
// pflag.Value interface and can be initialized from a command line flag.
// It recognizes the following input formats:
//    YYYY-MM-DD
//    YYYY-MM-DD HH:MM
//    YYYY-MM-DD HH:MM:SS
type timestampValue time.Time

// Type implements the pflag.Value interface.
func (t *timestampValue) Type() string {
	return "YYYY-MM-DD [HH:MM[:SS]]"
}

func (t *timestampValue) String() string {
	return (*time.Time)(t).Format("2006-01-02 15:04:05")
}

// Set implements the pflag.Value interface.
func (t *timestampValue) Set(v string) error {
	v = strings.TrimSpace(v)
	var tm time.Time
	var err error
	if len(v) <= len("YYYY-MM-DD") {
		tm, err = time.ParseInLocation("2006-01-02", v, time.UTC)
	} else if len(v) <= len("YYYY-MM-DD HH:MM") {
		tm, err = time.ParseInLocation("2006-01-02 15:04", v, time.UTC)
	} else {
		tm, err = time.ParseInLocation("2006-01-02 15:04:05", v, time.UTC)
	}
	if err != nil {
		return err
	}
	*t = timestampValue(tm)
	return nil
}
