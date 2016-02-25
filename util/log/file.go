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

// File I/O for logs.

// Author: Bram Gruneir (bram@cockroachlabs.com)

package log

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MaxSize is the maximum size of a log file in bytes.
var MaxSize uint64 = 1024 * 1024 * 10

// If non-empty, overrides the choice of directory in which to write logs. See
// createLogDirs for the full list of possible destinations. Note that the
// default is to log to stderr independent of this setting. See --logtostderr.
var logDir = os.TempDir()
var logDirSet bool

// DirSet returns true of the log directory has been changed from its default.
func DirSet() bool {
	return logDirSet
}

type stringValue struct {
	val *string
	set *bool
}

var _ flag.Value = &stringValue{}

func newStringValue(val *string, set *bool) *stringValue {
	return &stringValue{val: val, set: set}
}

func (s *stringValue) Set(val string) error {
	*s.val = val
	*s.set = true
	return nil
}

func (s *stringValue) Type() string {
	return "string"
}

func (s *stringValue) String() string {
	return *s.val
}

// logFileRE matches log files to avoid exposing non-log files accidentally
// and it splits the details of the filename into groups for easy parsing.
// The log file format is {process}.{host}.{username}.log.{severity}.{timestamp}
// cockroach.Brams-MacBook-Pro.bram.log.WARNING.2015-06-09T16_10_48-04_00.30209
// All underscore in process, host and username are escaped to double
// underscores and all periods are escaped to an underscore.
// For compatibility with Windows filenames, all colons from the timestamp
// (RFC3339) are converted to underscores.
var logFileRE = regexp.MustCompile(`([^\.]+)\.([^\.]+)\.([^\.]+)\.log\.(ERROR|WARNING|INFO)\.([^\.]+)\.(\d+)`)

var (
	pid      = os.Getpid()
	program  = filepath.Base(os.Args[0])
	host     = "unknownhost"
	userName = "unknownuser"
)

func init() {
	h, err := os.Hostname()
	if err == nil {
		host = shortHostname(h)
	}

	current, err := user.Current()
	if err == nil {
		userName = current.Username
	}

	// Sanitize userName since it may contain filepath separators on Windows.
	userName = strings.Replace(userName, `\`, "_", -1)
}

// shortHostname returns its argument, truncating at the first period.
// For instance, given "www.google.com" it returns "www".
func shortHostname(hostname string) string {
	if i := strings.Index(hostname, "."); i >= 0 {
		return hostname[:i]
	}
	return hostname
}

// removePeriods removes all extraneous periods. This is required to ensure that
// the only periods in the filename are the ones added by logName so it can
// be easily parsed.
func removePeriods(s string) string {
	return strings.Replace(s, ".", "", -1)
}

// logName returns a new log file name containing the severity, with start time
// t, and the name for the symlink for the severity.
func logName(severity Severity, t time.Time) (name, link string) {
	// Replace the ':'s in the time format with '_'s to allow for log files in
	// Windows.
	tFormatted := strings.Replace(t.Format(time.RFC3339), ":", "_", -1)

	name = fmt.Sprintf("%s.%s.%s.log.%s.%s.%d",
		removePeriods(program),
		removePeriods(host),
		removePeriods(userName),
		severity.Name(),
		tFormatted,
		pid)
	return name, removePeriods(program) + "." + severity.Name()
}

// A FileDetails holds all of the particulars that can be parsed by the name of
// a log file.
type FileDetails struct {
	Program  string
	Host     string
	UserName string
	Severity Severity
	Time     time.Time
	PID      uint
}

var errMalformedName = errors.New("malformed log filename")
var errMalformedSev = errors.New("malformed severity")

func parseLogFilename(filename string) (FileDetails, error) {
	matches := logFileRE.FindStringSubmatch(filename)
	if matches == nil || len(matches) != 7 {
		return FileDetails{}, errMalformedName
	}

	sev, sevFound := SeverityByName(matches[4])
	if !sevFound {
		return FileDetails{}, errMalformedSev
	}

	// Replace the '_'s with ':'s to restore the correct time format.
	fixTime := strings.Replace(matches[5], "_", ":", -1)
	time, err := time.Parse(time.RFC3339, fixTime)
	if err != nil {
		return FileDetails{}, err
	}

	pid, err := strconv.ParseInt(matches[6], 10, 0)
	if err != nil {
		return FileDetails{}, err
	}

	return FileDetails{
		Program:  matches[1],
		Host:     matches[2],
		UserName: matches[3],
		Severity: sev,
		Time:     time,
		PID:      uint(pid),
	}, nil
}

var errDirectoryNotSet = errors.New("log: log directory not set")

// create creates a new log file and returns the file and its filename, which
// contains severity ("INFO", "FATAL", etc.) and t. If the file is created
// successfully, create also attempts to update the symlink for that tag, ignoring
// errors.
func create(severity Severity, t time.Time) (f *os.File, filename string, err error) {
	if len(logDir) == 0 {
		return nil, "", errDirectoryNotSet
	}
	name, link := logName(severity, t)
	var lastErr error
	fname := filepath.Join(logDir, name)

	// Open the file os.O_APPEND|os.O_CREATE rather than use os.Create.
	// Append is almost always more efficient than O_RDRW on most modern file systems.
	f, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err == nil {
		symlink := filepath.Join(logDir, link)
		_ = os.Remove(symlink)        // ignore err
		_ = os.Symlink(name, symlink) // ignore err
		return f, fname, nil
	}
	lastErr = err
	return nil, "", fmt.Errorf("log: cannot create log: %v", lastErr)
}

var errNotAFile = errors.New("not a regular file")

// getFileDetails verifies that the file specified by filename is a
// regular file and filename matches the expected filename pattern.
// Returns the log file details success; otherwise error.
func getFileDetails(info os.FileInfo) (FileDetails, error) {
	if info.Mode()&os.ModeType != 0 {
		return FileDetails{}, errNotAFile
	}

	details, err := parseLogFilename(info.Name())
	if err != nil {
		return FileDetails{}, err
	}

	return details, nil
}

func verifyFile(filename string) error {
	info, err := os.Stat(filename)
	if err != nil {
		return err
	}
	_, err = getFileDetails(info)
	return err
}

// A FileInfo holds the filename and size of a log file.
type FileInfo struct {
	Name         string // base name
	SizeBytes    int64
	ModTimeNanos int64 // most recent mode time in unix nanos
	Details      FileDetails
}

// ListLogFiles returns a slice of FileInfo structs for each log file
// on the local node, in any of the configured log directories.
func ListLogFiles() ([]FileInfo, error) {
	var results []FileInfo
	if logDir == "" {
		return nil, nil
	}
	infos, err := ioutil.ReadDir(logDir)
	if err != nil {
		return results, err
	}
	for _, info := range infos {
		details, err := getFileDetails(info)
		if err == nil {
			results = append(results, FileInfo{
				Name:         info.Name(),
				SizeBytes:    info.Size(),
				ModTimeNanos: info.ModTime().UnixNano(),
				Details:      details,
			})
		}
	}
	return results, nil
}

// GetLogReader returns a reader for the specified filename. In
// restricted mode, the filename must be the base name of a file in
// this process's log directory (this is safe for cases when the
// filename comes from external sources, such as the admin UI via
// HTTP). In unrestricted mode any path is allowed, with the added
// feature that relative paths will be searched in both the current
// directory and this process's log directory.
func GetLogReader(filename string, restricted bool) (io.ReadCloser, error) {
	if !restricted {
		if resolved, err := filepath.EvalSymlinks(filename); err == nil {
			if verifyFile(resolved) == nil {
				return os.Open(resolved)
			}
		}
	}
	// Verify there are no path separators in a restricted-mode pathname.
	if restricted && filepath.Base(filename) != filename {
		return nil, fmt.Errorf("pathnames must be basenames only: %s", filename)
	}
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(logDir, filename)
	}
	if !restricted {
		var err error
		filename, err = filepath.EvalSymlinks(filename)
		if err != nil {
			return nil, err
		}
	}
	if err := verifyFile(filename); err != nil {
		return nil, err
	}
	return os.Open(filename)
}

// sortableFileInfoSlice is required so we can sort FileInfos.
type sortableFileInfoSlice []FileInfo

func (a sortableFileInfoSlice) Len() int      { return len(a) }
func (a sortableFileInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortableFileInfoSlice) Less(i, j int) bool {
	return a[i].Details.Time.UnixNano() < a[j].Details.Time.UnixNano()
}

// selectFiles selects all log files that have an timestamp before the endTime and
// the correct severity. It then sorts them in decreasing order, with the most
// recent as the first one.
func selectFiles(logFiles []FileInfo, severity Severity, endTimestamp int64) []FileInfo {
	files := sortableFileInfoSlice{}
	for _, logFile := range logFiles {
		if logFile.Details.Severity == severity && logFile.Details.Time.UnixNano() <= endTimestamp {
			files = append(files, logFile)
		}
	}

	// Sort the files in reverse order so we will fetch the newest first.
	sort.Sort(sort.Reverse(files))
	return files
}

// FetchEntriesFromFiles fetches all available log entires on disk that match
// the log 'severity' (or worse) and are between the 'startTimestamp' and
// 'endTimestamp'. It will stop reading new files if the number of entries
// exceeds 'maxEntries'. Log entries are further filtered by the regexp
// 'pattern' if provided. The logs entries are returned in reverse chronological
// order.
func FetchEntriesFromFiles(severity Severity, startTimestamp, endTimestamp int64, maxEntries int,
	pattern *regexp.Regexp) ([]Entry, error) {
	logFiles, err := ListLogFiles()
	if err != nil {
		return nil, err
	}

	selectedFiles := selectFiles(logFiles, severity, endTimestamp)

	entries := []Entry{}
	for _, file := range selectedFiles {
		newEntries, entryBeforeStart, err := readAllEntriesFromFile(
			file,
			startTimestamp,
			endTimestamp,
			maxEntries-len(entries),
			pattern)
		if err != nil {
			return nil, err
		}
		entries = append(entries, newEntries...)
		if len(entries) >= maxEntries {
			break
		}
		if entryBeforeStart {
			// Stop processing files that won't have any timestamps after
			// startTime.
			break
		}
	}
	return entries, nil
}

// readAllEntriesFromFile reads in all log entries from a given file that are
// between the 'startTimestamp' and 'endTimestamp' and match the 'pattern' if it
// exists. It returns the entries in the reverse chronological order. It also
// returns a flag that denotes if any timestamp occurred before the
// 'startTimestamp' to inform the caller that no more log files need to be
// processed. If the number of entries returned exceeds 'maxEntries' then
// processing of new entries is stopped immediately.
func readAllEntriesFromFile(file FileInfo, startTimestamp, endTimestamp int64, maxEntries int,
	pattern *regexp.Regexp) ([]Entry, bool, error) {
	reader, err := GetLogReader(file.Name, true /* restricted */)
	defer reader.Close()
	if reader == nil || err != nil {
		return nil, false, err
	}
	entries := []Entry{}
	decoder := NewEntryDecoder(reader)
	entryBeforeStart := false
	for {
		entry := Entry{}
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}
		var match bool
		if pattern == nil {
			match = true
		} else {
			match = pattern.MatchString(entry.Message) ||
				pattern.MatchString(entry.File)
		}
		if match && entry.Time >= startTimestamp && entry.Time <= endTimestamp {
			entries = append([]Entry{entry}, entries...)
			if len(entries) >= maxEntries {
				break
			}
		}
		if entry.Time < startTimestamp {
			entryBeforeStart = true
		}

	}
	return entries, entryBeforeStart, nil
}
