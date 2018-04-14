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

// File I/O for logs.

package log

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// LogFileMaxSize is the maximum size of a log file in bytes.
var LogFileMaxSize int64 = 10 << 20 // 10MiB

// LogFilesCombinedMaxSize is the maximum total size in bytes for log
// files. Note that this is only checked when log files are created,
// so the total size of log files per severity might temporarily be up
// to LogFileMaxSize larger.
var LogFilesCombinedMaxSize = LogFileMaxSize * 10 // 100MiB

// DirName overrides (if non-empty) the choice of directory in
// which to write logs. See createLogDirs for the full list of
// possible destinations. Note that the default is to log to stderr
// independent of this setting. See --logtostderr.
type DirName struct {
	syncutil.Mutex
	name string
}

var _ flag.Value = &DirName{}

// Set implements the flag.Value interface.
func (l *DirName) Set(dir string) error {
	if len(dir) > 0 && dir[0] == '~' {
		return fmt.Errorf("log directory cannot start with '~': %s", dir)
	}
	if len(dir) > 0 {
		absDir, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		dir = absDir
	}
	l.Lock()
	defer l.Unlock()
	l.name = dir
	return nil
}

// Type implements the flag.Value interface.
func (l *DirName) Type() string {
	return "string"
}

// String implements the flag.Value interface.
func (l *DirName) String() string {
	l.Lock()
	defer l.Unlock()
	return l.name
}

func (l *DirName) get() (string, error) {
	l.Lock()
	defer l.Unlock()
	if len(l.name) == 0 {
		return "", errDirectoryNotSet
	}
	return l.name, nil
}

// IsSet returns true iff the directory name is set.
func (l *DirName) IsSet() bool {
	l.Lock()
	res := l.name != ""
	l.Unlock()
	return res
}

// DirSet returns true of the log directory has been changed from its default.
func DirSet() bool { return logging.logDir.IsSet() }

// logFileRE matches log files to avoid exposing non-log files accidentally
// and it splits the details of the filename into groups for easy parsing.
// The log file format is {process}.{host}.{username}.{timestamp}.{pid}.log
// cockroach.Brams-MacBook-Pro.bram.2015-06-09T16-10-48Z.30209.log
// All underscore in process, host and username are escaped to double
// underscores and all periods are escaped to an underscore.
// For compatibility with Windows filenames, all colons from the timestamp
// (RFC3339) are converted from underscores.
var logFileRE = regexp.MustCompile(`^(?:.*/)?([^/.]+)\.([^/\.]+)\.([^/\.]+)\.([^/\.]+)\.(\d+)\.log$`)

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
	if i := strings.IndexByte(hostname, '.'); i >= 0 {
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

// logName returns a new log file name with start time t, and the name
// for the symlink.
func logName(prefix string, t time.Time) (name, link string) {
	// Replace the ':'s in the time format with '_'s to allow for log files in
	// Windows.
	tFormatted := strings.Replace(t.Format(time.RFC3339), ":", "_", -1)

	name = fmt.Sprintf("%s.%s.%s.%s.%06d.log",
		removePeriods(prefix),
		removePeriods(host),
		removePeriods(userName),
		tFormatted,
		pid)
	return name, removePeriods(prefix) + ".log"
}

var errMalformedName = errors.New("malformed log filename")

func parseLogFilename(filename string) (FileDetails, error) {
	matches := logFileRE.FindStringSubmatch(filename)
	if matches == nil || len(matches) != 6 {
		return FileDetails{}, errMalformedName
	}

	// Replace the '_'s with ':'s to restore the correct time format.
	fixTime := strings.Replace(matches[4], "_", ":", -1)
	time, err := time.Parse(time.RFC3339, fixTime)
	if err != nil {
		return FileDetails{}, err
	}

	pid, err := strconv.ParseInt(matches[5], 10, 0)
	if err != nil {
		return FileDetails{}, err
	}

	return FileDetails{
		Program:  matches[1],
		Host:     matches[2],
		UserName: matches[3],
		Time:     time.UnixNano(),
		PID:      pid,
	}, nil
}

var errDirectoryNotSet = errors.New("log: log directory not set")

// create creates a new log file and returns the file and its
// filename. If the file is created successfully, create also attempts
// to update the symlink for that tag, ignoring errors.
func create(
	logDir *DirName, prefix string, t time.Time, lastRotation int64,
) (f *os.File, updatedRotation int64, filename string, err error) {
	dir, err := logDir.get()
	if err != nil {
		return nil, lastRotation, "", err
	}

	// Ensure that the timestamp of the new file name is greater than
	// the timestamp of the previous generated file name.
	unix := t.Unix()
	if unix <= lastRotation {
		unix = lastRotation + 1
	}
	updatedRotation = unix
	t = timeutil.Unix(unix, 0)

	// Generate the file name.
	name, link := logName(prefix, t)
	fname := filepath.Join(dir, name)
	// Open the file os.O_APPEND|os.O_CREATE rather than use os.Create.
	// Append is almost always more efficient than O_RDRW on most modern file systems.
	f, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err == nil {
		symlink := filepath.Join(dir, link)

		// Symlinks are best-effort.

		if err := os.Remove(symlink); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(OrigStderr, "log: failed to remove symlink %s: %s", symlink, err)
		}
		if err := os.Symlink(filepath.Base(fname), symlink); err != nil {
			// On Windows, this will be the common case, as symlink creation
			// requires special privileges.
			// See: https://docs.microsoft.com/en-us/windows/device-security/security-policy-settings/create-symbolic-links
			if runtime.GOOS != "windows" {
				fmt.Fprintf(OrigStderr, "log: failed to create symlink %s: %s", symlink, err)
			}
		}
	}
	return f, updatedRotation, fname, errors.Wrapf(err, "log: cannot create log")
}

// ListLogFiles returns a slice of FileInfo structs for each log file
// on the local node, in any of the configured log directories.
func ListLogFiles() ([]FileInfo, error) {
	return logging.listLogFiles()
}

func (l *loggingT) listLogFiles() ([]FileInfo, error) {
	var results []FileInfo
	dir, err := logging.logDir.get()
	if err != nil {
		// No log directory configured: simply indicate that there are no
		// log files.
		return nil, nil
	}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return results, err
	}
	// The file names have a fixed structure with fields delimited by
	// periods. create() for new files removes the periods from the
	// provided prefix; do the same here to filter out selected names
	// below.
	programPrefix := removePeriods(l.prefix)
	for _, info := range infos {
		if info.Mode().IsRegular() {
			details, err := parseLogFilename(info.Name())
			if err == nil && details.Program == programPrefix {
				results = append(results, FileInfo{
					Name:         info.Name(),
					SizeBytes:    info.Size(),
					ModTimeNanos: info.ModTime().UnixNano(),
					Details:      details,
				})
			}
		}
	}
	return results, nil
}

// GetLogReader returns a reader for the specified filename. In
// restricted mode, the filename must be the base name of a file in
// this process's log directory (this is safe for cases when the
// filename comes from external sources, such as the admin UI via
// HTTP). In unrestricted mode any path is allowed, relative to the
// current directory, with the added feature that simple (base name)
// file names will be searched in this process's log directory if not
// found in the current directory.
func GetLogReader(filename string, restricted bool) (io.ReadCloser, error) {
	dir, err := logging.logDir.get()
	if err != nil {
		return nil, err
	}

	switch restricted {
	case true:
		// Verify there are no path separators in a restricted-mode pathname.
		if filepath.Base(filename) != filename {
			return nil, errors.Errorf("pathnames must be basenames only: %s", filename)
		}
		filename = filepath.Join(dir, filename)
		// Symlinks are not followed in restricted mode.
		info, err := os.Lstat(filename)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, errors.Errorf("no such file %s in the log directory", filename)
			}
			return nil, errors.Wrapf(err, "Lstat: %s", filename)
		}
		mode := info.Mode()
		if mode&os.ModeSymlink != 0 {
			return nil, errors.Errorf("symlinks are not allowed")
		}
		if !mode.IsRegular() {
			return nil, errors.Errorf("not a regular file")
		}
	case false:
		info, err := osStat(filename)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, errors.Wrapf(err, "Stat: %s", filename)
			}
			// The absolute filename didn't work, so try within the log
			// directory if the filename isn't a path.
			if filepath.IsAbs(filename) {
				return nil, errors.Errorf("no such file %s", filename)
			}
			filenameAttempt := filepath.Join(dir, filename)
			info, err = osStat(filenameAttempt)
			if err != nil {
				if os.IsNotExist(err) {
					return nil, errors.Errorf("no such file %s either in current directory or in %s", filename, dir)
				}
				return nil, errors.Wrapf(err, "Stat: %s", filename)
			}
			filename = filenameAttempt
		}
		filename, err = filepath.EvalSymlinks(filename)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, errors.Errorf("not a regular file")
		}
	}

	// Check that the file name is valid.
	if _, err := parseLogFilename(filepath.Base(filename)); err != nil {
		return nil, err
	}

	return os.Open(filename)
}

// TODO(bram): remove when Go1.9 is required.
//
// See https://github.com/golang/go/issues/19870.
func osStat(path string) (os.FileInfo, error) {
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	return os.Lstat(path)
}

// sortableFileInfoSlice is required so we can sort FileInfos.
type sortableFileInfoSlice []FileInfo

func (a sortableFileInfoSlice) Len() int      { return len(a) }
func (a sortableFileInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortableFileInfoSlice) Less(i, j int) bool {
	return a[i].Details.Time < a[j].Details.Time
}

// selectFiles selects all log files that have an timestamp before the
// endTime. It then sorts them in decreasing order, with the most
// recent as the first one.
func selectFiles(logFiles []FileInfo, endTimestamp int64) []FileInfo {
	files := sortableFileInfoSlice{}
	for _, logFile := range logFiles {
		if logFile.Details.Time <= endTimestamp {
			files = append(files, logFile)
		}
	}

	// Sort the files in reverse order so we will fetch the newest first.
	sort.Sort(sort.Reverse(files))
	return files
}

// FetchEntriesFromFiles fetches all available log entries on disk
// that are between the 'startTimestamp' and 'endTimestamp'. It will
// stop reading new files if the number of entries exceeds
// 'maxEntries'. Log entries are further filtered by the regexp
// 'pattern' if provided. The logs entries are returned in reverse
// chronological order.
func FetchEntriesFromFiles(
	startTimestamp, endTimestamp int64, maxEntries int, pattern *regexp.Regexp,
) ([]Entry, error) {
	logFiles, err := ListLogFiles()
	if err != nil {
		return nil, err
	}

	selectedFiles := selectFiles(logFiles, endTimestamp)

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
func readAllEntriesFromFile(
	file FileInfo, startTimestamp, endTimestamp int64, maxEntries int, pattern *regexp.Regexp,
) ([]Entry, bool, error) {
	reader, err := GetLogReader(file.Name, true /* restricted */)
	if reader == nil || err != nil {
		return nil, false, err
	}
	defer reader.Close()
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
