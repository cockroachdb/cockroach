// Copyright 2020 The Cockroach Authors.
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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

// FileNamePattern matches log files to avoid exposing non-log files
// accidentally and it splits the details of the filename into groups for easy
// parsing. The log file format is
//
//   {program}.{host}.{username}.{timestamp}.{pid}.log
//   cockroach.Brams-MacBook-Pro.bram.2015-06-09T16-10-48Z.30209.log
//
// All underscore in process, host and username are escaped to double
// underscores and all periods are escaped to an underscore.
// For compatibility with Windows filenames, all colons from the timestamp
// (RFC3339) are converted from underscores (see FileTimePattern).
// Note this pattern is unanchored and becomes anchored through its use in
// LogFilePattern.
const FileNamePattern = `(?P<program>[^/.]+)\.(?P<host>[^/\.]+)\.` +
	`(?P<user>[^/\.]+)\.(?P<ts>[^/\.]+)\.(?P<pid>\d+)\.log`

// FilePattern matches log file paths.
const FilePattern = "^(?:.*/)?" + FileNamePattern + "$"

var fileRE = regexp.MustCompile(FilePattern)

// MakeFileInfo constructs a FileInfo from FileDetails and os.FileInfo.
func MakeFileInfo(details logpb.FileDetails, info os.FileInfo) logpb.FileInfo {
	return logpb.FileInfo{
		Name:         info.Name(),
		SizeBytes:    info.Size(),
		ModTimeNanos: info.ModTime().UnixNano(),
		Details:      details,
	}
}

var errMalformedName = errors.New("malformed log filename")

// ParseLogFilename parses a filename into FileDetails if it matches the pattern
// for log files. If the filename does not match the log file pattern, an error
// is returned.
func ParseLogFilename(filename string) (logpb.FileDetails, error) {
	matches := fileRE.FindStringSubmatch(filename)
	if matches == nil || len(matches) != 6 {
		return logpb.FileDetails{}, errMalformedName
	}

	time, err := time.Parse(FileTimeFormat, matches[4])
	if err != nil {
		return logpb.FileDetails{}, err
	}

	pid, err := strconv.ParseInt(matches[5], 10, 0)
	if err != nil {
		return logpb.FileDetails{}, err
	}

	return logpb.FileDetails{
		Program:  matches[1],
		Host:     matches[2],
		UserName: matches[3],
		Time:     time.UnixNano(),
		PID:      pid,
	}, nil
}

var errNoFileLogging = errors.New("log: file logging is not configured")

// listLogGroups returns slices of logpb.FileInfo structs.
// There is one logpb.FileInfo slice per file sink.
func listLogGroups() (logGroups [][]logpb.FileInfo, err error) {
	err = allSinkInfos.iterFileSinks(func(l *fileSink) error {
		_, thisLoggerFiles, err := l.listLogFiles()
		if err != nil {
			return err
		}
		logGroups = append(logGroups, thisLoggerFiles)
		return nil
	})
	return logGroups, err
}

// ListLogFiles returns a slice of logpb.FileInfo structs for each log file
// on the local node, in any of the configured log directories.
func ListLogFiles() (logFiles []logpb.FileInfo, err error) {
	mainDir := func() string {
		fileSink := debugLog.getFileSink()
		if fileSink == nil {
			return ""
		}
		fileSink.mu.Lock()
		defer fileSink.mu.Unlock()
		return fileSink.mu.logDir
	}()

	err = allSinkInfos.iterFileSinks(func(l *fileSink) error {
		// For now, only gather logs from the main log directory.
		// This is because the other APIs don't yet understand
		// secondary log directories, and we don't want
		// to list a file that cannot be retrieved.
		l.mu.Lock()
		thisLogDir := l.mu.logDir
		l.mu.Unlock()
		if thisLogDir == "" || thisLogDir != mainDir {
			return nil
		}

		_, thisLoggerFiles, err := l.listLogFiles()
		if err != nil {
			return err
		}
		logFiles = append(logFiles, thisLoggerFiles...)
		return nil
	})
	return logFiles, err
}

func (l *fileSink) listLogFiles() (string, []logpb.FileInfo, error) {
	var results []logpb.FileInfo
	l.mu.Lock()
	dir := l.mu.logDir
	l.mu.Unlock()
	if dir == "" {
		// No log directory configured: simply indicate that there are no
		// log files.
		return "", nil, nil
	}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", results, err
	}
	// The file names have a fixed structure with fields delimited by
	// periods. create() for new files removes the periods from the
	// provided prefix; do the same here to filter out selected names
	// below.
	programPrefix := removePeriods(l.prefix)
	for _, info := range infos {
		if info.Mode().IsRegular() {
			details, err := ParseLogFilename(info.Name())
			if err == nil && details.Program == programPrefix {
				results = append(results, MakeFileInfo(details, info))
			}
		}
	}
	return dir, results, nil
}

// GetLogReader returns a reader for the specified filename. In
// restricted mode, the filename must be the base name of a file in
// this process's log directory (this is safe for cases when the
// filename comes from external sources, such as the admin UI via
// HTTP). In unrestricted mode any path is allowed, relative to the
// current directory, with the added feature that simple (base name)
// file names will be searched in this process's log directory if not
// found in the current directory.
//
// TODO(knz): make this work for secondary loggers too.
func GetLogReader(filename string, restricted bool) (io.ReadCloser, error) {
	fileSink := debugLog.getFileSink()
	if fileSink == nil || !fileSink.enabled.Get() {
		return nil, errNoFileLogging
	}

	fileSink.mu.Lock()
	dir := fileSink.mu.logDir
	fileSink.mu.Unlock()
	if dir == "" {
		return nil, errDirectoryNotSet
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
			if oserror.IsNotExist(err) {
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
		info, err := os.Stat(filename)
		if err != nil {
			if !oserror.IsNotExist(err) {
				return nil, errors.Wrapf(err, "Stat: %s", filename)
			}
			// The absolute filename didn't work, so try within the log
			// directory if the filename isn't a path.
			if filepath.IsAbs(filename) {
				return nil, errors.Errorf("no such file %s", filename)
			}
			filenameAttempt := filepath.Join(dir, filename)
			info, err = os.Stat(filenameAttempt)
			if err != nil {
				if oserror.IsNotExist(err) {
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
	if _, err := ParseLogFilename(filepath.Base(filename)); err != nil {
		return nil, err
	}

	return os.Open(filename)
}

// sortablelogpb.FileInfoSlice is required so we can sort logpb.FileInfos.
type sortableFileInfoSlice []logpb.FileInfo

func (a sortableFileInfoSlice) Len() int      { return len(a) }
func (a sortableFileInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortableFileInfoSlice) Less(i, j int) bool {
	return a[i].Details.Time < a[j].Details.Time
}

// selectFiles selects all log files that have an timestamp before the
// endTime. It then sorts them in decreasing order, with the most
// recent as the first one.
func selectFilesInGroup(logFiles []logpb.FileInfo, endTimestamp int64) []logpb.FileInfo {
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
	startTimestamp, endTimestamp int64,
	maxEntries int,
	pattern *regexp.Regexp,
	editMode EditSensitiveData,
) ([]logpb.Entry, error) {
	logGroups, err := listLogGroups()
	if err != nil {
		return nil, err
	}

	entries := []logpb.Entry{}
	numGroupsWithEntries := 0
	for _, logFiles := range logGroups {
		selectedFiles := selectFilesInGroup(logFiles, endTimestamp)

		groupHasEntries := false
		for _, file := range selectedFiles {
			newEntries, entryBeforeStart, err := readAllEntriesFromFile(
				file,
				startTimestamp,
				endTimestamp,
				maxEntries-len(entries),
				pattern,
				editMode)
			if err != nil {
				return nil, err
			}
			groupHasEntries = true
			entries = append(entries, newEntries...)
			if len(entries) >= maxEntries {
				break
			}
			if entryBeforeStart {
				// Stop processing files inside the group that won't have any
				// timestamps after startTime.
				break
			}
		}
		if groupHasEntries {
			numGroupsWithEntries++
		}
	}

	// Within each group, entries are sorted. However if there were
	// multiple groups, the final result is not sorted any more. Do it
	// now.
	if numGroupsWithEntries > 1 {
		e := sortableEntries(entries)
		sort.Stable(e)
	}

	return entries, nil
}

type sortableEntries []logpb.Entry

func (a sortableEntries) Len() int      { return len(a) }
func (a sortableEntries) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortableEntries) Less(i, j int) bool {
	// Note: FetchEntries returns entries in reverse order.
	return a[i].Time > a[j].Time
}

// readAllEntriesFromFile reads in all log entries from a given file that are
// between the 'startTimestamp' and 'endTimestamp' and match the 'pattern' if it
// exists. It returns the entries in the reverse chronological order. It also
// returns a flag that denotes if any timestamp occurred before the
// 'startTimestamp' to inform the caller that no more log files need to be
// processed. If the number of entries returned exceeds 'maxEntries' then
// processing of new entries is stopped immediately.
func readAllEntriesFromFile(
	file logpb.FileInfo,
	startTimestamp, endTimestamp int64,
	maxEntries int,
	pattern *regexp.Regexp,
	editMode EditSensitiveData,
) ([]logpb.Entry, bool, error) {
	reader, err := GetLogReader(file.Name, true /* restricted */)
	if reader == nil || err != nil {
		return nil, false, err
	}
	defer reader.Close()
	entries := []logpb.Entry{}
	decoder := NewEntryDecoder(reader, editMode)
	entryBeforeStart := false
	for {
		entry := logpb.Entry{}
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
			entries = append([]logpb.Entry{entry}, entries...)
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
