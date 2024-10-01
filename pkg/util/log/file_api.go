// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

// FileNamePattern matches log files to avoid exposing non-log files
// accidentally and it splits the details of the filename into groups for easy
// parsing. The log file format is
//
//	{program}.{host}.{username}.{timestamp}.{pid}.log
//	cockroach.Brams-MacBook-Pro.bram.2015-06-09T16-10-48Z.30209.log
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
		FileMode:     uint32(info.Mode()),
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

// listLogGroups returns slices of logpb.FileInfo structs.
// There is one logpb.FileInfo slice per file sink.
func listLogGroups() (logGroups [][]logpb.FileInfo, err error) {
	err = logging.allSinkInfos.iterFileSinks(func(l *fileSink) error {
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
//
// This function also ensures that only the files that belong to a
// file sink are listed. Unrelated files in the same directory are
// skipped over (ignored).
//
// Note that even though the FileInfo struct does not store the path
// to the log file(s), each file can be mapped back to its directory
// reliably via GetLogReader, thanks to the unique file group names in
// the log configuration. For example, consider the following config:
//
// file-groups:
//
//	groupA:
//	  dir: dir1
//	groupB:
//	  dir: dir2
//
// The result of ListLogFiles on this config will return the list
// {cockroach-groupA.XXX.log, cockroach-groupB.XXX.log}, without
// directory information. This can be mapped back to dir1 and dir2 via the
// configuration. We know that groupA files cannot be in dir2 because
// the group names are unique under file-groups and so there cannot be
// two different groups with the same name and different directories.
func ListLogFiles() (logFiles []logpb.FileInfo, err error) {
	err = logging.allSinkInfos.iterFileSinks(func(l *fileSink) error {
		l.mu.Lock()
		thisLogDir := l.mu.logDir
		l.mu.Unlock()
		if !l.enabled.Load() || thisLogDir == "" {
			// This file sink is detached from file storage.
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

// listLogFiles lists the files matching this sink in its target
// directory. Files that don't match the output name format of the
// sink are ignored. This makes it possible to share directories
// across multiple sinks.
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
	for _, info := range infos {
		if info.Mode().IsRegular() {
			details, err := ParseLogFilename(info.Name())
			if err == nil && l.nameGenerator.ownsFileByPrefix(details.Program) {
				results = append(results, MakeFileInfo(details, info))
			}
		}
	}
	return dir, results, nil
}

// GetLogReader returns a reader for the specified filename.
// The filename must be the base name of a file in
// this process's log directory (this is safe for cases when the
// filename comes from external sources, such as the admin UI via
// HTTP).
//
// See the comment on ListLogFiles() about how/why file names are
// mapped back to a directory name.
func GetLogReader(filename string) (io.ReadCloser, error) {
	// Verify there are no path separators.
	if filepath.Base(filename) != filename {
		return nil, errors.Errorf("pathnames must be basenames only: %s", filename)
	}
	// Check that the file name is valid.
	details, err := ParseLogFilename(filename)
	if err != nil {
		return nil, err
	}
	// Find the sink that matches the file name prefix.
	var fs *fileSink
	_ = logging.allSinkInfos.iterFileSinks(func(l *fileSink) error {
		if l.nameGenerator.ownsFileByPrefix(details.Program) {
			fs = l
			// Interrupt the loop.
			return io.EOF
		}
		return nil
	})
	// Check whether we found a sink and it has a log directory.
	if fs == nil || !fs.enabled.Load() {
		return nil, errors.Newf("no log directory found for %s", filename)
	}
	dir := func() string {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		return fs.mu.logDir
	}()
	if dir == "" {
		// This error should never happen: .enabled should be unset in
		// that case.
		return nil, errors.Newf("no log directory found for %s", filename)
	}

	baseFileName := filename
	filename = filepath.Join(dir, filename)

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

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	sb, ok := fs.mu.file.(*syncBuffer)
	if ok && baseFileName == filepath.Base(sb.file.Name()) {
		// If the file being read is also the file being written to, then we
		// want mutual exclusion between the reader and the runFlusher.
		lr := &lockedReader{}
		lr.mu.RWMutex = &fs.mu.RWMutex
		lr.mu.wrappedFile = file
		return lr, nil
	}
	return file, nil
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
	return FetchEntriesFromFilesWithFormat(startTimestamp, endTimestamp, maxEntries, pattern, editMode, "" /*format*/)
}

// FetchEntriesFromFilesWithFormat is like FetchEntriesFromFiles but the caller can specify the format of the log file.
func FetchEntriesFromFilesWithFormat(
	startTimestamp, endTimestamp int64,
	maxEntries int,
	pattern *regexp.Regexp,
	editMode EditSensitiveData,
	format string,
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
				editMode,
				format)
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

var _ io.ReadCloser = (*lockedReader)(nil)

// lockedReader locks accesses to a wrapped io.ReadCloser,
// using a RWMutex shared with another component.
// We use this when reading log files (using the GetLogReader API)
// that are concurrently being written to by the log runFlusher,
// to ensure that read operations cannot observe partial flushes.
type lockedReader struct {
	mu struct {
		// We use a mutex by reference, so that we can point this
		// lockedReader to the same mutex as used by the corresponding
		// fileSink.
		// This mutex is only defined if the file being read from
		// can also be written to concurrently.
		*syncutil.RWMutex

		wrappedFile io.ReadCloser
	}
}

func (r *lockedReader) Read(b []byte) (int, error) {
	if r.mu.RWMutex != nil {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}
	return r.mu.wrappedFile.Read(b)
}

func (r *lockedReader) Close() error {
	// We do not need to hold the mutex to call Close() since there is
	// no flushing needed on read-only file access during Close() calls.
	return r.mu.wrappedFile.Close()
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
	format string,
) ([]logpb.Entry, bool, error) {
	reader, err := GetLogReader(file.Name)
	if err != nil {
		return nil, false, err
	}
	defer reader.Close()

	entries := []logpb.Entry{}
	decoder, err := NewEntryDecoderWithFormat(reader, editMode, format)
	if err != nil {
		return nil, false, err
	}
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
