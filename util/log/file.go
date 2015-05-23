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

package log

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

// MaxSize is the maximum size of a log file in bytes.
var MaxSize uint64 = 1024 * 1024 * 1800

// If non-empty, overrides the choice of directory in which to write logs.
// See createLogDirs for the full list of possible destinations.
var logDir *string

// logDirs lists the candidate directories for new log files.
var logDirs []string

// logFileRE matches log files to avoid exposing non-log files accidentally.
var logFileRE = regexp.MustCompile(`log\.(INFO|WARNING|ERROR)\.`)

func createLogDirs() {
	if *logDir != "" {
		logDirs = append(logDirs, *logDir)
	}
}

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

// logName returns a new log file name containing tag, with start time t, and
// the name for the symlink for tag.
func logName(tag string, t time.Time) (name, link string) {
	name = fmt.Sprintf("%s.%s.%s.log.%s.%04d%02d%02d-%02d%02d%02d.%d",
		program,
		host,
		userName,
		tag,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		pid)
	return name, program + "." + tag
}

var onceLogDirs sync.Once

// create creates a new log file and returns the file and its filename, which
// contains tag ("INFO", "FATAL", etc.) and t.  If the file is created
// successfully, create also attempts to update the symlink for that tag, ignoring
// errors.
func create(tag string, t time.Time) (f *os.File, filename string, err error) {
	onceLogDirs.Do(createLogDirs)
	if len(logDirs) == 0 {
		return nil, "", errors.New("log: no log dirs")
	}
	name, link := logName(tag, t)
	var lastErr error
	for _, dir := range logDirs {
		fname := filepath.Join(dir, name)

		// Open the file os.O_APPEND|os.O_CREATE rather than use os.Create.
		// Append is almost always more efficient than O_RDRW on most modern file systems.
		f, err = os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
		if err != nil {
			return nil, "", fmt.Errorf("log: cannot create log: %v", err)
		}

		if err == nil {
			symlink := filepath.Join(dir, link)
			_ = os.Remove(symlink)        // ignore err
			_ = os.Symlink(name, symlink) // ignore err
			return f, fname, nil
		}
		lastErr = err
	}
	return nil, "", fmt.Errorf("log: cannot create log: %v", lastErr)
}

// verifyLogFileInfo verifies that the file specified by filename is a
// regular file and filename matches the expected filename pattern.
// Returns nil on success; otherwise error.
func verifyLogFileInfo(info os.FileInfo) error {
	if info.Mode()&os.ModeType != 0 {
		return util.Errorf("not a regular file")
	} else if !logFileRE.MatchString(info.Name()) {
		return util.Errorf("not a log file")
	}
	return nil
}

func verifyLogFile(filename string) error {
	info, err := os.Stat(filename)
	if err != nil {
		return err
	}
	return verifyLogFileInfo(info)
}

// A LogFileInfo holds the filename and size of a log file.
type LogFileInfo struct {
	Name         string // base name
	SizeBytes    int64
	ModTimeNanos int64 // most recent mode time in unix nanos
}

// ListLogFiles returns a slice of LogFileInfo structs for each log
// file on the local node, in any of the configured log directories.
func ListLogFiles() ([]LogFileInfo, error) {
	var results []LogFileInfo
	for _, dir := range logDirs {
		infos, err := ioutil.ReadDir(dir)
		if err != nil {
			return results, err
		}
		for _, info := range infos {
			if verifyLogFileInfo(info) == nil {
				results = append(results, LogFileInfo{
					Name:         info.Name(),
					SizeBytes:    info.Size(),
					ModTimeNanos: info.ModTime().UnixNano(),
				})
			}
		}
	}
	return results, nil
}

// GetLogReader returns a reader for the specified filename. Any
// external requests (say from the admin UI via HTTP) must specify
// allowAbsolute as false to prevent leakage of non-log
// files. Absolute filenames are allowed for the case of the cockroach "log"
// command, which provides human readable output from an arbitrary file,
// and is intended to be run locally in a terminal.
func GetLogReader(filename string, allowAbsolute bool) (io.ReadCloser, error) {
	if path.IsAbs(filename) {
		if !allowAbsolute {
			return nil, util.Errorf("absolute pathnames are forbidden: %s", filename)
		}
		if verifyLogFile(filename) == nil {
			return os.Open(filename)
		}
	}
	// Verify there are no path separators in the a non-absolute pathname.
	if path.Base(filename) != filename {
		return nil, util.Errorf("pathnames must be basenames only: %s", filename)
	}
	if !logFileRE.MatchString(filename) {
		return nil, util.Errorf("filename is not a cockroach log file: %s", filename)
	}
	var reader io.ReadCloser
	var err error
	for _, dir := range logDirs {
		filename := path.Join(dir, filename)
		if verifyLogFile(filename) == nil {
			reader, err = os.Open(filename)
			if err == nil {
				return reader, err
			}
		}
	}
	return nil, err
}
