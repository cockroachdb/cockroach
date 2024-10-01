// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

// FileTimeFormat is RFC3339 with the colons replaced with underscores.
// It is the format used for timestamps in log file names.
// This removal of colons creates log files safe for Windows file systems.
const FileTimeFormat = "2006-01-02T15_04_05Z07:00"

// fileNameConstantsT represents the fields that are common to all
// generated filenames.
// The fields are pre-normalized to ensure that they do not contain
// special characters or periods.
type fileNameConstantsT struct {
	pid      int
	program  string
	host     string
	userName string
}

// fullHostName is the machine's hostname. It is used as input to
// generate file names, and is also included in full at the start of
// output from every sink.
var fullHostName = func() (host string) {
	host = "unknownhost"
	h, err := os.Hostname()
	if err == nil {
		host = h
	}
	return host
}()

var fileNameConstants = func() (res fileNameConstantsT) {
	res.pid = os.Getpid()

	// We remove hyphens from the program name because we use the hyphen
	// to separate the program name from the file group name.
	res.program = normalizeFileName(filepath.Base(os.Args[0]), false /* keepHyphens */)

	// In contrast to the program name, the host part can contain
	// hyphens because its delimiter is a period.
	res.host = normalizeFileName(shortHostname(fullHostName), true /* keepHyphens */)

	// In contrast to the program name, the user part can contain
	// hyphens because its delimiter is a period.
	res.userName = "unknownuser"
	current, err := user.Current()
	if err == nil {
		res.userName = normalizeFileName(current.Username, true /* keepHyphens */)
	}

	return res
}()

// shortHostname returns its argument, truncating at the first period.
// For instance, given "www.google.com" it returns "www".
func shortHostname(hostname string) string {
	if i := strings.IndexByte(hostname, '.'); i >= 0 {
		return hostname[:i]
	}
	return hostname
}

// normalizeFileName removes special characters and periods in the
// string.
func normalizeFileName(s string, keepHyphens bool) string {
	var buf strings.Builder
	for _, c := range s {
		if unicode.IsLetter(c) || unicode.IsDigit(c) || (keepHyphens && c == '-') {
			buf.WriteRune(c)
		}
	}
	return buf.String()
}

// fileNameGenerator represents the algorithm that generates output log file names.
type fileNameGenerator struct {
	fileNameConstantsT
	fileNamePrefix string
}

func makeFileNameGenerator(fileGroupName string) (res fileNameGenerator) {
	res.fileNameConstantsT = fileNameConstants
	res.fileNamePrefix = res.program
	if fileGroupName != "" {
		// We can keep the hyphen in the group name because the delimiter
		// on the left is the first hyphen after the program and the
		// delimiter on the right is a period.
		res.fileNamePrefix = res.program + "-" + normalizeFileName(fileGroupName, true /* keepHyphens */)
	}
	return res
}

// logName returns a new log file name with start time t, and the name
// for the symlink.
func (g fileNameGenerator) logName(t time.Time) (name, link string) {
	name = fmt.Sprintf("%s.%s.%s.%s.%06d.log",
		g.fileNamePrefix,
		g.host,
		g.userName,
		t.Format(FileTimeFormat),
		g.pid)
	return name, g.fileNamePrefix + ".log"
}

// ownsFileByPrefix returns true iff this generator is responsible
// for generating file names starting with the given prefix.
// For example, if the generator is for files named "cockroach-xx",
// it returns true when given "cockroach-xx", but false on
// "cockroach-xx-yy".
func (g fileNameGenerator) ownsFileByPrefix(prefix string) bool {
	return g.fileNamePrefix == prefix
}
