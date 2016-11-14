// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package log

import (
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
)

// TestLogScope represents the lifetime of a logging output.  It
// ensures that the log files are stored in a directory specific to a
// test, and asserts that logging output is not written to this
// directory beyond the lifetime of the scope.
//
// Suggested call pattern, where t is a *testing.T:
//    l := log.MakeTestLogScope(t.Fatal)
//    defer func() { l.Close(t.Failed(), t.Fatal) }()
//
// This API cannot use testing.T directly, although it would make
// things simpler to read and write (MakeTestLogScope(t), Close(t)),
// because we also have a linter that verifies that no public API
// depends on the `testing` package.
type TestLogScope string

// MakeTestLogScope creates a TestLogScope which corresponds to the
// lifetime of a logging directory. The logging directory is named
// after the caller of MakeTestLogScope, up `skip` caller levels.
func MakeTestLogScope(skip int, fatalFn func(string, ...interface{})) TestLogScope {
	_, _, f := caller.Lookup(skip)
	testName := "logUnknown"
	if f != "" {
		parts := strings.Split(f, ".")
		testName = "log" + parts[len(parts)-1]
	}
	tempDir, err := ioutil.TempDir("", testName)
	if err != nil {
		fatalFn("cannot create temp dir: %s", err)
	}
	if err := dirTestOverride(tempDir); err != nil {
		fatalFn("%v", err)
	}
	return TestLogScope(tempDir)
}

// Close cleans up a TestLogScope. The directory and its contents are
// deleted, unless keepLogFiles is true and the directory is non-empty.
func (l *TestLogScope) Close(keepLogFiles bool, fatalFn func(string, ...interface{})) {
	// Flush/Close the log files.
	if err := dirTestOverride(""); err != nil {
		fatalFn("%v", err)
	}
	// Check whether there is something to remove.
	emptyDir, err := isDirEmpty(string(*l))
	if err != nil {
		fatalFn("%v", err)
	}
	if keepLogFiles && !emptyDir {
		// If the test failed, we keep the log files for further investigation,
		// but only if there were any.
		fatalFn("test log files left over in: %s\n", *l)
	}
	// Clean up.
	if err := os.RemoveAll(string(*l)); err != nil {
		fatalFn("%v", err)
	}
}

// dirTestOverride sets the default value for the logging output directory
// for use in tests.
func dirTestOverride(dir string) error {
	// Ensure any remaining logs are written.
	Flush()

	// When we change the directory we close the current logging
	// output, so that a rotation to the new directory is forced on
	// the next logging event.
	logging.mu.Lock()
	defer logging.mu.Unlock()
	if err := logging.closeFilesLocked(); err != nil {
		return err
	}

	logDir.Lock()
	defer logDir.Unlock()
	logDir.name = dir
	return nil
}

func isDirEmpty(dirname string) (bool, error) {
	f, err := os.Open(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	list, err := f.Readdir(1)
	errClose := f.Close()
	if err != nil && err != io.EOF {
		return false, err
	}
	if errClose != nil {
		return false, errClose
	}
	return len(list) > 0, nil
}
