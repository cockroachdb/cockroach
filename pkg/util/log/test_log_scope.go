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
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
)

// TestLogScope ensures that the log files are stored in a directory
// specific to a test.
type TestLogScope string

// MakeTestLogScope creates a TestLogScope which corresponds to the
// lifetime of a logging directory. The logging directory is named
// after the caller of MakeTestLogScope.
// Suggested call pattern: l := log.MakeTestLogScope(t.Fatal)
// This function cannot operate on *testing.T directly because it is
// exported and we are checking that no exported interface depends on
// package testing.
func MakeTestLogScope(fatalFn func(...interface{})) TestLogScope {
	pc, _, _, ok := runtime.Caller(1)
	testName := "logUnknown"
	if ok {
		testName = runtime.FuncForPC(pc).Name()
		parts := strings.Split(testName, ".")
		testName = "log" + parts[len(parts)-1]
	}
	tempDir, err := ioutil.TempDir("", testName)
	if err != nil {
		fatalFn(fmt.Sprintf("cannot create temp dir: %s", err))
	}
	if err := dirTestOverride(tempDir); err != nil {
		fatalFn(err.Error())
	}
	return TestLogScope(tempDir)
}

// Close cleans up a TestLogScope. The directory and its contents are
// deleted if skipRemove if false.
// Suggested call pattern: l.Close(t.Failed(), t.Fatal()).
// This function cannot operate on *testing.T directly because it is
// exported and we are checking that no exported interface depends on
// package testing.
func (l *TestLogScope) Close(skipRemove bool, fatalFn func(...interface{})) {
	if err := dirTestOverride(""); err != nil {
		fatalFn(err.Error())
	}
	if skipRemove {
		// If the test failed, we keep the log files.
		return
	}
	if err := os.RemoveAll(string(*l)); err != nil {
		fatalFn(err.Error())
	}
}

// dirTestOverride sets the default value for the logging output directory
// for use in tests.
func dirTestOverride(dir string) error {
	if dir == "" {
		logging.mu.Lock()
		defer logging.mu.Unlock()
		if err := logging.closeFilesLocked(); err != nil {
			return err
		}
	}
	logDir.Lock()
	defer logDir.Unlock()
	logDir.name = dir
	return nil
}
