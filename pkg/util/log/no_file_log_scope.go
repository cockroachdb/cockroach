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

import "github.com/cockroachdb/errors"

// NoFileLogScope represents the lifetime of a logging output with
// logging to file being disabled. It is used for e.g. in-memory only
// demo clusters.
type NoFileLogScope struct {
	prevDir string
	cleanup func()
}

// ScopeWithoutFiles starts a logging environment where logging to
// files is disabled. This is used for e.g. in-memory only
// demo clusters.
func ScopeWithoutFiles() (s NoFileLogScope) {
	restoreActive := resetActive()

	s.prevDir = mainLog.logDir.String()
	if err := dirTestOverride(s.prevDir, ""); err != nil {
		panic(errors.New("unable to override with empty directory"))
	}
	mainLog.mu.Lock()
	defer mainLog.mu.Unlock()
	prevStderrThreshold := mainLog.stderrThreshold.get()

	s.cleanup = func() {
		mainLog.mu.Lock()
		defer mainLog.mu.Unlock()
		mainLog.stderrThreshold.set(prevStderrThreshold)
		restoreActive()
	}

	return s
}

// Close finalizes the given scope.
func (s NoFileLogScope) Close() {
	Flush()
	// There cannot be an error here because the previous directory was
	// valid before.
	_ = mainLog.dirTestOverride("", s.prevDir)
	s.cleanup()
}
