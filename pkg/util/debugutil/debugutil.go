// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debugutil

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"

	"github.com/elastic/gosigar"
)

// IsLaunchedByDebugger returns true in cases where the delve debugger
// was used to launch the cockroach process.
func IsLaunchedByDebugger() bool {
	return isLaunchedByDebugger.Load()
}

var isLaunchedByDebugger atomic.Bool

func init() {
	isLaunchedByDebugger.Store(func(maybeDelvePID int) bool {
		// We loop in case there were intermediary processes like the gopls
		// language server.
		for maybeDelvePID != 0 {
			var exe gosigar.ProcExe
			if err := exe.Get(maybeDelvePID); err != nil {
				break
			}
			switch filepath.Base(exe.Name) {
			case "dlv":
				return true
			}
			var state gosigar.ProcState
			if err := state.Get(maybeDelvePID); err != nil {
				break
			}
			maybeDelvePID = state.Ppid
		}
		return false
	}(os.Getppid()))
}

// SafeStack is an alias for []byte that handles redaction. Use this type
// instead of []byte when you are sure that the stack trace does not contain
// sensitive information.
type SafeStack []byte

func (s SafeStack) SafeValue() {}

// Stack wraps the output of debug.Stack() with redact.Safe() to avoid
// unnecessary redaction.
//
// WARNING: Calling this function grabs system-level locks and could cause high
// system CPU usage resulting in the Go runtime to lock up if called too
// frequently, even if called only in error-handling pathways. Use sporadically
// and only when necessary.
func Stack() SafeStack {
	return debug.Stack()
}
