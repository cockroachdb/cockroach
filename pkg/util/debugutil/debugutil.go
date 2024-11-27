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

	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
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

// Stack wraps the output of debug.Stack() with redact.Safe() to avoid
// unnecessary redaction.
func Stack() interfaces.SafeValue {
	return redact.Safe(string(debug.Stack()))
}
