// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debugutil

import (
	"os"
	"path/filepath"
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
