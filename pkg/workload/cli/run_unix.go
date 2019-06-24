// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package cli

import (
	"os"

	"golang.org/x/sys/unix"
)

// exitSignals are the signals that will cause workload to exit.
var exitSignals = []os.Signal{unix.SIGINT, unix.SIGTERM}
