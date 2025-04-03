// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows

package cli

import (
	"os"

	"golang.org/x/sys/unix"
)

// exitSignals are the signals that will cause workload to exit.
var exitSignals = []os.Signal{unix.SIGINT, unix.SIGTERM}
