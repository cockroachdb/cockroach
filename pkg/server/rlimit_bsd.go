// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build freebsd dragonfly

package server

import (
	"math"

	"golang.org/x/sys/unix"
)

func setRlimitNoFile(limits *rlimit) error {
	rLimit := unix.Rlimit{Cur: int64(limits.Cur), Max: int64(limits.Max)}
	return unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
}

func getRlimitNoFile(limits *rlimit) error {
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return err
	}
	// Some (legacy?) FreeBSD platforms had RLIMIT_INFINITY set to -1.
	if rLimit.Cur == -1 {
		limits.Cur = math.MaxUint64
	} else {
		limits.Cur = uint64(rLimit.Cur)
	}
	if rLimit.Max == -1 {
		limits.Max = math.MaxUint64
	} else {
		limits.Max = uint64(rLimit.Max)
	}
	return nil
}
