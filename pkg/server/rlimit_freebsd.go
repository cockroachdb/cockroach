// Copyright 2017 The Cockroach Authors.
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

// +build freebsd

package server

import (
	"math"

	"golang.org/x/sys/unix"
)

func setrlimit_nofile(limits *rlimit) error {
	rLimit := unix.Rlimit{Cur: int64(limits.cur), Max: int64(limits.max)}
	return unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
}

func getrlimit_nofile(limits *rlimit) error {
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return err
	}
	if rLimit.Cur == -1 {
		limits.cur = math.MaxUint64
	} else {
		limits.cur = uint64(rLimit.Cur)
	}
	if rLimit.Max == -1 {
		limits.max = math.MaxUint64
	} else {
		limits.max = uint64(rLimit.Max)
	}
	return nil
}
