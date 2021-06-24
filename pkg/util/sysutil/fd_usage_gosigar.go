// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !darwin

package sysutil

import (
	"os"

	"github.com/elastic/gosigar"
)

// Get populates ProcFDUsage for the current process.
//
// This is the default implementation used on most platforms.
func (u *ProcFDUsage) Get() error {
	pid := os.Getpid()
	fds := gosigar.ProcFDUsage{}
	err := fds.Get(pid)
	if err != nil {
		return err
	}
	u.Open = fds.Open
	u.SoftLimit = fds.SoftLimit
	u.HardLimit = fds.HardLimit
	return nil
}
