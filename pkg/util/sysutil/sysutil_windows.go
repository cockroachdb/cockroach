// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build windows

package sysutil

import (
	"errors"
	"fmt"
	"os"
	"os/user"
)

// ProcessIdentity returns a string describing the user and group that this
// process is running as.
func ProcessIdentity() string {
	u, err := user.Current()
	if err != nil {
		return "<unknown>"
	}
	return fmt.Sprintf("uid %s, gid %s", u.Uid, u.Gid)
}

// StatFS returns an FSInfo describing the named filesystem. It is only
// supported on Unix-like platforms.
func StatFS(path string) (*FSInfo, error) {
	return nil, errors.New("unsupported on Windows")
}

// StatAndLinkCount wraps os.Stat, returning its result and a zero link count.
func StatAndLinkCount(path string) (os.FileInfo, int64, error) {
	stat, err := os.Stat(path)
	return stat, 0, err
}
