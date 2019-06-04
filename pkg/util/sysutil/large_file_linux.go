// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// +build linux

package sysutil

import (
	"os"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// CreateLargeFile creates a large file at the given path with bytes size. On
// Linux, it uses the fallocate syscall to efficiently create a file of the
// given size. On other platforms, it naively writes the specified number of
// bytes, which can take a long time.
func CreateLargeFile(path string, bytes int64) error {
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s", path)
	}
	defer f.Close()
	if err := unix.Fallocate(int(f.Fd()), 0, 0, bytes); err != nil {
		return err
	}
	return f.Sync()
}
