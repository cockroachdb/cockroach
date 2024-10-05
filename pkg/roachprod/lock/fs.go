// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lock

import (
	"os"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// AcquireFilesystemLock acquires a filesystem lock in order that concurrent
// operations or roachprod processes that access shared system resources do not
// conflict. Different locks can be specified by passing different paths.
func AcquireFilesystemLock(path string) (unlockFn func(), _ error) {
	lockFile := os.ExpandEnv(path)
	f, err := os.Create(lockFile)
	if err != nil {
		return nil, errors.Wrapf(err, "creating lock file %q", lockFile)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		f.Close()
		return nil, errors.Wrap(err, "acquiring lock on %q")
	}
	return func() {
		f.Close()
	}, nil
}
