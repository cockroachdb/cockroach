// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// +build !windows

package storage

import (
	"os"

	"github.com/pkg/errors"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
)

type lockStruct struct{
	file     *os.File
	filename string
}

// lockFile sets a lock on the specified file, using flock.
func lockFile(filename string) (lockStruct, error) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return lockStruct{}, err
	}
	if err := sysutil.LockFile(file.Fd()); err != nil {
		return lockStruct{}, errors.Wrapf(err, "could not lock file %s", filename)
	}
	return lockStruct{file: file, filename: filename}, nil
}

// unlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for the lock.
func unlockFile(lock lockStruct) error {
	if lock.file == nil {
		return errors.New("unlockFile called with nil file")
	}
	if err := sysutil.UnlockFile(lock.file.Fd()); err != nil {
		return errors.Wrapf(err, "could not unlock file %s", lock.filename)
	}
	defer lock.file.Close()
	return nil
}
