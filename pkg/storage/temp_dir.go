// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

const lockFilename = `TEMP_DIR.LOCK`

type lockStruct struct {
	closer io.Closer
}

// lockFile sets a lock on the specified file, using flock.
func lockFile(filename string) (lockStruct, error) {
	closer, err := vfs.Default.Lock(filename)
	if err != nil {
		return lockStruct{}, err
	}
	return lockStruct{closer: closer}, nil
}

// unlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for the lock.
func unlockFile(lock lockStruct) error {
	if lock.closer != nil {
		return lock.closer.Close()
	}
	return nil
}

// CreateTempDir creates a temporary directory with a prefix under the given
// parentDir and returns the absolute path of the temporary directory.
// It is advised to invoke CleanupTempDirs before creating new temporary
// directories in cases where the disk is completely full.
func CreateTempDir(parentDir, prefix string, stopper *stop.Stopper) (string, error) {
	// We generate a unique temporary directory with the specified prefix.
	tempPath, err := ioutil.TempDir(parentDir, prefix)
	if err != nil {
		return "", err
	}

	// TempDir creates a directory with permissions 0700. Manually change the
	// permissions to be 0755 like every other directory created by cockroach.
	if err := os.Chmod(tempPath, 0755); err != nil {
		return "", err
	}

	absPath, err := filepath.Abs(tempPath)
	if err != nil {
		return "", err
	}

	// Create a lock file.
	flock, err := lockFile(filepath.Join(absPath, lockFilename))
	if err != nil {
		return "", errors.Wrapf(err, "could not create lock on new temporary directory")
	}
	stopper.AddCloser(stop.CloserFn(func() {
		if err := unlockFile(flock); err != nil {
			log.Errorf(context.TODO(), "could not unlock file lock on temporary directory: %s", err.Error())
		}
	}))

	return absPath, nil
}

// RecordTempDir records tempPath to the record file specified by recordPath to
// facilitate cleanup of the temporary directory on subsequent startups.
func RecordTempDir(recordPath, tempPath string) error {
	// If the file does not exist, create it, or append to the file.
	f, err := os.OpenFile(recordPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Record tempPath to the record file.
	_, err = f.Write(append([]byte(tempPath), '\n'))
	return err
}

// CleanupTempDirs removes all directories listed in the record file specified
// by recordPath.
// It should be invoked before creating any new temporary directories to clean
// up abandoned temporary directories.
// It should also be invoked when a newly created temporary directory is no
// longer needed and needs to be removed from the record file.
func CleanupTempDirs(recordPath string) error {
	// Reading the entire file into memory shouldn't be a problem since
	// it is extremely rare for this record file to contain more than a few
	// entries.
	f, err := os.OpenFile(recordPath, os.O_RDWR, 0644)
	// There is no existing record file and thus nothing to clean up.
	if oserror.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Iterate through each temporary directory path and remove the
	// directory.
	for scanner.Scan() {
		path := scanner.Text()
		if path == "" {
			continue
		}

		// Check if the temporary directory exists; if it does not, skip over it.
		if _, err := os.Stat(path); oserror.IsNotExist(err) {
			log.Warningf(context.Background(), "could not locate previous temporary directory %s, might require manual cleanup, or might have already been cleaned up.", path)
			continue
		}

		// Check if another Cockroach instance is using this temporary
		// directory i.e. has a lock on the temp dir lock file.
		flock, err := lockFile(filepath.Join(path, lockFilename))
		if err != nil {
			return errors.Wrapf(err, "could not lock temporary directory %s, may still be in use", path)
		}
		// On Windows, file locks are mandatory, so we must remove our lock on the
		// lock file before we can remove the temporary directory. This yields a
		// race condition: another process could start using the now-unlocked
		// directory before we can remove it. Luckily, this doesn't matter, because
		// these temporary directories are never reused. Any other process trying to
		// lock this temporary directory is just trying to clean it up, too. Only
		// the original process wants the data in this directory, and we know that
		// process is dead because we were able to acquire the lock in the first
		// place.
		if err := unlockFile(flock); err != nil {
			log.Errorf(context.TODO(), "could not unlock file lock when removing temporary directory: %s", err.Error())
		}

		// If path/directory does not exist, error is nil.
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	// Clear out the record file now that we're done.
	return f.Truncate(0)
}
