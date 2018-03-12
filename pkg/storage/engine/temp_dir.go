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

package engine

import (
	"C"
	"bufio"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const lockFilename = `TEMP_DIR.LOCK`

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
	if os.IsNotExist(err) {
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
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Warningf(context.Background(), "could not locate previous temporary directory %s, might require manual cleanup, or might have already been cleaned up.", path)
			continue
		}

		// Check if another Cockroach instance is using this temporary
		// directory i.e. has a lock on the temp dir lock file.
		flock, err := lockFile(filepath.Join(path, lockFilename))
		if err != nil {
			return errors.Wrapf(err, "could not lock temporary directory %s, may still be in use", path)
		}
		defer func() {
			if err := unlockFile(flock); err != nil {
				log.Errorf(context.TODO(), "could not unlock file lock when removing temporary directory: %s", err.Error())
			}
		}()

		// If path/directory does not exist, error is nil.
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	// Clear out the record file now that we're done.
	return f.Truncate(0)
}
