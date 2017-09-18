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
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

const (
	defaultTempStorageDirPrefix = "cockroach-temp"
	// TempStorageDirsRecordFilename is the filename for the record file
	// that keeps track of the paths of the temporary directories created.
	// This file will be stored under the recordPath directory (which
	// is defaulted to the first store's root).
	TempStorageDirsRecordFilename = "temp-storage-dirs.txt"
)

func tempStorageDirsRecordFilepath(recordPath string) string {
	return filepath.Join(recordPath, TempStorageDirsRecordFilename)
}

// Use a mutex to synchronize writing to the temp store record file by the
// asynchronous cleanup goroutine (when a directory fails to delete) and
// writing the new temporary directory path to the record file.
var mu syncutil.Mutex

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory. It returns nil if it
// could not set up a temporary Engine. When closed, it destroys the
// underlying data.
// One can optionally pass in a WaitGroup wg that waits for the cleanup
// routine for abandoned temporary directories to finish.
func NewTempEngine(
	ctx context.Context, tempStorage base.TempStorage, wg *sync.WaitGroup,
) (Engine, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to emporary store.
		return NewInMem(roachpb.Attributes{} /* attrs */, 0 /* cacheSize */), nil
	}

	// We generate a unique temporary directory with the prefix defaultTempStorageDirPrefix.
	tempPath, err := ioutil.TempDir(tempStorage.ParentDir, defaultTempStorageDirPrefix)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"could not create temporary subdirectory under the specified path %s\n",
			tempStorage.ParentDir,
		)
	}

	tempFullPath, err := filepath.Abs(tempPath)
	if err != nil {
		return nil, err
	}

	// Cleanup any abandoned temp storage directories from before.
	if err := cleanupTempStorageDirs(ctx, tempStorage.RecordPath, wg /* *WaitGroup */); err != nil {
		return nil, err
	}

	// FIXME(tschottdorf): should be passed in.
	st := cluster.MakeClusterSettings(cluster.BinaryServerVersion, cluster.BinaryServerVersion)

	rocksDBCfg := RocksDBConfig{
		Settings: st,
		Attrs:    roachpb.Attributes{},
		Dir:      tempFullPath,
		// MaxSizeBytes doesn't matter for temp storage - it's not
		// enforced in any way.
		MaxSizeBytes: 0,
		MaxOpenFiles: 128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	rocksdb, err := NewRocksDB(rocksDBCfg, rocksDBCache)
	if err != nil {
		return nil, err
	}
	// Append the new, current temp storage directory to the record file.
	if err = appendTempStorageDir(tempStorage.RecordPath, tempFullPath); err != nil {
		return nil, err
	}

	return &tempEngine{
		RocksDB:    rocksdb,
		recordPath: tempStorage.RecordPath,
	}, nil
}

type tempEngine struct {
	*RocksDB
	// The path where the temporary directory record path is stored.
	recordPath string
}

func (e *tempEngine) Close() {
	e.RocksDB.Close()
	// We could call cleanupTempStorageDirs here, but since we prefer faster
	// shutdowns (whereas node start-up allow more time for us to cleanup
	// asynchronously) we simply remove our current temporary store.
	dir := e.RocksDB.cfg.Dir
	if dir == "" {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Errorf(context.TODO(), "could not remove temporary store directory: %v", err.Error())
	}
	// Perform a synchronous (with respect to Close()) removable of the
	// record from the file.
	if err := removeTempStorageDirRecord(e.recordPath, dir); err != nil {
		log.Errorf(context.TODO(), "could not remove temporary store from record file: %v", err.Error())
	}
}

// removeTempStorageDirRecord scans through the temporary store record file and
// removes the specified temporary path from the record.
func removeTempStorageDirRecord(recordPath, tempPath string) error {
	mu.Lock()
	defer mu.Unlock()
	f, err := os.OpenFile(tempStorageDirsRecordFilepath(recordPath), os.O_RDWR, 0644)
	// The record file does not exist, nothing to remove.
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	// Scan the file and only write back filepaths that are not the
	// one being removed.
	s := bufio.NewScanner(f)
	var writeBack bytes.Buffer
	for s.Scan() {
		if s.Text() != tempPath {
			writeBack.Write(append(s.Bytes(), '\n'))
		}
	}
	if err = s.Err(); err != nil {
		return err
	}

	// Clear file content first.
	if err = f.Truncate(0); err != nil {
		return err
	}
	// Write at offset 0 to overwrite NULL bytes left over from Truncate.
	if _, err = f.WriteAt(writeBack.Bytes(), 0); err != nil {
		return err
	}
	return f.Sync()
}

// appendTempStorageDir records the location of the temporary directory in a log
// file for cleanup on startup if necessary.
func appendTempStorageDir(recordPath, tempPath string) error {
	return appendTempStorageDirs(recordPath, append([]byte(tempPath), '\n'))
}

// appendTempStorageDirs appends tempPaths (list of temporary directory paths) to
// a record file for cleanup on startup if necessary.
// tempPaths is a POSIX newline-ending byte slice of paths to the temporary
// store directories.
func appendTempStorageDirs(recordPath string, tempPaths []byte) error {
	mu.Lock()
	defer mu.Unlock()
	// If the file does not exist, create it, or append to the file.
	f, err := os.OpenFile(tempStorageDirsRecordFilepath(recordPath), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Append the path to the new temporary directory to the record file.
	if _, err = f.Write(tempPaths); err != nil {
		return err
	}
	return f.Sync()
}

// cleanupTempStorageDirs is invoked on startup to clean up abandoned temp
// store directories from previous startups. It reads from the temporary store
// record file to figure out which temporary stores need to be cleaned up.
// wg is allowed to be nil, if the caller does not want to wait on the cleanup.
// Removing existing temporary directories might be slow.  We spawn a
// goroutine to clean it up asynchronously.
func cleanupTempStorageDirs(ctx context.Context, recordPath string, wg *sync.WaitGroup) error {
	// We must read the contents of the record file first. This is because we
	// later record our new temporary directory and we need to retrieve all
	// previous temporary directories for deletion before we record our new
	// one.
	// Reading the entire file into memory shouldn't be a problem since
	// it is extremely rare for this record file to contain more than a few
	// entries.
	mu.Lock()
	defer mu.Unlock()
	f, err := os.OpenFile(tempStorageDirsRecordFilepath(recordPath), os.O_RDWR, 0644)
	// There is no existing record file and thus nothing to cleanup.
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	content, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	// Clear out the file now that we've stored all records in memory.
	if err = f.Truncate(0); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}

	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		tempDirs := strings.Split(string(content), "\n")
		// Keep track of which directories could not be cleared.  We
		// need to write this back to our record file after for the next
		// cleanup.
		var remainingDirs bytes.Buffer

		// Iterate through each temporary directory path and remove
		// the directory.
		for _, path := range tempDirs {
			if path == "" {
				continue
			}
			// If path/directory does not exist, error is nil.
			if err := os.RemoveAll(path); err != nil {
				log.Warningf(ctx, "could not remove old temporary store directory: %v", err.Error())
				// Write always returns a nil error. It will
				// panic if memory runs out.
				remainingDirs.Write(append([]byte(path), '\n'))
			}
		}
		// Write back to record file.
		if remainingDirs.Len() != 0 {
			if err := appendTempStorageDirs(recordPath, remainingDirs.Bytes()); err != nil {
				log.Warningf(ctx, "could not record non-removable temporary store directories to record file: %v", err.Error())
			}
		}
	}()

	return nil
}
