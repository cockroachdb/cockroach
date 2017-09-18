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
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

const (
	defaultTempStorageDirPrefix = "cockroach-temp"
	// TempStorageDirsRecordFilename is the filename for the record file
	// that keeps track of the paths of the temporary directories created.
	// This file will be stored under the TempStorageConfig.RecordDir
	// directory (which is defaulted to the first store's root).
	TempStorageDirsRecordFilename = "temp-storage-dirs.txt"
)

func tempStorageDirsRecordFilepath(recordDir string) string {
	return filepath.Join(recordDir, TempStorageDirsRecordFilename)
}

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory.
// When closed, it destroys the underlying data by deleting the temporary
// subdirectory and all its temporary files (if TempStorageConfig.InMemory is
// false).
func NewTempEngine(tempStorage base.TempStorageConfig) (Engine, error) {
	// Clean up any abandoned temp storage directories from before.
	if err := cleanupTempStorageDirs(tempStorage.RecordDir); err != nil {
		return nil, errors.Wrapf(
			err,
			"could not clean up abandoned temp storage directories from record file located in %s",
			tempStorage.RecordDir,
		)
	}

	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
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
	// Append the current temp storage directory to the record file for
	// cleanup later.
	if err = appendTempStorageDir(tempStorage.RecordDir, tempFullPath); err != nil {
		return nil, err
	}

	return &tempEngine{
		RocksDB:   rocksdb,
		recordDir: tempStorage.RecordDir,
	}, nil
}

type tempEngine struct {
	*RocksDB
	// The path to the direectory where the temporary directory record file
	// is stored.
	recordDir string
}

func (e *tempEngine) Close() {
	e.RocksDB.Close()
	dir := e.RocksDB.cfg.Dir
	if dir == "" {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Errorf(context.TODO(), "could not remove temporary store directory: %v", err.Error())
	}
	// Clean up the temporary directory.
	if err := cleanupTempStorageDirs(e.recordDir); err != nil {
		log.Errorf(context.TODO(), "could not remove temporary store from record file: %v", err.Error())
	}
}

// appendTempStorageDir records the location of the temporary directory in a log
// file for cleanup on startup if necessary.
func appendTempStorageDir(recordDir, tempPath string) error {
	// If the file does not exist, create it, or append to the file.
	f, err := os.OpenFile(tempStorageDirsRecordFilepath(recordDir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Append the path to the new temporary directory to the record file.
	if _, err = f.Write(append([]byte(tempPath), '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// cleanupTempStorageDirs should be invoked on startup to clean up abandoned
// temp store directories from previous startups.
// It reads from the temporary store record file found in recordDir to figure
// out which temporary stores need to be cleaned up.
func cleanupTempStorageDirs(recordDir string) error {
	// Reading the entire file into memory shouldn't be a problem since
	// it is extremely rare for this record file to contain more than a few
	// entries.
	f, err := os.OpenFile(tempStorageDirsRecordFilepath(recordDir), os.O_RDWR, 0644)
	// There is no existing record file and thus nothing to clean up.
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// Iterate through each temporary directory path and remove
	// the directory.
	for scanner.Scan() {
		path := scanner.Text()
		if path == "" {
			continue
		}
		// If path/directory does not exist, error is nil.
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	// Clear out the file now that we've stored all records in memory.
	if err = f.Truncate(0); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}

	return nil
}
