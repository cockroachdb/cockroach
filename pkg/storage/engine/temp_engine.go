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
	"golang.org/x/net/context"
)

const (
	// TempStoreDirPrefix is the prefix for the directories used for the
	// temporary store.
	TempStoreDirPrefix = "cockroach-temp"
	// TempStoreDirsLogFilename is the filename for the log file that keeps
	// track of the paths of the temporary directories created.
	// This file will be stored under the store directory.
	TempStoreDirsLogFilename = "temp-store-dirs.txt"
)

func tempStoreDirsLogFilepath(storePath string) string {
	return filepath.Join(storePath, TempStoreDirsLogFilename)
}

// Use a mutex to synchronize writing to the temp store log file by the
// asynchronous cleanup goroutine (when a directory fails to delete) and
// writing the new temporary directory path to the log file.
var mu sync.Mutex

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory. It returns nil if it
// could not set up a temporary Engine. When closed, it destroys the
// underlying data.
// The (usually first) store's spec is passed in to provide a location to
// record the path of the temporary stores.
func NewTempEngine(ctx context.Context, firstStoreSpec, tempStoreSpec base.StoreSpec) (Engine, error) {
	if tempStoreSpec.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to emporary store.
		return NewInMem(tempStoreSpec.Attributes, 0 /* cacheSize */), nil
	}

	if err := cleanupTempStoreDirs(ctx, firstStoreSpec.Path, nil /* *WaitGroup */); err != nil {
		return nil, err
	}

	// FIXME(tschottdorf): should be passed in.
	st := cluster.MakeClusterSettings(cluster.BinaryServerVersion, cluster.BinaryServerVersion)

	rocksDBCfg := RocksDBConfig{
		Settings:     st,
		Attrs:        roachpb.Attributes{},
		Dir:          tempStoreSpec.Path,
		MaxSizeBytes: 0,   // TODO(arjun): Revisit this.
		MaxOpenFiles: 128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	rocksdb, err := NewRocksDB(rocksDBCfg, rocksDBCache)
	if err != nil {
		return nil, err
	}
	if err = logTempStoreDir(firstStoreSpec.Path, tempStoreSpec.Path); err != nil {
		return nil, err
	}

	return &tempEngine{RocksDB: rocksdb}, nil
}

type tempEngine struct {
	*RocksDB
}

func (e *tempEngine) Close() {
	e.RocksDB.Close()
	// We could call cleanupTempStoreDirs here, but since we prefer faster
	// shutdowns (whereas node start-up allow more time for us to cleanup
	// asynchronously) we simply remove our current temporary store.
	dir := e.RocksDB.cfg.Dir
	if dir == "" {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Errorf(context.TODO(), "could not remove temporary store directory: %v", err.Error())
	}
}

// logTempStoreDir records the location of the temporary directory in a log
// file for cleanup on startup if necessary.
func logTempStoreDir(storePath string, tempPath string) error {
	return logTempStoreDirs(storePath, append([]byte(tempPath), '\n'))
}

// logTempStoreDirs records tempPaths (list of temporary directory paths) to a log file
// file for cleanup on startup if necessary.
// tempPaths is a \n separated and ending list of paths to the temporary store directories.
func logTempStoreDirs(storePath string, tempPaths []byte) error {
	mu.Lock()
	defer mu.Unlock()
	// If the file does not exist, create it, or append to the file.
	f, err := os.OpenFile(tempStoreDirsLogFilepath(storePath), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() error {
		if err := f.Close(); err != nil {
			return err
		}
		return nil
	}()

	// Append the path to the new temporary directory to the log file.
	_, err = f.Write(tempPaths)
	return err
}

// cleanupTempStoreDirs is invoked on startup to clean up abandoned temp
// store directories from previous startups. It reads from the temporary store
// log file to figure out which temporary stores need to be cleaned up.
// wg is allowed to be nil, if the caller does not want to wait on the cleanup.
// Removing existing temporary directories might be slow.  We spawn a
// goroutine to clean it up asynchronously.
func cleanupTempStoreDirs(ctx context.Context, storePath string, wg *sync.WaitGroup) error {
	// We must read the contents of the log file first. This is because we
	// later record our new temporary directory and we need to retrieve all
	// previous temporary directories for deletion before we record our new
	// one.
	// Reading the entire file into memor shouldn't be a problem since
	// it is extremely rare for this log file to contain more than a few
	// entries.
	content, err := ioutil.ReadFile(tempStoreDirsLogFilepath(storePath))
	// There is no existing log file and thus nothing to cleanup.
	if _, ok := err.(*os.PathError); ok {
		return nil
	}
	if err != nil {
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
		// need to write this back to our log file after for the next
		// cleanup.
		// TODO(richardwu): do we need memory monitoring here with
		// BytesMonitor?
		var remainingDirs bytes.Buffer

		// Iterate through each temporary directory path and remove
		// the directory.
		for _, path := range tempDirs {
			if path == "" {
				continue
			}
			// If path/directory does not exist, error is nil.
			if err := os.RemoveAll(path); err != nil {
				// Log a warning.
				log.Warningf(ctx, "could not remove old temporary store directory: %v", err.Error())
				// Write always returns a nil error. It will
				// panic if memory runs out.
				remainingDirs.Write(append([]byte(path), '\n'))
			}
		}
		// Write back to log file.
		logTempStoreDirs(storePath, remainingDirs.Bytes())
	}()

	return nil
}
