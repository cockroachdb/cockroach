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
//
// Author: Arjun Narayan (arjun@cockroachlabs.com)

package engine

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"
)

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory.
func NewTempEngine(ctx context.Context, storeCfg base.StoreSpec) (Engine, error) {
	if storeCfg.InMemory {
		// TODO(arjun): Copy the size in a principled fashion from the main store
		// after #16750 is addressed.
		return NewInMem(roachpb.Attributes{}, 0), nil
	}

	if err := cleanupLocalStorageDirs(ctx, storeCfg.Path, nil); err != nil {
		return nil, err
	}

	rocksDBCfg := RocksDBConfig{
		Attrs:          roachpb.Attributes{},
		Dir:            storeCfg.Path,
		MaxSizeInBytes: 0,   // TODO(arjun): Revisit this.
		MaxOpenFiles:   128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	return NewRocksDB(rocksDBCfg, rocksDBCache)
}

func cleanupLocalStorageDirs(ctx context.Context, path string, wg *sync.WaitGroup) error {
	// Removing existing contents might be slow. Instead we rename it to a new
	// name, and spawn a goroutine to clean it up asynchronously.
	deletionDir, err := ioutil.TempDir(path, "TO-DELETE-")
	if err != nil {
		return err
	}

	filesToDelete, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, fileToDelete := range filesToDelete {
		toDeleteFull := filepath.Join(path, fileToDelete.Name())
		if toDeleteFull != deletionDir {
			if err := os.Rename(toDeleteFull, filepath.Join(deletionDir, fileToDelete.Name())); err != nil {
				return err
			}
		}
	}
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		if err := os.RemoveAll(deletionDir); err != nil {
			log.Warningf(ctx, "could not clear old TempEngine files: %v", err.Error())
			// Even if this errors, this is safe since it's in the marked-for-deletion subdirectory.
			return
		}
	}()

	return nil
}
