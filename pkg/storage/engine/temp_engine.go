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
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory. It returns nil if it
// could not set up a temporary Engine. When closed, it destroys the
// underlying data.
func NewTempEngine(ctx context.Context, storeCfg base.StoreSpec) (Engine, error) {
	if storeCfg.SizeInBytes != 0 {
		return nil, errors.Errorf("spec.SizeInBytes specified for temp store. " +
			"That's not allowed as the setting doesn't do anything.")
	}

	if storeCfg.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		return NewInMem(storeCfg.Attributes, 0 /* cacheSize */), nil
	}

	if err := cleanupTempStorageDirs(ctx, storeCfg.Path, nil /* *WaitGroup */); err != nil {
		return nil, err
	}

	// FIXME(tschottdorf): should be passed in.
	st := cluster.MakeClusterSettings(cluster.BinaryServerVersion, cluster.BinaryServerVersion)

	rocksDBCfg := RocksDBConfig{
		Settings: st,
		Attrs:    roachpb.Attributes{},
		Dir:      storeCfg.Path,
		// MaxSizeBytes doesn't matter for temp stores - it's not enforced in any way.
		MaxSizeBytes: 0,
		MaxOpenFiles: 128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	rocksdb, err := NewRocksDB(rocksDBCfg, rocksDBCache)
	return &tempEngine{RocksDB: rocksdb}, err
}

type tempEngine struct {
	*RocksDB
}

func (e *tempEngine) Close() {
	e.RocksDB.Close()
	dir := e.RocksDB.cfg.Dir
	if dir == "" {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Errorf(context.TODO(), "could not remove rocksdb dir: %v", err)
	}
}

// wg is allowed to be nil, if the caller does not want to wait on the cleanup.
func cleanupTempStorageDirs(ctx context.Context, path string, wg *sync.WaitGroup) error {
	// Removing existing contents might be slow. Instead we rename it to a new
	// name, and spawn a goroutine to clean it up asynchronously.
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
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
