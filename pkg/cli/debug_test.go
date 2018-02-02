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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func createStore(t *testing.T, path string) {
	t.Helper()
	cache := engine.NewRocksDBCache(server.DefaultCacheSize)
	defer cache.Release()
	db, err := engine.NewRocksDB(
		engine.RocksDBConfig{
			Dir:       path,
			MustExist: false,
		},
		cache,
	)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestOpenExistingStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	dirExists := filepath.Join(baseDir, "exists")
	dirMissing := filepath.Join(baseDir, "missing")
	createStore(t, dirExists)

	for _, test := range []struct {
		dir    string
		expErr string
	}{
		{
			dir:    dirExists,
			expErr: "",
		},
		{
			dir:    dirMissing,
			expErr: `could not open rocksdb instance: .* does not exist \(create_if_missing is false\)`,
		},
	} {
		t.Run(fmt.Sprintf("dir=%s", test.dir), func(t *testing.T) {
			_, err := openExistingStore(test.dir, stopper, false /* readOnly */)
			if !testutils.IsError(err, test.expErr) {
				t.Errorf("wanted %s but got %v", test.expErr, err)
			}
		})
	}
}

func TestOpenReadOnlyStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	storePath := filepath.Join(baseDir, "store")
	createStore(t, storePath)

	for _, test := range []struct {
		readOnly bool
		expErr   string
	}{
		{
			readOnly: false,
			expErr:   "",
		},
		{
			readOnly: true,
			expErr:   `Not supported operation in read only mode.`,
		},
	} {
		t.Run(fmt.Sprintf("readOnly=%t", test.readOnly), func(t *testing.T) {
			db, err := openExistingStore(storePath, stopper, test.readOnly)
			if err != nil {
				t.Fatal(err)
			}

			key := engine.MakeMVCCMetadataKey(roachpb.Key("key"))
			val := []byte("value")
			err = db.Put(key, val)
			if !testutils.IsError(err, test.expErr) {
				t.Fatalf("wanted %s but got %v", test.expErr, err)
			}
		})
	}
}
