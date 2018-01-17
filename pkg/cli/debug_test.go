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
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestOpenExistingStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	dirExists := filepath.Join(baseDir, "exists")
	dirMissing := filepath.Join(baseDir, "missing")

	func() {
		cache := engine.NewRocksDBCache(server.DefaultCacheSize)
		defer cache.Release()
		db, err := engine.NewRocksDB(
			engine.RocksDBConfig{
				Dir:       dirExists,
				MustExist: false,
			},
			cache,
		)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

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
		_, err := openExistingStore(test.dir, stopper)
		if !testutils.IsError(err, test.expErr) {
			t.Fatalf("%s: wanted %s but got %v", test.dir, test.expErr, err)
		}
	}
}
