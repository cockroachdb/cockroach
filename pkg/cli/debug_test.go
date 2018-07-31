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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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

func TestRemoveDeadReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	// The first node gets a real store, others are just in memory.
	storePath := filepath.Join(baseDir, "store")

	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				StoreSpecs:      []base.StoreSpec{{Path: storePath}},
				ScanMaxIdleTime: time.Millisecond,
			},
		},
	}
	// Start the cluster, let it replicate, then stop it. Since two
	// nodes use in-memory stores, this automatically causes the cluster
	// to lose its quorum.
	tc := testcluster.StartTestCluster(t, 3, clusterArgs)
	tc.Stopper().Stop(ctx)

	// Open the store directly to repair it.
	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		db, err := openExistingStore(storePath, stopper, false /* readOnly */)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		batch, err := removeDeadReplicas(db, map[roachpb.StoreID]struct{}{
			2: struct{}{},
			3: struct{}{},
		})
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			t.Fatal("expected non-nil batch")
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		batch.Close()

		// The repair process is idempotent and should give a nil batch the second time.
		batch, err = removeDeadReplicas(db, map[roachpb.StoreID]struct{}{
			2: struct{}{},
			3: struct{}{},
		})
		if err != nil {
			t.Fatal(err)
		}
		if batch != nil {
			t.Fatalf("expected nil batch on second attempt")
		}
	}()

	// Now that the data is salvaged, we can restart the cluster. The
	// nodes with the in-memory stores will be assigned new node IDs 4
	// and 5. StartTestCluster will even wait for all the ranges to be
	// replicated to the new nodes.
	tc = testcluster.StartTestCluster(t, 3, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	s := sqlutils.MakeSQLRunner(tc.Conns[0])
	row := s.QueryRow(t, "select replicas from [show experimental_ranges from table system.namespace] limit 1")
	var replicaStr string
	row.Scan(&replicaStr)
	if replicaStr != "{1,4,5}" {
		t.Fatalf("expected replicas on {1,4,5} but got %s", replicaStr)
	}
}
