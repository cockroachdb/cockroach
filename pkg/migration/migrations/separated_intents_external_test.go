// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSeparatedIntentsMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.ByKey(clusterversion.PostSeparatedIntentsMigration),
		clusterversion.ByKey(clusterversion.SeparatedIntentsMigration-1),
		false, /* initializeVersion */
	)
	const numServers int = 5
	stickyServerArgs := make(map[int]base.TestServerArgs)
	storeKnobs := &kvserver.StoreTestingKnobs{
		StorageKnobs: storage.TestingKnobs{DisableSeparatedIntents: true},
	}
	tempDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			Settings: settings,
			StoreSpecs: []base.StoreSpec{
				{
					InMemory: false,
					Path:     path.Join(tempDir, fmt.Sprintf("engine-%d", i)),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.SeparatedIntentsMigration - 1),
				},
				Store: storeKnobs,
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{
		ServerArgsPerNode: stickyServerArgs,
	})
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// Create tables and databases, and get their IDs.
	tdb := tc.ServerConn(0)

	getIntentCount := func(s *kvserver.Store) int {
		db := s.Engine()
		count := 0

		iter := db.NewEngineIterator(storage.IterOptions{
			LowerBound: roachpb.KeyMin,
			UpperBound: roachpb.KeyMax,
		})
		defer iter.Close()
		valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: roachpb.KeyMin})
		for ; valid && err == nil; valid, err = iter.NextEngineKey() {
			key, err := iter.EngineKey()
			if err != nil {
				t.Fatal(err)
			}
			if !key.IsMVCCKey() {
				continue
			}
			mvccKey, err := key.ToMVCCKey()
			if err != nil {
				t.Fatal(err)
			}
			if !mvccKey.Timestamp.IsEmpty() {
				continue
			}
			val := iter.Value()
			meta := enginepb.MVCCMetadata{}
			if err := protoutil.Unmarshal(val, &meta); err != nil {
				t.Fatal(err)
			}
			if meta.IsInline() {
				continue
			}
			count++
		}

		return count
	}

	_, err := tdb.Exec("CREATE DATABASE test;")
	require.NoError(t, err)
	_, err = tdb.Exec("CREATE TABLE test.kv (key INTEGER PRIMARY KEY, val STRING NOT NULL);")
	require.NoError(t, err)

	// Create a transaction, write a lot of rows (intents), then kill the
	// gateway node before
	txn, err := tdb.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = txn.Exec("INSERT INTO test.kv SELECT generate_series(1, 100), 'test123';")
	require.NoError(t, err)

	interleavedIntentCount := 0
	for i := 0; i < numServers; i++ {
		err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			interleavedIntentCount += getIntentCount(s)
			return nil
		})
		require.NoError(t, err)
		if interleavedIntentCount > 0 {
			// This is all we care about; no need to waste cycles.
			break
		}
	}
	require.Greater(t, interleavedIntentCount, 0)

	// Start writing separated intents.
	storeKnobs.StorageKnobs.DisableSeparatedIntents = false
	require.NoError(t, tc.Restart())
	time.Sleep(10 * time.Second)
	require.NoError(t, tc.WaitForFullReplication())
	tdb = tc.ServerConn(0)

	_, err = tdb.Exec(`SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.PostSeparatedIntentsMigration).String())
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	testutils.SucceedsSoon(t, func() error {
		interleavedIntentCount := 0
		for i := 0; i < 5; i++ {
			err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				s.WaitForInit()
				if !s.IsStarted() {
					return errors.New("store not started")
				}
				interleavedIntentCount += getIntentCount(s)
				return nil
			})
			if err != nil {
				return err
			}
			if interleavedIntentCount > 0 {
				// This is all we care about; no need to waste cycles.
				break
			}
		}
		if interleavedIntentCount > 0 {
			return errors.Errorf("expected 0 interleaved intents, got %d", interleavedIntentCount)
		}
		return nil
	})
}
