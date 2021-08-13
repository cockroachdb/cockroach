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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSeparatedIntentsMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "takes >5 mins under race")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.ByKey(clusterversion.PostSeparatedIntentsMigration),
		clusterversion.ByKey(clusterversion.SeparatedIntentsMigration-1),
		false, /* initializeVersion */
	)
	const numServers int = 3
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
	err := tc.WaitForFullReplication()
	require.NoError(t, err)

	tdb := tc.ServerConn(0)

	findInterleavedIntent := func(s *kvserver.Store) bool {
		db := s.Engine()

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
			return true
		}

		return false
	}

	_, err = tdb.Exec("CREATE DATABASE test;")
	require.NoError(t, err)
	_, err = tdb.Exec("CREATE TABLE test.kv (key INTEGER PRIMARY KEY, val STRING NOT NULL);")
	require.NoError(t, err)

	// Create a transaction, write a lot of rows (intents), then kill the
	// gateway node before
	txn, err := tdb.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = txn.Exec("INSERT INTO test.kv SELECT generate_series(1, 100), 'test123';")
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	interleavedIntentFound := false
	for i := 0; i < numServers; i++ {
		err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			s.WaitForInit()
			interleavedIntentFound = interleavedIntentFound || findInterleavedIntent(s)
			return nil
		})
		require.NoError(t, err)
		if interleavedIntentFound {
			// This is all we care about; no need to waste cycles.
			break
		}
	}
	require.True(t, interleavedIntentFound)

	// Start writing separated intents.
	storeKnobs.StorageKnobs.DisableSeparatedIntents = false
	require.NoError(t, tc.Restart())
	time.Sleep(10 * time.Second)
	tdb = tc.ServerConn(0)

	_, err = tdb.Exec(`SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.PostSeparatedIntentsMigration).String())
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	testutils.SucceedsSoon(t, func() error {
		interleavedIntentFound := false
		for i := 0; i < numServers; i++ {
			err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				s.WaitForInit()
				if !s.IsStarted() {
					return errors.New("store not started")
				}
				interleavedIntentFound = interleavedIntentFound || findInterleavedIntent(s)
				return nil
			})
			if err != nil {
				return err
			}
			if interleavedIntentFound {
				// This is all we care about; no need to waste cycles.
				break
			}
		}
		if interleavedIntentFound {
			return errors.Errorf("expected 0 interleaved intents, found one")
		}
		return nil
	})
}
