// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engineupgrade

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createEnegine creates an engine for testing.
// It should be closed when it is no longer needed via the Close() method.
func createEngine(t *testing.T) engine.Engine {
	ctx := context.Background()
	eng := engine.NewInMem(
		ctx,
		engine.DefaultStorageEngine,
		roachpb.Attributes{},
		1<<20,
	)
	val := roachpb.Value{}
	val.SetInt(0)

	for _, key := range []roachpb.Key{
		keys.StoreNodeUpgradePendingVersionKey(),
		keys.StoreNodeUpgradeCurrentVersionKey(),
	} {
		err := engine.MVCCPut(
			ctx,
			eng,
			nil, /* stats */
			key,
			hlc.Timestamp{},
			val,
			nil, /* txn */
		)
		require.NoError(t, err)
	}
	return eng
}

// assertStatus ensures the status from serv matches the expected status.
func assertStatus(t *testing.T, serv *nodeUpgrader, expected []*StatusResponse_Engine) {
	statusResp, err := serv.Status(context.Background(), &StatusRequest{})
	require.NoError(t, err)
	assert.Nil(t, statusResp.Err)
	assert.Equal(t, &StatusResponse{Engines: expected}, statusResp)
}

// forceEngineKey forces an engine to be a certain version for a given key.
func forceEngineKey(t *testing.T, eng engine.Engine, key roachpb.Key, v Version) {
	val := roachpb.Value{}
	val.SetInt(int64(v))
	err := engine.MVCCPut(
		context.Background(),
		eng,
		nil, /* stats */
		key,
		hlc.Timestamp{},
		val,
		nil, /* txn */
	)
	require.NoError(t, err)
}

func TestRunUpgrades(t *testing.T) {
	var hookCounterMutex syncutil.Mutex
	hookCounter := make(map[Version]int)
	makeFn := func(i Version) func(ctx context.Context, eng engine.Engine) error {
		return func(ctx context.Context, eng engine.Engine) error {
			hookCounterMutex.Lock()
			hookCounter[i]++
			hookCounterMutex.Unlock()
			return nil
		}
	}
	engs := []engine.Engine{
		createEngine(t),
		createEngine(t),
		createEngine(t),
	}
	for _, eng := range engs {
		defer eng.Close()
	}
	serv := &nodeUpgrader{
		engs:     engs,
		registry: &upgradeRegistry{},
	}
	serv.registry.mu.versionToFn = map[Version]Fn{
		Version(1): makeFn(1),
		Version(2): makeFn(2),
		Version(3): makeFn(3),
		Version(4): makeFn(4),
		Version(5): makeFn(5),
	}
	ctx := context.Background()

	t.Run("test upgrade after target version set", func(t *testing.T) {
		resp, err := serv.RunUpgrades(ctx, &RunUpgradesRequest{TargetVersion: 2})
		require.NoError(t, err)
		require.Nil(t, resp.Err)

		assertStatus(t, serv, []*StatusResponse_Engine{
			{CurrentVersion: 2, PendingVersion: 2},
			{CurrentVersion: 2, PendingVersion: 2},
			{CurrentVersion: 2, PendingVersion: 2},
		})

		assert.Equal(
			t,
			map[Version]int{
				Version(1): 3,
				Version(2): 3,
			},
			hookCounter,
		)
	})

	t.Run("test RunUpgrades idempotency", func(t *testing.T) {
		resp, err := serv.RunUpgrades(ctx, &RunUpgradesRequest{TargetVersion: 2})
		require.NoError(t, err)
		require.Nil(t, resp.Err)

		assertStatus(t, serv, []*StatusResponse_Engine{
			{CurrentVersion: 2, PendingVersion: 2},
			{CurrentVersion: 2, PendingVersion: 2},
			{CurrentVersion: 2, PendingVersion: 2},
		})

		assert.Equal(
			t,
			map[Version]int{
				Version(1): 3,
				Version(2): 3,
			},
			hookCounter,
		)
	})

	t.Run("test RunUpgrades moves forward if target version is higher", func(t *testing.T) {
		// Do a rollback, ru upgrades to 3.
		forceEngineKey(t, serv.engs[1], keys.StoreNodeUpgradePendingVersionKey(), Version(1))
		forceEngineKey(t, serv.engs[1], keys.StoreNodeUpgradeCurrentVersionKey(), Version(0))

		resp, err := serv.RunUpgrades(ctx, &RunUpgradesRequest{TargetVersion: 3})
		require.NoError(t, err)
		require.Nil(t, resp.Err)

		assertStatus(t, serv, []*StatusResponse_Engine{
			{CurrentVersion: 3, PendingVersion: 3},
			{CurrentVersion: 3, PendingVersion: 3},
			{CurrentVersion: 3, PendingVersion: 3},
		})

		assert.Equal(
			t,
			map[Version]int{
				Version(1): 4,
				Version(2): 4,
				Version(3): 3,
			},
			hookCounter,
		)
	})

	t.Run("test RunUpgrades defensive assert", func(t *testing.T) {
		forceEngineKey(t, serv.engs[0], keys.StoreNodeUpgradeCurrentVersionKey(), Version(5))
		resp, err := serv.RunUpgrades(ctx, &RunUpgradesRequest{TargetVersion: 5})
		require.NoError(t, err)
		require.NotNil(t, resp.Err)
		assert.Regexp(t, "current version .* > pending version", errors.DecodeError(ctx, *resp.Err).Error())
	})
}
