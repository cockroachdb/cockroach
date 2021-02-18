// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settingswatcher_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRowDecoder simply verifies that the row decoder can safely decode the
// rows stored in the settings table of a real cluster with a few values of a
// few different types set.
func TestRowDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	toSet := map[string]struct {
		val        interface{}
		expStr     string
		expValType string
	}{
		"kv.rangefeed.enabled": {
			val:        true,
			expStr:     "true",
			expValType: "b",
		},
		"kv.queue.process.guaranteed_time_budget": {
			val:        "17s",
			expStr:     "17s",
			expValType: "d",
		},
		"kv.closed_timestamp.close_fraction": {
			val:        .23,
			expStr:     "0.23",
			expValType: "f",
		},
		"cluster.organization": {
			val:        "foobar",
			expStr:     "foobar",
			expValType: "s",
		},
	}
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v.val)
	}

	k := keys.SystemSQLCodec.TablePrefix(keys.SettingsTableID)
	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	dec := settingswatcher.MakeRowDecoder(keys.SystemSQLCodec)
	for _, row := range rows {
		kv := roachpb.KeyValue{
			Key:   row.Key,
			Value: *row.Value,
		}

		k, val, valType, tombstone, err := dec.DecodeRow(kv)
		require.NoError(t, err)
		require.False(t, tombstone)
		if exp, ok := toSet[k]; ok {
			require.Equal(t, exp.expStr, val)
			require.Equal(t, exp.expValType, valType)
			delete(toSet, k)
		}

		// Test the tombstone logic while we're here.
		{
			kv.Value.Reset()
			tombstoneK, val, valType, tombstone, err := dec.DecodeRow(kv)
			require.NoError(t, err)
			require.True(t, tombstone)
			require.Equal(t, k, tombstoneK)
			require.Zero(t, val)
			require.Zero(t, valType)
		}
	}
	require.Len(t, toSet, 0)
}
