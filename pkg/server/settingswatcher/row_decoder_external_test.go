// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settingswatcher_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRowDecoder simply verifies that the row decoder can safely decode the
// rows stored in the settings table of a real cluster with a few values of a
// few different types set.
func TestRowDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(db)

	toSet := map[string]struct {
		val        interface{}
		expStr     string
		expValType string
	}{
		"trace.span_registry.enabled": {
			val:        true,
			expStr:     "true",
			expValType: "b",
		},
		"sql.trace.txn.enable_threshold": {
			val:        "17s",
			expStr:     "17s",
			expValType: "d",
		},
		"sql.txn_stats.sample_rate": {
			val:        .23,
			expStr:     "0.23",
			expValType: "f",
		},
		"cluster.label": {
			val:        "foobar",
			expStr:     "foobar",
			expValType: "s",
		},
	}
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v.val)
	}

	k := ts.Codec().TablePrefix(keys.SettingsTableID)
	rows, err := kvDB.Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	dec := settingswatcher.MakeRowDecoder(ts.Codec())
	var alloc *tree.DatumAlloc
	for _, row := range rows {
		kv := roachpb.KeyValue{
			Key:   row.Key,
			Value: *row.Value,
		}

		k, val, tombstone, err := dec.DecodeRow(kv, alloc)
		require.NoError(t, err)
		require.False(t, tombstone)
		if exp, ok := toSet[k]; ok {
			require.Equal(t, exp.expStr, val.Value)
			require.Equal(t, exp.expValType, val.Type)
			delete(toSet, k)
		}

		// Test the tombstone logic while we're here.
		{
			kv.Value.Reset()
			tombstoneK, val, tombstone, err := dec.DecodeRow(kv, alloc)
			require.NoError(t, err)
			require.True(t, tombstone)
			require.Equal(t, k, tombstoneK)
			require.Zero(t, val.Value)
			require.Zero(t, val.Type)
		}
	}
	require.Len(t, toSet, 0)
}
