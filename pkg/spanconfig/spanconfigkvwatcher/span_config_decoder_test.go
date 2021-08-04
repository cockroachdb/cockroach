// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvwatcher_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvwatcher"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestSpanConfigDecoder verifies that we can decode rows stored in the
// system.span_configurations table.
func TestSpanConfigDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	getCount := func() int {
		explain := tdb.Query(t, `SELECT count(*) FROM system.span_configurations`)
		explain.Next()
		var c int
		require.Nil(t, explain.Scan(&c))
		require.Nil(t, explain.Close())
		return c
	}

	initialCount := getCount()

	key := tc.ScratchRange(t)
	rng := tc.GetFirstStoreFromServer(t, 0).LookupReplica(keys.MustAddr(key))
	span := rng.Desc().RSpan().AsRawSpanWithNoLocals()
	conf := roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3}

	buf, err := protoutil.Marshal(&conf)
	require.NoError(t, err)
	tdb.Exec(t, "UPSERT INTO system.span_configurations (start_key, end_key, config) VALUES ($1, $2, $3)",
		span.Key, span.EndKey, buf)
	require.Equal(t, initialCount+1, getCount())

	k := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)
	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Len(t, rows, initialCount+1)

	last := rows[len(rows)-1]
	got, err := spanconfigkvwatcher.NewTestingDecoderFn()(
		roachpb.KeyValue{
			Key:   last.Key,
			Value: *last.Value,
		},
	)
	require.NoError(t, err)
	require.Truef(t, span.Equal(got.Span),
		"expected span=%s, got span=%s", span, got.Span)
	require.Truef(t, conf.Equal(got.Config),
		"expected config=%s, got config=%s", conf, got.Config)
}
