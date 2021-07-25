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
	span := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	entries := []roachpb.SpanConfigEntry{
		{
			Span:   span("a", "c"),
			Config: roachpb.SpanConfig{},
		},
		{
			Span:   span("d", "f"),
			Config: roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3},
		},
	}
	for _, entry := range entries {
		buf, err := protoutil.Marshal(&entry.Config)
		require.NoError(t, err)
		tdb.Exec(t, "UPSERT INTO system.span_configurations (start_key, end_key, config) VALUES ($1, $2, $3)",
			entry.Span.Key, entry.Span.EndKey, buf)
	}

	k := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)
	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Equal(t, len(rows), len(entries))

	dec := spanconfigkvwatcher.NewSpanConfigDecoder()
	for i, row := range rows {
		kv := roachpb.KeyValue{
			Key:   row.Key,
			Value: *row.Value,
		}
		got, err := dec.Decode(kv)
		require.NoError(t, err)

		require.Truef(t, entries[i].Span.Equal(got.Span),
			"entries[%d].span=%s != got.span=%s", i, entries[i].Span, got.Span)
		require.Truef(t, entries[i].Config.Equal(got.Config),
			"entries[%d].config=%s != config.span=%s", i, entries[i].Config, got.Config)
	}
}
