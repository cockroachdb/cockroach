// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// TestZoneDecoder verifies that we can decode the primary key stored in the
// system.zones table.
func TestZonesDecoder(t *testing.T) {
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
	sqlDB := tc.ServerConn(0)

	k := keys.SystemSQLCodec.TablePrefix(keys.ZonesTableID)

	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	initialCount := len(rows)

	entries := []struct {
		id    descpb.ID
		proto zonepb.ZoneConfig
	}{
		{
			id:    50,
			proto: zonepb.ZoneConfig{},
		},
		{
			id:    55,
			proto: zonepb.DefaultZoneConfig(),
		},
		{
			id: 60,
			proto: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
			},
		},
	}

	for _, entry := range entries {
		buf, err := protoutil.Marshal(&entry.proto)
		require.NoError(t, err)

		_, err = sqlDB.Exec(
			"UPSERT INTO system.zones (id, config) VALUES ($1, $2) ", entry.id, buf,
		)
		require.NoError(t, err)
	}

	rows, err = tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Equal(t, initialCount+len(entries)*2, len(rows))

	// Don't bother with the system populated zone configurations for the purposes
	// of this test.
	rows = rows[initialCount:]

	for i, row := range rows {
		got, err := spanconfigsqlwatcher.NewTestingDecoderFn(keys.SystemSQLCodec)(row.Key)
		require.NoError(t, err)

		// system.zones has 2 column families, so for every entry that was
		// upserted into the table we expect there to be 2 rows corresponding to
		// it.
		require.Equal(t, entries[i/2].id, got)
	}
}
