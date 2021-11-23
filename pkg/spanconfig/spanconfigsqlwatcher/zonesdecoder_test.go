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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// TestZoneDecoderDecodePrimaryKey verifies that we can decode the primary key
// stored in a system.zones like table.
func TestZonesDecoderDecodePrimaryKey(t *testing.T) {
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
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Create a dummy table, like system.zones, to modify in this test. This lets
	// us test things without bother with the prepoulated contents for
	// system.zones.
	//
	// Note that system.zones has two column families (for legacy) reasons, but
	// the table dummy table constructed below does not. This shouldn't matter
	// as the decoder is only decoding the primary key in this test, which doesn't
	// change across different column families.
	const dummyTableName = "dummy_zones"
	sqlDB.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.zones INCLUDING ALL)", dummyTableName))

	var dummyTableID uint32
	sqlDB.QueryRow(
		t,
		fmt.Sprintf("SELECT id FROM system.namespace WHERE name='%s'", dummyTableName),
	).Scan(&dummyTableID)

	k := keys.SystemSQLCodec.TablePrefix(dummyTableID)

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

		_ = sqlDB.Exec(
			t, fmt.Sprintf("UPSERT INTO %s (id, config) VALUES ($1, $2) ", dummyTableName), entry.id, buf,
		)
		require.NoError(t, err)
	}

	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Equal(t, len(entries), len(rows))

	for i, row := range rows {
		got, err := spanconfigsqlwatcher.TestingZonesDecoderDecodePrimaryKey(keys.SystemSQLCodec, row.Key)
		require.NoError(t, err)
		require.Equal(t, entries[i].id, got)
	}
}
