// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitieswatcher_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestDecodeCapabilities verifies that we can correctly decode
// TenantCapabilities stored in the system.tenants table.
func TestDecodeCapabilities(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableDefaultTestTenant: true, // system.tenants only exists for the system tenant
		},
	})
	defer tc.Stopper().Stop(ctx)

	const dummyTableName = "dummy_system_tenants"
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.tenants INCLUDING ALL)", dummyTableName))

	var dummyTableID uint32
	tdb.QueryRow(t, fmt.Sprintf(
		`SELECT table_id FROM crdb_internal.tables WHERE name = '%s'`, dummyTableName),
	).Scan(&dummyTableID)

	tenantID, err := roachpb.MakeTenantID(10)
	require.NoError(t, err)
	info := mtinfopb.ProtoInfo{
		Capabilities: tenantcapabilitiespb.TenantCapabilities{
			DisableAdminSplit: true,
		},
	}
	buf, err := protoutil.Marshal(&info)
	require.NoError(t, err)
	tdb.Exec(
		t,
		fmt.Sprintf("INSERT INTO %s (id, active, info) VALUES ($1, $2, $3)", dummyTableName),
		tenantID.ToUint64(), true /* active */, buf,
	)

	// Read the row	.
	k := keys.SystemSQLCodec.IndexPrefix(dummyTableID, keys.TenantsTablePrimaryKeyIndexID)
	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	// Decode and verify.
	row := rows[0]
	got, err := tenantcapabilitieswatcher.TestingDecoderFn()(roachpb.KeyValue{
		Key:   row.Key,
		Value: *row.Value,
	})
	require.NoError(t, err)

	require.Equal(t, tenantID, got.TenantID)
	require.Equal(t, &info.Capabilities, got.TenantCapabilities)
}
