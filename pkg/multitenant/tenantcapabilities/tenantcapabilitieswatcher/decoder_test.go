// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestDecodeCapabilities verifies that we can correctly decode
// TenantCapabilities stored in the system.tenants table.
func TestDecodeCapabilities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		// Capabilities are only available in the system tenant's
		// system.tenants table.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer ts.Stopper().Stop(ctx)

	const dummyTableName = "dummy_system_tenants"
	tdb := sqlutils.MakeSQLRunner(db)
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
	k := ts.Codec().IndexPrefix(dummyTableID, keys.TenantsTablePrimaryKeyIndexID)
	rows, err := kvDB.Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	// Decode and verify.
	row := rows[0]
	got, err := tenantcapabilitieswatcher.TestingDecoderFn(ts.ClusterSettings())(ctx, roachpb.KeyValue{
		Key:   row.Key,
		Value: *row.Value,
	})
	require.NoError(t, err)

	require.Equal(t, tenantID, got.TenantID)
	require.Equal(t, &info.Capabilities, got.TenantCapabilities)
}
