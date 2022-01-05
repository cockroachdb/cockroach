// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDestroyTenantSynchronous tests that destroying a tenant synchronously
// with crdb_internal.destroy_tenant(<ten_id>, true) works.
func TestDestroyTenantSynchronous(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tenantID := roachpb.MakeTenantID(10)
	codec := keys.MakeSQLCodec(tenantID)

	// Create the tenant, make sure it has data and state.
	tdb.Exec(t, "SELECT crdb_internal.create_tenant(10)")
	tdb.CheckQueryResults(t,
		"SELECT id, active FROM system.tenants WHERE id = 10",
		[][]string{{"10", "true"}})
	{
		rows, err := kvDB.Scan(
			ctx, codec.TenantPrefix(), codec.TenantPrefix().PrefixEnd(),
			1, /* maxRows */
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)
	}

	// Destroy the tenant, make sure it does not have data and state.
	tdb.Exec(t, "SELECT crdb_internal.destroy_tenant(10, true)")
	tdb.CheckQueryResults(t,
		"SELECT id, active FROM system.tenants WHERE id = 10",
		[][]string{})
	{
		rows, err := kvDB.Scan(
			ctx, codec.TenantPrefix(), codec.TenantPrefix().PrefixEnd(),
			1, /* maxRows */
		)
		require.NoError(t, err)
		require.Len(t, rows, 0)
	}
}
