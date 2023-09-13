// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestServerSQLDescIDOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sysDB := s.SystemLayer().SQLConn(t, "")

	tids := securitytest.EmbeddedTenantIDs()

	s1, err := s.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID:   roachpb.MustMakeTenantID(tids[0]),
		TenantName: "a",
	})
	require.NoError(t, err)
	defer s1.AppStopper().Stop(ctx)
	sqlDB1 := s1.SQLConn(t, "")

	s2, sqlDB2, err := s.TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantID:   roachpb.MustMakeTenantID(tids[1]),
		TenantName: "b",
	})
	require.NoError(t, err)
	defer s2.AppStopper().Stop(ctx)

	for _, tc := range []struct {
		name string
		db   *gosql.DB
	}{{"system", sysDB}, {"separate-process", sqlDB1}, {"shared-process", sqlDB2}} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlutils.MakeSQLRunner(tc.db)
			db.Exec(t, `CREATE DATABASE d`)
			db.Exec(t, `CREATE TABLE d.testtable (x INT PRIMARY KEY)`)
			db.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM system.namespace WHERE id >= %d", sql.TestingFixedDescIDOffset),
				[][]string{
					{"0", "0", "d", fmt.Sprint(sql.TestingFixedDescIDOffset)},
					{fmt.Sprint(sql.TestingFixedDescIDOffset), "0", "public", fmt.Sprint(sql.TestingFixedDescIDOffset + 1)},
					{fmt.Sprint(sql.TestingFixedDescIDOffset), fmt.Sprint(sql.TestingFixedDescIDOffset + 1), "testtable", fmt.Sprint(sql.TestingFixedDescIDOffset + 2)},
				})
		})
	}
}
