// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestClusterName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	checkName := func(t *testing.T, db *gosql.DB, expected string) {
		sql := sqlutils.MakeSQLRunner(db)
		sql.CheckQueryResults(t, `SHOW virtual_cluster_name`, [][]string{{expected}})
	}

	t.Run("system", func(t *testing.T) {
		checkName(t, db, "system")
	})

	t.Run("virtual", func(t *testing.T) {
		_, db2 := serverutils.StartTenant(t, s, base.TestTenantArgs{
			TenantID:   serverutils.TestTenantID(),
			TenantName: "virtual",
		})
		checkName(t, db2, "virtual")
	})
}
