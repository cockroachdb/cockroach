// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestCheckExternalConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "CREATE EXTERNAL CONNECTION foo_conn AS 'nodelocal://1/foo';")
	query := "CHECK EXTERNAL CONNECTION 'nodelocal://1/foo';"
	// Should execute successfully without a statement timeout.
	runner.Exec(t, query)
	// Run with a random statement timeout which will likely make the query
	// fail. We don't care whether it does nor which error is returned as long
	// as the process doesn't crash.
	runner.Exec(t, fmt.Sprintf("SET statement_timeout='%dms'", rng.Intn(100)+1))
	_, _ = sqlDB.Exec(query)
}
