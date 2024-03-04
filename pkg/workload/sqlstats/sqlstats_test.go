// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstats

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/stretchr/testify/require"
)

func TestSqlStatsWorkload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: defaultDbName})
	defer srv.Stopper().Stop(ctx)

	sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE `+defaultDbName)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `DROP TABLE IF EXISTS `+tableName)

	stats := workload.FromFlags(sqlStatsMeta)
	statsTable := stats.Tables()[0]
	sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, statsTable.Name, statsTable.Schema))

	gen := genPermutations()

	for i := 0; i < 10; i++ {
		query := gen.Next()
		_, err := db.Exec(query, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		require.NoError(t, err)
	}
}
