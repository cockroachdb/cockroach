// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestExternalHashAggregatorReusesBuckets verifies that the external hash
// aggregator doesn't hit --max-sql-memory limit when it needs to process many
// buckets (by reusing the same buckets on different partitions).
//
// Note that this test lives in colexec_test package because it needs
// serverutils package (which would create import cycle with colexec).
func TestExternalHashAggregatorReusesBuckets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "takes too long")
	skip.UnderRace(t, "takes too long")
	skip.UnderMetamorphic(t, "takes too long")
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)

	const maxSQLMemoryLimit = 16 << 20
	const workmemLimit = maxSQLMemoryLimit / 4
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: maxSQLMemoryLimit,
	})
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec(`
SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false;
CREATE DATABASE foo;
CREATE TABLE foo.kv (k INT PRIMARY KEY, v BYTES);
`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`SET distsql_workmem=$1;`, fmt.Sprintf("%dB", workmemLimit))
	require.NoError(t, err)

	// Insert many rows with unique values - this is needed so that there were
	// many groups during the aggregation and that we could split them up into
	// separate partitions.
	const rowSize = 100
	const numRows = maxSQLMemoryLimit / rowSize
	log.Infof(context.Background(), "inserting %d rows", numRows)
	_, err = sqlDB.Exec(
		`INSERT INTO foo.kv SELECT i, repeat('a', $1)::BYTES || i::STRING::BYTES FROM generate_series(1, $2) AS g(i)`,
		rowSize, numRows,
	)
	require.NoError(t, err)

	// Run the aggregation. Note that we're requesting the very last row in the
	// output so that the external hash aggregator doesn't short-circuit because
	// of limit and processes all partitions.
	log.Info(context.Background(), "running aggregation")
	_, err = sqlDB.Exec(`SELECT v, count(k) FROM foo.kv GROUP BY v OFFSET $1 LIMIT 1;`, numRows-1)
	require.NoError(t, err)
}
