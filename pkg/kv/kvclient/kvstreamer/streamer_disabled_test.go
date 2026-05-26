// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStreamerDisabledWithInternalExecutorQuery verifies that the streamer is
// not used when the plan has a planNode that will use the internal executor. It
// also confirms that the streamer is not used for queries issued by that
// planNode.
func TestStreamerDisabledWithInternalExecutorQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Trace a query which has a lookup join on top of a scan of a virtual
	// table, with that virtual table being populated by a query issued via the
	// internal executor.
	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "COMMENT ON DATABASE defaultdb IS 'foo'")
	runner.Exec(t, "SET tracing = on")
	runner.Exec(t, `
SELECT
    c.*
FROM
    crdb_internal.jobs AS j
    INNER LOOKUP JOIN system.comments AS c ON c.type = (j.coordinator_id - 1)::INT8
WHERE
    j.coordinator_id = 1;
`)
	runner.Exec(t, "SET tracing = off")

	// Ensure that no streamer spans were created (meaning that the streamer
	// wasn't used, neither for the "outer" query nor for any "internal" ones).
	r := runner.QueryRow(t, "SELECT count(*) FROM [SHOW TRACE FOR SESSION] WHERE operation ILIKE '%streamer%'")
	var numStreamerSpans int
	r.Scan(&numStreamerSpans)
	require.Zero(t, numStreamerSpans)
}
