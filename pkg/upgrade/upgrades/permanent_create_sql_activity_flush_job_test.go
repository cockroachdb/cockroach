// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateSqlStatsFlushJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)
	conn := sqlutils.MakeSQLRunner(db)

	row := conn.QueryRow(t,
		fmt.Sprintf("SELECT count(*) FROM system.public.jobs WHERE id = %d", jobs.SqlActivityFlushJobID))
	require.NotNil(t, row)
	var count int
	row.Scan(&count)
	require.Equal(t, 1, count)
}
