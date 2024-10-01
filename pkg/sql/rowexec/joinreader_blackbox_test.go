// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

// Check that the join reader uses bytes limits on its lookups.
func TestJoinReaderUsesBatchLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	recCh := make(chan tracingpb.Recording, 1)
	joinQuery := "SELECT count(1) FROM (SELECT * FROM test.b NATURAL INNER LOOKUP JOIN test.a)"
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				// Get a recording for the join query.
				WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
					if stmt == joinQuery {
						recCh <- trace
					}
				},
			},
			// Make the join's bytes limit artificially low so that we don't need too
			// big of a table to hit it.
			DistSQL: &execinfra.TestingKnobs{
				JoinReaderBatchBytesLimit: 1000,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// Disable the usage of the streamer since this test is designed for the old
	// non-streamer code path.
	// TODO(yuzefovich): remove the test altogether when the corresponding
	// cluster setting is removed (i.e. only the streamer code path remains).
	_, err := sqlDB.Exec("SET streamer_enabled = false;")
	require.NoError(t, err)

	// We're going to create a table with enough rows to exceed a batch's memory
	// limit. This table will represent the lookup side of a lookup join.
	const numRows = 50
	sqlutils.CreateTable(
		t,
		sqlDB,
		"a",
		"a INT, b int, PRIMARY KEY (a,b)",
		numRows,
		// rows will look like (1, <n>)
		sqlutils.ToRowFn(
			func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(1))
			},
			sqlutils.RowIdxFn,
		),
	)
	sqlutils.CreateTable(
		t,
		sqlDB,
		"b",
		"a INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(
			func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(1))
			}),
	)
	r := sqlDB.QueryRow(joinQuery)
	var rows int
	require.NoError(t, r.Scan(&rows))
	require.Equal(t, numRows, rows)

	// Look at the trace for the join and count how many (batch-)requests there
	// were on the lookup side. We expect more than one of them (it would be only
	// one if there was no limit on the size of results).
	rec := <-recCh
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "a")
	tableID := desc.TableDesc().ID
	sp, ok := rec.FindSpan("join reader")
	require.True(t, ok)
	require.Greater(t, tracing.CountLogMessages(sp, fmt.Sprintf("Scan /Table/%d", tableID)), 1)
}
