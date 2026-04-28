// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// writeFixtureLog uses Driver to populate dir with a small revlog:
// two closed ticks with a couple of events each.
func writeFixtureLog(t *testing.T, dir string) {
	t.Helper()
	es := nodelocal.TestingMakeNodelocalStorage(
		dir, cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
	t.Cleanup(func() { _ = es.Close() })

	ctx := context.Background()
	d, err := revlogjob.NewDriver(es, []roachpb.Span{allSpan},
		ts(100), testTickWidth, &seqFileIDs{}, revlogjob.ResumeState{})
	require.NoError(t, err)

	d.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v_a"), nil)
	d.OnValue(ctx, roachpb.Key("b"), ts(107), []byte("v_b"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(115)))

	d.OnValue(ctx, roachpb.Key("c"), ts(118), []byte("v_c"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(125)))
}

// TestRevlogShowTicksSurfacesWrittenTicks verifies the SQL builtin
// returns one row per closed tick when pointed at a directory the
// writer just populated. This is the first end-to-end exercise of
// the inspection-builtin SQL surface; previously the function was
// registered but no test confirmed it returned anything useful for
// real revlog content.
func TestRevlogShowTicksSurfacesWrittenTicks(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	extIODir := t.TempDir()
	srv, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: extIODir,
	})
	t.Cleanup(func() { srv.Stopper().Stop(ctx) })

	const subdir = "fixture"
	writeFixtureLog(t, filepath.Join(extIODir, subdir))

	sqlDB := sqlutils.MakeSQLRunner(sqlConn)
	sqlDB.Exec(t, `GRANT SYSTEM REPAIRCLUSTER TO root`)

	// Two ticks were written; expect ≥2 rows.
	rows := sqlDB.QueryStr(t,
		`SELECT count(*)::INT8 FROM crdb_internal.revlog_show_ticks($1)`,
		"nodelocal://1/"+subdir,
	)
	require.Equal(t, [][]string{{"2"}}, rows,
		"expected exactly 2 ticks in fixture")
}

// TestRevlogShowChangesReturnsKeyValueRows verifies the
// revlog_show_changes SQL builtin returns one row per event in the
// requested tick. Uses the same fixture as TestRevlogShowTicks
// above so the two builtins are tested against the same on-disk
// content.
//
// Fixture ticks end at 110s and 120s (tickWidth=10s, startHLC=100s).
// Tick (100, 110] contains 2 events (a, b); tick (110, 120] contains
// 1 event (c).
func TestRevlogShowChangesReturnsKeyValueRows(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	extIODir := t.TempDir()
	srv, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: extIODir,
	})
	t.Cleanup(func() { srv.Stopper().Stop(ctx) })

	const subdir = "fixture"
	writeFixtureLog(t, filepath.Join(extIODir, subdir))

	sqlDB := sqlutils.MakeSQLRunner(sqlConn)
	sqlDB.Exec(t, `GRANT SYSTEM REPAIRCLUSTER TO root`)

	// Tick ending at 110s (= '1970-01-01 00:01:50+00') has 2 events.
	rows := sqlDB.QueryStr(t,
		`SELECT count(*)::INT8 FROM crdb_internal.revlog_show_changes($1, $2::TIMESTAMPTZ)`,
		"nodelocal://1/"+subdir,
		"1970-01-01 00:01:50+00",
	)
	require.Equal(t, [][]string{{"2"}}, rows)

	// Tick ending at 120s has 1 event.
	rows = sqlDB.QueryStr(t,
		`SELECT count(*)::INT8 FROM crdb_internal.revlog_show_changes($1, $2::TIMESTAMPTZ)`,
		"nodelocal://1/"+subdir,
		"1970-01-01 00:02:00+00",
	)
	require.Equal(t, [][]string{{"1"}}, rows)
}
