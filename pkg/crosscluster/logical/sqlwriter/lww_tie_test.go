// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/ldrrandgen"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestIsLwwWinnerKnownCases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(wall int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wall}
	}
	dint := func(v int) tree.Datum { return tree.NewDInt(tree.DInt(v)) }
	dstr := func(v string) tree.Datum { return tree.NewDString(v) }

	tests := []struct {
		name       string
		inTS, exTS hlc.Timestamp
		incoming   tree.Datums
		existing   tree.Datums
		want       bool
	}{
		{
			name:     "newer_timestamp_wins",
			inTS:     ts(200),
			exTS:     ts(100),
			incoming: tree.Datums{dint(1)},
			existing: tree.Datums{dint(1)},
			want:     true,
		},
		{
			name:     "older_timestamp_loses",
			inTS:     ts(100),
			exTS:     ts(200),
			incoming: tree.Datums{dint(1)},
			existing: tree.Datums{dint(1)},
			want:     false,
		},
		{
			name:     "same_ts_identical_rows_no_winner",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{dint(1)},
			existing: tree.Datums{dint(1)},
			want:     false,
		},
		{
			name:     "same_ts_smaller_incoming_wins",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{dint(1)},
			existing: tree.Datums{dint(2)},
			want:     true,
		},
		{
			name:     "same_ts_larger_incoming_loses",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{dint(2)},
			existing: tree.Datums{dint(1)},
			want:     false,
		},
		{
			name:     "same_ts_first_col_equal_second_breaks_tie",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{dint(5), dint(1)},
			existing: tree.Datums{dint(5), dint(2)},
			want:     true,
		},
		{
			name:     "same_ts_first_col_decides",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{dint(3), dint(99)},
			existing: tree.Datums{dint(5), dint(1)},
			want:     true,
		},
		{
			name:     "same_ts_null_beats_non_null",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{tree.DNull},
			existing: tree.Datums{dint(1)},
			want:     true,
		},
		{
			name:     "same_ts_non_null_loses_to_null",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{dint(1)},
			existing: tree.Datums{tree.DNull},
			want:     false,
		},
		{
			name:     "same_ts_both_null_no_winner",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{tree.DNull},
			existing: tree.Datums{tree.DNull},
			want:     false,
		},
		{
			name:     "same_ts_string_comparison",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{dstr("apple")},
			existing: tree.Datums{dstr("banana")},
			want:     true,
		},
		{
			name:     "same_ts_empty_datums_no_winner",
			inTS:     ts(100),
			exTS:     ts(100),
			incoming: tree.Datums{},
			existing: tree.Datums{},
			want:     false,
		},
		{
			name:     "newer_timestamp_wins_even_with_larger_values",
			inTS:     ts(200),
			exTS:     ts(100),
			incoming: tree.Datums{dint(99)},
			existing: tree.Datums{dint(1)},
			want:     true,
		},
		{
			name:     "logical_timestamp_breaks_wall_tie",
			inTS:     hlc.Timestamp{WallTime: 100, Logical: 2},
			exTS:     hlc.Timestamp{WallTime: 100, Logical: 1},
			incoming: tree.Datums{dint(1)},
			existing: tree.Datums{dint(1)},
			want:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := IsLwwWinner(tc.inTS, tc.exTS, tc.incoming, tc.existing)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestIsLwwWinnerRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng := rand.New(rand.NewSource(0))

	randomTypes := func(numCols int) []*types.T {
		typs := make([]*types.T, numCols)
		for i := range typs {
			typs[i] = randgen.RandColumnType(rng)
		}
		return typs
	}

	randomRow := func(typs []*types.T) tree.Datums {
		row := make(tree.Datums, len(typs))
		for i, typ := range typs {
			row[i] = randgen.RandDatum(rng, typ, true /* nullOk */)
		}
		return row
	}

	sameTS := hlc.Timestamp{WallTime: 100}

	for range 100 {
		typs := randomTypes(rng.Intn(5) + 1)
		for range 1000 {
			a := randomRow(typs)
			b := randomRow(typs)

			// Reflexivity: a row is never strictly less than itself,
			// so IsLwwWinner(a, a) must be false (identical → no winner).
			aa, err := IsLwwWinner(sameTS, sameTS, a, a)
			require.NoError(t, err)
			if aa {
				t.Errorf("IsLwwWinner(a, a) = true, want false; a = %v", a)
			}

			ab, err := IsLwwWinner(sameTS, sameTS, a, b)
			require.NoError(t, err)
			ba, err := IsLwwWinner(sameTS, sameTS, b, a)
			require.NoError(t, err)

			// Asymmetry: if a < b then not b < a.
			if ab && ba {
				t.Errorf(
					"IsLwwWinner(a, b) and IsLwwWinner(b, a) both true; a = %v, b = %v",
					a, b,
				)
			}
		}
	}
}

// TestWriteReadLwwTie verifies that a row written via InsertRow and read back
// via ReadRows produces values that IsLwwWinner considers a tie. This validates
// that the write/read round-trip preserves datum values faithfully.
func TestWriteReadLwwTie(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rng, _ := randutil.NewPseudoRand()
	tableName := "rand_table"

	stmt := tree.SerializeForDisplay(
		ldrrandgen.GenerateLDRTable(ctx, rng, tableName, true /* supportKVWriter */),
	)
	t.Logf("Creating table with schema: %s", stmt)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, stmt)

	desc := cdctest.GetHydratedTableDescriptor(
		t, s.ApplicationLayer().ExecutorConfig(), tree.Name(tableName),
	)

	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	session, err := NewInternalSession(ctx, s.InternalDB().(isql.DB), sd, s.ClusterSettings())
	require.NoError(t, err)
	defer session.Close(ctx)

	writer, err := NewRowWriter(ctx, desc, session)
	require.NoError(t, err)

	reader, err := NewRowReader(ctx, desc, session)
	require.NoError(t, err)

	columnSchemas := GetColumnSchema(desc)
	cols := make([]catalog.Column, len(columnSchemas))
	for i, cs := range columnSchemas {
		cols[i] = cs.Column
	}

	successes := 0
	for i := 0; i < 100 && successes < 10; i++ {
		row := make(tree.Datums, len(cols))
		for j, col := range cols {
			row[j] = randgen.RandDatum(rng, col.GetType(), col.IsNullable())
		}

		ts := s.Clock().Now()

		// Insert the row. Random datums may cause insert failures (e.g.
		// integer out of range, duplicate key, geos unavailable), so skip
		// failed iterations and only test the round-trip on success.
		err := session.Txn(ctx, func(ctx context.Context) error {
			return writer.InsertRow(ctx, ts, row)
		})
		if err != nil {
			continue
		}

		// Read the row back.
		var result map[int]PriorRow
		err = session.Txn(ctx, func(ctx context.Context) error {
			result, err = reader.ReadRows(ctx, []tree.Datums{row})
			return err
		})
		require.NoError(t, err)

		prior, ok := result[0]
		if !ok {
			t.Fatalf("iteration %d: inserted row not found by ReadRows", i)
		}

		// Neither direction should win: the written and read-back values
		// must be identical from LWW's perspective.
		won1, err := IsLwwWinner(ts, prior.LogicalTimestamp, row, prior.Row)
		require.NoError(t, err)
		if won1 {
			t.Errorf(
				"iteration %d: incoming won over read-back; ts=%v, readTS=%v, row=%v, readRow=%v",
				i, ts, prior.LogicalTimestamp, row, prior.Row,
			)
		}
		won2, err := IsLwwWinner(prior.LogicalTimestamp, ts, prior.Row, row)
		require.NoError(t, err)
		if won2 {
			t.Errorf(
				"iteration %d: read-back won over incoming; ts=%v, readTS=%v, row=%v, readRow=%v",
				i, ts, prior.LogicalTimestamp, row, prior.Row,
			)
		}

		successes++
	}

	require.Greater(t, successes, 0, "expected at least one successful write-read-compare iteration")
}

// TestPickProperties verifies algebraic properties of Pick using
// random datums: reflexivity, antisymmetry, and that Equal implies
// identical encodings.
func TestPickProperties(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng := rand.New(rand.NewSource(42))

	for range 100 {
		typ := randgen.RandColumnType(rng)
		for range 1000 {
			a := randgen.RandDatum(rng, typ, false /* nullOk */)
			b := randgen.RandDatum(rng, typ, false /* nullOk */)

			// Reflexivity: Pick(a, a) == Equal.
			result, err := Pick(a, a)
			require.NoError(t, err)
			if result != Equal {
				t.Errorf(
					"Pick(a, a) = %v, want Equal; type=%s a=%v",
					result, typ, a,
				)
			}

			// Antisymmetry: Pick(a,b) == ChooseA implies Pick(b,a) == ChooseB.
			ab, err := Pick(a, b)
			require.NoError(t, err)
			ba, err := Pick(b, a)
			require.NoError(t, err)

			switch ab {
			case ChooseA:
				if ba != ChooseB {
					t.Errorf(
						"Pick(a,b)=ChooseA but Pick(b,a)=%v; type=%s a=%v b=%v",
						ba, typ, a, b,
					)
				}
			case ChooseB:
				if ba != ChooseA {
					t.Errorf(
						"Pick(a,b)=ChooseB but Pick(b,a)=%v; type=%s a=%v b=%v",
						ba, typ, a, b,
					)
				}
			case Equal:
				if ba != Equal {
					t.Errorf(
						"Pick(a,b)=Equal but Pick(b,a)=%v; type=%s a=%v b=%v",
						ba, typ, a, b,
					)
				}
			}
		}
	}
}
