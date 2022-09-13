// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcevent

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestEventDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT, 
  b STRING, 
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL, 
  e status,
  PRIMARY KEY (b, a),
  FAMILY main (a, b, e),
  FAMILY only_c (c)
)`)

	tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	mainFamily := mustGetFamily(t, tableDesc, 0)
	cFamily := mustGetFamily(t, tableDesc, 1)

	for _, tc := range []struct {
		family          *descpb.ColumnFamilyDescriptor
		includeVirtual  bool
		expectedKeyCols []ResultColumn
		expectedColumns []ResultColumn
		expectedUDTCols []ResultColumn
	}{
		{
			family:          mainFamily,
			includeVirtual:  false,
			expectedKeyCols: expectResultColumns(t, tableDesc, "b", "a"),
			expectedColumns: expectResultColumns(t, tableDesc, "a", "b", "e"),
			expectedUDTCols: expectResultColumns(t, tableDesc, "e"),
		},
		{
			family:          mainFamily,
			includeVirtual:  true,
			expectedKeyCols: expectResultColumns(t, tableDesc, "b", "a"),
			expectedColumns: expectResultColumns(t, tableDesc, "a", "b", "d", "e"),
			expectedUDTCols: expectResultColumns(t, tableDesc, "e"),
		},
		{
			family:          cFamily,
			includeVirtual:  false,
			expectedKeyCols: expectResultColumns(t, tableDesc, "b", "a"),
			expectedColumns: expectResultColumns(t, tableDesc, "c"),
		},
		{
			family:          cFamily,
			includeVirtual:  true,
			expectedKeyCols: expectResultColumns(t, tableDesc, "b", "a"),
			expectedColumns: expectResultColumns(t, tableDesc, "c", "d"),
		},
	} {
		t.Run(fmt.Sprintf("%s/includeVirtual=%t", tc.family.Name, tc.includeVirtual), func(t *testing.T) {
			ed, err := NewEventDescriptor(tableDesc, tc.family, tc.includeVirtual, s.Clock().Now())
			require.NoError(t, err)

			// Verify Metadata information for event descriptor.
			require.Equal(t, tableDesc.GetID(), ed.TableID)
			require.Equal(t, tableDesc.GetName(), ed.TableName)
			require.Equal(t, tc.family.Name, ed.FamilyName)
			require.True(t, ed.HasOtherFamilies)

			// Verify primary key and family columns are as expected.
			r := Row{EventDescriptor: ed}
			require.Equal(t, tc.expectedKeyCols, slurpColumns(t, r.ForEachKeyColumn()))
			require.Equal(t, tc.expectedColumns, slurpColumns(t, r.ForEachColumn()))
			require.Equal(t, tc.expectedUDTCols, slurpColumns(t, r.ForEachUDTColumn()))
		})
	}
}

func TestEventDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT, 
  b STRING, 
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL, 
  e status DEFAULT 'inactive',
  PRIMARY KEY (b, a),
  FAMILY main (a, b, e),
  FAMILY only_c (c)
)`)

	tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), tableDesc)
	defer cleanup()

	type decodeExpectation struct {
		expectUnwatchedErr bool

		// current value expectations.
		deleted   bool
		keyValues []string
		allValues []string

		// previous value expectations.
		prevDeleted   bool
		prevAllValues []string
	}

	for _, tc := range []struct {
		testName          string
		familyName        string // Must be set if targetType ChangefeedTargetSpecification_COLUMN_FAMILY
		includeVirtual    bool
		actions           []string
		expectMainFamily  []decodeExpectation
		expectOnlyCFamily []decodeExpectation
	}{
		{
			testName:   "main/primary_cols",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, 'first test')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"first test", "1"},
					allValues:   []string{"1", "first test", "inactive"},
					prevDeleted: true,
				},
			},
		},
		{
			testName:       "main/primary_cols_with_virtual",
			familyName:     "main",
			actions:        []string{"INSERT INTO foo (a, b) VALUES (1, 'second test')"},
			includeVirtual: true,
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"second test", "1"},
					allValues:   []string{"1", "second test", "NULL", "inactive"},
					prevDeleted: true,
				},
			},
		},
		{
			testName:   "main/all_cols",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b, c, e) VALUES (1, 'third test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"third test", "1"},
					allValues:   []string{"1", "third test", "open"},
					prevDeleted: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
		},
		{
			testName:   "c_family/all_cols",
			familyName: "only_c",
			actions:    []string{"INSERT INTO foo (a, b, c, e) VALUES (1, 'fourth test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					keyValues:   []string{"fourth test", "1"},
					allValues:   []string{"c value"},
					prevDeleted: true,
				},
			},
		},
		{
			testName: "all_families/all_cols",
			actions:  []string{"INSERT INTO foo (a, b, c, e) VALUES (1, 'fifth test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"fifth test", "1"},
					allValues:   []string{"1", "fifth test", "open"},
					prevDeleted: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					keyValues:   []string{"fifth test", "1"},
					allValues:   []string{"c value"},
					prevDeleted: true,
				},
			},
		},
		{
			// This test verifies that altering an enum works (i.e. we successfully refresh type information).
			testName:   "main/alter_enum",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b, e) VALUES (1, '6th test',  'open')",
				"ALTER TYPE status ADD value 'review'",
				"UPDATE foo SET e = 'review' WHERE a=1 and b='6th test'",
			},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"6th test", "1"},
					allValues:   []string{"1", "6th test", "open"},
					prevDeleted: true,
				},
				{
					keyValues:     []string{"6th test", "1"},
					allValues:     []string{"1", "6th test", "review"},
					prevAllValues: []string{"1", "6th test", "open"},
				},
			},
		},
		{
			testName:   "main/update_and_delete",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b) VALUES (1, '7th test')",
				"UPDATE foo SET e = 'open' WHERE a = 1 and b = '7th test'",
				"DELETE FROM foo WHERE a = 1 and b = '7th test'",
			},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"7th test", "1"},
					allValues:   []string{"1", "7th test", "inactive"},
					prevDeleted: true,
				},
				{
					keyValues:     []string{"7th test", "1"},
					allValues:     []string{"1", "7th test", "open"},
					prevAllValues: []string{"1", "7th test", "inactive"},
				},
				{
					deleted:       true,
					keyValues:     []string{"7th test", "1"},
					prevAllValues: []string{"1", "7th test", "open"},
				},
			},
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			targetType := jobspb.ChangefeedTargetSpecification_EACH_FAMILY
			if tc.familyName != "" {
				targetType = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
			}

			for _, action := range tc.actions {
				sqlDB.Exec(t, action)
			}

			targets := changefeedbase.Targets{}
			targets.Add(changefeedbase.Target{
				Type:       targetType,
				TableID:    tableDesc.GetID(),
				FamilyName: tc.familyName,
			})
			serverCfg := s.DistSQLServer().(*distsql.ServerImpl).ServerConfig
			ctx := context.Background()
			decoder, err := NewEventDecoder(ctx, &serverCfg, targets, tc.includeVirtual)
			require.NoError(t, err)
			expectedEvents := len(tc.expectMainFamily) + len(tc.expectOnlyCFamily)
			for i := 0; i < expectedEvents; i++ {
				v := popRow(t)

				eventFamilyID, err := TestingGetFamilyIDFromKey(decoder, v.Key, v.Timestamp())
				require.NoError(t, err)

				var expect decodeExpectation
				if eventFamilyID == 0 {
					expect, tc.expectMainFamily = tc.expectMainFamily[0], tc.expectMainFamily[1:]
				} else {
					expect, tc.expectOnlyCFamily = tc.expectOnlyCFamily[0], tc.expectOnlyCFamily[1:]
				}
				updatedRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.Value}, v.Timestamp())

				if expect.expectUnwatchedErr {
					require.ErrorIs(t, err, ErrUnwatchedFamily)
					continue
				}

				require.NoError(t, err)
				require.True(t, updatedRow.IsInitialized())
				if expect.deleted {
					require.True(t, updatedRow.IsDeleted())
				} else {
					require.Equal(t, expect.keyValues, slurpDatums(t, updatedRow.ForEachKeyColumn()))
					require.Equal(t, expect.allValues, slurpDatums(t, updatedRow.ForEachColumn()))
				}

				prevRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.PrevValue}, v.Timestamp())
				require.NoError(t, err)

				// prevRow always has key columns initialized.
				require.Equal(t, expect.keyValues, slurpDatums(t, prevRow.ForEachKeyColumn()))

				if expect.prevDeleted {
					require.True(t, prevRow.IsDeleted())
				} else {
					require.Equal(t, expect.prevAllValues, slurpDatums(t, prevRow.ForEachColumn()))
				}
			}
		})
	}

}

func mustGetFamily(
	t *testing.T, desc catalog.TableDescriptor, familyID descpb.FamilyID,
) *descpb.ColumnFamilyDescriptor {
	t.Helper()
	f, err := desc.FindFamilyByID(familyID)
	require.NoError(t, err)
	return f
}

func expectResultColumns(
	t *testing.T, desc catalog.TableDescriptor, colNames ...string,
) (res []ResultColumn) {
	t.Helper()
	for _, colName := range colNames {
		col, err := desc.FindColumnWithName(tree.Name(colName))
		require.NoError(t, err)
		res = append(res, ResultColumn{
			ResultColumn: colinfo.ResultColumn{
				Name:           col.GetName(),
				Typ:            col.GetType(),
				TableID:        desc.GetID(),
				PGAttributeNum: uint32(col.GetPGAttributeNum()),
			},
			ord:       col.Ordinal(),
			sqlString: col.ColumnDesc().SQLStringNotHumanReadable(),
		})
	}
	return res
}

func slurpColumns(t *testing.T, it Iterator) (res []ResultColumn) {
	t.Helper()
	require.NoError(t,
		it.Col(func(col ResultColumn) error {
			res = append(res, col)
			return nil
		}))
	return res
}

func slurpDatums(t *testing.T, it Iterator) (res []string) {
	t.Helper()
	require.NoError(t,
		it.Datum(func(d tree.Datum, col ResultColumn) error {
			res = append(res, tree.AsStringWithFlags(d, tree.FmtExport))
			return nil
		}))
	return res
}

func TestMakeRowFromTuple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	i := tree.NewDInt(1234)
	f := tree.NewDFloat(12.34)
	s := tree.NewDString("testing")
	typ := types.MakeTuple([]*types.T{types.Int, types.Float, types.String})
	unlabeledTuple := tree.NewDTuple(typ, i, f, s)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	rowFromUnlabeledTuple := MakeRowFromTuple(&evalCtx, unlabeledTuple)
	expectedCols := []struct {
		name        string
		typ         *types.T
		valAsString string
	}{
		{name: "col1", typ: types.Int, valAsString: "1234"},
		{name: "col2", typ: types.Float, valAsString: "12.34"},
		{name: "col3", typ: types.String, valAsString: "testing"},
	}

	remainingCols := expectedCols

	require.NoError(t, rowFromUnlabeledTuple.ForEachColumn().Datum(func(d tree.Datum, col ResultColumn) error {
		current := remainingCols[0]
		remainingCols = remainingCols[1:]
		require.Equal(t, current.name, col.Name)
		require.Equal(t, current.typ, col.Typ)
		require.Equal(t, current.valAsString, tree.AsStringWithFlags(d, tree.FmtExport))
		return nil
	}))

	require.Empty(t, remainingCols)

	typ.InternalType.TupleLabels = []string{"a", "b", "c"}
	labeledTuple := tree.NewDTuple(typ, i, f, s)

	expectedCols[0].name = "a"
	expectedCols[1].name = "b"
	expectedCols[2].name = "c"

	remainingCols = expectedCols

	rowFromLabeledTuple := MakeRowFromTuple(&evalCtx, labeledTuple)

	require.NoError(t, rowFromLabeledTuple.ForEachColumn().Datum(func(d tree.Datum, col ResultColumn) error {
		current := remainingCols[0]
		remainingCols = remainingCols[1:]
		require.Equal(t, current.name, col.Name)
		require.Equal(t, current.typ, col.Typ)
		require.Equal(t, current.valAsString, tree.AsStringWithFlags(d, tree.FmtExport))
		return nil
	}))

}
