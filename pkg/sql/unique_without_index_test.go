// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestUWIConstraintReferencingTypes tests that adding/dropping
// unique without index constraints that reference other type descriptors
// properly adds/drops back-references in the type descriptor.
func TestUWIConstraintReferencingTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "use-declarative-schema-changer", func(
		t *testing.T, useDeclarativeSchemaChanger bool,
	) {
		s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		if useDeclarativeSchemaChanger {
			tdb.Exec(t, "SET use_declarative_schema_changer = on;")
		} else {
			tdb.Exec(t, "SET use_declarative_schema_changer = off;")
		}
		tdb.Exec(t, "SET experimental_enable_unique_without_index_constraints = true;")
		tdb.Exec(t, "CREATE TYPE typ AS ENUM ('a', 'b');")
		tdb.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY, j STRING);")
		tdb.Exec(t, "ALTER TABLE t ADD UNIQUE WITHOUT INDEX (j) WHERE (j::typ != 'a');")

		// Ensure that `typ` has a back-reference to table `t`.
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec,
			"defaultdb", "t")
		typDesc := desctestutils.TestingGetPublicTypeDescriptor(kvDB, keys.SystemSQLCodec,
			"defaultdb", "typ")
		require.Equal(t, 1, typDesc.NumReferencingDescriptors())
		require.Equal(t, tableDesc.GetID(), typDesc.GetReferencingDescriptorID(0))

		// Ensure that dropping `typ` fails because `typ` is referenced by the constraint.
		tdb.ExpectErr(t, `pq: cannot drop type "typ" because other objects \(\[defaultdb.public.t\]\) still depend on it`, "DROP TYPE typ")

		// Ensure that dropping the constraint removes the back-reference from `typ`.
		tdb.Exec(t, "ALTER TABLE t DROP CONSTRAINT unique_j")
		typDesc = desctestutils.TestingGetPublicTypeDescriptor(kvDB, keys.SystemSQLCodec,
			"defaultdb", "typ")
		require.Zero(t, typDesc.NumReferencingDescriptors())

		// Ensure that now we can succeed dropping `typ`.
		tdb.Exec(t, "DROP TYPE typ")
	})
}

// Test adding a unique check to `insertFastPathRun` and verifying the rows
// to check.
func TestAddUniqChecks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData().EnableUniqueWithoutIndexConstraints = true

	catalog := testcat.New()
	if _, err := catalog.ExecuteDDL(
		"CREATE TABLE t1 (a INT, b INT, c INT, d INT, e INT, INDEX(a,b,c), UNIQUE WITHOUT INDEX(b))",
	); err != nil {
		t.Fatal(err)
	}

	var mem memo.Memo
	mem.Init(ctx, &evalCtx)
	tn := tree.NewUnqualifiedTableName("t1")
	tab := catalog.Table(tn)
	tabID := mem.Metadata().AddTable(tab, tn)
	referencedTable := tab
	referencedIndex := referencedTable.Index(1)
	out := exec.InsertFastPathCheck{ReferencedTable: tab, ReferencedIndex: referencedIndex}

	// Test that builds the DatumsFromConstraint element of
	// `exec.InsertFastPathCheck` and checks the output of calling
	// `addUniqChecks`.
	buildDatumsFromConstraintAndVerifyAddedUniqueChecks := func(c constraint.Constraint, expectedError string) {
		if c.Spans.Count() == 0 {
			if expectedError == "Index constraint with no spans" {
				return
			}
			t.Fatalf("Index constraint with no spans")
		}
		// Set up InsertCols with the index key columns.
		numKeyCols := c.Spans.Get(0).StartKey().Length()
		out.InsertCols = make([]exec.TableColumnOrdinal, numKeyCols)
		for j := 0; j < numKeyCols; j++ {
			column := referencedIndex.Column(j)
			ord := column.Ordinal()
			if c.Columns.Get(j).ID() != tabID.ColumnID(ord) {
				if expectedError == "Constraint column does not match index column" {
					return
				}
				t.Fatalf("Constraint column does not match index column")
			}
			out.InsertCols[j] = exec.TableColumnOrdinal(ord)
		}
		// The number of KV requests will match the number of spans.
		out.DatumsFromConstraint = make([]tree.Datums, c.Spans.Count())
		for j := 0; j < c.Spans.Count(); j++ {
			// DatumsFromConstraint is indexed by table column ordinal, so build
			// a slice which is large enough for any column.
			out.DatumsFromConstraint[j] = make(tree.Datums, tab.ColumnCount())
			span := c.Spans.Get(j)
			// The following 2 checks mimic ones found in a subsequent commit in
			// insert_funcs.go.
			// Verify there is a single key...
			if span.Prefix(ctx, &evalCtx) != span.StartKey().Length() {
				if expectedError == "More than one key found" {
					return
				}
				t.Fatalf("More than one key found")
			}
			// ... and that the span has the same number of columns as the index key.
			if span.StartKey().Length() != numKeyCols {
				if expectedError == "Wrong number of span keys found" {
					return
				}
				t.Fatalf("Wrong number of span keys found")
			}
			for k := 0; k < span.StartKey().Length(); k++ {
				// Get the key column's table column ordinal.
				ord := out.InsertCols[k]
				// Populate DatumsFromConstraint with that key column value.
				out.DatumsFromConstraint[j][ord] = span.StartKey().Value(k)
			}
		}
		if expectedError != "" {
			t.Fatalf("expected error in DatumsFromConstraint building, but none found")
		}

		uniqChecks := []insertFastPathCheck{{InsertFastPathCheck: out}}
		insFPRun := insertFastPathRun{uniqChecks: uniqChecks}

		inputRow := make(tree.Datums, 8)
		for k := range out.DatumsFromConstraint {
			hasNull := false
			for i := range inputRow {
				if out.DatumsFromConstraint[k][i] != nil {
					inputRow[i] = out.DatumsFromConstraint[k][i]
					if inputRow[i] == tree.DNull {
						hasNull = true
					}
				} else {
					inputRow[i] = tree.NewDInt(tree.DInt(i))
				}
			}

			resultRows, err := insFPRun.addUniqChecks(ctx, 0, inputRow, true /* forTesting */)
			if err != nil {
				panic(err)
			}
			if hasNull {
				if len(resultRows) != 0 {
					t.Fatalf("expected resultRows length with a null to be zero, found %d", len(resultRows))
				}
				continue
			}
			if len(out.DatumsFromConstraint) != len(resultRows) {
				t.Fatalf("expected DatumsFromConstraint length, %d, to match resultRows length, %d", len(out.DatumsFromConstraint), len(resultRows))
			}
			for i, row := range resultRows {
				if len(row) != len(out.DatumsFromConstraint[i]) {
					t.Fatalf("expected DatumsFromConstraint item length, %d, to match result row length, %d", len(out.DatumsFromConstraint[i]), len(row))
				}
				for j, datum := range row {
					if datum == nil && out.DatumsFromConstraint[i][j] != nil {
						t.Fatalf("expected row datum to be non-nil, matching %v", out.DatumsFromConstraint[i][j])
					}
					if datum != nil && out.DatumsFromConstraint[i][j] == nil {
						t.Fatalf("expected row datum to be nil, got %v", datum)
					}
					if datum == out.DatumsFromConstraint[i][j] {
						continue
					}
					if cmp, err := datum.Compare(ctx, &evalCtx, out.DatumsFromConstraint[i][j]); err != nil {
						t.Fatal(err)
					} else if cmp != 0 {
						t.Fatalf("expected built row datum, %v, to match DatumsFromConstraint item, %v", datum, out.DatumsFromConstraint[i][j])
					}
				}
			}
		}
	}

	type testCase struct {
		constraint    string
		expectedError string
	}
	cases := []testCase{
		{`/1/2: [/10/2 - /10/2] [/11/2 - /11/2]`, ""},
		{`/1/2: [/10/2 - /10/2] [/11/NULL - /11/NULL]`, ""},
		{`/1/2/3: [/10/2/1 - /10/2/1] [/11/4/3 - /11/4/3]`, ""},
		{`/1/2/3: [/10/2/1 - /10/2/1] [/11/NULL/3 - /11/NULL/3]`, ""},
		{`/1/2/3: [/10/2/1 - /10/2/1] [/11/NULL/3 - /11/5/3]`, "More than one key found"},
		{`/1/2/3: [/10/2/1 - /10/2/1] [/11/NULL/3/7 - /11/NULL/3/7]`, "Wrong number of span keys found"},
		{`/1: [/10 - /10] [/11 - /11] [/12 - /12] [/13 - /13] [/14 - /14] [/15 - /15]`, ""},
		{`/1/2: [/10/NULL - /10/NULL] [/NULL/11 - /NULL/11] [/NULL/12 - /NULL/12] [/13/NULL - /13/NULL]`, ""},
		{"/1/2/3/4: [ - /10/40] [/15/20 - /15/20/10] [/15/30 - /15/30] [/15/40 - /15/40] " +
			"[/30/20 - /40) [/80/20 - ]", "Wrong number of span keys found"},
		{"/1/2: contradiction", "Index constraint with no spans"},
		{"/2: [/8 - /8]", "Constraint column does not match index column"},
		{"/1/3: [/8/5 - /8/5]", "Constraint column does not match index column"},
	}
	for _, tc := range cases {
		buildDatumsFromConstraintAndVerifyAddedUniqueChecks(constraint.ParseConstraint(&evalCtx, tc.constraint), tc.expectedError)
	}
}
