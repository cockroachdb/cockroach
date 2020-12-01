// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func BenchmarkSequenceIncrement(b *testing.B) {
	cluster := serverutils.StartNewTestCluster(b, 3, base.TestClusterArgs{})
	defer cluster.Stopper().Stop(context.Background())

	sqlDB := cluster.ServerConn(0)

	if _, err := sqlDB.Exec(`
		CREATE DATABASE test;
		USE test;
		CREATE SEQUENCE seq;
		CREATE TABLE tbl (
			id INT PRIMARY KEY DEFAULT nextval('seq'),
			foo text
		);
	`); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := sqlDB.Exec("INSERT INTO tbl (foo) VALUES ('foo')"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkUniqueRowID(b *testing.B) {
	cluster := serverutils.StartNewTestCluster(b, 3, base.TestClusterArgs{})
	defer cluster.Stopper().Stop(context.Background())

	sqlDB := cluster.ServerConn(0)

	if _, err := sqlDB.Exec(`
		CREATE DATABASE test;
		USE test;
		CREATE TABLE tbl (
			id INT PRIMARY KEY DEFAULT unique_rowid(),
			foo text
		);
	`); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := sqlDB.Exec("INSERT INTO tbl (foo) VALUES ('foo')"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Regression test for #50711. The root cause of #50711 was the fact that a
// sequenceID popped up in multiple columns' column descriptor. This test
// inspects the table descriptor to ascertain that sequence ownership integrity
// is preserved in various scenarios.
// Scenarios tested:
// - ownership swaps between table columns
// - two sequences being owned simultaneously
// - sequence drops
// - ownership removal
func TestSequenceOwnershipDependencies(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, sqlConn, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlConn.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test(a INT PRIMARY KEY, b INT)`); err != nil {
		t.Fatal(err)
	}

	// Switch ownership between columns of the same table.
	if _, err := sqlConn.Exec("CREATE SEQUENCE t.seq1 OWNED BY t.test.a"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, []string{"seq1"})
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, nil /* seqNames */)

	if _, err := sqlConn.Exec("ALTER SEQUENCE t.seq1 OWNED BY t.test.b"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, nil /* seqNames */)
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, []string{"seq1"})

	if _, err := sqlConn.Exec("ALTER SEQUENCE t.seq1 OWNED BY t.test.a"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, []string{"seq1"})
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, nil /* seqNames */)

	// Add a second sequence in the mix and switch its ownership.
	if _, err := sqlConn.Exec("CREATE SEQUENCE t.seq2 OWNED BY t.test.a"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, []string{"seq1", "seq2"})
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, nil /* seqNames */)

	if _, err := sqlConn.Exec("ALTER SEQUENCE t.seq2 OWNED BY t.test.b"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, []string{"seq1"})
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, []string{"seq2"})

	if _, err := sqlConn.Exec("ALTER SEQUENCE t.seq2 OWNED BY t.test.a"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, []string{"seq1", "seq2"})
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, nil /* seqNames */)

	// Ensure dropping sequences removes the ownership dependencies.
	if _, err := sqlConn.Exec("DROP SEQUENCE t.seq1"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, []string{"seq2"})
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, nil /* seqNames */)

	// Ensure removing an owner removes the ownership dependency.
	if _, err := sqlConn.Exec("ALTER SEQUENCE t.seq2 OWNED BY NONE"); err != nil {
		t.Fatal(err)
	}
	assertColumnOwnsSequences(t, kvDB, "t", "test", 0 /* colIdx */, nil /* seqNames */)
	assertColumnOwnsSequences(t, kvDB, "t", "test", 1 /* colIdx */, nil /* seqNames */)
}

// assertColumnOwnsSequences ensures that the column at (DbName, tbName, colIdx)
// owns all the sequences passed to it (in order) by looking up descriptors in
// kvDB.
func assertColumnOwnsSequences(
	t *testing.T, kvDB *kv.DB, dbName string, tbName string, colIdx int, seqNames []string,
) {
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, tbName)
	col := tableDesc.GetColumns()[colIdx]
	var seqDescs []*tabledesc.Immutable
	for _, seqName := range seqNames {
		seqDescs = append(
			seqDescs,
			catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, seqName),
		)
	}

	if len(col.OwnsSequenceIds) != len(seqDescs) {
		t.Fatalf(
			"unexpected number of sequence ownership dependencies. expected: %d, got:%d",
			len(seqDescs), len(col.OwnsSequenceIds),
		)
	}

	for i, seqID := range col.OwnsSequenceIds {
		if seqID != seqDescs[i].GetID() {
			t.Fatalf("unexpected sequence id. expected %d got %d", seqDescs[i].GetID(), seqID)
		}

		ownerTableID := seqDescs[i].SequenceOpts.SequenceOwner.OwnerTableID
		ownerColID := seqDescs[i].SequenceOpts.SequenceOwner.OwnerColumnID
		if ownerTableID != tableDesc.GetID() || ownerColID != col.ID {
			t.Fatalf(
				"unexpected sequence owner. expected table id %d, got: %d; expected column id %d, got :%d",
				tableDesc.GetID(), ownerTableID, col.ID, ownerColID,
			)
		}
	}
}

// Tests for allowing drops on sequence descriptors in a bad state due to
// ownership bugs. It should be possible to drop tables/sequences that have
// descriptors in an invalid state. See tracking issue #51770 for more details.
// Relevant sub-issues are referenced in test names/inline comments.
func TestInvalidOwnedDescriptorsAreDroppable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name string
		test func(*testing.T, *kv.DB, *sqlutils.SQLRunner)
	}{
		// Tests simulating #50711 by breaking the invariant that sequences are owned
		// by at most one column at a time.

		// Dropping the table should work when the table descriptor is in an invalid
		// state. The owned sequence should also be dropped.
		{
			name: "#50711 drop table",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "seq")

				sqlDB.Exec(t, "DROP TABLE t.test")
				// The sequence should have been dropped as well.
				sqlDB.ExpectErr(t, `pq: relation "t.seq" does not exist`, "SELECT * FROM t.seq")
				// The valid sequence should have also been dropped.
				sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
			},
		},
		{
			name: "#50711 drop sequence followed by drop table",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "seq")

				sqlDB.Exec(t, "DROP SEQUENCE t.seq")
				sqlDB.Exec(t, "SELECT * FROM t.valid_seq")
				sqlDB.Exec(t, "DROP TABLE t.test")

				// The valid sequence should have also been dropped.
				sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
			},
		},
		{
			// This test invalidates both seq and useq as DROP DATABASE CASCADE operates
			// on objects lexicographically -- owned sequences can be dropped both as a
			// regular sequence drop and as a side effect of the owner table being dropped.
			name: "#50711 drop database cascade",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "seq")

				addOwnedSequence(t, kvDB, "t", "test", 0, "useq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "useq")

				sqlDB.Exec(t, "DROP DATABASE t CASCADE")
			},
		},

		// Tests simulating #50781 by modifying the sequence's owner to a table that
		// doesn't exist and column's `ownsSequenceIDs` to sequences that don't exist.

		{
			name: "#50781 drop table followed by drop sequence",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				breakOwnershipMapping(t, kvDB, "t", "test", "seq")

				sqlDB.Exec(t, "DROP TABLE t.test")
				// The valid sequence should have also been dropped.
				sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
				sqlDB.Exec(t, "DROP SEQUENCE t.seq")
			},
		},
		{
			name: "#50781 drop sequence followed by drop table",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				breakOwnershipMapping(t, kvDB, "t", "test", "seq")

				sqlDB.Exec(t, "DROP SEQUENCE t.seq")
				sqlDB.Exec(t, "DROP TABLE t.test")
				// The valid sequence should have also been dropped.
				sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
			},
		},

		// This test invalidates both seq and useq as DROP DATABASE CASCADE operates
		// on objects lexicographically -- owned sequences can be dropped both as a
		// regular sequence drop and as a side effect of the owner table being dropped.
		{
			name: "#50781 drop database cascade",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				breakOwnershipMapping(t, kvDB, "t", "test", "seq")
				breakOwnershipMapping(t, kvDB, "t", "test", "useq")
				sqlDB.Exec(t, "DROP DATABASE t CASCADE")
			},
		},
		{
			name: "combined #50711 #50781 drop table followed by sequence",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "seq")
				breakOwnershipMapping(t, kvDB, "t", "test", "seq")

				sqlDB.Exec(t, "DROP TABLE t.test")
				// The valid sequence should have also been dropped.
				sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
				sqlDB.Exec(t, "DROP SEQUENCE t.seq")
			},
		},
		{
			name: "combined #50711 #50781 drop sequence followed by table",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "seq")
				breakOwnershipMapping(t, kvDB, "t", "test", "seq")

				sqlDB.Exec(t, "DROP SEQUENCE t.seq")
				sqlDB.Exec(t, "DROP TABLE t.test")
				// The valid sequence should have also been dropped.
				sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
			},
		},
		// This test invalidates both seq and useq as DROP DATABASE CASCADE operates
		// on objects lexicographically -- owned sequences can be dropped both as a
		// regular sequence drop and as a side effect of the owner table being dropped.
		{
			name: "combined #50711 #50781 drop database cascade",
			test: func(t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner) {
				addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "seq")
				breakOwnershipMapping(t, kvDB, "t", "test", "seq")

				addOwnedSequence(t, kvDB, "t", "test", 0, "useq")
				addOwnedSequence(t, kvDB, "t", "test", 1, "useq")
				breakOwnershipMapping(t, kvDB, "t", "test", "useq")

				sqlDB.Exec(t, "DROP DATABASE t CASCADE")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			params := base.TestServerArgs{}
			s, sqlConn, kvDB := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)
			sqlDB := sqlutils.MakeSQLRunner(sqlConn)
			sqlDB.Exec(t, `CREATE DATABASE t;
CREATE TABLE t.test(a INT PRIMARY KEY, b INT);
CREATE SEQUENCE t.seq OWNED BY t.test.a;
CREATE SEQUENCE t.useq OWNED BY t.test.a;
CREATE SEQUENCE t.valid_seq OWNED BY t.test.a`)

			tc.test(t, kvDB, sqlDB)
		})
	}
}

// addOwnedSequence adds the sequence referenced by seqName to the
// ownsSequenceIDs of the column referenced by (dbName, tableName, colIdx).
func addOwnedSequence(
	t *testing.T, kvDB *kv.DB, dbName string, tableName string, colIdx int, seqName string,
) {
	seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, seqName)
	tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, dbName, tableName)

	tableDesc.GetColumns()[colIdx].OwnsSequenceIds = append(
		tableDesc.GetColumns()[colIdx].OwnsSequenceIds, seqDesc.ID)

	err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	)
	require.NoError(t, err)
}

// breakOwnershipMapping simulates #50781 by setting the sequence's owner table
// to a non-existent tableID and setting the column's `ownsSequenceID` to a
// non-existent sequenceID.
func breakOwnershipMapping(
	t *testing.T, kvDB *kv.DB, dbName string, tableName string, seqName string,
) {
	seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, seqName)
	tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, dbName, tableName)

	for colIdx := range tableDesc.GetColumns() {
		for i := range tableDesc.GetColumns()[colIdx].OwnsSequenceIds {
			if tableDesc.GetColumns()[colIdx].OwnsSequenceIds[i] == seqDesc.ID {
				tableDesc.GetColumns()[colIdx].OwnsSequenceIds[i] = math.MaxInt32
			}
		}
	}
	seqDesc.SequenceOpts.SequenceOwner.OwnerTableID = math.MaxInt32

	err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	)
	require.NoError(t, err)

	err = kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, seqDesc.GetID()),
		seqDesc.DescriptorProto(),
	)
	require.NoError(t, err)
}

func TestCreateSequenceAsOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDBRaw, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		name        string
		data        string
		verifyQuery string
		err         string
		expected    [][]string
	}{
		{
			name: "as integer",
			data: `
				CREATE SEQUENCE public.a_seq AS integer
				START WITH 2
				INCREMENT BY 1
				MINVALUE 0
				MAXVALUE 234567
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE 0 MAXVALUE 234567 INCREMENT 1 START 2`,
			}},
		},
		{
			name: "as integer desc",
			data: `
				CREATE SEQUENCE public.a_seq AS integer
				START WITH -20000 
				INCREMENT BY -1
				MINVALUE -20000
				MAXVALUE 0
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE -20000 MAXVALUE 0 INCREMENT -1 START -20000`,
			}},
		},
		{
			name: "as bigint",
			data: `
				CREATE SEQUENCE public.a_seq AS bigint
				START WITH 1
				INCREMENT BY 1
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
			}},
		},
		{
			name: "as smallint",
			data: `
				CREATE SEQUENCE public.a_seq AS smallint
				START WITH 2
				INCREMENT BY 3
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int2 MINVALUE 1 MAXVALUE 32767 INCREMENT 3 START 2`,
			}},
		},
		{
			name: "as smallint desc",
			data: `
				CREATE SEQUENCE public.a_seq AS smallint
				START WITH -4
				INCREMENT BY -3`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int2 MINVALUE -32768 MAXVALUE -1 INCREMENT -3 START -4`,
			}},
		},
		{
			name: "start value out of bounds",
			data: `
				CREATE SEQUENCE public.a_seq AS smallint
				START WITH 45678
				INCREMENT BY 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			err:         `START value`,
		},
		{
			name: "invalid syntax int2",
			data: `
				CREATE SEQUENCE public.a_seq
				AS int2
				START WITH 1
				INCREMENT BY 1
				CACHE 1;`,
			err: `syntax error`,
		},
		{
			name: "min out of inttype bounds",
			data: `
				CREATE SEQUENCE public.a_seq
				AS smallint
				START WITH 1
				INCREMENT BY 1
				MINVALUE -1000000
				CACHE 1;`,
			err: `must be greater than`,
		},
		{
			name: "max out of inttype bounds",
			data: `
				CREATE SEQUENCE public.a_seq
				AS smallint
				START WITH 1
				INCREMENT BY 1
				MAXVALUE 123456
				CACHE 1;`,
			err: `must be less than`,
		},
		{
			name: `MAXINT overrides integer type default max`,
			data: `CREATE SEQUENCE public.a_seq
						AS integer
						START WITH 1
						INCREMENT BY 1
						MAXVALUE 9001
						CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE 1 MAXVALUE 9001 INCREMENT 1 START 1`,
			}},
		},
	}
	for _, test := range tests {

		t.Run(test.name, func(t *testing.T) {
			// Set up clean testing enviornment.
			sqlDB.Exec(t, `DROP SEQUENCE IF EXISTS a_seq`)
			query := test.data

			if test.err != "" {
				sqlDB.ExpectErr(t, test.err, query)
				sqlDB.ExpectErr(t, `relation "a_seq" does not exist`, `DROP SEQUENCE a_seq`)

			} else {

				// Verify expected behaviour of CREATE SEQUENCE AS option.
				sqlDB.Exec(t, query)
				sqlDB.CheckQueryResults(t, test.verifyQuery, test.expected)
				sqlDB.Exec(t, `DROP SEQUENCE a_seq`)
			}
		})
	}
}
