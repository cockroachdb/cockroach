// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
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

// TestCachedSequences tests the behavior of cached sequences.
func TestCachedSequences(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Start test cluster.
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	execStmt := func(node int, statement string) {
		if _, err := tc.ServerConn(node).Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	anyNode := func() int {
		return rand.Intn(3)
	}

	queryInt := func(node int, statement string) int {
		rows, err := tc.ServerConn(node).Query(statement)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("No rows returned.")
		}
		var intVal int
		if err := rows.Scan(&intVal); err != nil {
			t.Fatal(err)
		}
		return intVal
	}

	testCases := []struct {
		name string
		test func(*testing.T)
	}{
		// Test a cached sequence on the single node.
		{
			name: "Single Node Cache Test",
			test: func(t *testing.T) {
				execStmt(0, `
				CREATE SEQUENCE s
									CACHE 5
				   INCREMENT BY 2
					   START WITH 2
			  `)

				// The cache starts out empty. When the cache is empty, the underlying sequence in the database
				// should be incremented by the cache size * increment amount, so it should increase by 10 each time.

				// Node 0 caches 5 values (2,4,6,8,10) and uses the first value (2).
				//
				// caches:
				//  node 0: 4,6,8,10
				// db:
				//  s: 10
				assert.Equal(t, 2, queryInt(0, "SELECT nextval('s')"))
				assert.Equal(t, 10, queryInt(anyNode(), "SELECT last_value FROM s"))

				// caches:
				//  node 0: -
				// db:
				//  s: 10
				for sequenceNumber := 4; sequenceNumber <= 10; sequenceNumber += 2 {
					assert.Equal(t, sequenceNumber, queryInt(0, "SELECT nextval('s')"))
				}
				assert.Equal(t, 10, queryInt(anyNode(), "SELECT last_value FROM s"))

				// Node 0 caches 5 values (12,14,16,18,20) and uses the first value (12).
				// caches:
				//  node 0: 14,16,18,20
				//  node 1: -
				//  node 2: -
				// db:
				//  s: 20
				assert.Equal(t, 12, queryInt(0, "SELECT nextval('s')"))
				assert.Equal(t, 20, queryInt(anyNode(), "SELECT last_value FROM s"))

				// caches:
				//  node 0: -
				// db:
				//  s: 20
				for sequenceNumber := 14; sequenceNumber <= 20; sequenceNumber += 2 {
					assert.Equal(t, sequenceNumber, queryInt(0, "SELECT nextval('s')"))
				}
				assert.Equal(t, 20, queryInt(anyNode(), "SELECT last_value FROM s"))

				execStmt(0, "DROP SEQUENCE s")
			},
		},
		// Test a cached sequence on multiple nodes.
		{
			name: "Multi-Node Cache Test",
			test: func(t *testing.T) {
				execStmt(0, `
				CREATE SEQUENCE s
									CACHE 5
				   INCREMENT BY 2
					   START WITH 2
			  `)

				// The caches all start out empty. When a cache is empty, the underlying sequence in the database
				// should be incremented by the cache size * increment amount, so it should increase by 10 each time.

				// Node 0 caches 5 values (2,4,6,8,10) and uses the first value (2).
				//
				// caches:
				//  node 0: 4,6,8,10
				//  node 1: -
				//  node 2: -
				// db:
				//  s: 10
				assert.Equal(t, 2, queryInt(0, "SELECT nextval('s')"))
				assert.Equal(t, 10, queryInt(anyNode(), "SELECT last_value FROM s"))

				// Node 1 caches 5 values (12,14,16,18,20) and uses the first value (12).
				// caches:
				//  node 0: 4,6,8,10
				//  node 1: 14,16,18,20
				//  node 2: -
				// db:
				//  s: 20
				assert.Equal(t, 12, queryInt(1, "SELECT nextval('s')"))
				assert.Equal(t, 20, queryInt(anyNode(), "SELECT last_value FROM s"))

				// Node 2 caches 5 values (22,24,26,28,30) and uses the first value (22).
				// caches:
				//  node 0: 4,6,8,10
				//  node 1: 14,16,18,20
				//  node 2: 24,26,28,30
				// db:
				//  s: 30
				assert.Equal(t, 22, queryInt(2, "SELECT nextval('s')"))
				assert.Equal(t, 30, queryInt(anyNode(), "SELECT last_value FROM s"))

				// caches:
				//  node 0: -
				//  node 1: 14,16,18,20
				//  node 2: 24,26,28,30
				// db:
				//  s: 30
				for sequenceNumber := 4; sequenceNumber <= 10; sequenceNumber += 2 {
					assert.Equal(t, sequenceNumber, queryInt(0, "SELECT nextval('s')"))
				}

				// caches:
				//  node 0: -
				//  node 1: -
				//  node 2: 24,26,28,30
				// db:
				//  s: 30
				for sequenceNumber := 14; sequenceNumber <= 20; sequenceNumber += 2 {
					assert.Equal(t, sequenceNumber, queryInt(1, "SELECT nextval('s')"))
				}

				// caches:
				//  node 0: -
				//  node 1: -
				//  node 2: -
				// db:
				//  s: 30
				for sequenceNumber := 24; sequenceNumber <= 30; sequenceNumber += 2 {
					assert.Equal(t, sequenceNumber, queryInt(2, "SELECT nextval('s')"))
				}

				assert.Equal(t, 30, queryInt(anyNode(), "SELECT last_value FROM s"))

				// caches:
				//  node 0: 34,36,38,40
				//  node 1: -
				//  node 2: -
				// db:
				//  s: 40
				assert.Equal(t, 32, queryInt(0, "SELECT nextval('s')"))
				assert.Equal(t, 40, queryInt(anyNode(), "SELECT last_value FROM s"))

				// caches:
				//  node 0: 34,36,38,40
				//  node 1: 44,46,48,50
				//  node 2: -
				// db:
				//  s: 50
				assert.Equal(t, 42, queryInt(1, "SELECT nextval('s')"))
				assert.Equal(t, 50, queryInt(anyNode(), "SELECT last_value FROM s"))

				// caches:
				//  node 0: 34,36,38,40
				//  node 1: 44,46,48,50
				//  node 2: 54,56,58,60
				// db:
				//  s: 60
				assert.Equal(t, 52, queryInt(2, "SELECT nextval('s')"))
				assert.Equal(t, 60, queryInt(anyNode(), "SELECT last_value FROM s"))

				execStmt(0, "DROP SEQUENCE s")
			},
		},
		// Test multiple cached sequences on multiple nodes.
		{
			name: "Multi-Node, Multi-Sequence Cache Test",
			test: func(t *testing.T) {
				execStmt(0, `
				CREATE SEQUENCE s1
									CACHE 5
				   INCREMENT BY 2
					   START WITH 2
			  `)

				execStmt(0, `
				CREATE SEQUENCE s2
									CACHE 4
				   INCREMENT BY 3
					   START WITH 3
			  `)

				// The caches all start out empty. When a cache is empty, the underlying sequence in the database
				// should be incremented by the cache size * increment amount.
				//
				// s1 increases by 10 each time, and s2 increases by 12 each time.

				// caches:
				//  node 0:
				//   s1: 4,6,8,10
				//  node 1: -
				//  node 2: -
				// db:
				//  s1: 10
				assert.Equal(t, 2, queryInt(0, "SELECT nextval('s1')"))
				assert.Equal(t, 10, queryInt(anyNode(), "SELECT last_value FROM s1"))

				// caches:
				//  node 0:
				//   s1: 4,6,8,10
				//   s2: 6,9,12
				//  node 1: -
				//  node 2: -
				// db:
				//  s1: 10
				//  s2: 12
				assert.Equal(t, 3, queryInt(0, "SELECT nextval('s2')"))
				assert.Equal(t, 12, queryInt(anyNode(), "SELECT last_value FROM s2"))

				// caches:
				//  node 0:
				//   s1: 4,6,8,10
				//   s2: 6, 9, 12
				//  node 1:
				//   s1: 14,16,18,20
				//  node 2: -
				// db:
				//  s1: 20
				//  s2: 12
				assert.Equal(t, 12, queryInt(1, "SELECT nextval('s1')"))
				assert.Equal(t, 20, queryInt(anyNode(), "SELECT last_value FROM s1"))

				// caches:
				//  node 0:
				//   s1: 4,6,8,10
				//   s2: 6, 9, 12
				//  node 1:
				//   s1: 14,16,18,20
				//   s2: 18,21,24
				//  node 2: -
				// db:
				//  s1: 20
				//  s2: 24
				assert.Equal(t, 15, queryInt(1, "SELECT nextval('s2')"))
				assert.Equal(t, 24, queryInt(anyNode(), "SELECT last_value FROM s2"))

				// caches:
				//  node 0:
				//   s1: 4,6,8,10
				//   s2: 6, 9, 12
				//  node 1:
				//   s1: 14,16,18,20
				//   s2: 18,21,24
				//  node 2:
				//   s1: 24,26,28,30
				// db:
				//  s1: 30
				//  s2: 24
				assert.Equal(t, 22, queryInt(2, "SELECT nextval('s1')"))
				assert.Equal(t, 30, queryInt(anyNode(), "SELECT last_value FROM s1"))

				// caches:
				//  node 0:
				//   s1: 4,6,8,10
				//   s2: 6, 9, 12
				//  node 1:
				//   s1: 14,16,18,20
				//   s2: 18,21,24
				//  node 2:
				//   s1: 24,26,28,30
				//   s2: 30,33,36
				// db:
				//  s1: 30
				//  s2: 36
				assert.Equal(t, 27, queryInt(2, "SELECT nextval('s2')"))
				assert.Equal(t, 36, queryInt(anyNode(), "SELECT last_value FROM s2"))

				// caches:
				//  node 0:
				//   s1: 4,6,8,10
				//   s2: 6, 9, 12
				//  node 1:
				//   s1: 14,16,18,20
				//   s2: 18,21,24
				//  node 2:
				//   s1: 24,26,28,30
				//   s2: 30,33,36
				// db:
				//  s1: 30
				//  s2: 36
				wg := sync.WaitGroup{}
				emptyCache := func(node, start, finish, inc int, seq string) {
					for sequenceNumber := start; sequenceNumber <= finish; sequenceNumber += inc {
						assert.Equal(t, sequenceNumber, queryInt(node, fmt.Sprintf("SELECT nextval('%s')", seq)))
					}
					wg.Done()
				}
				wg.Add(6)
				go emptyCache(0, 4, 10, 2, "s1")
				go emptyCache(0, 6, 12, 3, "s2")
				go emptyCache(1, 14, 20, 2, "s1")
				go emptyCache(1, 18, 24, 3, "s2")
				go emptyCache(2, 24, 30, 2, "s1")
				go emptyCache(2, 30, 36, 3, "s2")
				wg.Wait()

				// caches:
				//  node 0: -
				//  node 1: -
				//  node 2: -
				// db:
				//  s1: 30
				//  s2: 36
				assert.Equal(t, 30, queryInt(anyNode(), "SELECT last_value FROM s1"))
				assert.Equal(t, 36, queryInt(anyNode(), "SELECT last_value FROM s2"))

				execStmt(0, "DROP SEQUENCE s1")
				execStmt(0, "DROP SEQUENCE s2")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.test(t)
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
