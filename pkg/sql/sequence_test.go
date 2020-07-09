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
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func BenchmarkSequenceIncrement(b *testing.B) {
	cluster := serverutils.StartTestCluster(b, 3, base.TestClusterArgs{})
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
	cluster := serverutils.StartTestCluster(b, 3, base.TestClusterArgs{})
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
	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, tbName)
	col := tableDesc.GetColumns()[colIdx]
	var seqDescs []*SequenceDescriptor
	for _, seqName := range seqNames {
		seqDescs = append(
			seqDescs,
			sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, seqName),
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

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, sqlConn, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlConn)
	defer s.Stopper().Stop(ctx)

	// setup function (used by most sub-tests). Creates sequence/table objects
	// that can be invalidated differently by the sub-tests to ensure things can
	// be dropped even in the invalid state. Caller must cleanup using the
	// returned function after.
	setup :=  func() (cleanup func()) {
		 _, err := sqlConn.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test(a INT PRIMARY KEY, b INT);
CREATE SEQUENCE t.seq OWNED BY t.test.a;
CREATE SEQUENCE t.useq OWNED BY t.test.a;
CREATE SEQUENCE t.valid_seq OWNED BY t.test.a`)
		require.NoError(t, err)

		return func() {
			sqlConn.Exec("DROP DATABASE IF EXISTS t CASCADE")
		}
	}

	// Tests simulating #50711 by breaking the invariant that sequences are owned
	// by at most one column at a time.

	// Dropping the table should work when the table descriptor is in an invalid
	// state. The owned sequence should also be dropped.
	t.Run("#50711 drop table", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()
		addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "seq")

		_, err := sqlConn.Exec("DROP TABLE t.test")
		require.NoError(t, err)

		// The sequence should have been dropped as well.
		sqlDB.ExpectErr(t, `pq: relation "t.seq" does not exist`, "SELECT * FROM t.seq")
		// The valid sequence should have also been dropped.
		sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
	})

	// Same setup as above, except now we drop the sequence first and then
	// the table.
	t.Run("#50711 drop sequence followed by drop table", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()
		addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "seq")

		 _, err := sqlConn.Exec("DROP SEQUENCE t.seq")
		 require.NoError(t, err)

		_, err = sqlConn.Exec("SELECT * FROM t.valid_seq")
			require.NoError(t, err)

		 _, err = sqlConn.Exec("DROP TABLE t.test")
		 require.NoError(t, err)

		// The valid sequence should have also been dropped.
		sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
	})

	// This test invalidates both seq and useq as DROP DATABASE CASCADE operates
	// on objects lexicographically -- owned sequences can be dropped both as a
	// regular sequence drop and as a side effect of the owner table being dropped.
	t.Run("#50711 drop database cascade", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()

		addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "seq")

		addOwnedSequence(t, kvDB, "t", "test", 0, "useq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "useq")

		_, err := sqlConn.Exec("DROP DATABASE t CASCADE")
		require.NoError(t, err)
	})

	// Tests simulating #50781 by modifying the sequence's owner to a table that
	// doesn't exist and column's `ownsSequenceIDs` to sequences that don't exist.

	t.Run("#50781 drop table followed by drop sequence", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()

		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "seq")

		_, err := sqlConn.Exec("DROP TABLE t.test")
		require.NoError(t, err)

		// The valid sequence should have also been dropped.
		sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")

		_, err = sqlConn.Exec("DROP SEQUENCE t.seq")
		require.NoError(t, err)
	})

	t.Run("#50781 drop sequence followed by drop table", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()

		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "seq")

		 _, err := sqlConn.Exec("DROP SEQUENCE t.seq")
			require.NoError(t, err)

		 _, err = sqlConn.Exec("DROP TABLE t.test")
		 require.NoError(t, err)

		// The valid sequence should have also been dropped.
		sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
	})

	// This test invalidates both seq and useq as DROP DATABASE CASCADE operates
	// on objects lexicographically -- owned sequences can be dropped both as a
	// regular sequence drop and as a side effect of the owner table being dropped.
	t.Run("#50781 drop database cascade", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()

		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "seq")
		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "useq")

		_, err := sqlConn.Exec("DROP DATABASE t CASCADE")
		require.NoError(t, err)
	})

	// Combine both #50711 and #50781.
	t.Run("combined #50711 #50781 drop table followed by sequence", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()

		addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "seq")
		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "seq")

		_, err := sqlConn.Exec("DROP TABLE t.test")
		require.NoError(t, err)

		// The valid sequence should have also been dropped.
		sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")

		_, err = sqlConn.Exec("DROP SEQUENCE t.seq")
		require.NoError(t, err)
	})

	t.Run("combined #50711 #50781 drop sequence followed by table", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()

		addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "seq")
		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "seq")

		_, err := sqlConn.Exec("DROP SEQUENCE t.seq")
		require.NoError(t, err)

		 _, err = sqlConn.Exec("DROP TABLE t.test")
		 require.NoError(t, err)

		// The valid sequence should have also been dropped.
		sqlDB.ExpectErr(t, `pq: relation "t.valid_seq" does not exist`, "SELECT * FROM t.valid_seq")
	})

	// This test invalidates both seq and useq as DROP DATABASE CASCADE operates
	// on objects lexicographically -- owned sequences can be dropped both as a
	// regular sequence drop and as a side effect of the owner table being dropped.
	t.Run("combined #50711 #50781 drop database cascade", func(t *testing.T) {
		cleanup := setup()
		defer cleanup()

		addOwnedSequence(t, kvDB, "t", "test", 0, "seq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "seq")
		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "seq")

		addOwnedSequence(t, kvDB, "t", "test", 0, "useq")
		addOwnedSequence(t, kvDB, "t", "test", 1, "useq")
		breakOwnershipMapping(t, sqlConn, kvDB, "t", "test", "useq")

		_, err := sqlConn.Exec("DROP DATABASE t CASCADE")
		require.NoError(t, err)
	})
}

// addOwnedSequence adds the sequence referenced by seqName to the
// ownsSequenceIDs of the column referenced by (dbName, tableName, colIdx).
func addOwnedSequence(
	t *testing.T, kvDB *kv.DB, dbName string, tableName string , colIdx int, seqName string,
) {
	seqDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, seqName)
	tableDesc := sqlbase.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, dbName, tableName)

	tableDesc.GetColumns()[colIdx].OwnsSequenceIds = append(
		tableDesc.GetColumns()[colIdx].OwnsSequenceIds, seqDesc.ID)

	err := kvDB.Put(
		context.Background(),
		sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	)
	require.NoError(t, err)
}

// breakOwnershipMapping simulates #50781 by setting the sequence's owner table
// to a non-existent tableID and setting the column's `ownsSequenceID` to a
// non-existent sequenceID.
func breakOwnershipMapping(
	t *testing.T, conn *sql.DB, kvDB *kv.DB, dbName string, tableName string, seqName string,
) {
	seqDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, seqName)
	tableDesc := sqlbase.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, dbName, tableName)

	// Go through all the columns of the table and set the sequenceID owned by
	// the column to seqID + 10 if the seqID refers to the given seqNam if the
	// seqID refers to the given seqName.
	for colIdx := range tableDesc.GetColumns() {
		for i := range tableDesc.GetColumns()[colIdx].OwnsSequenceIds {
			if tableDesc.GetColumns()[colIdx].OwnsSequenceIds[i] == seqDesc.ID {
				tableDesc.GetColumns()[colIdx].OwnsSequenceIds[i] = getNextUnusedID(t, conn, seqDesc.ID)
			}
		}
	}
	// Set the sequence's owner table ID to tableID + 20.
	seqDesc.SequenceOpts.SequenceOwner.OwnerTableID = getNextUnusedID(
		t, conn, seqDesc.SequenceOpts.SequenceOwner.OwnerTableID)

	err := kvDB.Put(
		context.Background(),
		sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	)
	require.NoError(t, err)

	err = kvDB.Put(
		context.Background(),
		sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, seqDesc.GetID()),
		seqDesc.DescriptorProto(),
	)
	require.NoError(t, err)
}

// getNextUnusedID returns the next ID that is not used by any descriptor by
// checking against the namespace table.
func getNextUnusedID(t *testing.T, conn *sql.DB, id sqlbase.ID) sqlbase.ID {
	unusedID := id + 1
	for {
		rows, err := conn.Query(fmt.Sprintf(
			"SELECT * FROM system.namespace WHERE id=%d", unusedID),
		)
		require.NoError(t, err)
		if !rows.Next() {
			return unusedID
		}
		unusedID += 1
	}
}
