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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
