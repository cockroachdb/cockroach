// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub/scrubtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestScrubUniqueIndex tests SCRUB on a table that violates a UNIQUE
// constraint.
func TestScrubUniqueIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	utilccl.TestingEnableEnterprise()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and row entries.
	if _, err := db.Exec(`
CREATE DATABASE db;
SET experimental_enable_implicit_column_partitioning = true;
CREATE TABLE db.t (
	id INT PRIMARY KEY,
	id2 INT UNIQUE,
	partition_by INT
) PARTITION ALL BY LIST (partition_by) (
    PARTITION one VALUES IN (1),
    PARTITION two VALUES IN (2)
);


INSERT INTO db.t VALUES (1, 2, 1), (2, 3, 2);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Overwrite the value on partition 1 with a duplicate unique index value.
	values := []tree.Datum{tree.NewDInt(1), tree.NewDInt(3), tree.NewDInt(1)}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(keys.SystemSQLCodec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(primaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(primaryIndexKey))
	}

	// Add the primary key via the KV API.
	if err := kvDB.Put(context.Background(), primaryIndexKey[0].Key, &primaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(1, 1)",
			DetailsRegex: `{"constraint_name": "t_id2_key", "row_data": {"id": "1", "id2": "3", "partition_by": "1"}`,
		},
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(2, 2)",
			DetailsRegex: `{"constraint_name": "t_id2_key", "row_data": {"id": "2", "id2": "3", "partition_by": "2"}`,
		},
	}
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t WITH OPTIONS CONSTRAINT ALL`, exp)
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)
}

// TestScrubUniqueIndexMultiCol tests SCRUB on a table that violates a UNIQUE
// constraint.
func TestScrubUniqueIndexMultiCol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	utilccl.TestingEnableEnterprise()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and row entries.
	if _, err := db.Exec(`
CREATE DATABASE db;
SET experimental_enable_implicit_column_partitioning = true;
CREATE TABLE db.t (
	pk INT PRIMARY KEY,
	id INT,
	id2 INT,
	partition_by INT,
	UNIQUE (id, id2)
) PARTITION ALL BY LIST (partition_by) (
    PARTITION one VALUES IN (1),
    PARTITION two VALUES IN (2)
);


INSERT INTO db.t VALUES (1, 1, 2, 1);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Insert a row on partition 2 with a duplicate unique index value.
	values := []tree.Datum{tree.NewDInt(2), tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(2)}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[3].GetID(), 3)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(keys.SystemSQLCodec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(primaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(primaryIndexKey))
	}

	// Add the primary key via the KV API.
	if err := kvDB.Put(context.Background(), primaryIndexKey[0].Key, &primaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(1, 1)",
			DetailsRegex: `{"constraint_name": "t_id_id2_key", "row_data": {"id": "1", "id2": "2", "partition_by": "1", "pk": "1"}`,
		},
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(2, 2)",
			DetailsRegex: `{"constraint_name": "t_id_id2_key", "row_data": {"id": "1", "id2": "2", "partition_by": "2", "pk": "2"}`,
		},
	}
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t WITH OPTIONS CONSTRAINT ALL`, exp)
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)
}

// TestScrubPrimaryKey tests SCRUB on a table that violates a PRIMARY KEY
// constraint.
func TestScrubPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	utilccl.TestingEnableEnterprise()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and row entries.
	if _, err := db.Exec(`
CREATE DATABASE db;
SET experimental_enable_implicit_column_partitioning = true;
CREATE TABLE db.t (
	id INT PRIMARY KEY,
	partition_by INT
) PARTITION ALL BY LIST (partition_by) (
    PARTITION one VALUES IN (1),
    PARTITION two VALUES IN (2)
);


INSERT INTO db.t VALUES (1, 1);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Insert a duplicate primary key into a different partition.
	values := []tree.Datum{tree.NewDInt(1), tree.NewDInt(2)}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(keys.SystemSQLCodec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(primaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(primaryIndexKey))
	}

	// Insert primary key via KV.
	if err := kvDB.Put(context.Background(), primaryIndexKey[0].Key, &primaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(1, 1)",
			DetailsRegex: `{"constraint_name": "t_pkey", "row_data": {"id": "1", "partition_by": "1"}`,
		},
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(2, 1)",
			DetailsRegex: `{"constraint_name": "t_pkey", "row_data": {"id": "1", "partition_by": "2"}`,
		},
	}

	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t WITH OPTIONS CONSTRAINT ALL`, exp)
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)
}
