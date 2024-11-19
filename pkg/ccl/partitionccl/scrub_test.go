// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package partitionccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
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
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

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

	// Overwrite the value on partition one with a duplicate unique index value.
	values := []tree.Datum{tree.NewDInt(1), tree.NewDInt(3), tree.NewDInt(1)}
	codec := s.Codec()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(codec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
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

	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, secondaryIndex,
		colIDtoRowIndex, values, true, /* includeEmpty */
	)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(secondaryIndexKey))
	}

	// Add the secondary key via the KV API.
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB.
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

// TestScrubUniqueIndexWithNulls tests SCRUB on a table with NULLs, which does
// not violate UNIQUE constraints.
func TestScrubUniqueIndexWithNulls(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	utilccl.TestingEnableEnterprise()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

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

INSERT INTO db.t VALUES (1, 2, 1), (2, NULL, 2);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Overwrite the value on partition one with a NULL index value.
	values := []tree.Datum{tree.NewDInt(1), tree.DNull, tree.NewDInt(1)}
	codec := s.Codec()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(codec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
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

	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, secondaryIndex,
		colIDtoRowIndex, values, true, /* includeEmpty */
	)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(secondaryIndexKey))
	}

	// Add the secondary key via the KV API.
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB.
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t WITH OPTIONS CONSTRAINT ALL`, []scrubtestutils.ExpectedScrubResult{})
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, []scrubtestutils.ExpectedScrubResult{})
}

// TestScrubUniqueIndexExplicitPartition tests SCRUB on a table that uses an
// explicit partitioning scheme, and therefore should not violate unique
// constraints if one of the two values in the constraint is duplicated, even
// if the individual value has a unique constraint.
func TestScrubUniqueIndexExplicitPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	utilccl.TestingEnableEnterprise()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	// Create the table and row entries.
	if _, err := db.Exec(`
CREATE DATABASE db;
CREATE TABLE db.t (
	id INT PRIMARY KEY,
	id2 INT,
	UNIQUE (id, id2) PARTITION BY LIST (id) (
    PARTITION one VALUES IN (1),
    PARTITION two VALUES IN (2)
	)
);


INSERT INTO db.t VALUES (1, 3), (2, 4);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Overwrite the value on partition one with a duplicate unique value.
	values := []tree.Datum{tree.NewDInt(1), tree.NewDInt(4)}
	codec := s.Codec()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(codec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(primaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(primaryIndexKey))
	}

	// Add the primary key via the KV API. This will overwrite the old primary
	// index KV, so no need to perform a Del.
	if err := kvDB.Put(context.Background(), primaryIndexKey[0].Key, &primaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	oldValues := []tree.Datum{tree.NewDInt(1), tree.NewDInt(3)}
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	secondaryIndexDelKey, err := rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, secondaryIndex,
		colIDtoRowIndex, oldValues, true, /* includeEmpty */
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, secondaryIndex,
		colIDtoRowIndex, values, true, /* includeEmpty */
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}
	// Delete the old secondary index KV before inserting the new one.
	if _, err := kvDB.Del(context.Background(), secondaryIndexDelKey[0].Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB.
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t WITH OPTIONS CONSTRAINT ALL`, []scrubtestutils.ExpectedScrubResult{})
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, []scrubtestutils.ExpectedScrubResult{})
}

// TestScrubPartialUniqueIndex tests SCRUB on a table that violates a UNIQUE
// constraint on a partial unique index.
func TestScrubPartialUniqueIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	utilccl.TestingEnableEnterprise()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	// Create the table and row entries.
	if _, err := db.Exec(`
CREATE DATABASE db;
SET experimental_enable_implicit_column_partitioning = true;
CREATE TABLE db.t (
	id INT PRIMARY KEY,
	id2 INT,
	partition_by INT,
	UNIQUE INDEX idx (id2) WHERE id2 > 4
) PARTITION ALL BY LIST (partition_by) (
    PARTITION one VALUES IN (1),
    PARTITION two VALUES IN (2)
);


INSERT INTO db.t VALUES (1, 2, 1), (2, 3, 2), (3, 5, 1), (4, 6, 2);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Overwrite the values on partition 1 with duplicate idx2 values: one that
	// falls under the unique index constraint, and one that does not.
	valuesConstrained := []tree.Datum{tree.NewDInt(3), tree.NewDInt(6), tree.NewDInt(1)}
	valuesNotConstrained := []tree.Datum{tree.NewDInt(1), tree.NewDInt(3), tree.NewDInt(1)}
	codec := s.Codec()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)

	// Modify the primary index with a duplicate constrained value.
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(codec, tableDesc, primaryIndex, colIDtoRowIndex, valuesConstrained, true)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(primaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(primaryIndexKey))
	}
	if err := kvDB.Put(context.Background(), primaryIndexKey[0].Key, &primaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Modify the secondary index with a duplicate constrained value.
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, secondaryIndex,
		colIDtoRowIndex, valuesConstrained, true, /* includeEmpty */
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Modify the primary index with a duplicate value not in the constrained
	// range.
	primaryIndexKey, err = rowenc.EncodePrimaryIndex(codec, tableDesc, primaryIndex, colIDtoRowIndex, valuesNotConstrained, true)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(primaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(primaryIndexKey))
	}
	if err := kvDB.Put(context.Background(), primaryIndexKey[0].Key, &primaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Modify the secondary index with a duplicate value not in the constrained
	// range.
	secondaryIndexKey, err = rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, secondaryIndex,
		colIDtoRowIndex, valuesNotConstrained, true, /* includeEmpty */
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB.
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(1, 3)",
			DetailsRegex: `{"constraint_name": "idx", "row_data": {"id": "3", "id2": "6", "partition_by": "1"}`,
		},
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(2, 4)",
			DetailsRegex: `{"constraint_name": "idx", "row_data": {"id": "4", "id2": "6", "partition_by": "2"}`,
		},
	}
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t WITH OPTIONS CONSTRAINT ALL`, exp)
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)
}

// TestScrubUniqueIndexMultiCol tests SCRUB on a table that violates a UNIQUE
// constraint for multiple columns.
func TestScrubUniqueIndexMultiCol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	utilccl.TestingEnableEnterprise()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

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
	codec := s.Codec()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[3].GetID(), 3)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(codec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
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

	// Modify the secondary index with the duplicate value.
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		context.Background(), codec, tableDesc, secondaryIndex,
		colIDtoRowIndex, values, true, /* includeEmpty */
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB.
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
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

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
	codec := s.Codec()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(codec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
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

	// Run SCRUB.
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
