// Copyright 2020 The Cockroach Authors.
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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// The goal of this test is to validate a case that could happen in a
// multi-version cluster setting.
// The foreign key representation has been updated in many ways,
// and it is possible that a pre 19.2 table descriptor foreign key
// representation could lead to descriptor information loss when
// performing index drops in some cases. This test constructs an
// old version descriptor and ensures that everything is OK.
func TestOldForeignKeyRepresentationGetsUpgraded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t1 (x INT);
CREATE TABLE t.t2 (x INT, UNIQUE INDEX (x));
ALTER TABLE t.t1 ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES t.t2 (x);
CREATE INDEX ON t.t1 (x);
`); err != nil {
		t.Fatal(err)
	}
	desc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t1")
	desc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t2")
	// Remember the old foreign keys.
	oldInboundFKs := append([]sqlbase.ForeignKeyConstraint{}, desc.InboundFKs...)
	// downgradeForeignKey downgrades a table descriptor's foreign key representation
	// to the pre-19.2 table descriptor format where foreign key information
	// is stored on the index.
	downgradeForeignKey := func(tbl *sqlbase.TableDescriptor) *sqlbase.TableDescriptor {
		// Downgrade the outbound foreign keys.
		for i := range tbl.OutboundFKs {
			fk := &tbl.OutboundFKs[i]
			idx, err := sqlbase.FindFKOriginIndex(tbl, fk.OriginColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			referencedTbl, err := sqlbase.GetTableDescFromID(ctx, kvDB, keys.SystemSQLCodec, fk.ReferencedTableID)
			if err != nil {
				t.Fatal(err)
			}
			refIdx, err := sqlbase.FindFKReferencedIndex(referencedTbl, fk.ReferencedColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			idx.ForeignKey = sqlbase.ForeignKeyReference{
				Name:            fk.Name,
				Table:           fk.ReferencedTableID,
				Index:           refIdx.ID,
				Validity:        fk.Validity,
				SharedPrefixLen: int32(len(fk.OriginColumnIDs)),
				OnDelete:        fk.OnDelete,
				OnUpdate:        fk.OnUpdate,
				Match:           fk.Match,
			}
		}
		tbl.OutboundFKs = nil
		// Downgrade the inbound foreign keys.
		for i := range tbl.InboundFKs {
			fk := &tbl.InboundFKs[i]
			idx, err := sqlbase.FindFKReferencedIndex(desc, fk.ReferencedColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			originTbl, err := sqlbase.GetTableDescFromID(ctx, kvDB, keys.SystemSQLCodec, fk.OriginTableID)
			if err != nil {
				t.Fatal(err)
			}
			originIdx, err := sqlbase.FindFKOriginIndex(originTbl, fk.OriginColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			// Back references only contain the table and index IDs in old format versions.
			fkRef := sqlbase.ForeignKeyReference{
				Table: fk.OriginTableID,
				Index: originIdx.ID,
			}
			idx.ReferencedBy = append(idx.ReferencedBy, fkRef)
		}
		tbl.InboundFKs = nil
		return tbl
	}
	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		newDesc := sqlbase.NewImmutableTableDescriptor(*downgradeForeignKey(desc))
		if err := catalogkv.WriteDescToBatch(ctx, false, s.ClusterSettings(), b, keys.SystemSQLCodec, desc.ID, newDesc); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Run a DROP INDEX statement and ensure that the downgraded descriptor gets upgraded successfully.
	if _, err := sqlDB.Exec(`DROP INDEX t.t1@t1_auto_index_fk1`); err != nil {
		t.Fatal(err)
	}
	desc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t2")
	// Remove the validity field on all the descriptors for comparison, since
	// foreign keys on the referenced side's validity is not always updated correctly.
	for i := range desc.InboundFKs {
		desc.InboundFKs[i].Validity = sqlbase.ConstraintValidity_Validated
	}
	for i := range oldInboundFKs {
		oldInboundFKs[i].Validity = sqlbase.ConstraintValidity_Validated
	}
	if !reflect.DeepEqual(desc.InboundFKs, oldInboundFKs) {
		t.Error("expected fks", oldInboundFKs, "but found", desc.InboundFKs)
	}
}
