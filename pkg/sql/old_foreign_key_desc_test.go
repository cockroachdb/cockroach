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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t1 (x INT, INDEX i (x));
CREATE TABLE t.t2 (x INT, UNIQUE INDEX (x));
ALTER TABLE t.t1 ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES t.t2 (x);
CREATE INDEX ON t.t1 (x);
`); err != nil {
		t.Fatal(err)
	}
	desc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t1")
	desc = catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t2")
	// Remember the old foreign keys.
	oldInboundFKs := append([]descpb.ForeignKeyConstraint{}, desc.InboundFKs...)
	// downgradeForeignKey downgrades a table descriptor's foreign key representation
	// to the pre-19.2 table descriptor format where foreign key information
	// is stored on the index.
	downgradeForeignKey := func(tbl *tabledesc.Immutable) *tabledesc.Immutable {
		// Downgrade the outbound foreign keys.
		for i := range tbl.OutboundFKs {
			fk := &tbl.OutboundFKs[i]
			idx, err := tabledesc.FindFKOriginIndex(tbl, fk.OriginColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			var referencedTbl *tabledesc.Immutable
			err = kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				referencedTbl, err = catalogkv.MustGetTableDescByID(ctx, txn, keys.SystemSQLCodec, fk.ReferencedTableID)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			refIdx, err := tabledesc.FindFKReferencedUniqueConstraint(referencedTbl, fk.ReferencedColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			idx.ForeignKey = descpb.ForeignKeyReference{
				Name:            fk.Name,
				Table:           fk.ReferencedTableID,
				Index:           refIdx.(*descpb.IndexDescriptor).ID,
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
			refIdx, err := tabledesc.FindFKReferencedUniqueConstraint(desc, fk.ReferencedColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			var originTbl *tabledesc.Immutable
			if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				originTbl, err = catalogkv.MustGetTableDescByID(ctx, txn, keys.SystemSQLCodec, fk.OriginTableID)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			originIdx, err := tabledesc.FindFKOriginIndex(originTbl, fk.OriginColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			// Back references only contain the table and index IDs in old format versions.
			fkRef := descpb.ForeignKeyReference{
				Table: fk.OriginTableID,
				Index: originIdx.ID,
			}
			idx := refIdx.(*descpb.IndexDescriptor)
			idx.ReferencedBy = append(idx.ReferencedBy, fkRef)
		}
		tbl.InboundFKs = nil
		return tbl
	}
	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		newDesc := downgradeForeignKey(desc)
		if err := catalogkv.WriteDescToBatch(ctx, false, s.ClusterSettings(), b, keys.SystemSQLCodec, desc.ID, newDesc); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Run a DROP INDEX statement and ensure that the downgraded descriptor gets upgraded successfully.
	if _, err := sqlDB.Exec(`DROP INDEX t.t1@i`); err != nil {
		t.Fatal(err)
	}
	desc = catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t2")
	// Remove the validity field on all the descriptors for comparison, since
	// foreign keys on the referenced side's validity is not always updated correctly.
	for i := range desc.InboundFKs {
		desc.InboundFKs[i].Validity = descpb.ConstraintValidity_Validated
	}
	for i := range oldInboundFKs {
		oldInboundFKs[i].Validity = descpb.ConstraintValidity_Validated
	}
	if !reflect.DeepEqual(desc.InboundFKs, oldInboundFKs) {
		t.Error("expected fks", oldInboundFKs, "but found", desc.InboundFKs)
	}
}
