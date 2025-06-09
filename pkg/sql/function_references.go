// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
)

func (p *planner) updateFunctionReferencesForCheck(
	ctx context.Context, tblDesc catalog.TableDescriptor, ck *descpb.TableDescriptor_CheckConstraint,
) error {
	udfIDs, err := tblDesc.GetAllReferencedFunctionIDsInConstraint(ck.ConstraintID)
	if err != nil {
		return err
	}
	for _, id := range udfIDs.Ordered() {
		fnDesc, err := p.descCollection.MutableByID(p.txn).Function(ctx, id)
		if err != nil {
			return err
		}
		if err := fnDesc.AddConstraintReference(tblDesc.GetID(), ck.ConstraintID); err != nil {
			return err
		}
		if err := p.writeFuncSchemaChange(ctx, fnDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) maybeUpdateFunctionReferencesForColumn(
	ctx context.Context, tblDesc catalog.TableDescriptor, col *descpb.ColumnDescriptor,
) error {
	// Remove back references in old referenced functions.
	for _, id := range col.UsesFunctionIds {
		fnDesc, err := p.descCollection.MutableByID(p.txn).Function(ctx, id)
		if err != nil {
			return err
		}
		fnDesc.RemoveColumnReference(tblDesc.GetID(), col.ID)
		if err := p.writeFuncSchemaChange(ctx, fnDesc); err != nil {
			return err
		}
	}

	udfIDs, err := tblDesc.GetAllReferencedFunctionIDsInColumnExprs(col.ID)
	if err != nil {
		return err
	}
	col.UsesFunctionIds = udfIDs.Ordered()

	// Add new back references.
	for _, id := range col.UsesFunctionIds {
		fnDesc, err := p.descCollection.MutableByID(p.txn).Function(ctx, id)
		if err != nil {
			return err
		}
		if err := fnDesc.AddColumnReference(tblDesc.GetID(), col.ID); err != nil {
			return err
		}
		if err := p.writeFuncSchemaChange(ctx, fnDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) maybeAddFunctionReferencesForIndex(
	ctx context.Context, tblDesc catalog.TableDescriptor, idx *descpb.IndexDescriptor,
) error {
	if !idx.IsPartial() {
		return nil
	}
	if idx.UseDeletePreservingEncoding {
		// These indexes are only used during the backfill and are discarded after.
		return nil
	}

	// Extract function IDs from the partial index predicate.
	fnIDs, err := schemaexpr.GetUDFIDsFromExprStr(idx.Predicate)
	if err != nil {
		return err
	}

	// Add back-references in function descriptors.
	for _, id := range fnIDs.Ordered() {
		fnDesc, err := p.descCollection.MutableByID(p.txn).Function(ctx, id)
		if err != nil {
			return err
		}
		if err := fnDesc.AddIndexReference(tblDesc.GetID(), idx.ID); err != nil {
			return err
		}
		if err := p.writeFuncSchemaChange(ctx, fnDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) maybeRemoveFunctionReferencesForIndex(
	ctx context.Context, tblDesc catalog.TableDescriptor, idx *descpb.IndexDescriptor,
) error {
	if !idx.IsPartial() {
		return nil
	}

	// Extract function IDs from the partial index predicate.
	fnIDs, err := schemaexpr.GetUDFIDsFromExprStr(idx.Predicate)
	if err != nil {
		return err
	}

	// Remove back-references in function descriptors.
	for _, id := range fnIDs.Ordered() {
		fnDesc, err := p.descCollection.MutableByID(p.txn).Function(ctx, id)
		if err != nil {
			return err
		}
		fnDesc.RemoveIndexReference(tblDesc.GetID(), idx.ID)
		if err := p.writeFuncSchemaChange(ctx, fnDesc); err != nil {
			return err
		}
	}
	return nil
}
