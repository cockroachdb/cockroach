// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

func (i *immediateVisitor) CreateViewDescriptor(
	_ context.Context, op scop.CreateViewDescriptor,
) error {
	mut := tabledesc.NewBuilder(&descpb.TableDescriptor{
		ParentID:      catid.InvalidDescID, // Set by SchemaChild element.
		Name:          "",                  // Set by Namespace element.
		ID:            op.ViewID,
		Privileges:    &catpb.PrivilegeDescriptor{Version: catpb.Version23_2}, // Populated by UserPrivileges and Owner elements.
		Version:       1,
		FormatVersion: descpb.InterleavedFormatVersion,
		ViewQuery:     " ", // Placeholder; overwritten by SetViewQuery.
		Temporary:     op.Temporary,
	}).BuildCreatedMutable()
	tableDesc := mut.(*tabledesc.Mutable)
	tableDesc.State = descpb.DescriptorState_ADD
	i.CreateDescriptor(mut)
	return nil
}

func (i *immediateVisitor) SetViewQuery(ctx context.Context, op scop.SetViewQuery) error {
	tbl, err := i.checkOutTable(ctx, op.ViewID)
	if err != nil {
		return err
	}
	tbl.ViewQuery = op.Query
	return nil
}

func (i *immediateVisitor) UpdateViewBackReferencesInRelations(
	ctx context.Context, op scop.UpdateViewBackReferencesInRelations,
) error {
	tbl, err := i.checkOutTable(ctx, op.ViewID)
	if err != nil {
		return err
	}

	relIDs := catalog.DescriptorIDSet{}
	relIDToReferences := make(map[descpb.ID][]descpb.TableDescriptor_Reference)

	for _, ref := range op.TableReferences {
		relIDs.Add(ref.TableID)
		dep := descpb.TableDescriptor_Reference{
			ID:        op.ViewID,
			IndexID:   ref.IndexID,
			ColumnIDs: ref.ColumnIDs,
		}
		relIDToReferences[ref.TableID] = append(relIDToReferences[ref.TableID], dep)
	}

	for _, ref := range op.ViewReferences {
		relIDs.Add(ref.ViewID)
		dep := descpb.TableDescriptor_Reference{
			ID:        op.ViewID,
			ColumnIDs: ref.ColumnIDs,
		}
		relIDToReferences[ref.ViewID] = append(relIDToReferences[ref.ViewID], dep)
	}

	for _, seqID := range op.SequenceIDs {
		relIDs.Add(seqID)
		dep := descpb.TableDescriptor_Reference{
			ID:   op.ViewID,
			ByID: true,
		}
		relIDToReferences[seqID] = append(relIDToReferences[seqID], dep)
	}

	for relID, refs := range relIDToReferences {
		if err := updateBackReferencesInRelation(ctx, i, relID, op.ViewID, refs); err != nil {
			return err
		}
	}

	tbl.DependsOn = relIDs.Ordered()
	return nil
}

func (i *immediateVisitor) UpdateViewBackReferencesInTypes(
	ctx context.Context, op scop.UpdateViewBackReferencesInTypes,
) error {
	tbl, err := i.checkOutTable(ctx, op.ViewID)
	if err != nil {
		return err
	}

	newForwardRefs := catalog.MakeDescriptorIDSet(op.TypeIDs...)
	if err := updateBackReferencesInTypes(ctx, i, op.TypeIDs, op.ViewID, newForwardRefs); err != nil {
		return err
	}

	tbl.DependsOnTypes = op.TypeIDs
	return nil
}

func (i *immediateVisitor) UpdateViewBackReferencesInRoutines(
	ctx context.Context, op scop.UpdateViewBackReferencesInRoutines,
) error {
	tbl, err := i.checkOutTable(ctx, op.ViewID)
	if err != nil {
		return err
	}

	for _, fnID := range op.FunctionIDs {
		backRefFunc, err := i.checkOutFunction(ctx, fnID)
		if err != nil {
			return err
		}
		if err := backRefFunc.AddFunctionReference(op.ViewID); err != nil {
			return err
		}
	}

	tbl.DependsOnFunctions = op.FunctionIDs
	return nil
}
