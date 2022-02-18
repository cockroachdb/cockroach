// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func alterTableAddColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddColumn,
) {
	d := t.ColumnDef
	// Check column non-existence.
	{
		elts := b.ResolveColumn(tbl.TableID, d.Name, ResolveParams{
			IsExistenceOptional: true,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, _, col := scpb.FindColumn(elts)
		if col != nil {
			if t.IfNotExists {
				return
			}
			panic(sqlerrors.NewColumnAlreadyExistsError(string(d.Name), tn.Object()))
		}
	}
	if d.IsSerial {
		panic(scerrors.NotImplementedErrorf(d, "contains serial data type"))
	}
	if d.IsComputed() {
		d.Computed.Expr = schemaexpr.MaybeRewriteComputedColumn(d.Computed.Expr, b.SessionData())
	}
	// Some of the building for the index exists below but end-to-end support is
	// not complete so we return an error.
	if d.Unique.IsUnique {
		panic(scerrors.NotImplementedErrorf(d, "contains unique constraint"))
	}
	cdd, err := tabledesc.MakeColumnDefDescs(b, d, b.SemaCtx(), b.EvalCtx())
	if err != nil {
		panic(err)
	}
	desc := cdd.ColumnDescriptor
	col := &scpb.Column{
		TableID:                 tbl.TableID,
		ColumnID:                b.NextTableColumnID(tbl),
		Nullable:                desc.Nullable,
		Virtual:                 desc.Virtual,
		Hidden:                  desc.Hidden,
		Inaccessible:            desc.Inaccessible,
		GeneratedAsIdentityType: desc.GeneratedAsIdentityType,
		PgAttributeNum:          desc.PGAttributeNum,
		SystemColumnKind:        desc.SystemColumnKind,
	}
	if desc.IsComputed() {
		expr, typ := b.ComputedColumnExpression(tbl, d)
		col.ComputeExpr = newExpression(b, expr)
		col.TypeT = typ
	} else {
		col.TypeT = b.ResolveTypeRef(d.Type)
	}
	if ptr := desc.GeneratedAsIdentitySequenceOption; ptr != nil {
		col.GeneratedAsIdentitySequenceOption = *ptr
	}
	if d.HasColumnFamily() {
		elts := b.QueryByID(tbl.TableID)
		scpb.ForEachColumnFamily(elts, func(_, targetStatus scpb.Status, cf *scpb.ColumnFamily) {
			if targetStatus == scpb.Status_PUBLIC && cf.Name == string(d.Family.Name) {
				col.FamilyID = cf.FamilyID
			}
		})
		if col.FamilyID == 0 {
			if !d.Family.Create {
				panic(errors.Errorf("unknown family %q", d.Family.Name))
			}
			cf := &scpb.ColumnFamily{
				TableID:  tbl.TableID,
				FamilyID: b.NextColumnFamilyID(tbl),
				Name:     string(d.Family.Name),
			}
			b.Add(cf)
			col.FamilyID = cf.FamilyID
		} else if d.Family.Create && !d.Family.IfNotExists {
			panic(errors.Errorf("family %q already exists", d.Family.Name))
		}
	}
	if desc.HasDefault() {
		b.Add(&scpb.ColumnDefaultExpression{
			TableID:    col.TableID,
			ColumnID:   col.ColumnID,
			Expression: *newExpression(b, cdd.DefaultExpr),
		})
	}
	if desc.HasOnUpdate() {
		b.Add(&scpb.ColumnOnUpdateExpression{
			TableID:    col.TableID,
			ColumnID:   col.ColumnID,
			Expression: *newExpression(b, cdd.OnUpdateExpr),
		})
	}
	b.Add(col)
	b.Add(&scpb.ColumnName{
		TableID:  col.TableID,
		ColumnID: col.ColumnID,
		Name:     string(d.Name),
	})
	// Virtual computed columns do not exist inside the primary index, exit early.
	if col.Virtual {
		return
	}
	// Add and update indexes.
	sourceID := addOrUpdatePrimaryIndexTargetsForAddColumn(b, tbl, col)
	if idx := cdd.PrimaryKeyOrUniqueIndexDescriptor; idx != nil {
		idx.ID = b.NextTableIndexID(tbl)
		addSecondaryIndexTargetsForAddColumn(b, tbl, idx, sourceID)
	}
}

func addOrUpdatePrimaryIndexTargetsForAddColumn(
	b BuildCtx, tbl *scpb.Table, col *scpb.Column,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	var existing, latestAdded *scpb.PrimaryIndex
	elts := b.QueryByID(tbl.TableID).Filter(func(_, targetStatus scpb.Status, _ scpb.Element) bool {
		return targetStatus == scpb.Status_PUBLIC
	})
	scpb.ForEachPrimaryIndex(elts, func(status, _ scpb.Status, idx *scpb.PrimaryIndex) {
		existing = idx
		if status == scpb.Status_ABSENT {
			latestAdded = idx
		}
	})
	if latestAdded != nil {
		latestAdded.StoringColumnIDs = append(latestAdded.StoringColumnIDs, col.ColumnID)
		return latestAdded.SourceIndexID
	}
	if existing == nil {
		// TODO(postamar): can this even be possible?
		panic(pgerror.Newf(pgcode.NoPrimaryKey, "missing active primary key"))
	}
	// Drop all existing primary index elements.
	b.Drop(existing)
	var existingName *scpb.IndexName
	var existingPartitioning *scpb.IndexPartitioning
	scpb.ForEachIndexName(elts, func(status, _ scpb.Status, name *scpb.IndexName) {
		if name.IndexID == existing.IndexID {
			existingName = name
		}
	})
	scpb.ForEachIndexPartitioning(elts, func(status, _ scpb.Status, part *scpb.IndexPartitioning) {
		if part.IndexID == existing.IndexID {
			existingPartitioning = part
		}
	})
	if existingPartitioning != nil {
		b.Drop(existingPartitioning)
	}
	if existingName != nil {
		b.Drop(existingName)
	}
	// Create all new primary index elements.
	updated := protoutil.Clone(existing).(*scpb.PrimaryIndex)
	updated.IndexID = b.NextTableIndexID(tbl)
	updated.SourceIndexID = existing.IndexID
	updated.StoringColumnIDs = append(updated.StoringColumnIDs, col.ColumnID)
	b.Add(updated)
	if existingName != nil {
		updatedName := protoutil.Clone(existingName).(*scpb.IndexName)
		updatedName.IndexID = updated.IndexID
		b.Add(updatedName)
	}
	if existingPartitioning != nil {
		updatedPartitioning := protoutil.Clone(existingPartitioning).(*scpb.IndexPartitioning)
		updatedPartitioning.IndexID = updated.IndexID
		b.Add(updatedPartitioning)
	}
	return updated.SourceIndexID
}

func addSecondaryIndexTargetsForAddColumn(
	b BuildCtx, tbl *scpb.Table, desc *descpb.IndexDescriptor, sourceID catid.IndexID,
) {
	index := scpb.Index{
		TableID:             tbl.TableID,
		IndexID:             desc.ID,
		KeyColumnIDs:        desc.KeyColumnIDs,
		KeyColumnDirections: make([]scpb.Index_Direction, len(desc.KeyColumnIDs)),
		KeySuffixColumnIDs:  desc.KeySuffixColumnIDs,
		StoringColumnIDs:    desc.StoreColumnIDs,
		CompositeColumnIDs:  desc.CompositeColumnIDs,
		Unique:              desc.Unique,
		Inverted:            desc.Type == descpb.IndexDescriptor_INVERTED,
		SourceIndexID:       sourceID,
	}
	for i, dir := range desc.KeyColumnDirections {
		if dir == descpb.IndexDescriptor_DESC {
			index.KeyColumnDirections[i] = scpb.Index_DESC
		}
	}
	if desc.Sharded.IsSharded {
		index.Sharding = &desc.Sharded
	}
	b.Add(&scpb.SecondaryIndex{Index: index})
	b.Add(&scpb.IndexName{
		TableID: tbl.TableID,
		IndexID: index.IndexID,
		Name:    desc.Name,
	})
	if p := &desc.Partitioning; len(p.List)+len(p.Range) > 0 {
		b.Add(&scpb.IndexPartitioning{
			TableID:                tbl.TableID,
			IndexID:                index.IndexID,
			PartitioningDescriptor: *protoutil.Clone(p).(*catpb.PartitioningDescriptor),
		})
	}
}
