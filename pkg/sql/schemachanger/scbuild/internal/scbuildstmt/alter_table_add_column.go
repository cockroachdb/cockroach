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
	b.IncrementSchemaChangeAlterCounter("table", "add_column")
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
	spec := addColumnSpec{
		tbl: tbl,
		col: &scpb.Column{
			TableID:                 tbl.TableID,
			ColumnID:                b.NextTableColumnID(tbl),
			IsHidden:                desc.Hidden,
			IsInaccessible:          desc.Inaccessible,
			GeneratedAsIdentityType: desc.GeneratedAsIdentityType,
			PgAttributeNum:          desc.PGAttributeNum,
		},
	}
	if ptr := desc.GeneratedAsIdentitySequenceOption; ptr != nil {
		spec.col.GeneratedAsIdentitySequenceOption = *ptr
	}
	spec.name = &scpb.ColumnName{
		TableID:  tbl.TableID,
		ColumnID: spec.col.ColumnID,
		Name:     string(d.Name),
	}
	spec.colType = &scpb.ColumnType{
		TableID:    tbl.TableID,
		ColumnID:   spec.col.ColumnID,
		IsNullable: desc.Nullable,
		IsVirtual:  desc.Virtual,
	}
	if desc.IsComputed() {
		expr, typ := b.ComputedColumnExpression(tbl, d)
		spec.colType.ComputeExpr = b.WrapExpression(expr)
		spec.colType.TypeT = typ
	} else {
		spec.colType.TypeT = b.ResolveTypeRef(d.Type)
	}
	if d.HasColumnFamily() {
		elts := b.QueryByID(tbl.TableID)
		var found bool
		scpb.ForEachColumnFamily(elts, func(_ scpb.Status, target scpb.TargetStatus, cf *scpb.ColumnFamily) {
			if target == scpb.ToPublic && cf.Name == string(d.Family.Name) {
				spec.colType.FamilyID = cf.FamilyID
				found = true
			}
		})
		if !found {
			if !d.Family.Create {
				panic(errors.Errorf("unknown family %q", d.Family.Name))
			}
			spec.fam = &scpb.ColumnFamily{
				TableID:  tbl.TableID,
				FamilyID: b.NextColumnFamilyID(tbl),
				Name:     string(d.Family.Name),
			}
			spec.colType.FamilyID = spec.fam.FamilyID
		} else if d.Family.Create && !d.Family.IfNotExists {
			panic(errors.Errorf("family %q already exists", d.Family.Name))
		}
	}
	if desc.HasDefault() {
		spec.def = &scpb.ColumnDefaultExpression{
			TableID:    tbl.TableID,
			ColumnID:   spec.col.ColumnID,
			Expression: *b.WrapExpression(cdd.DefaultExpr),
		}
	}
	if desc.HasOnUpdate() {
		spec.onUpdate = &scpb.ColumnOnUpdateExpression{
			TableID:    tbl.TableID,
			ColumnID:   spec.col.ColumnID,
			Expression: *b.WrapExpression(cdd.OnUpdateExpr),
		}
	}
	// Add secondary indexes for this column.
	if newPrimary := addColumn(b, spec); newPrimary != nil {
		if idx := cdd.PrimaryKeyOrUniqueIndexDescriptor; idx != nil {
			idx.ID = b.NextTableIndexID(tbl)
			addSecondaryIndexTargetsForAddColumn(b, tbl, idx, newPrimary.SourceIndexID)
		}
	}
}

type addColumnSpec struct {
	tbl      *scpb.Table
	col      *scpb.Column
	fam      *scpb.ColumnFamily
	name     *scpb.ColumnName
	colType  *scpb.ColumnType
	def      *scpb.ColumnDefaultExpression
	onUpdate *scpb.ColumnOnUpdateExpression
	comment  *scpb.ColumnComment
}

// addColumn is a helper function which adds column element targets and ensures
// that the new column is backed by a primary index, which it returns.
func addColumn(b BuildCtx, spec addColumnSpec) (backing *scpb.PrimaryIndex) {
	b.Add(spec.col)
	if spec.fam != nil {
		b.Add(spec.fam)
	}
	b.Add(spec.name)
	b.Add(spec.colType)
	if spec.def != nil {
		b.Add(spec.def)
	}
	if spec.onUpdate != nil {
		b.Add(spec.onUpdate)
	}
	if spec.comment != nil {
		b.Add(spec.comment)
	}
	// Add or update primary index for non-virtual columns.
	if spec.colType.IsVirtual {
		return nil
	}
	// Check whether a target to add a new primary index already exists. If so,
	// simply add the new column to its storing columns.
	var existing, freshlyAdded *scpb.PrimaryIndex
	publicTargets := b.QueryByID(spec.tbl.TableID).Filter(
		func(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
			return target == scpb.ToPublic
		},
	)
	scpb.ForEachPrimaryIndex(publicTargets, func(status scpb.Status, _ scpb.TargetStatus, idx *scpb.PrimaryIndex) {
		existing = idx
		if status == scpb.Status_ABSENT {
			// TODO(postamar): does it matter that there could be more than one?
			freshlyAdded = idx
		}
	})
	if freshlyAdded != nil {
		// Exceptionally, we can edit the element directly here, by virtue of it
		// currently being in the ABSENT state we know that it was introduced as a
		// PUBLIC target by the current statement.
		freshlyAdded.StoringColumnIDs = append(freshlyAdded.StoringColumnIDs, spec.col.ColumnID)
		return freshlyAdded
	}
	// Otherwise, create a new primary index target and swap it with the existing
	// primary index.
	if existing == nil {
		// TODO(postamar): can this even be possible?
		panic(pgerror.Newf(pgcode.NoPrimaryKey, "missing active primary key"))
	}
	// Drop all existing primary index elements.
	b.Drop(existing)
	var existingName *scpb.IndexName
	var existingPartitioning *scpb.IndexPartitioning
	scpb.ForEachIndexName(publicTargets, func(_ scpb.Status, _ scpb.TargetStatus, name *scpb.IndexName) {
		if name.IndexID == existing.IndexID {
			existingName = name
		}
	})
	scpb.ForEachIndexPartitioning(publicTargets, func(_ scpb.Status, _ scpb.TargetStatus, part *scpb.IndexPartitioning) {
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
	// Create the new primary index element and its dependents.
	replacement := protoutil.Clone(existing).(*scpb.PrimaryIndex)
	replacement.IndexID = b.NextTableIndexID(spec.tbl)
	replacement.SourceIndexID = existing.IndexID
	replacement.StoringColumnIDs = append(replacement.StoringColumnIDs, spec.col.ColumnID)
	b.Add(replacement)
	if existingName != nil {
		updatedName := protoutil.Clone(existingName).(*scpb.IndexName)
		updatedName.IndexID = replacement.IndexID
		b.Add(updatedName)
	}
	if existingPartitioning != nil {
		updatedPartitioning := protoutil.Clone(existingPartitioning).(*scpb.IndexPartitioning)
		updatedPartitioning.IndexID = replacement.IndexID
		b.Add(updatedPartitioning)
	}
	return replacement
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
		IsUnique:            desc.Unique,
		IsInverted:          desc.Type == descpb.IndexDescriptor_INVERTED,
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
