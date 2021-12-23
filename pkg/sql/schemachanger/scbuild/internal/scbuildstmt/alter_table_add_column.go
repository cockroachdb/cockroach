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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func alterTableAddColumn(
	b BuildCtx, table catalog.TableDescriptor, t *tree.AlterTableAddColumn, tn *tree.TableName,
) {
	d := t.ColumnDef

	if d.IsComputed() {
		d.Computed.Expr = schemaexpr.MaybeRewriteComputedColumn(d.Computed.Expr, b.SessionData())
	}

	toType, err := tree.ResolveType(b, d.Type, b.CatalogReader())
	onErrPanic(err)

	version := b.ClusterSettings().Version.ActiveVersionOrEmpty(b)
	supported := types.IsTypeSupportedInVersion(version, toType)
	if !supported {
		panic(pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			toType.SQLString(),
		))
	}

	// User defined columns are not supported, since we don't
	// do type back references correctly.
	if toType.UserDefined() {
		panic(scerrors.NotImplementedErrorf(t, "user defined type in column"))
	}

	if d.IsSerial {
		panic(scerrors.NotImplementedErrorf(t.ColumnDef, "contains serial data type"))
	}
	// Some of the building for the index exists below but end-to-end support is
	// not complete so we return an error.
	if d.Unique.IsUnique {
		panic(scerrors.NotImplementedErrorf(t.ColumnDef, "contains unique constraint"))
	}
	cdd, err := tabledesc.MakeColumnDefDescs(b, d, b.SemaCtx(), b.EvalCtx())
	onErrPanic(err)

	col := cdd.ColumnDescriptor
	colID := b.NextColumnID(table)
	col.ID = colID

	// If the new column has a DEFAULT or ON UPDATE expression that uses a
	// sequence, add references between its descriptor and this column descriptor.
	_ = cdd.ForEachTypedExpr(func(expr tree.TypedExpr) error {
		maybeAddSequenceReferenceDependencies(b, table.GetID(), col, expr)
		return nil
	})

	validateColumnName(b, table, d, col, t.IfNotExists)

	familyID := descpb.FamilyID(0)
	familyName := string(d.Family.Name)
	// TODO(ajwerner,lucy-zhang): Figure out how to compute the default column ID
	// for the family.
	if d.HasColumnFamily() {
		familyID = findOrAddColumnFamily(
			b, table, familyName, d.Family.Create, d.Family.IfNotExists,
		)
	} else if !d.IsVirtual() { // FIXME: Compute columns should not have families?
		// TODO(ajwerner,lucy-zhang): Deal with adding the first column to the
		// table.
		fam := table.GetFamilies()[0]
		familyID = fam.ID
		familyName = fam.Name
	}

	if d.IsComputed() {
		// TODO (lucy): This is not going to work when the computed column
		// references columns created in the same transaction.
		serializedExpr, _, err := schemaexpr.ValidateComputedColumnExpression(
			b, table, d, tn, "computed column", b.SemaCtx(),
		)
		onErrPanic(err)
		col.ComputeExpr = &serializedExpr
	}

	if toType.UserDefined() {
		typeID, err := typedesc.UserDefinedTypeOIDToID(toType.Oid())
		onErrPanic(err)
		typeDesc := b.MustReadType(typeID)
		// Only add a type reference node only if there isn't
		// any existing reference inside this table. This makes
		// it easier to handle drop columns and other operations,
		// since those can for example only remove nodes.
		found := false
		for _, refID := range typeDesc.TypeDesc().GetReferencingDescriptorIDs() {
			if refID == table.GetID() {
				found = true
				break
			}
		}
		if !found {
			b.EnqueueAdd(&scpb.ColumnTypeReference{
				TypeID:   typeDesc.GetID(),
				ColumnID: col.ID,
				TableID:  table.GetID(),
			})
		}
	}

	b.EnqueueAdd(
		columnDescToElement(table, *col, &familyName, &familyID),
	)
	b.EnqueueAdd(&scpb.ColumnName{
		TableID:  table.GetID(),
		ColumnID: col.ID,
		Name:     col.Name,
	})
	// Virtual computed columns do not exist inside the primary index,
	if !col.Virtual {
		addOrUpdatePrimaryIndexTargetsForAddColumn(b, table, colID, col.Name)
		if idx := cdd.PrimaryKeyOrUniqueIndexDescriptor; idx != nil {
			idxID := b.NextIndexID(table)
			idx.ID = idxID
			secondaryIndex, secondaryIndexName := secondaryIndexElemFromDescriptor(idx, table)
			b.EnqueueAdd(secondaryIndex)
			b.EnqueueAdd(secondaryIndexName)
		}
	}
}

func validateColumnName(
	b BuildCtx,
	table catalog.TableDescriptor,
	d *tree.ColumnTableDef,
	col *descpb.ColumnDescriptor,
	ifNotExists bool,
) {
	_, err := tabledesc.FindPublicColumnWithName(table, d.Name)
	if err == nil {
		if ifNotExists {
			return
		}
		panic(sqlerrors.NewColumnAlreadyExistsError(string(d.Name), table.GetName()))
	}
	scpb.ForEachColumnName(b, func(_ scpb.Status, dir scpb.Target_Direction, t *scpb.ColumnName) {
		if t.TableID != table.GetID() || t.Name != d.Name.String() {
			return
		}
		switch dir {
		case scpb.Target_ADD:
			panic(pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public",
				col.Name))
		case scpb.Target_DROP:
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q being dropped, try again later", col.Name))
		default:
			panic(errors.AssertionFailedf("unknown direction %v", dir))
		}
	})
}

func findOrAddColumnFamily(
	b BuildCtx, table catalog.TableDescriptor, family string, create bool, ifNotExists bool,
) descpb.FamilyID {
	if len(family) > 0 {
		for i := range table.GetFamilies() {
			f := &table.GetFamilies()[i]
			if f.Name == family {
				if create && !ifNotExists {
					panic(errors.Errorf("family %q already exists", family))
				}
				return f.ID
			}
		}
	}
	// See if we're in the process of adding a column or dropping a column in this
	// family.
	//
	// TODO(ajwerner): Decide what to do if the only column in a family of this
	// name is being dropped and then if there is or isn't a create directive.
	found := false
	var familyID descpb.FamilyID
	scpb.ForEachColumn(b, func(_ scpb.Status, dir scpb.Target_Direction, col *scpb.Column) {
		if dir == scpb.Target_ADD && col.FamilyName == family {
			if create && !ifNotExists {
				panic(errors.Errorf("family %q already exists", family))
			}
			found = true
			familyID = col.FamilyID
		}
	})
	if found {
		return familyID
	}
	if !create {
		panic(errors.Errorf("unknown family %q", family))
	}
	return b.NextColumnFamilyID(table)
}

func maybeAddSequenceReferenceDependencies(
	b BuildCtx, tableID descpb.ID, col *descpb.ColumnDescriptor, defaultExpr tree.TypedExpr,
) {
	seqIdentifiers, err := seqexpr.GetUsedSequences(defaultExpr)
	onErrPanic(err)

	seqNameToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		var seq catalog.TableDescriptor
		if seqIdentifier.IsByID() {
			seq = b.MustReadTable(descpb.ID(seqIdentifier.SeqID))
		} else {
			parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
			onErrPanic(err)
			_, seq = b.CatalogReader().MayResolveTable(b, *parsedSeqName)
			if seq == nil {
				panic(errors.WithAssertionFailure(sqlerrors.NewUndefinedRelationError(parsedSeqName)))
			}
			seqNameToID[seqIdentifier.SeqName] = int64(seq.GetID())
		}
		col.UsesSequenceIds = append(col.UsesSequenceIds, seq.GetID())
		b.EnqueueAdd(&scpb.SequenceDependency{
			SequenceID: seq.GetID(),
			TableID:    tableID,
			ColumnID:   col.ID,
		})
	}

	if len(seqIdentifiers) > 0 {
		newExpr, err := seqexpr.ReplaceSequenceNamesWithIDs(defaultExpr, seqNameToID)
		onErrPanic(err)
		s := tree.Serialize(newExpr)
		col.DefaultExpr = &s
	}
}

func addOrUpdatePrimaryIndexTargetsForAddColumn(
	b BuildCtx, table catalog.TableDescriptor, colID descpb.ColumnID, colName string,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	{
		var latestAdded *scpb.PrimaryIndex
		scpb.ForEachPrimaryIndex(b, func(_ scpb.Status, dir scpb.Target_Direction, idx *scpb.PrimaryIndex) {
			if dir == scpb.Target_ADD && idx.TableID == table.GetID() {
				latestAdded = idx
			}
		})
		if latestAdded != nil {
			latestAdded.StoringColumnIDs = append(latestAdded.StoringColumnIDs, colID)
			return latestAdded.IndexID
		}
	}

	// Create a new primary index identical to the existing one except for its ID.
	idxID = b.NextIndexID(table)
	newIdx := table.GetPrimaryIndex().IndexDescDeepCopy()
	newIdx.ID = idxID

	if !table.GetPrimaryIndex().CollectKeyColumnIDs().Contains(colID) &&
		!table.GetPrimaryIndex().CollectPrimaryStoredColumnIDs().Contains(colID) {
		newIdx.StoreColumnIDs = append(newIdx.StoreColumnIDs, colID)
		newIdx.StoreColumnNames = append(newIdx.StoreColumnNames, colName)
	}

	newPrimaryIndex, newPrimaryIndexName := primaryIndexElemFromDescriptor(&newIdx, table)
	b.EnqueueAdd(newPrimaryIndex)
	b.EnqueueAdd(newPrimaryIndexName)

	// Drop the existing primary index.
	oldPrimaryIndex, oldPrimaryIndexName := primaryIndexElemFromDescriptor(table.GetPrimaryIndex().IndexDesc(), table)
	b.EnqueueDrop(oldPrimaryIndex)
	b.EnqueueDrop(oldPrimaryIndexName)

	return idxID
}
