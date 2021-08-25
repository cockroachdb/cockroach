// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// alterTable builds targets and transforms the provided schema change nodes
// accordingly, given an ALTER TABLE statement.
func (b *buildContext) alterTable(ctx context.Context, n *tree.AlterTable) {
	// Hoist the constraints to separate clauses because other code assumes that
	// that is how the commands will look.
	//
	// TODO(ajwerner): Clone the AST here because this mutates it in place and
	// that is bad.
	n.HoistAddColumnConstraints()

	tn := n.Table.ToTableName()
	_, table := b.CatalogReader().MayResolveTable(ctx, *n.Table)
	if table == nil {
		if n.IfExists {
			return
		}
		panic(sqlerrors.NewUndefinedRelationError(n.Table))
	}
	if HasConcurrentSchemaChanges(table) {
		panic(&ConcurrentSchemaChangeError{descID: table.GetID()})
	}
	for _, cmd := range n.Cmds {
		b.alterTableCmd(ctx, table, cmd, &tn)
	}
}

func (b *buildContext) alterTableCmd(
	ctx context.Context, table catalog.TableDescriptor, cmd tree.AlterTableCmd, tn *tree.TableName,
) {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		b.alterTableAddColumn(ctx, table, t, tn)
	default:
		panic(&notImplementedError{n: cmd})
	}
}

func primaryIndexElemFromDescriptor(
	indexDesc *descpb.IndexDescriptor, tableDesc catalog.TableDescriptor,
) (*scpb.PrimaryIndex, *scpb.IndexName) {
	if len(indexDesc.Partitioning.Range) > 0 ||
		len(indexDesc.Partitioning.List) > 0 {
		panic(notImplementedError{n: nil, detail: "partitioning on new indexes is not supported."})
	}
	keyColumnDirs := make([]scpb.PrimaryIndex_Direction, 0, len(indexDesc.KeyColumnDirections))
	for _, dir := range indexDesc.KeyColumnDirections {
		switch dir {
		case descpb.IndexDescriptor_DESC:
			keyColumnDirs = append(keyColumnDirs, scpb.PrimaryIndex_DESC)
		case descpb.IndexDescriptor_ASC:
			keyColumnDirs = append(keyColumnDirs, scpb.PrimaryIndex_ASC)
		default:
			panic(errors.AssertionFailedf("Unknown direction type %s", dir))
		}
	}
	return &scpb.PrimaryIndex{TableID: tableDesc.GetID(),
			IndexID:             indexDesc.ID,
			Unique:              indexDesc.Unique,
			KeyColumnIDs:        indexDesc.KeyColumnIDs,
			KeyColumnDirections: keyColumnDirs,
			KeySuffixColumnIDs:  indexDesc.KeySuffixColumnIDs,
			StoringColumnIDs:    indexDesc.StoreColumnIDs,
			CompositeColumnIDs:  indexDesc.CompositeColumnIDs,
			Inverted:            indexDesc.Type == descpb.IndexDescriptor_INVERTED,
			ShardedDescriptor:   &indexDesc.Sharded},
		&scpb.IndexName{
			TableID: tableDesc.GetID(),
			IndexID: indexDesc.ID,
			Name:    indexDesc.Name,
		}
}

func secondaryIndexElemFromDescriptor(
	indexDesc *descpb.IndexDescriptor, tableDesc catalog.TableDescriptor,
) (*scpb.SecondaryIndex, *scpb.IndexName) {
	if len(indexDesc.Partitioning.Range) > 0 ||
		len(indexDesc.Partitioning.List) > 0 {
		panic(notImplementedError{n: nil, detail: "partitioning on new indexes is not supported."})
	}
	keyColumnDirs := make([]scpb.SecondaryIndex_Direction, 0, len(indexDesc.KeyColumnDirections))
	for _, dir := range indexDesc.KeyColumnDirections {
		switch dir {
		case descpb.IndexDescriptor_DESC:
			keyColumnDirs = append(keyColumnDirs, scpb.SecondaryIndex_DESC)
		case descpb.IndexDescriptor_ASC:
			keyColumnDirs = append(keyColumnDirs, scpb.SecondaryIndex_ASC)
		default:
			panic(errors.AssertionFailedf("Unknown direction type %s", dir))
		}
	}
	return &scpb.SecondaryIndex{TableID: tableDesc.GetID(),
			IndexID:             indexDesc.ID,
			Unique:              indexDesc.Unique,
			KeyColumnIDs:        indexDesc.KeyColumnIDs,
			KeyColumnDirections: keyColumnDirs,
			KeySuffixColumnIDs:  indexDesc.KeySuffixColumnIDs,
			StoringColumnIDs:    indexDesc.StoreColumnIDs,
			CompositeColumnIDs:  indexDesc.CompositeColumnIDs,
			Inverted:            indexDesc.Type == descpb.IndexDescriptor_INVERTED,
			ShardedDescriptor:   &indexDesc.Sharded},
		&scpb.IndexName{
			TableID: tableDesc.GetID(),
			IndexID: indexDesc.ID,
			Name:    indexDesc.Name,
		}
}

func (b *buildContext) alterTableAddColumn(
	ctx context.Context,
	table catalog.TableDescriptor,
	t *tree.AlterTableAddColumn,
	tn *tree.TableName,
) {
	d := t.ColumnDef

	if d.IsComputed() {
		d.Computed.Expr = schemaexpr.MaybeRewriteComputedColumn(d.Computed.Expr, b.SessionData())
	}

	toType, err := tree.ResolveType(ctx, d.Type, b.CatalogReader())
	onErrPanic(err)

	version := b.ClusterSettings().Version.ActiveVersionOrEmpty(ctx)
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
		panic(&notImplementedError{n: t, detail: "user defined type in column."})
	}

	if d.IsSerial {
		panic(&notImplementedError{n: t.ColumnDef, detail: "contains serial data type"})
	}
	// Some of the building for the index exists below but end-to-end support is
	// not complete so we return an error.
	if d.Unique.IsUnique {
		panic(&notImplementedError{n: t.ColumnDef, detail: "contains unique constraint"})
	}
	cdd, err := tabledesc.MakeColumnDefDescs(ctx, d, semaCtx(b), evalCtx(ctx, b))
	onErrPanic(err)

	col := cdd.ColumnDescriptor
	colID := b.nextColumnID(table)
	col.ID = colID

	// If the new column has a DEFAULT or ON UPDATE expression that uses a
	// sequence, add references between its descriptor and this column descriptor.
	_ = cdd.ForEachTypedExpr(func(expr tree.TypedExpr) error {
		b.maybeAddSequenceReferenceDependencies(ctx, table.GetID(), col, expr)
		return nil
	})

	b.validateColumnName(table, d, col, t.IfNotExists)

	familyName := string(d.Family.Name)
	var familyID descpb.FamilyID
	// TODO(ajwerner,lucy-zhang): Figure out how to compute the default column ID
	// for the family.
	if d.HasColumnFamily() {
		familyID = b.findOrAddColumnFamily(
			table, familyName, d.Family.Create, d.Family.IfNotExists,
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
			ctx, table, d, tn, "computed column", semaCtx(b),
		)
		onErrPanic(err)
		col.ComputeExpr = &serializedExpr
	}

	if toType.UserDefined() {
		typeID, err := typedesc.UserDefinedTypeOIDToID(toType.Oid())
		onErrPanic(err)
		typeDesc := mustReadType(ctx, b, typeID)
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
			b.addNode(scpb.Target_ADD, &scpb.ColumnTypeReference{
				TableID:  table.GetID(),
				ColumnID: col.ID,
				TypeID:   typeDesc.GetID(),
			})
		}
	}
	b.addNode(scpb.Target_ADD,
		&scpb.ColumnName{
			TableID:  table.GetID(),
			ColumnID: col.ID,
			Name:     col.Name,
		})
	b.addNode(scpb.Target_ADD,
		b.columnDescToElement(table, *col, &familyName, &familyID))
	// Virtual computed columns do not exist inside the primary index,
	if !col.Virtual {
		b.addOrUpdatePrimaryIndexTargetsForAddColumn(table, colID, col.Name)
		if idx := cdd.PrimaryKeyOrUniqueIndexDescriptor; idx != nil {
			idxID := b.nextIndexID(table)
			idx.ID = idxID
			secondaryIndex, secondaryIndexName := secondaryIndexElemFromDescriptor(idx, table)
			b.addNode(scpb.Target_ADD, secondaryIndex)
			b.addNode(scpb.Target_ADD, secondaryIndexName)

		}
	}
	b.incrementSubWorkID()
}

func (b *buildContext) validateColumnName(
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
	for _, n := range b.output.Nodes {
		switch t := n.Element().(type) {
		case *scpb.ColumnName:
			if t.TableID != table.GetID() || t.Name != string(d.Name) {
				continue
			}
			switch dir := n.Target.Direction; dir {
			case scpb.Target_ADD:
				panic(pgerror.Newf(pgcode.DuplicateColumn,
					"duplicate: column %q in the middle of being added, not yet public",
					col.Name))
			case scpb.Target_DROP:
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q being dropped, try again later", col.Name))
			default:
				panic(errors.AssertionFailedf("unknown direction %v in %v", dir, n.Target))
			}
		}
	}
}

func (b *buildContext) findOrAddColumnFamily(
	table catalog.TableDescriptor, family string, create bool, ifNotExists bool,
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
	nextFamilyID := table.GetNextFamilyID()
	for _, n := range b.output.Nodes {
		switch col := n.Element().(type) {
		case *scpb.Column:
			if col.TableID != table.GetID() {
				continue
			}
			if col.FamilyName == family {
				if create && !ifNotExists {
					panic(errors.Errorf("family %q already exists", family))
				}
				return col.FamilyID
			}
			if col.FamilyID >= nextFamilyID {
				nextFamilyID = col.FamilyID + 1
			}
		}
	}
	if !create {
		panic(errors.Errorf("unknown family %q", family))
	}
	return nextFamilyID
}

func (b *buildContext) alterTableDropColumn(
	ctx context.Context, table catalog.TableDescriptor, t *tree.AlterTableDropColumn,
) {
	if b.SessionData().SafeUpdates {
		panic(pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will " +
			"remove all data in that column"))
	}

	// TODO(ajwerner): Deal with drop column for columns which are being added
	// currently.
	colToDrop, err := table.FindColumnWithName(t.Column)
	if err != nil {
		if t.IfExists {
			// Noop.
			return
		}
		panic(err)
	}
	var columnID descpb.ColumnID
	columnIDFound := false
	for _, n := range b.output.Nodes {
		switch col := n.Element().(type) {
		case *scpb.ColumnName:
			if col.Name == t.Column.String() {
				columnID = col.ColumnID
				columnIDFound = true
			}
		}
	}
	if columnIDFound {
		// Check whether the column is being dropped.
		for _, n := range b.output.Nodes {
			switch col := n.Element().(type) {
			case *scpb.Column:
				if col.TableID != table.GetID() ||
					n.Target.Direction != scpb.Target_DROP ||
					columnID != col.ColumnID {
					continue
				}
				// Column drops are, while the column is in the process of being dropped,
				// for whatever reason, idempotent. Return silently here.
				return
			}
		}
	}

	// TODO:
	// remove sequence dependencies
	// drop sequences owned by column (if not referenced by other columns)
	// drop view (if cascade specified)
	// check that no computed columns reference this column
	// check that column is not in the PK
	// drop secondary indexes
	// drop all indexes that index/store the column or use it as a partial index predicate
	// drop check constraints
	// remove comments
	// drop foreign keys

	// Clean up type backreferences if no other column
	// refers to the same type.
	if colToDrop.HasType() && colToDrop.GetType().UserDefined() {
		colType := colToDrop.GetType()
		needsDrop := true
		for _, column := range table.AllColumns() {
			if column.HasType() && column.GetID() != colToDrop.GetID() &&
				column.GetType().Oid() == colType.Oid() {
				needsDrop = false
				break
			}
		}
		if needsDrop {
			typeID, err := typedesc.UserDefinedTypeOIDToID(colType.Oid())
			onErrPanic(err)
			typ := mustReadType(ctx, b, typeID)
			b.addNode(scpb.Target_DROP, &scpb.ColumnTypeReference{
				TypeID:   typ.GetID(),
				TableID:  table.GetID(),
				ColumnID: colToDrop.GetID(),
			})
		}
	}

	// TODO(ajwerner): Add family information to the column.
	b.addNode(scpb.Target_DROP,
		&scpb.ColumnName{
			TableID:  table.GetID(),
			ColumnID: colToDrop.GetID(),
			Name:     colToDrop.GetName(),
		})
	b.addNode(scpb.Target_DROP,
		b.columnDescToElement(table, *colToDrop.ColumnDesc(), nil, nil))

	b.addOrUpdatePrimaryIndexTargetsForDropColumn(table, colToDrop.GetID())
}

// Suppress the linter. We're not ready to fully implement this schema change
// yet.
var _ = (*buildContext)(nil).alterTableDropColumn

func (b *buildContext) maybeAddSequenceReferenceDependencies(
	ctx context.Context, tableID descpb.ID, col *descpb.ColumnDescriptor, defaultExpr tree.TypedExpr,
) {
	seqIdentifiers, err := seqexpr.GetUsedSequences(defaultExpr)
	onErrPanic(err)

	seqNameToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		var seq catalog.TableDescriptor
		if seqIdentifier.IsByID() {
			seq = mustReadTable(ctx, b, descpb.ID(seqIdentifier.SeqID))
		} else {
			parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
			onErrPanic(err)
			_, seq = b.CatalogReader().MayResolveTable(ctx, *parsedSeqName)
			if seq == nil {
				panic(errors.WithAssertionFailure(sqlerrors.NewUndefinedRelationError(parsedSeqName)))
			}
			seqNameToID[seqIdentifier.SeqName] = int64(seq.GetID())
		}
		col.UsesSequenceIds = append(col.UsesSequenceIds, seq.GetID())
		b.addNode(scpb.Target_ADD, &scpb.SequenceDependency{
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

func (b *buildContext) addOrUpdatePrimaryIndexTargetsForAddColumn(
	table catalog.TableDescriptor, colID descpb.ColumnID, colName string,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for i, n := range b.output.Nodes {
		if t, ok := n.Element().(*scpb.PrimaryIndex); ok &&
			b.output.Nodes[i].Target.Direction == scpb.Target_ADD &&
			t.TableID == table.GetID() {
			t.StoringColumnIDs = append(t.StoringColumnIDs, colID)
			return t.IndexID
		}
	}

	// Create a new primary index, identical to the existing one except for its
	// ID and name.
	idxID = b.nextIndexID(table)
	newIdx := table.GetPrimaryIndex().IndexDescDeepCopy()
	newIdx.Name = tabledesc.GenerateUniqueName(
		"new_primary_key",
		func(name string) bool {
			// TODO (lucy): Also check the new indexes specified in the targets.
			_, err := table.FindIndexWithName(name)
			return err == nil
		},
	)
	newIdx.ID = idxID

	if !table.GetPrimaryIndex().CollectKeyColumnIDs().Contains(colID) &&
		!table.GetPrimaryIndex().CollectPrimaryStoredColumnIDs().Contains(colID) {
		newIdx.StoreColumnIDs = append(newIdx.StoreColumnIDs, colID)
		newIdx.StoreColumnNames = append(newIdx.StoreColumnNames, colName)
	}

	newPrimaryIndex, newPrimaryIndexName := primaryIndexElemFromDescriptor(&newIdx, table)
	b.addNode(scpb.Target_ADD, newPrimaryIndex)
	b.addNode(scpb.Target_ADD, newPrimaryIndexName)

	// Drop the existing primary index.
	oldPrimaryIndex, oldPrimaryIndexName := primaryIndexElemFromDescriptor(table.GetPrimaryIndex().IndexDesc(), table)
	b.addNode(scpb.Target_DROP, oldPrimaryIndex)
	b.addNode(scpb.Target_DROP, oldPrimaryIndexName)

	return idxID
}

// TODO (lucy): refactor this to share with the add column case.
func (b *buildContext) addOrUpdatePrimaryIndexTargetsForDropColumn(
	table catalog.TableDescriptor, colID descpb.ColumnID,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for _, n := range b.output.Nodes {
		if t, ok := n.Element().(*scpb.PrimaryIndex); ok &&
			n.Target.Direction == scpb.Target_ADD &&
			t.TableID == table.GetID() {
			for j := range t.StoringColumnIDs {
				if t.StoringColumnIDs[j] == colID {
					t.StoringColumnIDs = append(t.StoringColumnIDs[:j], t.StoringColumnIDs[j+1:]...)
					return t.IndexID
				}

				panic("index not found")
			}
		}
	}

	// Create a new primary index, identical to the existing one except for its
	// ID and name.
	idxID = b.nextIndexID(table)
	newIdx := table.GetPrimaryIndex().IndexDescDeepCopy()
	newIdx.Name = tabledesc.GenerateUniqueName(
		"new_primary_key",
		func(name string) bool {
			// TODO (lucy): Also check the new indexes specified in the targets.
			_, err := table.FindIndexWithName(name)
			return err == nil
		},
	)
	newIdx.ID = idxID
	for j, id := range newIdx.KeyColumnIDs {
		if id == colID {
			newIdx.KeyColumnIDs = append(newIdx.KeyColumnIDs[:j], newIdx.KeyColumnIDs[j+1:]...)
			newIdx.KeyColumnNames = append(newIdx.KeyColumnNames[:j], newIdx.KeyColumnNames[j+1:]...)
			break
		}
	}
	for j, id := range newIdx.StoreColumnIDs {
		if id == colID {
			newIdx.StoreColumnIDs = append(newIdx.StoreColumnIDs[:j], newIdx.StoreColumnIDs[j+1:]...)
			newIdx.StoreColumnNames = append(newIdx.StoreColumnNames[:j], newIdx.StoreColumnNames[j+1:]...)
			break
		}
	}

	newPrimaryIndex, newPrimaryIndexName := primaryIndexElemFromDescriptor(&newIdx, table)
	b.addNode(scpb.Target_ADD, newPrimaryIndex)
	b.addNode(scpb.Target_ADD, newPrimaryIndexName)

	// Drop the existing primary index.
	oldPrimaryIndex, oldPrimaryIndexName :=
		primaryIndexElemFromDescriptor(table.GetPrimaryIndex().IndexDesc(), table)
	b.addNode(scpb.Target_DROP, oldPrimaryIndex)
	b.addNode(scpb.Target_DROP, oldPrimaryIndexName)
	return idxID
}

// Suppress the linter. We're not ready to fully implement this schema change
// yet.
var _ = (*buildContext)(nil).addOrUpdatePrimaryIndexTargetsForDropColumn

func (b *buildContext) nextColumnID(table catalog.TableDescriptor) descpb.ColumnID {
	nextColID := table.GetNextColumnID()
	var maxColID descpb.ColumnID

	for _, n := range b.output.Nodes {
		if n.Target.Direction != scpb.Target_ADD || screl.GetDescID(n.Element()) != table.GetID() {
			continue
		}
		if ac, ok := n.Element().(*scpb.Column); ok {
			if ac.ColumnID > maxColID {
				maxColID = ac.ColumnID
			}
		}
	}
	if maxColID != 0 {
		nextColID = maxColID + 1
	}
	return nextColID
}

func (b *buildContext) nextIndexID(table catalog.TableDescriptor) descpb.IndexID {
	nextMaxID := table.GetNextIndexID()
	var maxIdxID descpb.IndexID
	for _, n := range b.output.Nodes {
		if n.Target.Direction != scpb.Target_ADD || screl.GetDescID(n.Element()) != table.GetID() {
			continue
		}
		if ai, ok := n.Element().(*scpb.SecondaryIndex); ok {
			if ai.IndexID > maxIdxID {
				maxIdxID = ai.IndexID
			}
		} else if ai, ok := n.Element().(*scpb.PrimaryIndex); ok {
			if ai.IndexID > maxIdxID {
				maxIdxID = ai.IndexID
			}
		}
	}
	if maxIdxID != 0 {
		nextMaxID = maxIdxID + 1
	}
	return nextMaxID
}

func (b *buildContext) dropTableDesc(
	ctx context.Context, table catalog.TableDescriptor, behavior tree.DropBehavior,
) {
	onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, table, privilege.DROP))
	b.decomposeTableDescToElements(ctx, table, scpb.Target_DROP)
	lastSourceID := b.setSourceElementID(b.newSourceElementID())
	// Go over the dependencies and generate drop targets
	// for them. In our case they should only be views.
	scpb.ForEachRelationDependedOnBy(b.output, func(dep *scpb.RelationDependedOnBy) error {
		if dep.TableID != table.GetID() {
			return nil
		}
		dependentDesc := mustReadTable(ctx, b.Dependencies, dep.DependedOnBy)
		if !dependentDesc.IsView() {
			panic(errors.AssertionFailedf("descriptor :%s is not a view", dependentDesc.GetName()))
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, dependentDesc, privilege.DROP))
		if behavior != tree.DropCascade {

			name, err := b.CatalogReader().GetQualifiedTableNameByID(ctx,
				int64(table.GetID()),
				tree.ResolveRequireTableDesc)
			onErrPanic(err)

			depViewName, err := b.CatalogReader().GetQualifiedTableNameByID(ctx,
				int64(dep.DependedOnBy),
				tree.ResolveRequireViewDesc)
			onErrPanic(err)
			panic(errors.WithHintf(
				sqlerrors.NewDependentObjectErrorf("cannot drop table %q because view %q depends on it",
					name, depViewName.FQString()),
				"you can drop %s instead.", depViewName.FQString()))
		}
		// Decompose and recursively attempt to drop
		b.maybeDropViewAndDependents(ctx, dependentDesc, behavior)
		b.setSourceElementID(lastSourceID)
		return nil
	})
	// Detect if foreign keys will end up preventing this drop behavior.
	scpb.ForEachForeignKeyBackReference(b.output,
		func(fk *scpb.ForeignKeyBackReference) error {
			dependentTable := mustReadTable(ctx, b.Dependencies, fk.ReferenceID)
			if fk.OriginID == table.GetID() {
				if behavior != tree.DropCascade {
					panic(pgerror.Newf(
						pgcode.DependentObjectsStillExist,
						"%q is referenced by foreign key from table %q", fk.Name, dependentTable.GetName()))
				}
			}
			return nil
		})
	// Detect any sequence ownerships and prevent clean up if cascades
	// are disallowed.
	scpb.ForEachSequenceOwnedBy(b.output, func(sequenceOwnedBy *scpb.SequenceOwnedBy) error {
		if sequenceOwnedBy.OwnerTableID != table.GetID() {
			return nil
		}
		sequence := mustReadTable(ctx, b.Dependencies, sequenceOwnedBy.SequenceID)
		if behavior != tree.DropCascade {
			panic(pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop table %s because other objects depend on it",
				sequence.GetName(),
			))
		}
		return nil
	})
}

func (b *buildContext) dropTable(ctx context.Context, n *tree.DropTable) {
	// Find the table first.
	for _, name := range n.Names {
		_, table := b.CatalogReader().MayResolveTable(ctx, *name.ToUnresolvedObjectName())
		if table == nil {
			if n.IfExists {
				continue
			}
			panic(sqlerrors.NewUndefinedRelationError(&name))
		}
		if !table.IsTable() {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a table", table.GetName()))
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, table, privilege.DROP))
		b.dropTableDesc(ctx, table, n.DropBehavior)
		b.incrementSubWorkID()
	}
}
