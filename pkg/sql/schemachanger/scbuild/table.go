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
	"github.com/lib/pq/oid"
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
) *scpb.PrimaryIndex {
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
		IndexId:             indexDesc.ID,
		IndexName:           indexDesc.Name,
		Unique:              indexDesc.Unique,
		KeyColumnIDs:        indexDesc.KeyColumnIDs,
		KeyColumnDirections: keyColumnDirs,
		KeySuffixColumnIDs:  indexDesc.KeySuffixColumnIDs,
		StoringColumnIDs:    indexDesc.StoreColumnIDs,
		CompositeColumnIDs:  indexDesc.CompositeColumnIDs,
		Inverted:            indexDesc.Type == descpb.IndexDescriptor_INVERTED,
		ShardedDescriptor:   &indexDesc.Sharded}
}

func secondaryIndexElemFromDescriptor(
	indexDesc *descpb.IndexDescriptor, tableDesc catalog.TableDescriptor,
) *scpb.SecondaryIndex {
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
		IndexId:             indexDesc.ID,
		IndexName:           indexDesc.Name,
		Unique:              indexDesc.Unique,
		KeyColumnIDs:        indexDesc.KeyColumnIDs,
		KeyColumnDirections: keyColumnDirs,
		KeySuffixColumnIDs:  indexDesc.KeySuffixColumnIDs,
		StoringColumnIDs:    indexDesc.StoreColumnIDs,
		CompositeColumnIDs:  indexDesc.CompositeColumnIDs,
		Inverted:            indexDesc.Type == descpb.IndexDescriptor_INVERTED,
		ShardedDescriptor:   &indexDesc.Sharded}
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

	familyID := descpb.FamilyID(0)
	familyName := string(d.Family.Name)
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
			b.addNode(scpb.Target_ADD, &scpb.TypeReference{
				TypeID: typeDesc.GetID(),
				DescID: table.GetID(),
			})
		}
	}
	b.addNode(scpb.Target_ADD, &scpb.Column{
		TableID:    table.GetID(),
		Column:     *col,
		FamilyID:   familyID,
		FamilyName: familyName,
	})
	// Virtual computed columns do not exist inside the primary index,
	if !col.Virtual {
		b.addOrUpdatePrimaryIndexTargetsForAddColumn(table, colID, col.Name)
		if idx := cdd.PrimaryKeyOrUniqueIndexDescriptor; idx != nil {
			idxID := b.nextIndexID(table)
			idx.ID = idxID
			secondaryIndex := secondaryIndexElemFromDescriptor(idx, table)
			b.addNode(scpb.Target_ADD, secondaryIndex)
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
		case *scpb.Column:
			if t.TableID != table.GetID() || t.Column.Name != string(d.Name) {
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
	// Check whether the column is being dropped.
	for _, n := range b.output.Nodes {
		switch col := n.Element().(type) {
		case *scpb.Column:
			if col.TableID != table.GetID() ||
				n.Target.Direction != scpb.Target_DROP ||
				col.Column.ColName() != t.Column {
				continue
			}
			// Column drops are, while the column is in the process of being dropped,
			// for whatever reason, idempotent. Return silently here.
			return
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
			b.addNode(scpb.Target_DROP, &scpb.TypeReference{
				TypeID: typ.GetID(),
				DescID: table.GetID(),
			})
		}
	}

	// TODO(ajwerner): Add family information to the column.
	b.addNode(scpb.Target_DROP, &scpb.Column{
		TableID: table.GetID(),
		Column:  *colToDrop.ColumnDesc(),
	})

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
			return t.IndexId
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

	b.addNode(scpb.Target_ADD, primaryIndexElemFromDescriptor(&newIdx, table))

	// Drop the existing primary index.
	b.addNode(scpb.Target_DROP, primaryIndexElemFromDescriptor(table.GetPrimaryIndex().IndexDesc(), table))

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
					return t.IndexId
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

	b.addNode(scpb.Target_ADD, primaryIndexElemFromDescriptor(&newIdx, table))

	// Drop the existing primary index.
	b.addNode(scpb.Target_DROP, primaryIndexElemFromDescriptor(table.GetPrimaryIndex().IndexDesc(), table))
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
			if ac.Column.ID > maxColID {
				maxColID = ac.Column.ID
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
			if ai.IndexId > maxIdxID {
				maxIdxID = ai.IndexId
			}
		} else if ai, ok := n.Element().(*scpb.PrimaryIndex); ok {
			if ai.IndexId > maxIdxID {
				maxIdxID = ai.IndexId
			}
		}
	}
	if maxIdxID != 0 {
		nextMaxID = maxIdxID + 1
	}
	return nextMaxID
}

func (b *buildContext) maybeCleanTableSequenceRefs(
	ctx context.Context, table catalog.TableDescriptor, behavior tree.DropBehavior,
) {
	// Setup nodes for dropping sequences
	// and cleaning up default expressions.
	for _, col := range table.PublicColumns() {
		// Loop over owned sequences
		for seqIdx := 0; seqIdx < col.NumOwnsSequences(); seqIdx++ {
			seqID := col.GetOwnsSequenceID(seqIdx)
			seq := mustReadTable(ctx, b, seqID)
			if behavior != tree.DropCascade {
				panic(pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"cannot drop table %s because other objects depend on it",
					seq.GetName(),
				))
			}
			onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, seq, privilege.DROP))
			b.dropSequenceDesc(ctx, seq, tree.DropCascade)
		}
		// Setup logic to clean up the default expression always.
		defaultExpr := &scpb.DefaultExpression{
			DefaultExpr:     col.GetDefaultExpr(),
			TableID:         table.GetID(),
			UsesSequenceIDs: col.ColumnDesc().UsesSequenceIds,
			ColumnID:        col.GetID()}
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, defaultExpr); !exists {
			b.addNode(scpb.Target_DROP, defaultExpr)
		}
		// Get all available type references and create nodes
		// for dropping these type references.
		visitor := &tree.TypeCollectorVisitor{
			OIDs: make(map[oid.Oid]struct{}),
		}
		if col.HasDefault() && !col.ColumnDesc().HasNullDefault() {
			expr, err := parser.ParseExpr(col.GetDefaultExpr())
			onErrPanic(err)
			tree.WalkExpr(visitor, expr)
			for oid := range visitor.OIDs {
				typeID, err := typedesc.UserDefinedTypeOIDToID(oid)
				onErrPanic(err)
				typeRef := &scpb.TypeReference{
					TypeID: typeID,
					DescID: table.GetID(),
				}
				if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, typeRef); !exists {
					b.addNode(scpb.Target_DROP, typeRef)
				}
			}
		}

		// If there was a sequence dependency clean that up next.
		if col.NumUsesSequences() > 0 {
			// Drop the depends on within the sequence side.
			for seqOrd := 0; seqOrd < col.NumUsesSequences(); seqOrd++ {
				seqID := col.GetUsesSequenceID(seqOrd)
				// Remove dependencies to this sequences.
				dropDep := &scpb.RelationDependedOnBy{TableID: seqID,
					DependedOnBy: table.GetID()}
				if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, dropDep); !exists {
					b.addNode(scpb.Target_DROP, dropDep)
				}
			}
		}
	}
}

func (b *buildContext) maybeCleanTableFKs(
	ctx context.Context, table catalog.TableDescriptor, behavior tree.DropBehavior,
) { // Loop through and update inbound and outbound
	// foreign key references.
	_ = table.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		dependentTable := mustReadTable(ctx, b, fk.OriginTableID)
		if behavior != tree.DropCascade {
			panic(pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"%q is referenced by foreign key from table %q", fk.Name, dependentTable.GetName()))
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, dependentTable, privilege.DROP))
		outFkNode := &scpb.OutboundForeignKey{
			OriginID:         fk.OriginTableID,
			OriginColumns:    fk.OriginColumnIDs,
			ReferenceID:      fk.ReferencedTableID,
			ReferenceColumns: fk.ReferencedColumnIDs,
			Name:             fk.Name,
		}
		inFkNode := &scpb.InboundForeignKey{
			OriginID:         fk.ReferencedTableID,
			OriginColumns:    fk.ReferencedColumnIDs,
			ReferenceID:      fk.OriginTableID,
			ReferenceColumns: fk.OriginColumnIDs,
			Name:             fk.Name,
		}
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, outFkNode); !exists {
			b.addNode(scpb.Target_DROP,
				outFkNode)
		}
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, inFkNode); !exists {
			b.addNode(scpb.Target_DROP,
				inFkNode)
		}
		return nil
	})

	_ = table.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		outFkNode := &scpb.OutboundForeignKey{
			OriginID:         fk.OriginTableID,
			OriginColumns:    fk.OriginColumnIDs,
			ReferenceID:      fk.ReferencedTableID,
			ReferenceColumns: fk.ReferencedColumnIDs,
			Name:             fk.Name,
		}
		inFkNode := &scpb.InboundForeignKey{
			OriginID:         fk.ReferencedTableID,
			OriginColumns:    fk.ReferencedColumnIDs,
			ReferenceID:      fk.OriginTableID,
			ReferenceColumns: fk.OriginColumnIDs,
			Name:             fk.Name,
		}
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, outFkNode); !exists {
			b.addNode(scpb.Target_DROP,
				outFkNode)
		}
		if exists, _ := b.checkIfNodeExists(scpb.Target_DROP, inFkNode); !exists {
			b.addNode(scpb.Target_DROP,
				inFkNode)
		}
		return nil
	})
}

func (b *buildContext) dropTableDesc(
	ctx context.Context, table catalog.TableDescriptor, behavior tree.DropBehavior,
) {
	lastSourceID := b.setSourceElementID(b.newSourceElementID())
	// Drop dependent views
	onErrPanic(table.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		dependentDesc := mustReadTable(ctx, b, dep.ID)
		if behavior != tree.DropCascade {
			return pgerror.Newf(
				pgcode.DependentObjectsStillExist, "cannot drop table %q because view %q depends on it",
				table.GetName(), dependentDesc.GetName())
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, dependentDesc, privilege.DROP))
		b.maybeDropViewAndDependents(ctx, dependentDesc, behavior)
		return nil
	}))
	// Clean up foreign key references (both inbound
	// and out bound).
	b.maybeCleanTableFKs(ctx, table, behavior)
	// Clean up sequence references and ownerships.
	b.maybeCleanTableSequenceRefs(ctx, table, behavior)
	// Clean up type back references
	b.removeTypeBackRefDeps(ctx, table)
	b.setSourceElementID(lastSourceID)
	b.addNode(scpb.Target_DROP,
		&scpb.Table{TableID: table.GetID()})
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
