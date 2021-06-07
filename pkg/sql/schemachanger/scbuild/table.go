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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
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

	// Resolve the table.
	tn := n.Table.ToTableName()
	table, err := b.getTableDescriptorForLockingChange(ctx, &tn)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
			return
		}
		panic(err)
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

func (b *buildContext) alterTableAddColumn(
	ctx context.Context,
	table catalog.TableDescriptor,
	t *tree.AlterTableAddColumn,
	tn *tree.TableName,
) {
	d := t.ColumnDef

	version := b.EvalCtx.Settings.Version.ActiveVersionOrEmpty(ctx)
	toType, err := tree.ResolveType(ctx, d.Type, b.SemaCtx.GetTypeResolver())
	if err != nil {
		panic(err)
	}
	supported := isTypeSupportedInVersion(version, toType)
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
	col, idx, defaultExpr, err := tabledesc.MakeColumnDefDescs(ctx, d, b.SemaCtx, b.EvalCtx)
	if err != nil {
		panic(err)
	}
	colID := b.nextColumnID(table)
	col.ID = colID

	// If the new column has a DEFAULT expression that uses a sequence, add
	// references between its descriptor and this column descriptor.
	if d.HasDefaultExpr() {
		b.maybeAddSequenceReferenceDependencies(
			ctx, b.EvalCtx.Settings, table.GetID(), col, defaultExpr,
		)
	}

	b.validateColumnName(table, d, col, t.IfNotExists)

	familyID := descpb.FamilyID(0)
	familyName := string(d.Family.Name)
	// TODO(ajwerner,lucy-zhang): Figure out how to compute the default column ID
	// for the family.
	if d.HasColumnFamily() {
		familyID = b.findOrAddColumnFamily(
			table, familyName, d.Family.Create, d.Family.IfNotExists,
		)
	} else {
		// TODO(ajwerner,lucy-zhang): Deal with adding the first column to the
		// table.
		fam := table.GetFamilies()[0]
		familyID = fam.ID
		familyName = fam.Name
	}

	if d.IsComputed() {
		// TODO (lucy): This is not going to work when the computed column
		// references columns created in the same transaction.
		serializedExpr, err := schemaexpr.ValidateComputedColumnExpression(
			ctx, table, d, tn, b.SemaCtx,
		)
		if err != nil {
			panic(err)
		}
		col.ComputeExpr = &serializedExpr
	}

	b.addNode(scpb.Target_ADD, &scpb.Column{
		TableID:    table.GetID(),
		Column:     *col,
		FamilyID:   familyID,
		FamilyName: familyName,
	})
	newPrimaryIdxID := b.addOrUpdatePrimaryIndexTargetsForAddColumn(table, colID, col.Name)

	if idx != nil {
		idxID := b.nextIndexID(table)
		idx.ID = idxID
		b.addNode(scpb.Target_ADD, &scpb.SecondaryIndex{
			TableID:      table.GetID(),
			Index:        *idx,
			PrimaryIndex: newPrimaryIdxID,
		})
	}
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
	for _, n := range b.outputNodes {
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
	for _, n := range b.outputNodes {
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
	if b.EvalCtx.SessionData.SafeUpdates {
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
	for _, n := range b.outputNodes {
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
	ctx context.Context,
	st *cluster.Settings,
	tableID descpb.ID,
	col *descpb.ColumnDescriptor,
	defaultExpr tree.TypedExpr,
) {
	seqIdentifiers, err := sequence.GetUsedSequences(defaultExpr)
	if err != nil {
		panic(err)
	}
	version := st.Version.ActiveVersionOrEmpty(ctx)
	byID := version != (clusterversion.ClusterVersion{}) &&
		version.IsActive(clusterversion.SequencesRegclass)

	var tn tree.TableName
	seqNameToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		if seqIdentifier.IsByID() {
			name, err := b.SemaCtx.TableNameResolver.GetQualifiedTableNameByID(
				ctx, seqIdentifier.SeqID, tree.ResolveRequireSequenceDesc)
			if err != nil {
				panic(err)
			}
			tn = *name
		} else {
			parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
			if err != nil {
				panic(err)
			}
			tn = parsedSeqName.ToTableName()
		}

		seqDesc, err := b.getTableDescriptor(ctx, &tn)
		if err != nil {
			panic(err)
		}
		seqNameToID[seqIdentifier.SeqName] = int64(seqDesc.GetID())

		col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.GetID())
		b.addNode(scpb.Target_ADD, &scpb.SequenceDependency{
			SequenceID: seqDesc.GetID(),
			TableID:    tableID,
			ColumnID:   col.ID,
			ByID:       byID,
		})
	}

	if len(seqIdentifiers) > 0 && byID {
		newExpr, err := sequence.ReplaceSequenceNamesWithIDs(defaultExpr, seqNameToID)
		if err != nil {
			panic(err)
		}
		s := tree.Serialize(newExpr)
		col.DefaultExpr = &s
	}
}

func (b *buildContext) addOrUpdatePrimaryIndexTargetsForAddColumn(
	table catalog.TableDescriptor, colID descpb.ColumnID, colName string,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for i, n := range b.outputNodes {
		if t, ok := n.Element().(*scpb.PrimaryIndex); ok &&
			b.outputNodes[i].Target.Direction == scpb.Target_ADD &&
			t.TableID == table.GetID() {
			t.StoreColumnIDs = append(t.StoreColumnIDs, colID)
			t.StoreColumnNames = append(t.StoreColumnNames, colName)
			return t.Index.ID
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

	var storeColIDs []descpb.ColumnID
	var storeColNames []string
	for _, col := range table.PublicColumns() {
		containsCol := false
		for _, id := range newIdx.KeyColumnIDs {
			if id == col.GetID() {
				containsCol = true
				break
			}
		}
		if !containsCol {
			storeColIDs = append(storeColIDs, col.GetID())
			storeColNames = append(storeColNames, col.GetName())
		}
	}

	b.addNode(scpb.Target_ADD, &scpb.PrimaryIndex{
		TableID:             table.GetID(),
		Index:               newIdx,
		OtherPrimaryIndexID: table.GetPrimaryIndexID(),
		StoreColumnIDs:      append(storeColIDs, colID),
		StoreColumnNames:    append(storeColNames, colName),
	})

	// Drop the existing primary index.
	b.addNode(scpb.Target_DROP, &scpb.PrimaryIndex{
		TableID:             table.GetID(),
		Index:               table.GetPrimaryIndex().IndexDescDeepCopy(),
		OtherPrimaryIndexID: newIdx.ID,
		StoreColumnIDs:      storeColIDs,
		StoreColumnNames:    storeColNames,
	})

	return idxID
}

// TODO (lucy): refactor this to share with the add column case.
func (b *buildContext) addOrUpdatePrimaryIndexTargetsForDropColumn(
	table catalog.TableDescriptor, colID descpb.ColumnID,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for _, n := range b.outputNodes {
		if t, ok := n.Element().(*scpb.PrimaryIndex); ok &&
			n.Target.Direction == scpb.Target_ADD &&
			t.TableID == table.GetID() {
			for j := range t.StoreColumnIDs {
				if t.StoreColumnIDs[j] == colID {
					t.StoreColumnIDs = append(t.StoreColumnIDs[:j], t.StoreColumnIDs[j+1:]...)
					t.StoreColumnNames = append(t.StoreColumnNames[:j], t.StoreColumnNames[j+1:]...)
					return t.Index.ID
				}

				panic("index not found")
			}
		}
	}

	// Create a new primary index, identical to the existing one except for its
	// ID and name.
	idxID = b.nextIndexID(table)
	newIdx := protoutil.Clone(table.GetPrimaryIndex().IndexDesc()).(*descpb.IndexDescriptor)
	newIdx.Name = tabledesc.GenerateUniqueName(
		"new_primary_key",
		func(name string) bool {
			// TODO (lucy): Also check the new indexes specified in the targets.
			_, err := table.FindIndexWithName(name)
			return err == nil
		},
	)
	newIdx.ID = idxID

	var addStoreColIDs []descpb.ColumnID
	var addStoreColNames []string
	var dropStoreColIDs []descpb.ColumnID
	var dropStoreColNames []string
	for _, col := range table.PublicColumns() {
		containsCol := false
		for _, id := range newIdx.KeyColumnIDs {
			if id == col.GetID() {
				containsCol = true
				break
			}
		}
		if !containsCol {
			if colID != col.GetID() {
				addStoreColIDs = append(addStoreColIDs, col.GetID())
				addStoreColNames = append(addStoreColNames, col.GetName())
			}
			dropStoreColIDs = append(dropStoreColIDs, col.GetID())
			dropStoreColNames = append(dropStoreColNames, col.GetName())
		}
	}

	b.addNode(scpb.Target_ADD, &scpb.PrimaryIndex{
		TableID:             table.GetID(),
		Index:               *newIdx,
		OtherPrimaryIndexID: table.GetPrimaryIndexID(),
		StoreColumnIDs:      addStoreColIDs,
		StoreColumnNames:    addStoreColNames,
	})

	// Drop the existing primary index.
	b.addNode(scpb.Target_DROP, &scpb.PrimaryIndex{
		TableID:             table.GetID(),
		Index:               *(protoutil.Clone(table.GetPrimaryIndex().IndexDesc()).(*descpb.IndexDescriptor)),
		OtherPrimaryIndexID: idxID,
		StoreColumnIDs:      dropStoreColIDs,
		StoreColumnNames:    dropStoreColNames,
	})
	return idxID
}

// Suppress the linter. We're not ready to fully implement this schema change
// yet.
var _ = (*buildContext)(nil).addOrUpdatePrimaryIndexTargetsForDropColumn

func (b *buildContext) nextColumnID(table catalog.TableDescriptor) descpb.ColumnID {
	nextColID := table.GetNextColumnID()
	var maxColID descpb.ColumnID

	for _, n := range b.outputNodes {
		if n.Target.Direction != scpb.Target_ADD || n.Element().DescriptorID() != table.GetID() {
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
	for _, n := range b.outputNodes {
		if n.Target.Direction != scpb.Target_ADD || n.Element().DescriptorID() != table.GetID() {
			continue
		}
		if ai, ok := n.Element().(*scpb.SecondaryIndex); ok {
			if ai.Index.ID > maxIdxID {
				maxIdxID = ai.Index.ID
			}
		} else if ai, ok := n.Element().(*scpb.PrimaryIndex); ok {
			if ai.Index.ID > maxIdxID {
				maxIdxID = ai.Index.ID
			}
		}
	}
	if maxIdxID != 0 {
		nextMaxID = maxIdxID + 1
	}
	return nextMaxID
}

// minimumTypeUsageVersions defines the minimum version needed for a new
// data type.
var minimumTypeUsageVersions = map[types.Family]clusterversion.Key{
	types.GeographyFamily: clusterversion.GeospatialType,
	types.GeometryFamily:  clusterversion.GeospatialType,
	types.Box2DFamily:     clusterversion.Box2DType,
}

// isTypeSupportedInVersion returns whether a given type is supported in the given version.
// This is copied straight from the sql package.
func isTypeSupportedInVersion(v clusterversion.ClusterVersion, t *types.T) bool {
	// For these checks, if we have an array, we only want to find whether
	// we support the array contents.
	if t.Family() == types.ArrayFamily {
		t = t.ArrayContents()
	}

	minVersion, ok := minimumTypeUsageVersions[t.Family()]
	if !ok {
		return true
	}
	return v.IsActive(minVersion)
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
			table, err := b.Descs.GetMutableTableByID(ctx, b.EvalCtx.Txn, seqID, tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc))
			if err != nil {
				panic(err)
			}
			if behavior != tree.DropCascade {
				panic(pgerror.Newf(
					pgcode.DependentObjectsStillExist,
					"cannot drop table %s because other objects depend on it",
					table.GetName(),
				))
			}
			err = b.AuthAccessor.CheckPrivilege(ctx, table, privilege.DROP)
			if err != nil {
				panic(err)
			}
			b.dropSequenceDesc(ctx, table, tree.DropCascade)
		}
		// Setup logic to clean up the default expression,
		// only if sequences are depending on it.
		if col.NumUsesSequences() > 0 {
			b.addNode(scpb.Target_DROP,
				&scpb.DefaultExpression{
					DefaultExpr:     col.GetDefaultExpr(),
					TableID:         table.GetID(),
					UsesSequenceIDs: col.ColumnDesc().UsesSequenceIds,
					ColumnID:        col.GetID()})
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
) {
	// Loop through and update inbound and outbound
	// foreign key references.
	for _, fk := range table.GetInboundFKs() {
		dependentTable, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn, fk.OriginTableID, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			panic(err)
		}
		if behavior != tree.DropCascade {
			panic(pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"%q is referenced by foreign key from table %q", fk.Name, dependentTable.GetName()))
		}
		err = b.AuthAccessor.CheckPrivilege(ctx, dependentTable, privilege.DROP)
		if err != nil {
			panic(err)
		}
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
	}

	for _, fk := range table.GetOutboundFKs() {
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
	}
}

func (b *buildContext) dropTableDesc(
	ctx context.Context, table catalog.TableDescriptor, behavior tree.DropBehavior,
) {
	// Interleaved tables not supported in new schema changer.
	if table.IsInterleaved() {
		panic(&notImplementedError{
			n: &tree.DropTable{
				Names: []tree.TableName{
					tree.MakeUnqualifiedTableName(tree.Name(table.GetName())),
				},
			},
			detail: "drop on interleaved table"})
	}

	// Drop dependent views
	err := table.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		dependentDesc, err := b.Descs.GetImmutableTableByID(ctx, b.EvalCtx.Txn, dep.ID, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			panic(err)
		}
		if behavior != tree.DropCascade {
			return pgerror.Newf(
				pgcode.DependentObjectsStillExist, "cannot drop table %q because view %q depends on it",
				table.GetName(), dependentDesc.GetName())
		}
		err = b.AuthAccessor.CheckPrivilege(ctx, dependentDesc, privilege.DROP)
		if err != nil {
			panic(err)
		}
		b.maybeDropViewAndDependents(ctx, dependentDesc, behavior)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Clean up foreign key references (both inbound
	// and out bound).
	b.maybeCleanTableFKs(ctx, table, behavior)

	// Clean up sequence references and ownerships.
	b.maybeCleanTableSequenceRefs(ctx, table, behavior)

	// Clean up type back references
	b.removeTypeBackRefDeps(ctx, table)
	b.addNode(scpb.Target_DROP,
		&scpb.Table{TableID: table.GetID()})
}

func (b *buildContext) dropTable(ctx context.Context, n *tree.DropTable) {
	// Find the table first.
	for _, name := range n.Names {
		table, err := resolver.ResolveExistingTableObject(ctx, b.Res, &name,
			tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
				return
			}
			panic(err)
		}
		if table == nil {
			panic(errors.AssertionFailedf("Unable to resolve table %s",
				name.FQString()))
		}
		b.dropTableDesc(ctx, table, n.DropBehavior)
	}
}
