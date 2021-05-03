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
	"fmt"
	"strings"

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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Eliminate all panics or add principled recovery.
// TODO(ajwerner): Add privilege checking.

// The Builder is the entry point for planning schema changes. From AST nodes
// for DDL statements, it constructs targets which represent schema changes to
// be performed.
//
// The builder itself is essentially stateless aside from the dependencies it
// needs to resolve (immutable) descriptors, evaluate expressions, etc. The
// methods in its API take schema change graph nodes (i.e., targets and their
// current states) and DDL statement AST nodes, and output new schema change
// graph nodes that incorporate targets that were added or changed.
type Builder struct {
	// TODO(ajwerner): Inject a better interface than this.
	res     resolver.SchemaResolver
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext

	// nodes contains the internal state when building targets for an individual
	// statement.
	nodes []*scpb.Node
}

type notImplementedError struct {
	n      tree.NodeFormatter
	detail string
}

// TODO(ajwerner): Deal with redaction.

var _ error = (*notImplementedError)(nil)

// HasNotImplemented returns true if the error indicates that the builder does
// not support the provided statement.
func HasNotImplemented(err error) bool {
	return errors.HasType(err, (*notImplementedError)(nil))
}

func (e *notImplementedError) Error() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%T not implemented in the new schema changer", e.n)
	if e.detail != "" {
		fmt.Fprintf(&buf, ": %s", e.detail)
	}
	return buf.String()
}

// ConcurrentSchemaChangeError indicates that building the schema change plan
// is not currently possible because there are other concurrent schema changes
// on one of the descriptors.
type ConcurrentSchemaChangeError struct {
	// TODO(ajwerner): Instead of waiting for one descriptor at a time, we should
	// get all the IDs of the descriptors we might be waiting for and return them
	// from the builder.
	descID descpb.ID
}

func (e *ConcurrentSchemaChangeError) Error() string {
	return fmt.Sprintf("descriptor %d is undergoing another schema change", e.descID)
}

// DescriptorID is the ID of the descriptor undergoing concurrent schema
// changes.
func (e *ConcurrentSchemaChangeError) DescriptorID() descpb.ID {
	return e.descID
}

// NewBuilder creates a new Builder.
func NewBuilder(
	res resolver.SchemaResolver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) *Builder {
	return &Builder{
		res:     res,
		semaCtx: semaCtx,
		evalCtx: evalCtx,
	}
}

// Build builds targets and transforms the provided schema change nodes
// accordingly, given a statement.
//
// TODO(ajwerner): Clarify whether the nodes will be mutated. Potentially just
// clone them defensively here. Similarly, close the statement as some schema
// changes mutate the AST. It's best if this method had a clear contract that
// it did not mutate its arguments.
func (b *Builder) Build(
	ctx context.Context, nodes []*scpb.Node, n tree.Statement,
) ([]*scpb.Node, error) {
	switch n := n.(type) {
	case *tree.AlterTable:
		return b.AlterTable(ctx, nodes, n)
	default:
		return nil, &notImplementedError{n: n}
	}
}

// AlterTable builds targets and transforms the provided schema change nodes
// accordingly, given an ALTER TABLE statement.
func (b *Builder) AlterTable(
	ctx context.Context, nodes []*scpb.Node, n *tree.AlterTable,
) ([]*scpb.Node, error) {
	// TODO (lucy): Clean this up.
	b.nodes = nodes
	defer func() {
		b.nodes = nil
	}()

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
			return nodes, nil
		}
		return nil, err
	}
	for _, cmd := range n.Cmds {
		if err := b.alterTableCmd(ctx, table, cmd, &tn); err != nil {
			return nil, err
		}
	}

	result := make([]*scpb.Node, len(b.nodes))
	for i := range b.nodes {
		result[i] = b.nodes[i]
	}
	return result, nil
}

func (b *Builder) alterTableCmd(
	ctx context.Context, table catalog.TableDescriptor, cmd tree.AlterTableCmd, tn *tree.TableName,
) error {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		return b.alterTableAddColumn(ctx, table, t, tn)
	default:
		return &notImplementedError{n: cmd}
	}
}

func (b *Builder) alterTableAddColumn(
	ctx context.Context,
	table catalog.TableDescriptor,
	t *tree.AlterTableAddColumn,
	tn *tree.TableName,
) error {
	d := t.ColumnDef

	version := b.evalCtx.Settings.Version.ActiveVersionOrEmpty(ctx)
	toType, err := tree.ResolveType(ctx, d.Type, b.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}
	if supported, err := isTypeSupportedInVersion(version, toType); err != nil {
		return err
	} else if !supported {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			toType.SQLString(),
		)
	}

	if d.IsSerial {
		return &notImplementedError{n: t.ColumnDef, detail: "contains serial data type"}
	}
	// Some of the building for the index exists below but end-to-end support is
	// not complete so we return an error.
	if d.Unique.IsUnique {
		return &notImplementedError{n: t.ColumnDef, detail: "contains unique constraint"}
	}
	col, idx, defaultExpr, err := tabledesc.MakeColumnDefDescs(ctx, d, b.semaCtx, b.evalCtx)
	if err != nil {
		return err
	}
	colID := b.nextColumnID(table)
	col.ID = colID

	// If the new column has a DEFAULT expression that uses a sequence, add
	// references between its descriptor and this column descriptor.
	if d.HasDefaultExpr() {
		if err := b.maybeAddSequenceReferenceDependencies(
			ctx, b.evalCtx.Settings, table.GetID(), col, defaultExpr,
		); err != nil {
			return err
		}
	}

	if err := b.validateColumnName(table, d, col, t.IfNotExists); err != nil {
		return err
	}

	familyID := descpb.FamilyID(0)
	familyName := string(d.Family.Name)
	// TODO(ajwerner,lucy-zhang): Figure out how to compute the default column ID
	// for the family.
	if d.HasColumnFamily() {
		if familyID, err = b.findOrAddColumnFamily(
			table, familyName, d.Family.Create, d.Family.IfNotExists,
		); err != nil {
			return err
		}
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
		computedColValidator := schemaexpr.MakeComputedColumnValidator(
			ctx,
			table,
			b.semaCtx,
			tn,
		)
		serializedExpr, err := computedColValidator.Validate(d)
		if err != nil {
			return err
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
	return nil
}

func (b *Builder) validateColumnName(
	table catalog.TableDescriptor,
	d *tree.ColumnTableDef,
	col *descpb.ColumnDescriptor,
	ifNotExists bool,
) error {
	_, err := tabledesc.FindPublicColumnWithName(table, d.Name)
	if err == nil {
		if ifNotExists {
			return nil
		}
		return sqlerrors.NewColumnAlreadyExistsError(string(d.Name), table.GetName())
	}
	for _, n := range b.nodes {
		switch t := n.Element().(type) {
		case *scpb.Column:
			if t.TableID != table.GetID() || t.Column.Name != string(d.Name) {
				continue
			}
			switch dir := n.Target.Direction; dir {
			case scpb.Target_ADD:
				return pgerror.Newf(pgcode.DuplicateColumn,
					"duplicate: column %q in the middle of being added, not yet public",
					col.Name)
			case scpb.Target_DROP:
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q being dropped, try again later", col.Name)
			default:
				return errors.AssertionFailedf("unknown direction %v in %v", dir, n.Target)
			}
		}
	}
	return nil
}

func (b *Builder) findOrAddColumnFamily(
	table catalog.TableDescriptor, family string, create bool, ifNotExists bool,
) (descpb.FamilyID, error) {
	if len(family) > 0 {
		for i := range table.GetFamilies() {
			f := &table.GetFamilies()[i]
			if f.Name == family {
				if create && !ifNotExists {
					return 0, errors.Errorf("family %q already exists", family)
				}
				return f.ID, nil
			}
		}
	}
	// See if we're in the process of adding a column or dropping a column in this
	// family.
	//
	// TODO(ajwerner): Decide what to do if the only column in a family of this
	// name is being dropped and then if there is or isn't a create directive.
	nextFamilyID := table.GetNextFamilyID()
	for _, n := range b.nodes {
		switch col := n.Element().(type) {
		case *scpb.Column:
			if col.TableID != table.GetID() {
				continue
			}
			if col.FamilyName == family {
				if create && !ifNotExists {
					return 0, errors.Errorf("family %q already exists", family)
				}
				return col.FamilyID, nil
			}
			if col.FamilyID >= nextFamilyID {
				nextFamilyID = col.FamilyID + 1
			}
		}
	}
	if !create {
		return 0, errors.Errorf("unknown family %q", family)
	}
	return nextFamilyID, nil
}

func (b *Builder) alterTableDropColumn(
	ctx context.Context, table catalog.TableDescriptor, t *tree.AlterTableDropColumn,
) error {
	if b.evalCtx.SessionData.SafeUpdates {
		return pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will " +
			"remove all data in that column")
	}

	// TODO(ajwerner): Deal with drop column for columns which are being added
	// currently.
	colToDrop, err := table.FindColumnWithName(t.Column)
	if err != nil {
		if t.IfExists {
			// Noop.
			return nil
		}
		return err
	}
	// Check whether the column is being dropped.
	for _, n := range b.nodes {
		switch col := n.Element().(type) {
		case *scpb.Column:
			if col.TableID != table.GetID() ||
				n.Target.Direction != scpb.Target_DROP ||
				col.Column.ColName() != t.Column {
				continue
			}
			// Column drops are, while the column is in the process of being dropped,
			// for whatever reason, idempotent. Return silently here.
			return nil
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
	return nil
}

// Suppress the linter. We're not ready to fully implement this schema change
// yet.
var _ = (*Builder)(nil).alterTableDropColumn

func (b *Builder) maybeAddSequenceReferenceDependencies(
	ctx context.Context,
	st *cluster.Settings,
	tableID descpb.ID,
	col *descpb.ColumnDescriptor,
	defaultExpr tree.TypedExpr,
) error {
	seqIdentifiers, err := sequence.GetUsedSequences(defaultExpr)
	if err != nil {
		return err
	}
	version := st.Version.ActiveVersionOrEmpty(ctx)
	byID := version != (clusterversion.ClusterVersion{}) &&
		version.IsActive(clusterversion.SequencesRegclass)

	var tn tree.TableName
	seqNameToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		if seqIdentifier.IsByID() {
			name, err := b.semaCtx.TableNameResolver.GetQualifiedTableNameByID(
				ctx, seqIdentifier.SeqID, tree.ResolveRequireSequenceDesc)
			if err != nil {
				return err
			}
			tn = *name
		} else {
			parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
			if err != nil {
				return err
			}
			tn = parsedSeqName.ToTableName()
		}
		seqDesc, err := b.getTableDescriptor(ctx, &tn)
		if err != nil {
			return err
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
			return err
		}
		s := tree.Serialize(newExpr)
		col.DefaultExpr = &s
	}

	return nil
}

func (b *Builder) addOrUpdatePrimaryIndexTargetsForAddColumn(
	table catalog.TableDescriptor, colID descpb.ColumnID, colName string,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for i, n := range b.nodes {
		if t, ok := n.Element().(*scpb.PrimaryIndex); ok &&
			b.nodes[i].Target.Direction == scpb.Target_ADD &&
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
	newIdx.Name = tabledesc.GenerateUniqueConstraintName(
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
		for _, id := range newIdx.ColumnIDs {
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
func (b *Builder) addOrUpdatePrimaryIndexTargetsForDropColumn(
	table catalog.TableDescriptor, colID descpb.ColumnID,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for _, n := range b.nodes {
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
	newIdx.Name = tabledesc.GenerateUniqueConstraintName(
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
		for _, id := range newIdx.ColumnIDs {
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
var _ = (*Builder)(nil).addOrUpdatePrimaryIndexTargetsForDropColumn

func (b *Builder) nextColumnID(table catalog.TableDescriptor) descpb.ColumnID {
	nextColID := table.GetNextColumnID()
	var maxColID descpb.ColumnID

	for _, n := range b.nodes {
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

func (b *Builder) nextIndexID(table catalog.TableDescriptor) descpb.IndexID {
	nextMaxID := table.GetNextIndexID()
	var maxIdxID descpb.IndexID
	for _, n := range b.nodes {
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

func (b *Builder) addNode(dir scpb.Target_Direction, elem scpb.Element) {
	var s scpb.State
	switch dir {
	case scpb.Target_ADD:
		s = scpb.State_ABSENT
	case scpb.Target_DROP:
		s = scpb.State_PUBLIC
	default:
		panic(errors.Errorf("unknown direction %s", dir))
	}
	b.nodes = append(b.nodes, &scpb.Node{
		Target: scpb.NewTarget(dir, elem),
		State:  s,
	})
}

// getTableDescriptorForLockingChange returns a table descriptor that is
// guaranteed to have no concurrent running schema changes and can therefore
// undergo a "locking" change, or else a ConcurrentSchemaChangeError if the
// table is not currently in the required state. Locking changes roughly
// correspond to schema changes with mutations, which must be serialized and
// (in the new schema changer) require mutual exclusion.
func (b *Builder) getTableDescriptorForLockingChange(
	ctx context.Context, tn *tree.TableName,
) (catalog.TableDescriptor, error) {
	table, err := b.getTableDescriptor(ctx, tn)
	if err != nil {
		return nil, err
	}
	if HasConcurrentSchemaChanges(table) {
		return nil, &ConcurrentSchemaChangeError{descID: table.GetID()}
	}
	return table, nil
}

func (b *Builder) getTableDescriptor(
	ctx context.Context, tn *tree.TableName,
) (catalog.TableDescriptor, error) {
	// This will return an error for dropped and offline tables, but it's possible
	// that later iterations of the builder will want to handle those cases
	// in a different way.
	return resolver.ResolveExistingTableObject(ctx, b.res, tn,
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:    true,
				AvoidCached: true,
			},
		},
	)
}

// HasConcurrentSchemaChanges returns whether the table descriptor is undergoing
// concurrent schema changes.
func HasConcurrentSchemaChanges(table catalog.TableDescriptor) bool {
	// TODO(ajwerner): For now we simply check for the absence of mutations. Once
	// we start implementing schema changes with ops to be executed during
	// statement execution, we'll have to take into account mutations that were
	// written in this transaction.
	return len(table.AllMutations()) > 0
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
func isTypeSupportedInVersion(v clusterversion.ClusterVersion, t *types.T) (bool, error) {
	// For these checks, if we have an array, we only want to find whether
	// we support the array contents.
	if t.Family() == types.ArrayFamily {
		t = t.ArrayContents()
	}

	minVersion, ok := minimumTypeUsageVersions[t.Family()]
	if !ok {
		return true, nil
	}
	return v.IsActive(minVersion), nil
}
