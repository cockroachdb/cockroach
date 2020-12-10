package builder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

type Builder struct {
	// TODO(ajwerner): Inject a better interface than this.
	res     resolver.SchemaResolver
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext

	targetStates []*targets.TargetState
}

func NewBuilder(
	res resolver.SchemaResolver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) *Builder {
	return &Builder{
		res:     res,
		semaCtx: semaCtx,
		evalCtx: evalCtx,
	}
}

func (b *Builder) AlterTable(
	ctx context.Context, ts []*targets.TargetState, n *tree.AlterTable,
) ([]*targets.TargetState, error) {
	// TODO (lucy): Clean this up.
	b.targetStates = ts
	defer func() {
		b.targetStates = nil
	}()

	// Resolve the table.
	tn := n.Table.ToTableName()
	table, err := resolver.ResolveExistingTableObject(ctx, b.res, &tn,
		tree.ObjectLookupFlagsWithRequired())
	if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
		return nil, err
	}
	for _, cmd := range n.Cmds {
		if err := b.alterTableCmd(ctx, table, cmd, &tn); err != nil {
			return nil, err
		}
	}

	result := make([]*targets.TargetState, len(b.targetStates))
	for i := range b.targetStates {
		result[i] = b.targetStates[i]
	}
	return result, nil
}

func (b *Builder) alterTableCmd(
	ctx context.Context, table *tabledesc.Immutable, cmd tree.AlterTableCmd, tn *tree.TableName,
) error {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		return b.alterTableAddColumn(ctx, table, t, tn)
	case *tree.AlterTableAddConstraint:

	case *tree.AlterTableAlterPrimaryKey:

	case *tree.AlterTableDropColumn:
		return b.alterTableDropColumn(ctx, table, t)
	case *tree.AlterTableDropConstraint:

	case *tree.AlterTableValidateConstraint:

	case tree.ColumnMutationCmd:

	case *tree.AlterTablePartitionBy:

	case *tree.AlterTableSetAudit:

	case *tree.AlterTableRenameColumn:

	case *tree.AlterTableOwner:

	default:
		return errors.AssertionFailedf("unsupported alter command: %T", cmd)
	}
	panic("not implemented")
}

func (b *Builder) alterTableAddColumn(
	ctx context.Context, table *tabledesc.Immutable, t *tree.AlterTableAddColumn, tn *tree.TableName,
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
		panic("not implemented")
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
		if err := b.maybeAddSequenceDependencies(ctx, table.ID, col, defaultExpr); err != nil {
			return err
		}
	}

	if err := b.validateColumnName(table, d, col, t.IfNotExists); err != nil {
		return err
	}

	b.addTargetState(
		&targets.AddColumn{
			TableID: table.GetID(),
			Column:  *col,
		},
		targets.StateAbsent,
	)

	newIdxID := b.maybeAddPrimaryIndexTargetsForColumnChange(table)

	if idx != nil {
		idxID := b.nextIndexID(table)
		idx.ID = idxID
		b.addTargetState(
			&targets.AddIndex{
				TableID:      table.GetID(),
				Index:        *idx,
				PrimaryIndex: newIdxID,
			},
			targets.StateAbsent,
		)
	}

	if d.HasColumnFamily() {
		if err := b.createOrUpdateColumnFamily(
			table, col, d.Family.Name, d.Family.Create, d.Family.IfNotExists,
		); err != nil {
			return err
		}
	}

	if d.IsComputed() {
		computedColValidator := schemaexpr.MakeComputedColumnValidator(
			ctx,
			table,
			b.semaCtx,
			tn,
		)
		if err := computedColValidator.Validate(d); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) validateColumnName(
	table *tabledesc.Immutable,
	d *tree.ColumnTableDef,
	col *descpb.ColumnDescriptor,
	ifNotExists bool,
) error {
	_, err := table.FindActiveColumnByName(string(d.Name))
	if err == nil {
		if ifNotExists {
			return nil
		}
		return sqlerrors.NewColumnAlreadyExistsError(string(d.Name), table.Name)
	}
	// We still need to look at the mutations to find columns being added from
	// other ongoing schema changes.
	if m := table.FindColumnMutationByName(d.Name); m != nil {
		switch m.Direction {
		case descpb.DescriptorMutation_ADD:
			return pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public",
				col.Name)
		case descpb.DescriptorMutation_DROP:
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q being dropped, try again later", col.Name)
		default:
			return errors.AssertionFailedf("invalid direction %s for column mutation %d",
				errors.Safe(m.Direction), col.ID)
		}
	}
	for _, ts := range b.targetStates {
		switch t := ts.Target.(type) {
		case *targets.AddColumn:
			if t.TableID == table.GetID() && t.Column.Name == string(d.Name) {
				return pgerror.Newf(pgcode.DuplicateColumn,
					"duplicate: column %q in the middle of being added, not yet public",
					col.Name)
			}
		case *targets.DropColumn:
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q being dropped, try again later", col.Name)
		}
	}
	return nil
}

// TODO: Deal with the rest of the logic in allocateColumnFamilyIDs.
func (b *Builder) createOrUpdateColumnFamily(
	table *tabledesc.Immutable,
	col *descpb.ColumnDescriptor,
	family tree.Name,
	create bool,
	ifNotExists bool,
) error {
	panic("unimplemented")
	idx := -1
	if len(family) > 0 {
		for i := range table.Families {
			if table.Families[i].Name == string(family) {
				idx = i
				break
			}
		}
	}

	if idx == -1 {
		if create {
			b.addTargetState(&targets.AddColumnFamilyDependency{
				TableID: table.GetID(),
				Family: descpb.ColumnFamilyDescriptor{
					Name:            string(family),
					ID:              0,
					ColumnNames:     []string{col.Name},
					ColumnIDs:       []descpb.ColumnID{col.ID},
					DefaultColumnID: 0,
				},
			}, targets.StateAbsent)
		}
	}
	return nil
}

func (b *Builder) alterTableDropColumn(
	ctx context.Context, table *tabledesc.Immutable, t *tree.AlterTableDropColumn,
) error {
	panic("unimplemented")

	colToDrop, dropped, err := table.FindColumnByName(t.Column)
	if err != nil {
		if t.IfExists {
			// Noop.
			return nil
		}
		return err
	}
	// TODO: this check should be coming from the targets, not the descriptor.
	if dropped {
		return err
	}

	// TODO: validation, cascades, etc.

	// Assume we've validated that the column isn't part of the PK.
	b.maybeAddPrimaryIndexTargetsForColumnChange(table)
	b.addTargetState(
		&targets.DropColumn{
			TableID:  table.GetID(),
			ColumnID: colToDrop.ID,
		},
		targets.StatePublic,
	)
	return nil
}

func (b *Builder) CreateIndex(ctx context.Context, n *tree.CreateIndex) error {
	// TODO: currently indexes are created in sql.MakeIndexDescriptor, but
	// populating the index with IDs, etc. happens in AllocateIDs.
	panic("unimplemented")
}

func (b *Builder) maybeAddSequenceDependencies(
	ctx context.Context, tableID descpb.ID, col *descpb.ColumnDescriptor, defaultExpr tree.TypedExpr,
) error {
	seqNames, err := sequence.GetUsedSequenceNames(defaultExpr)
	if err != nil {
		return err
	}
	for _, seqName := range seqNames {
		parsedSeqName, err := parser.ParseTableName(seqName)
		if err != nil {
			return err
		}
		tn := parsedSeqName.ToTableName()
		seqDesc, err := resolver.ResolveExistingTableObject(ctx, b.res, &tn,
			tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}

		col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.ID)
		b.addTargetState(
			&targets.AddSequenceDependency{
				TableID:    tableID,
				ColumnID:   col.ID,
				SequenceID: seqDesc.GetID(),
			},
			targets.StateAbsent,
		)
	}
	return nil
}

func (b *Builder) maybeAddPrimaryIndexTargetsForColumnChange(
	table *tabledesc.Immutable,
) (newIdxID descpb.IndexID) {
	// We need to build a new primary index in the presence of any column changes,
	// but the index doesn't otherwise depend any other metadata about the column
	// changes themselves, since the primary key itself is unaffected.
	for i := range b.targetStates {
		if t, ok := b.targetStates[i].Target.(*targets.AddIndex); ok &&
			t.TableID == table.GetID() && t.Primary && t.ReplacementFor != 0 {
			return t.Index.ID
		}
	}

	// Create a new primary index, identical to the existing one except for its
	// ID and name.
	newIdxID = b.nextIndexID(table)
	newIdx := protoutil.Clone(&table.PrimaryIndex).(*descpb.IndexDescriptor)
	newIdx.Name = tabledesc.GenerateUniqueConstraintName(
		"new_primary_key",
		func(name string) bool {
			// TODO (lucy): Also check the new indexes specified in the targets.
			_, _, err := table.FindIndexByName(name)
			return err == nil
		},
	)
	newIdx.ID = newIdxID

	b.addTargetState(
		&targets.AddIndex{
			TableID:        table.GetID(),
			Index:          *newIdx,
			PrimaryIndex:   table.GetPrimaryIndexID(),
			ReplacementFor: table.GetPrimaryIndexID(),
			Primary:        true,
		},
		targets.StateAbsent,
	)

	// Drop the existing primary index.
	b.addTargetState(
		&targets.DropIndex{
			TableID:    table.GetID(),
			IndexID:    table.GetPrimaryIndexID(),
			ReplacedBy: newIdxID,
			ColumnIDs:  table.PrimaryIndex.ColumnIDs,
		},
		targets.StatePublic,
	)

	return newIdxID
}

func (b *Builder) nextColumnID(table *tabledesc.Immutable) descpb.ColumnID {
	nextColID := table.GetNextColumnID()
	var maxColID descpb.ColumnID
	for _, ts := range b.targetStates {
		if ac, ok := ts.Target.(*targets.AddColumn); ok && ac.TableID == table.GetID() {
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

func (b *Builder) nextIndexID(table *tabledesc.Immutable) descpb.IndexID {
	nextMaxID := table.GetNextIndexID()
	var maxIdxID descpb.IndexID
	for _, ts := range b.targetStates {
		if ai, ok := ts.Target.(*targets.AddIndex); ok && ai.TableID == table.GetID() {
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

func (b *Builder) nextFamilyID(table *tabledesc.Immutable) descpb.FamilyID {
	nextMaxID := table.GetNextFamilyID()
	var maxFamilyID descpb.FamilyID
	for _, ts := range b.targetStates {
		if af, ok := ts.Target.(*targets.AddColumnFamilyDependency); ok &&
			af.TableID == table.GetID() {
			if af.Family.ID > maxFamilyID {
				maxFamilyID = af.Family.ID
			}
		}
	}
	if maxFamilyID != 0 {
		nextMaxID = maxFamilyID + 1
	}
	return nextMaxID
}

func (b *Builder) addTargetState(t targets.Target, s targets.State) {
	b.targetStates = append(b.targetStates, &targets.TargetState{
		Target: t,
		State:  s,
	})
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
