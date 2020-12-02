package builder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type Builder struct {
	// TODO(ajwerner): Inject a better interface than this.
	res     resolver.SchemaResolver
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext

	targetStates []*targets.TargetState
}

func (b *Builder) AlterTable(ctx context.Context, n *tree.AlterTable) error {
	// Resolve the table.
	tn := n.Table.ToTableName()
	// TODO: We actually want an immutable descriptor here.
	table, err := resolver.ResolveMutableExistingTableObject(ctx, b.res, &tn,
		true /* required */, tree.ResolveAnyTableKind)
	if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
		return nil
	}
	for _, cmd := range n.Cmds {
		if err := b.alterTableCmd(ctx, table, cmd); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) alterTableCmd(
	ctx context.Context, table *tabledesc.Mutable, cmd tree.AlterTableCmd,
) error {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		return b.alterTableAddColumn(ctx, table, t)
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
	ctx context.Context, table *tabledesc.Mutable, t *tree.AlterTableAddColumn,
) error {
	// TODO: lots of other stuff: serial, type resolution, sequence backrefs, name
	// validation, new column family, etc.
	if t.ColumnDef.IsSerial {
		panic("not implemented")
	}
	col, idx, _, err := tabledesc.MakeColumnDefDescs(ctx, t.ColumnDef, b.semaCtx, b.evalCtx)
	if err != nil {
		return err
	}

	colID := b.nextColumnID(table)
	col.ID = colID
	b.targetStates = append(b.targetStates, &targets.TargetState{
		Target: &targets.AddColumn{
			TableID: table.GetID(),
			Column:  *col,
		},
		State: targets.StateAbsent,
	})
	b.updatePrimaryIndexTargetsForColumnChange(
		table, append(table.PrimaryIndex.ColumnIDs, colID))

	if idx != nil {
		idxID := b.nextIndexID(table)
		idx.ID = idxID
		b.targetStates = append(b.targetStates, &targets.TargetState{
			Target: &targets.AddIndex{
				TableID:      table.GetID(),
				Index:        *idx,
				PrimaryIndex: table.GetPrimaryIndexID(), // ?
			},
		})

	}
	return nil
}

func (b *Builder) alterTableDropColumn(
	ctx context.Context, table *tabledesc.Mutable, t *tree.AlterTableDropColumn,
) error {
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
	idx := 0
	for i, id := range table.PrimaryIndex.ExtraColumnIDs {
		if id == colToDrop.ID {
			idx = i
			break
		}
	}
	replacementExtraColIDs := append(table.PrimaryIndex.ExtraColumnIDs[:idx],
		table.PrimaryIndex.ExtraColumnIDs[idx+1:]...)

	b.updatePrimaryIndexTargetsForColumnChange(table, replacementExtraColIDs)
	b.targetStates = append(b.targetStates, &targets.TargetState{
		Target: &targets.DropColumn{
			TableID:  table.GetID(),
			ColumnID: colToDrop.ID,
		},
		State: targets.StatePublic,
	})
	return nil
}

func (b *Builder) CreateIndex(ctx context.Context, n *tree.CreateIndex) error {
	// TODO: currently indexes are created in sql.MakeIndexDescriptor, but
	// populating the index
	panic("unimplemented")
}

func (b *Builder) updatePrimaryIndexTargetsForColumnChange(
	table *tabledesc.Mutable, replacementExtraColIDs []descpb.ColumnID,
) {
	existingPKChange := false
	for i := range b.targetStates {
		if t, ok := b.targetStates[i].Target.(*targets.AddIndex); ok &&
			t.TableID == table.GetID() && t.Primary && t.ReplacementFor != 0 {
			b.targetStates[i] = &targets.TargetState{
				Target: &targets.AddIndex{
					TableID:        t.TableID,
					Index:          descpb.IndexDescriptor{},
					PrimaryIndex:   t.PrimaryIndex,
					ReplacementFor: t.ReplacementFor,
					Primary:        true,
				},
				State: targets.StateAbsent,
			}
			existingPKChange = true
			break
		}
	}

	if !existingPKChange {
		newIdxID := b.nextIndexID(table)
		b.targetStates = append(b.targetStates, &targets.TargetState{
			Target: &targets.AddIndex{
				TableID:        table.GetID(),
				Index:          descpb.IndexDescriptor{},
				PrimaryIndex:   table.GetPrimaryIndexID(), // TODO: is this right?
				ReplacementFor: table.GetPrimaryIndexID(),
				Primary:        true,
			},
			State: targets.StateAbsent,
		})
		b.targetStates = append(b.targetStates, &targets.TargetState{
			Target: &targets.DropIndex{
				TableID:    table.GetID(),
				IndexID:    table.GetPrimaryIndexID(),
				ReplacedBy: newIdxID,
				ColumnIDs:  table.PrimaryIndex.ColumnIDs,
			},
			State: targets.StatePublic,
		})
	}
}

func (b *Builder) nextColumnID(table *tabledesc.Mutable) descpb.ColumnID {
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

func (b *Builder) nextIndexID(table *tabledesc.Mutable) descpb.IndexID {
	nextMaxID := table.GetNextIndexID()
	var maxIdxID descpb.IndexID
	for _, ts := range b.targetStates {
		if ac, ok := ts.Target.(*targets.AddIndex); ok && ac.TableID == table.GetID() {
			if ac.Index.ID > maxIdxID {
				maxIdxID = ac.Index.ID
			}
		}
	}
	if maxIdxID != 0 {
		nextMaxID = maxIdxID + 1
	}
	return nextMaxID
}
