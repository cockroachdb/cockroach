package schemachanger

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

// TODO better name
type Builder struct {

	// TODO(ajwerner): Inject a better interface than this.
	res     resolver.SchemaResolver
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext

	sc schemaChangerState

	modifiedDescriptors []catalog.MutableDescriptor
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

func (b *Builder) Build() (*SchemaChanger, error) {
	return &SchemaChanger{state: b.sc}, nil
}

// AlterTable augments the state of the SchemaChanger to include the implied
// changes of the AlterTable statement.
//
// Note that this does not actually perform any changes to any resources but
// rather changes the behavior of the schema changer for the next Step() call.
func (b *Builder) AlterTable(ctx context.Context, n *tree.AlterTable) error {
	statementID := stmtID(len(b.sc.stmts))
	b.sc.stmts = append(b.sc.stmts, &stmtInfo{
		id:   statementID,
		stmt: n,
	})
	// Resolve the table
	tn := n.Table.ToTableName()
	table, err := resolver.ResolveMutableExistingTableObject(ctx, b.res, &tn,
		true /* required */, tree.ResolveAnyTableKind)
	if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
		return nil
	}
	if err != nil {
		return err
	}
	for i := range n.Cmds {
		if err := b.alterTableCmd(ctx, table, n.Cmds[i], statementID); err != nil {
			return err
		}
	}
	// Allocate IDs?
	return nil
}

func (b *Builder) alterTableCmd(
	ctx context.Context, table *tabledesc.Mutable, cmd tree.AlterTableCmd, statementID stmtID,
) error {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		return b.alterTableAddColumn(ctx, table, t, statementID)
	case *tree.AlterTableAddConstraint:

	case *tree.AlterTableAlterPrimaryKey:

	case *tree.AlterTableDropColumn:

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
	ctx context.Context, table *tabledesc.Mutable, t *tree.AlterTableAddColumn, statementID stmtID,
) error {

	// Resolve type
	// Deal with SERIAL
	// Deal with sequence stuff based on SERIAL
	// Construct the column descriptor
	// Column families
	// Validation
	if t.ColumnDef.IsSerial {
		panic("not implemented")
	}
	col, idx, _, err := tabledesc.MakeColumnDefDescs(ctx, t.ColumnDef, b.semaCtx, b.evalCtx)
	if err != nil {
		return err
	}

	// TODO(ajwerner): Check for empty table with non-null, no default case.
	// TODO(ajwerner): Validate computed column.
	// TODO(ajwerner): Validate name conflicts.

	table.AddColumnMutation(col, descpb.DescriptorMutation_ADD)
	if idx != nil {
		if err := table.AddIndexMutation(idx, descpb.DescriptorMutation_ADD); err != nil {
			return err
		}
	}
	if t.ColumnDef.HasColumnFamily() {
		fam := &t.ColumnDef.Family
		if err := table.AddColumnToFamilyMaybeCreate(
			col.Name, string(fam.Name), fam.Create, fam.IfNotExists,
		); err != nil {
			return err
		}
	}

	if err := table.AllocateIDs(ctx); err != nil {
		return err
	}
	table.MaybeIncrementVersion()
	b.modifiedDescriptors = append(b.modifiedDescriptors, table)

	b.sc.elements = append(b.sc.elements, &targets.AddColumn{
		statementID: statementID,
		tableID:     table.GetID(),
		columnID:    col.ID,

		state: targets.elemDeleteOnly,
		// TODO(ajwerner): indicate whether we need a backfill.
	})
	if idx != nil {
		b.sc.elements = append(b.sc.elements, &targets.AddIndex{
			statementID: statementID,
			tableID:     table.GetID(),
			indexID:     idx.ID,
			state:       targets.elemDeleteOnly,
		})
	}
	return nil
}
