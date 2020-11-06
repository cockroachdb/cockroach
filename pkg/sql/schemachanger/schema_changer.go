package schemachanger

import (
	"context"
	"go/ast"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// SchemaChanger coordinates schema changes.
//
// It exists in the following contexts:
//
//  1) Exeuction of a transaction which issues DDLs
//  2) Schema change jobs
//  3) ? Type change jobs ?
//
type SchemaChanger struct {
	stmts    []*stmtInfo
	elements []element
}

// An ID scoped to a given transaction and thus
// schema changer.
type stmtID int

type stmtInfo struct {
	id   stmtID
	stmt ast.Stmt
}

type element interface {
	stmtID() stmtID
	// ...
}

// TODO list:
//
// - Define op interface
// - Define a basic library of ops needed for our prototype
// - Implement those ops
// - Define set of elements needed
// - Compile elements to success path (defer failure path)
// - Deal with serialization and deserialization of schema changer state
//   into jobs and table descriptor
// - Plumb it together

type step []op

type op interface{}

var ops = []step{
	{
		addColumnChangeState{tableID, columnID, nextState},
		addIndexChanageState{tableID, indexID, nextState},
	},
	{
		paralelBackfill{
			backfill{foo},
			backfill{bar},
		},
		addIndexChangeState{foo, Backfilled},
		addIndexChangeState{bar, Backfilled},
	},
}

type addColumn struct {
	statementID stmtID

	tableID  descpb.ID
	columnID descpb.ColumnID

	// Refer to a descriptor
	// And column ID
	// And maybe higher level change?
	state addColumnState
}

type addColumnState int

const (
	addColumnDeleteOnly addColumnState = iota
	addColumnWriteAndDeleteOnly
	addColumnBackfilled
	addColumnPublic
	addColumnRemoved
)

var _ Element = (*column)(nil)

// TODO better name
type Builder struct {
	res resolver.SchemaResolver

	sc *SchemaChanger
}

func (b *Builder) Build() (*SchemaChanger, error) {
	return b.sc, nil
}

// AlterTable augments the state of the SchemaChanger to include the implied
// changes of the AlterTable statement.
//
// Note that this does not actually perform any changes to any resources but
// rather changes the behavior of the schema changer for the next Step() call.
func (b *Builder) AlterTable(ctx context.Context, n *tree.AlterTable) error {
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
		if err := b.alterTableCmd(ctx, table, &n.Cmds[i]); err != nil {
			return err
		}
	}
	// Allocate IDs?
	return nil
}

func (b *Builder) alterTableCmd(
	ctx context.Context, table *tabledesc.Mutable, cmd *tree.AlterTableCmd,
) error {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		return b.alterTableAddColumn(ctx, table, t)
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
	ctx context.Context, table *tabledesc.Mutable, t *tree.AlterTableAddColumn,
) error {

	// Resolve type
	// Deal with SERIAL
	// Deal with sequence stuff based on SERIAL
	// Construct the column descriptor
	// Column families
	// Validation

	type AddColumn struct {
		Column       descpb.ColumnDescriptor
		ColumnFamily struct {
			Name string
			ID   descpb.FamilyID
		}
	}

	//table.ApplyAddColumn(AddColumn{
	//		ColumnDescriptor: descpb.ColumnDescriptor{},
	//	})

	// What happens to b.sc?

	// We want to add information to sc to track this
	// column addition
	//
	// Also, type descriptor tracking.

	// Allocating column IDs

	// Some validation?

}

// TODO: Implement
func (sc *SchemaChanger) Step(ctx context.Context) error {
	panic("not implemented")
}

// func (sc *SchemaChanger) alterTableCmd()
