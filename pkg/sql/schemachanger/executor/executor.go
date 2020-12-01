package executor

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/errors"
)

type Executor struct {
	txn             *kv.Txn
	descsCollection descs.Collection

	// ...
}

func New(txn *kv.Txn, descsCollection descs.Collection) *Executor {
	return &Executor{
		txn:             txn,
		descsCollection: descsCollection,
	}
}

// ExecuteOps executes the provided ops. The ops must all be of the same type.
func (ex *Executor) ExecuteOps(ctx context.Context, toExecute []ops.Op) error {
	if len(toExecute) == 0 {
		return nil
	}
	typ, err := getOpsType(toExecute)
	if err != nil {
		return err
	}
	switch typ {
	case ops.DescriptorMutationType:
		return ex.executeDescriptorMutationOps(ctx, toExecute)
	case ops.BackfillType:
		return ex.executeBackfillOps(ctx, toExecute)
	case ops.ValidationType:
		return ex.executeValidationOps(ctx, toExecute)
	default:
		return errors.AssertionFailedf("unknown ops type %d", typ)
	}
}

func (ex *Executor) executeDescriptorMutationOps(ctx context.Context, execute []ops.Op) error {
	for _, op := range execute {
		var err error
		switch op := op.(type) {
		case ops.AddCheckConstraint:
			err = ex.executeAddCheckConstraint(ctx, op)
		}
		if err != nil {
			return err
		}

	}
	panic("not implemented")
}

func (ex *Executor) executeAddCheckConstraint(
	ctx context.Context, op ops.AddCheckConstraint,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	ck := &descpb.TableDescriptor_CheckConstraint{
		Expr:      op.Expr,
		Name:      op.Name,
		ColumnIDs: op.ColumnIDs,
		Hidden:    op.Hidden,
	}
	if op.Unvalidated {
		ck.Validity = descpb.ConstraintValidity_Unvalidated
	} else {
		ck.Validity = descpb.ConstraintValidity_Validating
	}
	table.Checks = append(table.Checks)
	return ex.descsCollection.AddUncommittedDescriptor(table)
}

func (ex *Executor) executeDescriptorMutationOp(ctx context.Context, op ops.Op) {

}

func (ex *Executor) executeBackfillOps(ctx context.Context, execute []ops.Op) error {
	panic("not implemented")
}

func (ex *Executor) executeValidationOps(ctx context.Context, execute []ops.Op) error {
	panic("not implemented")
}

func getOpsType(execute []ops.Op) (ops.Type, error) {
	typ := execute[0].Type()
	for i := 1; i < len(execute); i++ {
		if execute[i].Type() != typ {
			return 0, errors.Errorf("operations contain multiple types: %s and %s",
				typ, execute[i].Type())
		}
	}
	return typ, nil
}
