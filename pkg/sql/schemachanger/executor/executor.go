package executor

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type Executor struct {
	txn             *kv.Txn
	descsCollection *descs.Collection

	// ...
}

func New(txn *kv.Txn, descsCollection *descs.Collection) *Executor {
	return &Executor{
		txn:             txn,
		descsCollection: descsCollection,
	}
}

// TODO(ajwerner): Stop writing descriptors during execution at all.
// This constant is for writing descriptors through the descs.Collection.
const kvTrace = false

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
		case ops.AddIndexDescriptor:
			err = ex.executeAddIndexDescriptor(ctx, op)
		case ops.AddColumnDescriptor:
			err = ex.executeAddColumnDescriptor(ctx, op)
		case ops.ColumnDescriptorStateChange:
			err = ex.executeColumnDescriptorStateChange(ctx, op)
		case ops.IndexDescriptorStateChange:
			err = ex.executeIndexDescriptorStateChange(ctx, op)
		default:
			err = errors.AssertionFailedf("descriptor mutation op not implemented for %T", op)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (ex *Executor) executeIndexDescriptorStateChange(
	ctx context.Context, op ops.IndexDescriptorStateChange,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	// We want to find the index and then change its state.
	if op.State == targets.StatePublic && op.NextState == targets.StateDeleteAndWriteOnly {
		var idx descpb.IndexDescriptor
		if op.IsPrimary {
			if table.PrimaryIndex.ID != op.IndexID {
				// TODO(ajwerner): Better error.
				return errors.AssertionFailedf("expected index to be primary")
			}
			idx = table.PrimaryIndex
			table.PrimaryIndex = descpb.IndexDescriptor{}
		} else {
			for i := range table.Indexes {
				if table.Indexes[i].ID != op.IndexID {
					continue
				}
				idx = table.Indexes[i]
				table.Indexes = append(table.Indexes[:i], table.Indexes[i+1:]...)
				break
			}
			if idx.ID == 0 {
				return errors.AssertionFailedf("failed to find index")
			}
		}
		table.AddIndexMutation(&idx, descpb.DescriptorMutation_DROP)
	} else {
		// Moving from a mutation to something.
		mutations := table.GetMutations()
		foundIdx := -1
		for i := range mutations {
			mut := &mutations[i]
			idx := mut.GetIndex()
			if idx != nil && idx.ID == op.IndexID {
				foundIdx = i
				break
			}
		}
		if foundIdx == -1 {
			return errors.AssertionFailedf("")
		}
		mut := &mutations[foundIdx]
		idx := mut.GetIndex()
		switch op.NextState {

		case targets.StateAbsent:
			// Can happen in revert or when removing an index.
			if mut.State != descpb.DescriptorMutation_DELETE_ONLY {
				return errors.AssertionFailedf("expected index to be in %v for %v, got %v",
					descpb.DescriptorMutation_DELETE_ONLY, op.NextState, mut.State)
			}
			table.Mutations = append(mutations[:foundIdx], mutations[foundIdx+1:]...)
		case targets.StateDeleteAndWriteOnly:
			mut.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
		case targets.StateDeleteOnly:
			mut.State = descpb.DescriptorMutation_DELETE_ONLY
		case targets.StatePublic:
			if mut.State != descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				return errors.AssertionFailedf("expected index to be in %v for %v, got %v",
					descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY, op.NextState, mut.State)
			}
			if op.IsPrimary {
				if table.PrimaryIndex.ID != 0 {
					return errors.AssertionFailedf("expected primary index to be unset")
				}
				table.PrimaryIndex = *idx
			} else {
				table.Indexes = append(table.Indexes, *idx)
			}
			table.Mutations = append(mutations[:foundIdx], mutations[foundIdx+1:]...)
		default:
			return errors.AssertionFailedf("unknown transition for index to %s", op.NextState)
		}
	}
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
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
	table.Checks = append(table.Checks, ck)
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeBackfillOps(ctx context.Context, execute []ops.Op) error {
	log.Errorf(ctx, "not implemented")
	return nil
}

func (ex *Executor) executeValidationOps(ctx context.Context, execute []ops.Op) error {
	log.Errorf(ctx, "not implemented")
	return nil
}

func (ex *Executor) executeAddIndexDescriptor(
	ctx context.Context, op ops.AddIndexDescriptor,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Index.ID >= table.NextIndexID {
		table.NextIndexID = op.Index.ID + 1
	}
	table.AddIndexMutation(&op.Index, descpb.DescriptorMutation_ADD)
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeAddColumnDescriptor(
	ctx context.Context, op ops.AddColumnDescriptor,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Column.ID >= table.NextColumnID {
		table.NextColumnID = op.Column.ID + 1
	}
	var foundFamily bool
	for i := range table.Families {
		fam := &table.Families[i]
		if foundFamily = fam.ID == op.ColumnFamily; foundFamily {
			fam.ColumnIDs = append(fam.ColumnIDs, op.Column.ID)
			fam.ColumnNames = append(fam.ColumnNames, op.Column.Name)
			break
		}
	}
	if !foundFamily {
		return errors.AssertionFailedf("failed to find column family")
	}
	table.AddColumnMutation(&op.Column, descpb.DescriptorMutation_ADD)
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeColumnDescriptorStateChange(
	ctx context.Context, op ops.ColumnDescriptorStateChange,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	// We want to find the index and then change its state.
	if op.State == targets.StatePublic && op.NextState == targets.StateDeleteAndWriteOnly {
		var col descpb.ColumnDescriptor

		for i := range table.Columns {
			if table.Columns[i].ID != op.ColumnID {
				continue
			}
			col = table.Columns[i]
			table.Columns = append(table.Columns[:i], table.Columns[i+1:]...)
			break
		}
		if col.ID == 0 {
			return errors.AssertionFailedf("failed to find column")
		}
		table.AddColumnMutation(&col, descpb.DescriptorMutation_DROP)
	} else {
		// Moving from a mutation to something.
		mutations := table.GetMutations()
		foundIdx := -1
		for i := range mutations {
			mut := &mutations[i]
			col := mut.GetColumn()
			if col != nil && col.ID == op.ColumnID {
				foundIdx = i
				break
			}
		}
		if foundIdx == -1 {
			return errors.AssertionFailedf("")
		}
		mut := &mutations[foundIdx]
		col := mut.GetColumn()
		switch op.NextState {

		case targets.StateAbsent:
			// Can happen in revert or when removing an index.
			if mut.State != descpb.DescriptorMutation_DELETE_ONLY {
				return errors.AssertionFailedf("expected index to be in %v for %v, got %v",
					descpb.DescriptorMutation_DELETE_ONLY, op.NextState, mut.State)
			}
			for i := range table.Families {
				fam := &table.Families[i]
				famIdx := -1
				for i, id := range fam.ColumnIDs {
					if id == op.ColumnID {
						famIdx = i
						break
					}
				}
				if famIdx == -1 {
					return errors.AssertionFailedf("failed to find column family when removing column")
				}
				fam.ColumnIDs = append(fam.ColumnIDs[:famIdx], fam.ColumnIDs[famIdx+1:]...)
				fam.ColumnNames = append(fam.ColumnNames[:famIdx], fam.ColumnNames[famIdx+1:]...)
			}
			table.Mutations = append(mutations[:foundIdx], mutations[foundIdx+1:]...)
		case targets.StateDeleteAndWriteOnly:
			mut.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
		case targets.StateDeleteOnly:
			mut.State = descpb.DescriptorMutation_DELETE_ONLY
		case targets.StatePublic:
			if mut.State != descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				return errors.AssertionFailedf("expected column to be in %v for %v, got %v",
					descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY, op.NextState, mut.State)
			}
			table.Columns = append(table.Columns, *col)
			table.Mutations = append(mutations[:foundIdx], mutations[foundIdx+1:]...)
		default:
			return errors.AssertionFailedf("unknown transition for index to %s", op.NextState)
		}
	}
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
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
