package executor

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type Executor struct {
	txn             *kv.Txn
	descsCollection *descs.Collection
	codec           keys.SQLCodec
	indexBackfiller IndexBackfiller
	jobTracker      JobProgressTracker
	// ...
}

func New(
	txn *kv.Txn,
	descsCollection *descs.Collection,
	codec keys.SQLCodec,
	backfiller IndexBackfiller,
	tracker JobProgressTracker,
) *Executor {
	return &Executor{
		txn:             txn,
		descsCollection: descsCollection,
		codec:           codec,
		indexBackfiller: backfiller,
		jobTracker:      tracker,
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
		case ops.MakeAddedPrimaryIndexDeleteOnly:
			err = ex.executeMakeAddedPrimaryIndexDeleteOnly(ctx, op)
		case ops.MakeAddedIndexDeleteAndWriteOnly:
			err = ex.executeMakeAddedIndexDeleteAndWriteOnly(ctx, op)
		case ops.MakeAddedPrimaryIndexPublic:
			err = ex.executeMakeAddedPrimaryIndexPublic(ctx, op)
		case ops.MakeDroppedPrimaryIndexDeleteAndWriteOnly:
			err = ex.executeMakeDroppedPrimaryIndexDeleteAndWriteOnly(ctx, op)
		case ops.MakeDroppedIndexDeleteOnly:
			err = ex.executeMakeDroppedIndexDeleteOnly(ctx, op)
		case ops.MakeDroppedIndexAbsent:
			err = ex.executeMakeDroppedIndexAbsent(ctx, op)
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

func (ex *Executor) executeMakeAddedPrimaryIndexDeleteOnly(
	ctx context.Context, op ops.MakeAddedPrimaryIndexDeleteOnly,
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
	// Make some adjustments to the index descriptor so that it behaves correctly
	// as a secondary index while being added.
	idx := protoutil.Clone(&op.Index).(*descpb.IndexDescriptor)
	idx.EncodingType = descpb.PrimaryIndexEncoding
	idx.StoreColumnIDs = op.StoreColumnIDs
	idx.StoreColumnNames = op.StoreColumnNames
	if err := table.AddIndexMutation(idx, descpb.DescriptorMutation_ADD); err != nil {
		return err
	}
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeMakeAddedIndexDeleteAndWriteOnly(
	ctx context.Context, op ops.MakeAddedIndexDeleteAndWriteOnly,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	mut, _, err := getIndexMutation(table, op.IndexID)
	if err != nil {
		return err
	}
	if mut.Direction != descpb.DescriptorMutation_ADD ||
		mut.State != descpb.DescriptorMutation_DELETE_ONLY {
		return errors.AssertionFailedf("unexpected state")
	}
	mut.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeMakeAddedPrimaryIndexPublic(
	ctx context.Context, op ops.MakeAddedPrimaryIndexPublic,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	mut, foundIdx, err := getIndexMutation(table, op.IndexID)
	if err != nil {
		return err
	}
	if mut.Direction != descpb.DescriptorMutation_ADD ||
		mut.State != descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY {
		return errors.AssertionFailedf("unexpected state")
	}
	idx := mut.GetIndex()
	table.PrimaryIndex = *idx
	// Most of this logic is taken from MakeMutationComplete().
	// TODO (lucy): We do this in the alter PK implementation, but why does it
	// work? Do we ever rename the old primary index to something else?
	table.PrimaryIndex.Name = "primary"
	table.PrimaryIndex.StoreColumnIDs = nil
	table.PrimaryIndex.StoreColumnNames = nil
	table.Mutations = append(table.Mutations[:foundIdx], table.Mutations[foundIdx+1:]...)
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeMakeDroppedPrimaryIndexDeleteAndWriteOnly(
	ctx context.Context, op ops.MakeDroppedPrimaryIndexDeleteAndWriteOnly,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	// TODO (lucy): Figure out whether we want to do anything different here for
	// the drop constraint PK + add constraint PK case. Or consider just merging
	// the PK swap into a single op (and target) so we don't have to worry about
	// the ordering of the add and drop ops.
	idx := table.PrimaryIndex
	if idx.ID != op.IndexID {
		return errors.AssertionFailedf("unexpected index ID")
	}
	// Most of this logic is taken from MakeMutationComplete().
	idx.EncodingType = descpb.PrimaryIndexEncoding
	idx.StoreColumnIDs = op.StoreColumnIDs
	idx.StoreColumnNames = op.StoreColumnNames
	if err := table.AddIndexMutation(&idx, descpb.DescriptorMutation_DROP); err != nil {
		return err
	}
	table.PrimaryIndex = descpb.IndexDescriptor{}
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeMakeDroppedIndexDeleteOnly(
	ctx context.Context, op ops.MakeDroppedIndexDeleteOnly,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	mut, _, err := getIndexMutation(table, op.IndexID)
	if err != nil {
		return err
	}
	if mut.Direction != descpb.DescriptorMutation_DROP ||
		mut.State != descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY {
		return errors.AssertionFailedf("unexpected state")
	}
	mut.State = descpb.DescriptorMutation_DELETE_ONLY
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func (ex *Executor) executeMakeDroppedIndexAbsent(
	ctx context.Context, op ops.MakeDroppedIndexAbsent,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	mut, foundIdx, err := getIndexMutation(table, op.IndexID)
	if err != nil {
		return err
	}
	if mut.Direction != descpb.DescriptorMutation_DROP ||
		mut.State != descpb.DescriptorMutation_DELETE_ONLY {
		return errors.AssertionFailedf("unexpected state")
	}
	table.Mutations = append(table.Mutations[:foundIdx], table.Mutations[foundIdx+1:]...)
	return ex.descsCollection.WriteDesc(ctx, kvTrace, table, ex.txn)
}

func getIndexMutation(
	table catalog.TableDescriptor, idxID descpb.IndexID,
) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
	mutations := table.GetMutations()
	for i := range mutations {
		mut := &mutations[i]
		idx := mut.GetIndex()
		if idx != nil && idx.ID == idxID {
			return mut, i, nil
		}
	}
	return nil, 0, errors.AssertionFailedf("mutation not found")
}

func (ex *Executor) executeIndexDescriptorStateChange(
	ctx context.Context, op ops.IndexDescriptorStateChange,
) error {
	table, err := ex.descsCollection.GetMutableTableVersionByID(ctx, op.TableID, ex.txn)
	if err != nil {
		return err
	}
	// We want to find the index and then change its state.
	if op.State == targets.State_PUBLIC && op.NextState == targets.State_DELETE_AND_WRITE_ONLY {
		var idx descpb.IndexDescriptor
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
		table.AddIndexMutation(&idx, descpb.DescriptorMutation_DROP)
	} else {
		// Moving from a mutation to something.
		mut, foundIdx, err := getIndexMutation(table, op.IndexID)
		if err != nil {
			return err
		}
		idx := mut.GetIndex()
		switch op.NextState {

		case targets.State_ABSENT:
			// Can happen in revert or when removing an index.
			if mut.State != descpb.DescriptorMutation_DELETE_ONLY {
				return errors.AssertionFailedf("expected index to be in %v for %v, got %v",
					descpb.DescriptorMutation_DELETE_ONLY, op.NextState, mut.State)
			}
			table.Mutations = append(table.Mutations[:foundIdx], table.Mutations[foundIdx+1:]...)
		case targets.State_DELETE_AND_WRITE_ONLY:
			mut.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
		case targets.State_DELETE_ONLY:
			mut.State = descpb.DescriptorMutation_DELETE_ONLY
		case targets.State_PUBLIC:
			if mut.State != descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				return errors.AssertionFailedf("expected index to be in %v for %v, got %v",
					descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY, op.NextState, mut.State)
			}
			if table.PrimaryIndex.ID != 0 {
				return errors.AssertionFailedf("expected primary index to be unset")
			}
			table.PrimaryIndex = *idx
			table.Indexes = append(table.Indexes, *idx)
			table.Mutations = append(table.Mutations[:foundIdx], table.Mutations[foundIdx+1:]...)
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
	// TODO(ajwerner): Run backfills in parallel. Will require some plumbing for
	// checkpointing at the very least.

	for _, op := range execute {
		var err error
		switch op := op.(type) {
		case ops.IndexBackfill:
			err = ex.executeIndexBackfillOp(ctx, op)
		default:
			panic("unimplemented")
		}
		if err != nil {
			return err
		}
	}
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
	if op.State == targets.State_PUBLIC && op.NextState == targets.State_DELETE_AND_WRITE_ONLY {
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

		case targets.State_ABSENT:
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
		case targets.State_DELETE_AND_WRITE_ONLY:
			mut.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
		case targets.State_DELETE_ONLY:
			mut.State = descpb.DescriptorMutation_DELETE_ONLY
		case targets.State_PUBLIC:
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

func (ex *Executor) executeIndexBackfillOp(ctx context.Context, op ops.IndexBackfill) error {
	// Note that the leasing here is subtle. We'll avoid the cache and ensure that
	// the descriptor is read from the store. That means it will not be leased.
	// This relies on changed to the descriptor not messing with this index
	// backfill.
	table, err := ex.descsCollection.GetTableVersionByID(ctx, ex.txn, op.TableID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:       true,
			RequireMutable: false,
			AvoidCached:    true,
		},
	})
	if err != nil {
		return err
	}
	mut, _, err := getIndexMutation(table, op.IndexID)
	if err != nil {
		return err
	}

	// Must be the right index given the above call.
	idxToBackfill := mut.GetIndex()

	// Split off the index span prior to backfilling.
	if err := ex.maybeSplitIndexSpans(ctx, table.IndexSpan(ex.codec, idxToBackfill.ID)); err != nil {
		return err
	}
	return ex.indexBackfiller.BackfillIndex(ctx, ex.jobTracker, table, table.GetPrimaryIndexID(), idxToBackfill.ID)
}

type IndexBackfiller interface {
	BackfillIndex(
		ctx context.Context,
		_ JobProgressTracker,
		_ catalog.TableDescriptor,
		source descpb.IndexID,
		destinations ...descpb.IndexID,
	) error
}

type JobProgressTracker interface {

	// This interface is implicitly implying that there is only one stage of
	// index backfills for a given table in a schema change. It implies that
	// because it assumes that it's safe and reasonable to just store one set of
	// resume spans per table on the job.
	//
	// Potentially something close to interface could still work if there were
	// multiple stages of backfills for a table if we tracked which stage this
	// were somehow. Maybe we could do something like increment a stage counter
	// per table after finishing the backfills.
	//
	// It definitely is possible that there are multiple index backfills on a
	// table in the context of a single schema change that changes the set of
	// columns (primary index) and adds secondary indexes.
	//
	// Really this complexity arises in the computation of the fraction completed.
	// We'll want to know whether there are more index backfills to come.
	//
	// One idea is to index secondarily on the source index.

	GetResumeSpans(ctx context.Context, tableID descpb.ID, indexID descpb.IndexID) ([]roachpb.Span, error)
	SetResumeSpans(ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, total, done []roachpb.Span) error
}

func (ex *Executor) maybeSplitIndexSpans(ctx context.Context, span roachpb.Span) error {
	// Only perform splits on the system tenant.
	if !ex.codec.ForSystemTenant() {
		return nil
	}
	const backfillSplitExpiration = time.Hour
	expirationTime := ex.txn.DB().Clock().Now().Add(backfillSplitExpiration.Nanoseconds(), 0)
	return ex.txn.DB().AdminSplit(ctx, span.Key, expirationTime)
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
