package scexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/descriptorutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
func (ex *Executor) ExecuteOps(ctx context.Context, toExecute []scop.Op) error {
	if len(toExecute) == 0 {
		return nil
	}
	typ, err := getOpsType(toExecute)
	if err != nil {
		return err
	}
	switch typ {
	case scop.DescriptorMutationType:
		return ex.executeDescriptorMutationOps(ctx, toExecute)
	case scop.BackfillType:
		return ex.executeBackfillOps(ctx, toExecute)
	case scop.ValidationType:
		return ex.executeValidationOps(ctx, toExecute)
	default:
		return errors.AssertionFailedf("unknown ops type %d", typ)
	}
}

func (ex *Executor) executeValidationOps(ctx context.Context, execute []scop.Op) error {
	log.Errorf(ctx, "not implemented")
	return nil
}

func (ex *Executor) executeBackfillOps(ctx context.Context, execute []scop.Op) error {
	// TODO(ajwerner): Run backfills in parallel. Will require some plumbing for
	// checkpointing at the very least.

	for _, op := range execute {
		var err error
		switch op := op.(type) {
		case scop.IndexBackfill:
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

func (ex *Executor) executeIndexBackfillOp(ctx context.Context, op scop.IndexBackfill) error {
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
	mut, _, err := descriptorutils.GetIndexMutation(table, op.IndexID)
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

func (ex *Executor) executeDescriptorMutationOps(ctx context.Context, ops []scop.Op) error {
	dg := &mutationDescGetter{
		descs: ex.descsCollection,
		txn:   ex.txn,
	}
	v := scmutationexec.NewMutationVisitor(dg)
	for _, op := range ops {
		if err := op.(scop.MutationOp).Visit(ctx, v); err != nil {
			return err
		}
	}
	ba := ex.txn.NewBatch()
	for _, id := range dg.retrieved.Ordered() {
		desc, err := ex.descsCollection.GetMutableDescriptorByID(ctx, id, ex.txn)
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "failed to retrieve modified descriptor")
		}
		if err := ex.descsCollection.WriteDescToBatch(ctx, false, desc, ba); err != nil {
			return err
		}
	}
	if err := ex.txn.Run(ctx, ba); err != nil {
		return errors.Wrap(err, "writing descriptors")
	}
	return nil
}

func getOpsType(execute []scop.Op) (scop.Type, error) {
	typ := execute[0].Type()
	for i := 1; i < len(execute); i++ {
		if execute[i].Type() != typ {
			return 0, errors.Errorf("operations contain multiple types: %s and %s",
				typ, execute[i].Type())
		}
	}
	return typ, nil
}
