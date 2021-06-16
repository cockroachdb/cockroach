// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/descriptorutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// An Executor executes ops generated during planning. It mostly holds
// dependencies for execution and has little additional logic of its own.
type Executor struct {
	txn             *kv.Txn
	descsCollection *descs.Collection
	codec           keys.SQLCodec
	indexBackfiller IndexBackfiller
	jobTracker      JobProgressTracker
	testingKnobs    *NewSchemaChangerTestingKnobs
	jobRegistry     *jobs.Registry
	executor        sqlutil.InternalExecutor
}

// NewExecutor creates a new Executor.
func NewExecutor(
	txn *kv.Txn,
	descsCollection *descs.Collection,
	codec keys.SQLCodec,
	backfiller IndexBackfiller,
	tracker JobProgressTracker,
	testingKnobs *NewSchemaChangerTestingKnobs,
	jobRegistry *jobs.Registry,
	executor sqlutil.InternalExecutor,
) *Executor {
	return &Executor{
		txn:             txn,
		descsCollection: descsCollection,
		codec:           codec,
		indexBackfiller: backfiller,
		jobTracker:      tracker,
		testingKnobs:    testingKnobs,
		jobRegistry:     jobRegistry,
		executor:        executor,
	}
}

// NewSchemaChangerTestingKnobs are testing knobs for the executor.
type NewSchemaChangerTestingKnobs struct {
	// BeforeStage is called before ops passed to the executor are executed.
	// Errors returned are injected into the executor.
	BeforeStage func(ops scop.Ops, m TestingKnobMetadata) error
	// BeforeWaitingForConcurrentSchemaChanges is called at the start of waiting
	// for concurrent schema changes to finish.
	BeforeWaitingForConcurrentSchemaChanges func(stmts []string)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*NewSchemaChangerTestingKnobs) ModuleTestingKnobs() {}

// TestingKnobMetadata holds additional information about the execution of the
// schema change that is used by the testing knobs.
type TestingKnobMetadata struct {
	Statements []string
	Phase      scplan.Phase
}

// ExecuteOps executes the provided ops. The ops must all be of the same type.
func (ex *Executor) ExecuteOps(
	ctx context.Context, toExecute scop.Ops, m TestingKnobMetadata,
) error {
	log.Infof(ctx, "executing %d ops of type %s", len(toExecute.Slice()), toExecute.Type().String())

	if ex.testingKnobs != nil && ex.testingKnobs.BeforeStage != nil {
		if err := ex.testingKnobs.BeforeStage(toExecute, m); err != nil {
			return err
		}
	}
	switch typ := toExecute.Type(); typ {
	case scop.MutationType:
		return ex.executeDescriptorMutationOps(ctx, toExecute.Slice())
	case scop.BackfillType:
		return ex.executeBackfillOps(ctx, toExecute.Slice())
	case scop.ValidationType:
		return ex.executeValidationOps(ctx, toExecute.Slice())
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
		case *scop.BackfillIndex:
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

func (ex *Executor) executeIndexBackfillOp(ctx context.Context, op *scop.BackfillIndex) error {
	// Note that the leasing here is subtle. We'll avoid the cache and ensure that
	// the descriptor is read from the store. That means it will not be leased.
	// This relies on changed to the descriptor not messing with this index
	// backfill.
	table, err := ex.descsCollection.GetImmutableTableByID(ctx, ex.txn, op.TableID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:       true,
			RequireMutable: false,
			AvoidCached:    true,
		},
	})
	if err != nil {
		return err
	}
	mut, err := descriptorutils.FindMutation(table, descriptorutils.MakeIndexIDMutationSelector(op.IndexID))
	if err != nil {
		return err
	}

	// Must be the right index given the above call.
	idxToBackfill := mut.AsIndex()

	// Split off the index span prior to backfilling.
	if err := ex.maybeSplitIndexSpans(ctx, table.IndexSpan(ex.codec, idxToBackfill.GetID())); err != nil {
		return err
	}
	return ex.indexBackfiller.BackfillIndex(ctx, ex.jobTracker, table, table.GetPrimaryIndexID(), idxToBackfill.GetID())
}

// IndexBackfiller is an abstract index backfiller that performs index backfills
// when provided with a specification of tables and indexes and a way to track
// job progress.
type IndexBackfiller interface {
	BackfillIndex(
		ctx context.Context,
		_ JobProgressTracker,
		_ catalog.TableDescriptor,
		source descpb.IndexID,
		destinations ...descpb.IndexID,
	) error
}

// JobProgressTracker abstracts the infrastructure to read and write backfill
// progress to job state.
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
	dg := newMutationDescGetter(ex.descsCollection, ex.txn, ex.executor)
	mj := &mutationJobs{jobRegistry: ex.jobRegistry}
	v := scmutationexec.NewMutationVisitor(dg, mj)
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
	err := dg.SubmitDrainedNames(ctx, ex.codec, ba)
	if err != nil {
		return err
	}
	_, err = mj.SubmitAllJobs(ctx, ex.txn)
	if err != nil {
		return err
	}
	err = ex.descsCollection.ValidateUncommittedDescriptors(ctx, ex.txn)
	if err != nil {
		return err
	}
	if err := ex.txn.Run(ctx, ba); err != nil {
		return errors.Wrap(err, "writing descriptors")
	}
	return nil
}

// UpdateDescriptorJobIDs updates the job ID for the schema change on the
// specified set of table descriptors.
func UpdateDescriptorJobIDs(
	ctx context.Context,
	txn *kv.Txn,
	descriptors *descs.Collection,
	descIDs []descpb.ID,
	expectedID jobspb.JobID,
	newID jobspb.JobID,
) error {
	b := txn.NewBatch()
	for _, id := range descIDs {
		// Confirm the descriptor is a table, view or sequence
		// since we can only lock those types.
		desc, err := descriptors.GetImmutableDescriptorByID(ctx, txn, id,
			tree.CommonLookupFlags{
				Required:       true,
				IncludeDropped: true},
		)
		if err != nil {
			return err
		}
		if desc.DescriptorType() != catalog.Table {
			continue
		}
		if err != nil {
			return err
		}
		// Currently all "locking" schema changes are on tables. This will probably
		// need to be expanded at least to types.
		table, err := descriptors.GetMutableTableByID(ctx, txn, id,
			tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					Required:       true,
					IncludeDropped: true},
			})
		if err != nil {
			return err
		}
		if oldID := jobspb.JobID(table.NewSchemaChangeJobID); oldID != expectedID {
			return errors.AssertionFailedf(
				"unexpected schema change job ID %d on table %d, expected %d", oldID, table.GetID(), expectedID)
		}
		table.NewSchemaChangeJobID = int64(newID)
		if err := descriptors.WriteDescToBatch(ctx, true /* kvTrace */, table, b); err != nil {
			return err
		}
	}
	return txn.Run(ctx, b)
}
