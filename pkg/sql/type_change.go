// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// writeTypeChange should be called on a mutated type descriptor to ensure that
// the descriptor gets written to a batch, as well as ensuring that a job is
// created to perform the schema change on the type.
func (p *planner) writeTypeChange(
	ctx context.Context, typeDesc *sqlbase.MutableTypeDescriptor, jobDesc string,
) error {
	// Check if there is an active job for this type, otherwise create one.
	job, jobExists := p.extendedEvalCtx.SchemaChangeJobCache[typeDesc.ID]
	if jobExists {
		// Update it.
		if err := job.WithTxn(p.txn).SetDescription(ctx,
			func(ctx context.Context, description string) (string, error) {
				return description + "; " + jobDesc, nil
			},
		); err != nil {
			return err
		}
		log.Infof(ctx, "job %d: updated with type change for type %d", *job.ID(), typeDesc.ID)
	} else {
		// Or, create a new job.
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: sqlbase.IDs{typeDesc.ID},
			Details: jobspb.TypeSchemaChangeDetails{
				TypeID: typeDesc.ID,
			},
			Progress: jobspb.TypeSchemaChangeProgress{},
			// Type change jobs are not cancellable.
			NonCancelable: true,
		}
		newJob, err := p.extendedEvalCtx.QueueJob(jobRecord)
		if err != nil {
			return err
		}
		p.extendedEvalCtx.SchemaChangeJobCache[typeDesc.ID] = newJob
		log.Infof(ctx, "queued new type change job %d for type %d", *newJob.ID(), typeDesc.ID)
	}

	// Maybe increment the type's version.
	typeDesc.MaybeIncrementVersion()

	// Add the modified descriptor to the descriptor collection.
	if err := p.Descriptors().AddUncommittedDescriptor(typeDesc); err != nil {
		return err
	}

	// Write the type out to a batch.
	b := p.txn.NewBatch()
	if err := catalogkv.WriteDescToBatch(
		ctx,
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		p.ExecCfg().Settings,
		b,
		p.ExecCfg().Codec,
		typeDesc.ID,
		typeDesc,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

// typeSchemaChanger is the struct that actually runs the type schema change.
type typeSchemaChanger struct {
	typeID  sqlbase.ID
	execCfg *ExecutorConfig
}

// TypeSchemaChangerTestingKnobs contains testing knobs for the typeSchemaChanger.
type TypeSchemaChangerTestingKnobs struct {
	// RunBeforeExec runs at the start of the typeSchemaChanger.
	RunBeforeExec func() error
}

// ModuleTestingKnobs implements the ModuleTestingKnobs interface.
func (TypeSchemaChangerTestingKnobs) ModuleTestingKnobs() {}

func (t *typeSchemaChanger) getTypeDescFromStore(
	ctx context.Context,
) (*sqlbase.ImmutableTypeDescriptor, error) {
	var typeDesc *sqlbase.ImmutableTypeDescriptor
	if err := t.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		typeDesc, err = sqlbase.GetTypeDescFromID(ctx, txn, t.execCfg.Codec, t.typeID)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return typeDesc, nil
}

// exec is the entry point for the type schema change process.
func (t *typeSchemaChanger) exec(ctx context.Context) error {
	if t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeExec != nil {
		if err := t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeExec(); err != nil {
			return err
		}
	}
	ctx = logtags.AddTags(ctx, t.logTags())
	leaseMgr := t.execCfg.LeaseManager
	codec := t.execCfg.Codec

	typeDesc, err := t.getTypeDescFromStore(ctx)
	if err != nil {
		return err
	}

	// If there are any names to drain, then do so.
	if len(typeDesc.DrainingNames) > 0 {
		if err := drainNamesForDescriptor(
			ctx,
			t.typeID,
			leaseMgr,
			codec,
			nil, /* beforeDrainNames */
		); err != nil {
			return err
		}
	}

	// If there are any read only enum members, promote them to writeable.
	if typeDesc.Kind == sqlbase.TypeDescriptor_ENUM {
		hasNonPublic := false
		for _, member := range typeDesc.EnumMembers {
			if member.Capability == sqlbase.TypeDescriptor_EnumMember_READ_ONLY {
				hasNonPublic = true
				break
			}
		}
		if hasNonPublic {
			// The version of the array type needs to get bumped as well so that
			// changes to the underlying type are picked up.
			update := func(_ *kv.Txn, descs map[sqlbase.ID]catalog.MutableDescriptor) error {
				typeDesc := descs[typeDesc.ID].(*sqlbase.MutableTypeDescriptor)
				didModify := false
				for i := range typeDesc.EnumMembers {
					member := &typeDesc.EnumMembers[i]
					if member.Capability == sqlbase.TypeDescriptor_EnumMember_READ_ONLY {
						member.Capability = sqlbase.TypeDescriptor_EnumMember_ALL
						didModify = true
					}
				}
				if !didModify {
					return lease.ErrDidntUpdateDescriptor
				}
				return nil
			}
			if _, err := leaseMgr.PublishMultiple(
				ctx,
				[]sqlbase.ID{typeDesc.ID, typeDesc.ArrayTypeID},
				update,
				func(*kv.Txn) error { return nil },
			); err != nil {
				return err
			}
		}
	}

	// Finally, make sure all of the leases are updated.
	if err := WaitToUpdateLeases(ctx, leaseMgr, t.typeID); err != nil {
		if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			return nil
		}
		return err
	}

	// If the type is being dropped, remove the descriptor here.
	if typeDesc.Dropped() {
		if err := t.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := txn.NewBatch()
			b.Del(sqlbase.MakeDescMetadataKey(codec, typeDesc.ID))
			return txn.Run(ctx, b)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (t *typeSchemaChanger) logTags() *logtags.Buffer {
	buf := &logtags.Buffer{}
	buf.Add("typeChangeExec", nil)
	buf.Add("type", t.typeID)
	return buf
}

// typeChangeResumer is the anchor struct for the type change job.
type typeChangeResumer struct {
	job *jobs.Job
}

// Resume implements the jobs.Resumer interface.
func (t *typeChangeResumer) Resume(
	ctx context.Context, phs interface{}, _ chan<- tree.Datums,
) error {
	tc := &typeSchemaChanger{
		typeID:  t.job.Details().(jobspb.TypeSchemaChangeDetails).TypeID,
		execCfg: phs.(*planner).execCfg,
	}
	// Set up the type changer to be retried.
	opts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     20 * time.Second,
		Multiplier:     1.5,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		tcErr := tc.exec(ctx)
		switch {
		case tcErr == nil:
			return nil
		case errors.Is(tcErr, sqlbase.ErrDescriptorNotFound):
			// If the descriptor for the ID can't be found, we assume that another
			// job executed already and dropped the type.
			log.Infof(
				ctx,
				"descriptor %d not found for type change job; assuming it was dropped, and exiting",
				tc.typeID,
			)
			return nil
		case !isPermanentSchemaChangeError(tcErr):
			// If this isn't a permanent error, then retry.
			log.Infof(ctx, "retrying type schema change due to retriable error %v", tcErr)
		default:
			return tcErr
		}
	}
	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t *typeChangeResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	// If the job failed, just try again to clean up any draining names.
	tc := &typeSchemaChanger{
		typeID:  t.job.Details().(jobspb.TypeSchemaChangeDetails).TypeID,
		execCfg: phs.(*planner).ExecCfg(),
	}
	typeDesc, err := tc.getTypeDescFromStore(ctx)
	if err != nil {
		return err
	}
	if len(typeDesc.DrainingNames) > 0 {
		if err := drainNamesForDescriptor(
			ctx,
			tc.typeID,
			tc.execCfg.LeaseManager,
			tc.execCfg.Codec,
			nil, /* beforeDrainNames */
		); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &typeChangeResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeTypeSchemaChange, createResumerFn)
}
