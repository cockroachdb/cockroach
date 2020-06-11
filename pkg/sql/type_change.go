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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// writeTypeChange should be called on a mutated type descriptor to ensure that
// the descriptor gets written to a batch, as well as ensuring that a job is
// created to perform the schema change on the type. If jobDesc is "", then the
// job description is not updated, if an existing job is present.
func (p *planner) writeTypeChange(
	ctx context.Context, typeDesc *sqlbase.MutableTypeDescriptor, jobDesc string,
) error {
	// Check if there is an active job for this type, otherwise create one.
	job, jobExists := p.extendedEvalCtx.SchemaChangeJobCache[typeDesc.ID]
	if jobExists {
		// Update it.
		if jobDesc != "" {
			if err := job.WithTxn(p.txn).SetDescription(ctx,
				func(ctx context.Context, description string) (string, error) {
					return strings.Join([]string{description, jobDesc}, ";" /* sep */), nil
				},
			); err != nil {
				return err
			}
		}
		log.Infof(ctx, "job %d: updated with type change for type %d", *job.ID(), typeDesc.ID)
	} else {
		// Or, create a new job.
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: sqlbase.IDs{typeDesc.ID},
			Details: jobspb.TypeChangeDetails{
				TypeID: typeDesc.ID,
			},
			Progress: jobspb.TypeChangeProgress{},
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

	// TODO (rohany): Once the desc.Collection has a hook to register a modified
	//  type, call into that hook here.

	// Maybe increment the type's version.
	typeDesc.MaybeIncrementVersion()

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

// typeChanger is the struct that actually runs the type schema change.
type typeChanger struct {
	typeID  sqlbase.ID
	execCfg *ExecutorConfig
}

// exec is the entry point for the type schema change process.
func (t *typeChanger) exec(ctx context.Context) error {
	ctx = logtags.AddTags(ctx, t.logTags())
	db := t.execCfg.DB
	leaseMgr := t.execCfg.LeaseManager
	codec := t.execCfg.Codec

	var typeDesc *sqlbase.ImmutableTypeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		typeDesc, err = sqlbase.GetTypeDescFromID(ctx, txn, codec, t.typeID)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
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

	// Finally, make sure all of the leases are updated.
	if err := waitToUpdateLeases(ctx, leaseMgr, t.typeID); err != nil {
		if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			return nil
		}
		return err
	}

	return nil
}

func (t *typeChanger) logTags() *logtags.Buffer {
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
	err := (&typeChanger{
		typeID:  t.job.Details().(jobspb.TypeChangeDetails).TypeID,
		execCfg: phs.(*planner).ExecCfg(),
	}).exec(ctx)
	// TODO (rohany): record this error, maybe retry?
	return err
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t *typeChangeResumer) OnFailOrCancel(context.Context, interface{}) error {
	// TODO (rohany): I don't think that there is any thing that needs to be
	//  done if a type job fails.
	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &typeChangeResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeTypeChange, createResumerFn)
}
