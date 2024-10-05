// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// importRollbackResumer is an IMPORT ROLLBACK job that uses
// ImportEpoch based delete range's to roll back imported data. This
// job is synthesized during online restore to provide rollback of
// IMPORT data without relying on MVCC timestamps.
//
// NB: An alternative here would be to create a Import job in a
// reverting state with some new booleans to handle this
// ImportEpoch-based rollback.
type importRollbackResumer struct {
	job      *jobs.Job
	settings *cluster.Settings
}

func (r *importRollbackResumer) Resume(ctx context.Context, execCtx interface{}) error {
	cfg := execCtx.(sql.JobExecContext).ExecCfg()
	importRollbackPayload := r.job.Payload().Details.(*jobspb.Payload_ImportRollbackDetails).ImportRollbackDetails
	tableID := importRollbackPayload.TableID

	// We retry until paused or canceled. This job must eventually
	// complete for the table to come back online without manual
	// intervention.
	retryOpts := retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     10 * time.Minute,
	}
	for re := retry.StartWithCtx(ctx, retryOpts); re.Next(); {
		err := r.rollbackTable(ctx, cfg, tableID)
		if err != nil {
			log.Errorf(ctx, "rollback of table %d failed: %s", tableID, err.Error())
		} else {
			return nil
		}
	}
	return ctx.Err()
}

func (r *importRollbackResumer) rollbackTable(
	ctx context.Context, cfg *sql.ExecutorConfig, tableID catid.DescID,
) error {
	return sql.DescsTxn(ctx, cfg, func(ctx context.Context, txn isql.Txn, descsCol *descs.Collection) error {
		desc, err := descsCol.MutableByID(txn.KV()).Table(ctx, tableID)
		if err != nil {
			return errors.Wrapf(err, "looking up descriptor %d", tableID)
		}

		// TODO(ssd): We could fail here instead if we start tracking
		// progress. Right now, this could happen in normal operation
		// if we finish bringing the table online but then can't move
		// the job to succeeded for some reason.
		if desc.Public() {
			log.Infof(ctx, "table %d already PUBLIC cannot rollback", tableID)
			return nil
		}

		importEpoch := desc.TableDesc().ImportEpoch
		if importEpoch <= 0 {
			return errors.Errorf("cannot ROLLBACK table %d with ImportEpoch = 0", tableID)
		}

		// TODO(ssd): This isn't transactional, but we
		// currently do this inside the DescsTxn in the normal
		// import rollback path so I've done that here for
		// consistency.
		if err := sql.DeleteTableWithPredicate(
			ctx,
			cfg.DB,
			cfg.Codec,
			&cfg.Settings.SV,
			cfg.DistSender,
			tableID,
			kvpb.DeleteRangePredicates{
				ImportEpoch: importEpoch,
			},
			sql.RevertTableDefaultBatchSize); err != nil {
			return errors.Wrap(err, "rolling back IMPORT INTO in non empty table via DeleteRange")
		}

		log.Infof(ctx, "transitioning table %q (%d) to PUBLIC", desc.GetName(), desc.GetID())
		desc.SetPublic()
		desc.FinalizeImport()
		b := txn.KV().NewBatch()
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, desc, b,
		); err != nil {
			return errors.Wrapf(err, "publishing table %d", desc.ID)
		}
		return txn.KV().Run(ctx, b)
	})
}

func (*importRollbackResumer) CollectProfile(context.Context, interface{}) error {
	return nil
}

func (r *importRollbackResumer) OnFailOrCancel(context.Context, interface{}, error) error {
	return errors.AssertionFailedf("OnFailOrCancel called on non-cancellable job %d", r.job.ID())
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeImportRollback,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &importRollbackResumer{
				job:      job,
				settings: settings,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
