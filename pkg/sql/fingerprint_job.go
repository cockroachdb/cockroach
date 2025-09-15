// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

type fingerprintResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*fingerprintResumer)(nil)

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeFingerprint,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &fingerprintResumer{job: job}
		},
		jobs.UsesTenantCostControl,
	)
}

// Resume implements the jobs.Resumer interface.
func (f *fingerprintResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	execCfg := p.ExecCfg()
	details := f.job.Details().(jobspb.FingerprintDetails)
	
	log.Infof(ctx, "starting fingerprint job %d", f.job.ID())

	switch target := details.Target.(type) {
	case *jobspb.FingerprintDetails_Table:
		return f.fingerprintTable(ctx, execCfg, target.Table)
	case *jobspb.FingerprintDetails_Tenant:
		return f.fingerprintTenant(ctx, execCfg, target.Tenant)
	default:
		return errors.New("fingerprint job details must specify either table or tenant target")
	}
}

// fingerprintTable performs fingerprinting for a table target.
func (f *fingerprintResumer) fingerprintTable(
	ctx context.Context, execCfg *ExecutorConfig, target *jobspb.FingerprintDetails_FingerprintTableTarget,
) error {
	// Get the table descriptor
	tableID := target.TableID
	
	var tableDesc catalog.TableDescriptor
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		descsCol := execCfg.CollectionFactory.NewCollection(ctx)
		defer descsCol.ReleaseAll(ctx)
		
		var err error
		tableDesc, err = descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
		return err
	}); err != nil {
		return errors.Wrapf(err, "failed to resolve table %d", tableID)
	}

	log.Infof(ctx, "fingerprinting table %s (%d)", tableDesc.GetName(), tableID)

	// Get current progress
	progress := f.job.Progress()
	fingerprintProgress := progress.Details.(*jobspb.Progress_Fingerprint).Fingerprint
	
	// Initialize progress if empty
	if fingerprintProgress.TotalIndexes == 0 {
		fingerprintProgress.TotalIndexes = int64(len(tableDesc.PublicNonPrimaryIndexes()) + 1) // +1 for primary
		if err := f.updateProgress(ctx, fingerprintProgress); err != nil {
			return err
		}
	}

	// Track completed indexes to avoid re-processing
	completedIndexes := make(map[descpb.IndexID]bool)
	for _, indexID := range fingerprintProgress.CompletedIndexes {
		completedIndexes[indexID] = true
	}

	// Fingerprint primary index
	primaryIndex := tableDesc.GetPrimaryIndex()
	if !completedIndexes[primaryIndex.GetID()] {
		fp, err := f.fingerprintIndex(ctx, execCfg, tableDesc, primaryIndex, target.ExcludedColumns)
		if err != nil {
			return errors.Wrapf(err, "failed to fingerprint primary index")
		}
		
		// Update progress
		fingerprintProgress.CompletedIndexes = append(fingerprintProgress.CompletedIndexes, primaryIndex.GetID())
		fingerprintProgress.Results = append(fingerprintProgress.Results, &jobspb.FingerprintResult{
			IndexID:     primaryIndex.GetID(),
			IndexName:   primaryIndex.GetName(),
			Fingerprint: fp,
		})
		
		if err := f.updateProgress(ctx, fingerprintProgress); err != nil {
			return err
		}
		
		log.Infof(ctx, "fingerprinted primary index %s: %x", primaryIndex.GetName(), fp)
	}

	// Fingerprint secondary indexes
	for _, index := range tableDesc.PublicNonPrimaryIndexes() {
		if completedIndexes[index.GetID()] {
			continue // Already processed
		}
		
		fp, err := f.fingerprintIndex(ctx, execCfg, tableDesc, index, target.ExcludedColumns)
		if err != nil {
			return errors.Wrapf(err, "failed to fingerprint index %s", index.GetName())
		}
		
		// Update progress
		fingerprintProgress.CompletedIndexes = append(fingerprintProgress.CompletedIndexes, index.GetID())
		fingerprintProgress.Results = append(fingerprintProgress.Results, &jobspb.FingerprintResult{
			IndexID:     index.GetID(),
			IndexName:   index.GetName(),
			Fingerprint: fp,
		})
		
		if err := f.updateProgress(ctx, fingerprintProgress); err != nil {
			return err
		}
		
		log.Infof(ctx, "fingerprinted index %s: %x", index.GetName(), fp)
	}

	log.Infof(ctx, "completed fingerprinting table %s", tableDesc.GetName())
	return nil
}

// fingerprintTenant performs fingerprinting for a tenant target.
func (f *fingerprintResumer) fingerprintTenant(
	ctx context.Context, execCfg *ExecutorConfig, target *jobspb.FingerprintDetails_FingerprintTenantTarget,
) error {
	log.Infof(ctx, "fingerprinting tenant %s (%s)", target.TenantName, target.TenantId.String())
	
	// Create a span for the entire tenant keyspace
	tenantSpan := roachpb.Span{
		Key:    roachpb.Key("tenant_" + target.TenantId.String()),
		EndKey: roachpb.Key("tenant_" + target.TenantId.String()).PrefixEnd(),
	}
	
	// Use the existing fingerprint span functionality
	p, cleanup := NewInternalPlanner("fingerprint-job", nil, username.MakeSQLUsernameFromPreNormalizedString("admin"), &MemoryMetrics{}, execCfg, &sessiondata.SessionData{})
	defer cleanup()
	
	planner := p.(*planner)
	
	var startTime hlc.Timestamp
	if target.StartTimestamp != nil && !target.StartTimestamp.IsEmpty() {
		startTime = *target.StartTimestamp
	}
	
	fp, err := planner.FingerprintSpan(ctx, tenantSpan, startTime, target.AllRevisions, target.Stripped)
	if err != nil {
		return errors.Wrap(err, "failed to fingerprint tenant span")
	}
	
	// Store result
	progress := f.job.Progress()
	fingerprintProgress := progress.Details.(*jobspb.Progress_Fingerprint).Fingerprint
	fingerprintProgress.Results = append(fingerprintProgress.Results, &jobspb.FingerprintResult{
		IndexID:     0, // 0 for tenant fingerprints
		IndexName:   "tenant_span",
		Fingerprint: fp,
	})
	
	if err := f.updateProgress(ctx, fingerprintProgress); err != nil {
		return err
	}
	
	log.Infof(ctx, "completed fingerprinting tenant %s: %x", target.TenantName, fp)
	return nil
}

// fingerprintIndex computes the fingerprint for a specific index.
func (f *fingerprintResumer) fingerprintIndex(
	ctx context.Context,
	execCfg *ExecutorConfig,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
	excludedColumns []string,
) (uint64, error) {
	// Use existing BuildFingerprintQueryForIndex function
	query, err := BuildFingerprintQueryForIndex(tableDesc, index, excludedColumns)
	if err != nil {
		return 0, err
	}
	
	log.VEventf(ctx, 2, "executing fingerprint query: %s", query)
	
	// Execute the query using InternalDB
	var rows tree.Datums
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		fingerprintCols, err := txn.QueryRowEx(
			ctx, "fingerprint-job", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			query,
		)
		if err != nil {
			return err
		}
		rows = fingerprintCols
		return nil
	}); err != nil {
		return 0, err
	}
	
	if len(rows) != 1 {
		return 0, errors.New("fingerprint query returned unexpected results")
	}
	
	if rows[0] == tree.DNull {
		return 0, nil
	}
	
	fingerprintDatum := rows[0]
	fingerprint := uint64(*fingerprintDatum.(*tree.DInt))
	
	return fingerprint, nil
}

// updateProgress updates the job's progress information.
func (f *fingerprintResumer) updateProgress(
	ctx context.Context, fingerprintProgress *jobspb.FingerprintProgress,
) error {
	return f.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		prog := md.Progress
		prog.Details.(*jobspb.Progress_Fingerprint).Fingerprint = fingerprintProgress
		ju.UpdateProgress(prog)
		return nil
	})
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (f *fingerprintResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	log.Infof(ctx, "fingerprint job %d failed or canceled: %v", f.job.ID(), jobErr)
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (f *fingerprintResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

// Metrics implements the jobs.Resumer interface.
func (f *fingerprintResumer) Metrics() metric.Struct {
	return nil
}

// DumpTraceAfterRun implements the jobs.Resumer interface.
func (f *fingerprintResumer) DumpTraceAfterRun() bool {
	return false
}