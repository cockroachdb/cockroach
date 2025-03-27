// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// runCompactionPlan creates and runs a distsql plan to compact the spans
// in the backup chain that need to be compacted. It sends updates from the
// BulkProcessor to the provided progress channel. It is the caller's
// responsibility to close the progress channel.
func (c *compactionChain) runCompactionPlan(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	manifest *backuppb.BackupManifest,
	details jobspb.BackupDetails,
	defaultStore cloud.ExternalStorage,
	kmsEnv cloud.KMSEnv,
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	log.Infof(
		ctx, "planning compaction of %d backups: %s",
		len(c.chainToCompact), util.Map(c.chainToCompact, func(m backuppb.BackupManifest) string {
			return m.ID.String()
		}),
	)
	backupLocalityMap, err := makeBackupLocalityMap(c.compactedLocalityInfo, execCtx.User())
	if err != nil {
		return err
	}
	introducedSpanFrontier, err := createIntroducedSpanFrontier(c.backupChain, manifest.EndTime)
	if err != nil {
		return err
	}
	defer introducedSpanFrontier.Release()
	targetSize := targetRestoreSpanSize.Get(&execCtx.ExecCfg().Settings.SV)
	maxFiles := maxFileCount.Get(&execCtx.ExecCfg().Settings.SV)
	var fsc fileSpanComparator = &exclusiveEndKeyComparator{}
	filter, err := makeSpanCoveringFilter(
		manifest.Spans,
		[]jobspb.RestoreProgress_FrontierEntry{},
		introducedSpanFrontier,
		targetSize,
		maxFiles,
	)
	if err != nil {
		return err
	}

	spansToCompact, err := getSpansToCompact(
		ctx, execCtx, manifest, c.chainToCompact, details, defaultStore, kmsEnv,
	)
	if err != nil {
		return err
	}
	genSpan := func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
		defer close(spanCh)
		return errors.Wrap(generateAndSendImportSpans(
			ctx,
			spansToCompact,
			c.chainToCompact,
			c.compactedIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			spanCh,
		), "generateAndSendImportSpans")
	}
	dsp := execCtx.DistSQLPlanner()
	plan, planCtx, err := createCompactionPlan(
		ctx, execCtx, jobID, details, manifest, dsp, genSpan, spansToCompact,
	)
	if err != nil {
		return errors.Wrap(err, "creating compaction plan")
	}
	sql.FinalizePlan(ctx, planCtx, plan)

	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			progCh <- meta.BulkProcessorProgress
		}
		return nil
	}
	recv := sql.MakeDistSQLReceiver(
		ctx,
		sql.NewMetadataCallbackWriter(nil, metaFn),
		tree.Rows,
		nil, /* rangeCache */
		nil, /* txn */
		nil, /* clockUpdater */
		execCtx.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	jobsprofiler.StorePlanDiagram(
		ctx, execCtx.ExecCfg().DistSQLSrv.Stopper, plan, execCtx.ExecCfg().InternalDB, jobID,
	)

	evalCtxCopy := execCtx.ExtendedEvalContext().Copy()
	dsp.Run(ctx, planCtx, nil /* txn */, plan, recv, evalCtxCopy, nil /* finishedSetupFn */)
	return nil
}

// createCompactionPlan creates an unfinalized physical plan that will
// distribute spans from a generator across the cluster for compaction.
func createCompactionPlan(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.BackupDetails,
	manifest *backuppb.BackupManifest,
	dsp *sql.DistSQLPlanner,
	genSpan func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
	spansToCompact roachpb.Spans,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	numEntries, err := countRestoreSpanEntries(ctx, genSpan)
	if err != nil {
		return nil, nil, errors.Wrap(err, "counting number of restore span entries")
	}

	planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanningWithOracle(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg(),
		physicalplan.DefaultReplicaChooser, roachpb.Locality{},
	)
	if err != nil {
		return nil, nil, err
	}

	plan := planCtx.NewPhysicalPlan()
	corePlacements, err := createCompactionCorePlacements(
		ctx, jobID, execCtx.User(), details, manifest.ElidedPrefix,
		genSpan, spansToCompact, sqlInstanceIDs, numEntries,
	)
	if err != nil {
		return nil, nil, err
	}
	plan.AddNoInputStage(
		corePlacements,
		execinfrapb.PostProcessSpec{},
		[]*types.T{},
		execinfrapb.Ordering{},
		nil, /* finalizeLastStageCb */
	)
	return plan, planCtx, nil
}

// countRestoreSpanEntries counts the number of restore span entries that will be
// be delivered by the given generator.
func countRestoreSpanEntries(
	ctx context.Context,
	genSpan func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
) (int, error) {
	countSpansCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	var numImportSpans int
	countTasks := []func(ctx context.Context) error{
		func(ctx context.Context) error {
			for range countSpansCh {
				numImportSpans++
			}
			return nil
		},
		func(ctx context.Context) error {
			return genSpan(ctx, countSpansCh)
		},
	}
	if err := ctxgroup.GoAndWait(ctx, countTasks...); err != nil {
		return 0, errors.Wrapf(err, "counting number of spans to compact")
	}
	return numImportSpans, nil
}

// createCompactionCorePlacements takes spans from a generator and evenly
// distributes them across nodes in the cluster, returning the core core placements
// reflecting that distribution.
func createCompactionCorePlacements(
	ctx context.Context,
	jobID jobspb.JobID,
	user username.SQLUsername,
	details jobspb.BackupDetails,
	elideMode execinfrapb.ElidePrefix,
	genSpan func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
	spansToCompact roachpb.Spans,
	sqlInstanceIDs []base.SQLInstanceID,
	numEntries int,
) ([]physicalplan.ProcessorCorePlacement, error) {
	numNodes := len(sqlInstanceIDs)
	corePlacements := make([]physicalplan.ProcessorCorePlacement, numNodes)

	spanEntryCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	var tasks []func(ctx context.Context) error
	tasks = append(tasks, func(ctx context.Context) error {
		return genSpan(ctx, spanEntryCh)
	})
	tasks = append(tasks, func(ctx context.Context) error {
		numEntriesPerNode := numEntries / numNodes
		leftoverEntries := numEntries % numNodes
		getTargetNumEntries := func(nodeIdx int) int {
			if nodeIdx <= leftoverEntries {
				// This more evenly distributes the leftover entries across the nodes
				// after doing integer division to assign the entries to the nodes.
				return numEntriesPerNode + 1
			}
			return numEntriesPerNode
		}
		currNode := 0
		currEntries := make([]execinfrapb.RestoreSpanEntry, 0, numEntriesPerNode+1)
		targetNumEntries := getTargetNumEntries(currNode)

		for entry := range spanEntryCh {
			if len(currEntries) == targetNumEntries {
				corePlacements[currNode].SQLInstanceID = sqlInstanceIDs[currNode]
				corePlacements[currNode].Core.CompactBackups = &execinfrapb.CompactBackupsSpec{
					JobID:       int64(jobID),
					Destination: details.Destination,
					Encryption:  details.EncryptionOptions,
					StartTime:   details.StartTime,
					EndTime:     details.EndTime,
					ElideMode:   elideMode,
					UserProto:   user.EncodeProto(),
					Spans:       spansToCompact,
					AssignedSpans: util.Map(currEntries, func(entry execinfrapb.RestoreSpanEntry) roachpb.Span {
						return entry.Span
					}),
				}
				currNode++
				targetNumEntries = getTargetNumEntries(currNode)
				currEntries = currEntries[:0]
			}
			if currNode == numNodes {
				return nil
			}
			currEntries = append(currEntries, entry)
		}
		return nil
	})
	if err := ctxgroup.GoAndWait(ctx, tasks...); err != nil {
		return nil, errors.Wrapf(err, "distributing span entries to processors")
	}
	return corePlacements, nil
}

// getSpansToCompact returns all remaining spans the backup manifest that
// need to be compacted.
func getSpansToCompact(
	ctx context.Context,
	execCtx sql.JobExecContext,
	manifest *backuppb.BackupManifest,
	backupChain []backuppb.BackupManifest,
	details jobspb.BackupDetails,
	defaultStore cloud.ExternalStorage,
	kmsEnv cloud.KMSEnv,
) (roachpb.Spans, error) {
	var tables []catalog.TableDescriptor
	for _, desc := range manifest.Descriptors {
		catDesc := backupinfo.NewDescriptorForManifest(&desc)
		if table, ok := catDesc.(catalog.TableDescriptor); ok {
			tables = append(tables, table)
		}
	}
	backupCodec, err := backupinfo.MakeBackupCodec(backupChain)
	if err != nil {
		return nil, err
	}
	spans, err := spansForAllRestoreTableIndexes(
		backupCodec,
		tables,
		nil,   /* revs */
		false, /* schemaOnly */
		false, /* forOnlineRestore */
	)
	if err != nil {
		return nil, err
	}
	completedSpans, completedIntroducedSpans, err := getCompletedSpans(
		ctx, execCtx, manifest, defaultStore, details.EncryptionOptions, kmsEnv,
	)
	if err != nil {
		return nil, err
	}
	spans = filterSpans(spans, completedSpans)
	spans = filterSpans(spans, completedIntroducedSpans)
	return spans, nil
}
