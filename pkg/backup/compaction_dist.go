// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"net/url"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/kv/followerreads"
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

// createCompactionPlan creates an un-finalized physical plan that will
// distribute spans from a generator across the cluster for compaction.
func createCompactionPlan(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.BackupDetails,
	compactChain compactionChain,
	manifest *backuppb.BackupManifest,
	defaultStore cloud.ExternalStorage,
	kmsEnv cloud.KMSEnv,
) (compactionPlan, error) {
	log.Dev.Infof(
		ctx, "planning compaction of %d backups: %s",
		len(compactChain.chainToCompact),
		util.Map(compactChain.chainToCompact, func(m backuppb.BackupManifest) string {
			return m.ID.String()
		}),
	)
	backupLocalityMap, err := makeBackupLocalityMap(
		compactChain.compactedLocalityInfo, execCtx.User(),
	)
	if err != nil {
		return compactionPlan{}, err
	}
	introducedSpanFrontier, err := createIntroducedSpanFrontier(
		compactChain.backupChain, manifest.EndTime,
	)
	if err != nil {
		return compactionPlan{}, err
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
		return compactionPlan{}, err
	}

	spansToCompact, err := getSpansToCompact(
		ctx, execCtx, manifest, compactChain.chainToCompact, details, defaultStore, kmsEnv,
	)
	if err != nil {
		return compactionPlan{}, err
	}
	genSpan := func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
		return errors.Wrap(generateAndSendImportSpans(
			ctx,
			spansToCompact,
			compactChain.chainToCompact,
			compactChain.compactedIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			spanCh,
			false, /* useLink */
		), "generateAndSendImportSpans")
	}
	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()
	oracle := physicalplan.DefaultReplicaChooser

	locFilter := sql.SingleLocalityFilter(details.ExecutionLocality)
	if useBulkOracle.Get(&evalCtx.Settings.SV) {
		var err error
		oracle, err = followerreads.NewLocalityFilteringBulkOracle(
			dsp.ReplicaOracleConfig(evalCtx.Locality),
			locFilter,
		)
		if err != nil {
			return compactionPlan{}, errors.Wrap(err, "failed to create locality filtering bulk oracle")
		}
	}

	planCtx, instanceIDs, err := dsp.SetupAllNodesPlanningWithOracle(
		ctx, evalCtx, execCtx.ExecCfg(),
		oracle, locFilter, sql.NoStrictLocalityFiltering,
	)
	if err != nil {
		return compactionPlan{}, errors.Wrap(err, "setting up node planning")
	}
	instanceLocalities := make([]roachpb.Locality, len(instanceIDs))
	for i, id := range instanceIDs {
		sqlInstanceInfo, err := dsp.GetSQLInstanceInfo(ctx, id)
		if err != nil {
			return compactionPlan{}, errors.Wrap(err, "getting instance info")
		}
		instanceLocalities[i] = sqlInstanceInfo.Locality
	}
	localitySets, err := buildLocalitySets(
		ctx, instanceIDs, instanceLocalities, details.StrictLocalityFiltering, genSpan,
	)
	if err != nil {
		return compactionPlan{}, errors.Wrap(err, "building locality sets")
	}
	corePlacements, err := createCompactionCorePlacements(
		ctx, jobID, execCtx.User(), details, manifest.ElidedPrefix, genSpan,
		spansToCompact, instanceIDs, localitySets, targetSize, maxFiles,
	)
	if err != nil {
		return compactionPlan{}, errors.Wrap(err, "creating core placements")
	}

	plan := planCtx.NewPhysicalPlan()
	plan.AddNoInputStage(
		corePlacements,
		execinfrapb.PostProcessSpec{},
		[]*types.T{},
		execinfrapb.Ordering{},
		nil, /* finalizeLastStageCb */
	)
	return compactionPlan{plan: plan, planCtx: planCtx, localitySets: localitySets}, nil
}

type compactionPlan struct {
	plan         *sql.PhysicalPlan
	planCtx      *sql.PlanningCtx
	localitySets map[string]*localitySet
}

func runCompactionPlan(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	compactionPlan compactionPlan,
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	sql.FinalizePlan(ctx, compactionPlan.planCtx, compactionPlan.plan)

	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			progCh <- meta.BulkProcessorProgress
		}

		// TODO (kev-cao): Add channel for tracing aggregator events.
		return nil
	}
	rowResultWriter := sql.NewRowResultWriter(nil)
	recv := sql.MakeDistSQLReceiver(
		ctx,
		sql.NewMetadataCallbackWriter(rowResultWriter, metaFn),
		tree.Rows,
		nil, /* rangeCache */
		nil, /* txn */
		nil, /* clockUpdater */
		execCtx.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	jobsprofiler.StorePlanDiagram(
		ctx, execCtx.ExecCfg().DistSQLSrv.Stopper,
		compactionPlan.plan, execCtx.ExecCfg().InternalDB, jobID,
	)

	evalCtxCopy := execCtx.ExtendedEvalContext().Copy()
	execCtx.DistSQLPlanner().Run(
		ctx, compactionPlan.planCtx,
		nil /* txn */, compactionPlan.plan, recv, evalCtxCopy, nil, /* finishedSetupFn */
	)
	return rowResultWriter.Err()
}

type localitySet struct {
	instanceIDs  []base.SQLInstanceID
	totalEntries int

	currInstanceIdx  int
	currEntriesAdded int
}

// maybeUpdateTargetInstance increments currInstanceIdx if currEntriesAdded equals the target
// number of entries for the current instance.
func (set *localitySet) maybeUpdateTargetInstance() {
	var targetNumEntries int
	numEntriesPerNode := set.totalEntries / len(set.instanceIDs)
	leftoverEntries := set.totalEntries % len(set.instanceIDs)
	if set.currInstanceIdx < leftoverEntries {
		// This more evenly distributes the leftover entries across the nodes
		// after doing integer division to assign the entries to the nodes.
		targetNumEntries = numEntriesPerNode + 1
	} else {
		targetNumEntries = numEntriesPerNode
	}
	if set.currEntriesAdded == targetNumEntries {
		set.currInstanceIdx++
		set.currEntriesAdded = 0
	}
}

// buildLocalitySets does a dry run of genSpan in order to build a locality to localitySet mapping
// which contains the available nodes and number of expected entries per locality.
//
// In a non locality-aware compaction, the returned map will contain a single entry
// keyed by "default" which is assigned all availble nodes and all entries.
//
// In a locality-aware compaction, the localitySet keyed by "default" will contain nodes which
// don't match one of the locality-specific uris, and is assigned entries from the default URI.
// If there is no data in the default URI to compact, the default set will not be in the returned map.
//
// If any of our sets (including the default set) has a locality which does not match any
// available nodes, this function has two behaviors, depending on whether strict is set.
// If strict is true, this function will return an error. If strict is false, we fall back to
// assigning all available nodes. In that case, compactions may output data to a different URI
// than they recieved it from. This is an edge case that would occur if the cluster topology has
// changed between backup time and compaction time such that all nodes matching one of our URIs
// become unavailable (for example, a region going down).
func buildLocalitySets(
	ctx context.Context,
	instanceIDs []base.SQLInstanceID,
	instanceLocalities []roachpb.Locality,
	strict bool,
	genSpan func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
) (map[string]*localitySet, error) {
	localitySets := make(map[string]*localitySet)

	countSpansCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	countTasks := []func(ctx context.Context) error{
		func(ctx context.Context) error {
			for entry := range countSpansCh {
				locality, err := entryLocality(entry)
				if err != nil {
					return err
				}
				if locality == "" {
					locality = "default"
				}

				if set, ok := localitySets[locality]; ok {
					set.totalEntries += 1
				} else {
					localitySets[locality] = &localitySet{totalEntries: 1}
				}
			}
			return nil
		},
		func(ctx context.Context) error {
			defer close(countSpansCh)
			return genSpan(ctx, countSpansCh)
		},
	}
	if err := ctxgroup.GoAndWait(ctx, countTasks...); err != nil {
		return nil, errors.Wrapf(err, "counting number of spans to compact")
	}

	for i, id := range instanceIDs {
		instanceLocality := instanceLocalities[i]
		matched := false
		for tierStr, set := range localitySets {
			if tierStr == "default" {
				continue
			}

			tier := roachpb.Tier{}
			if err := tier.FromString(tierStr); err != nil {
				return nil, errors.Wrap(err, "failed to parse locality set tier")
			}
			setLocality := roachpb.Locality{Tiers: []roachpb.Tier{tier}}

			if ok, _ := instanceLocality.Matches(setLocality); ok {
				set.instanceIDs = append(set.instanceIDs, id)
				matched = true
				break
			}
		}

		if !matched {
			// The instance does not match any of our filters, and should be assigned to the default set.
			if set, ok := localitySets["default"]; ok {
				set.instanceIDs = append(set.instanceIDs, id)
			} else {
				localitySets["default"] = &localitySet{instanceIDs: []base.SQLInstanceID{id}}
			}
		}
	}

	for setLocality, set := range localitySets {
		if len(set.instanceIDs) == 0 {
			if strict {
				return nil, errors.Newf(
					"no nodes available for processing data from locality %s in strict locality-aware compaction",
					setLocality,
				)
			}
			// In a non-strict setting, if we don't have any nodes available that match the locality
			// filter, we just evenly distribute the work across all available nodes.
			set.instanceIDs = instanceIDs
		}
		slices.Sort(set.instanceIDs)
	}

	return localitySets, nil
}

// createCompactionCorePlacements takes spans from a generator and, per localitySet, evenly
// distributes matching spans across nodes in the set, returning the core placements
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
	localitySets map[string]*localitySet,
	targetSize int64,
	maxFiles int64,
) ([]physicalplan.ProcessorCorePlacement, error) {
	instanceEntries := make(map[base.SQLInstanceID]*roachpb.SpanGroup)
	for _, id := range sqlInstanceIDs {
		instanceEntries[id] = &roachpb.SpanGroup{}
	}

	spanEntryCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	var tasks []func(ctx context.Context) error
	tasks = append(tasks, func(ctx context.Context) error {
		defer close(spanEntryCh)
		return genSpan(ctx, spanEntryCh)
	})
	tasks = append(tasks, func(ctx context.Context) error {
		for entry := range spanEntryCh {
			entryLocality, err := entryLocality(entry)
			if err != nil {
				return err
			}
			if entryLocality == "" {
				entryLocality = "default"
			}

			set, ok := localitySets[entryLocality]
			if !ok {
				return errors.AssertionFailedf("no locality set for entry with locality %s", entryLocality)
			}

			instanceID := set.instanceIDs[set.currInstanceIdx]
			instanceEntries[instanceID].Add(entry.Span)
			set.currEntriesAdded++
			set.maybeUpdateTargetInstance()
		}
		return nil
	})
	if err := ctxgroup.GoAndWait(ctx, tasks...); err != nil {
		return nil, errors.Wrapf(err, "distributing span entries to processors")
	}

	corePlacements := make([]physicalplan.ProcessorCorePlacement, 0, len(sqlInstanceIDs))
	for id, entries := range instanceEntries {
		corePlacements = append(corePlacements, physicalplan.ProcessorCorePlacement{
			SQLInstanceID: id,
			Core: execinfrapb.ProcessorCoreUnion{
				CompactBackups: &execinfrapb.CompactBackupsSpec{
					JobID:            int64(jobID),
					DefaultURI:       details.URI,
					Destination:      details.Destination,
					Encryption:       details.EncryptionOptions,
					StartTime:        details.StartTime,
					EndTime:          details.EndTime,
					ElideMode:        elideMode,
					UserProto:        user.EncodeProto(),
					Spans:            spansToCompact,
					TargetSize:       targetSize,
					MaxFiles:         maxFiles,
					URIsByLocalityKV: details.URIsByLocalityKV,
					StrictLocality:   details.StrictLocalityFiltering,
					AssignedSpans:    entries.Slice(),
				}}})
	}

	return corePlacements, nil
}

// entryLocality returns the locality filter string attached to the URI of the most recent file in
// the entry. If the URI does not have a locality filter attached to it, it will return an empty string.
func entryLocality(entry execinfrapb.RestoreSpanEntry) (string, error) {
	if len(entry.Files) == 0 {
		return "", errors.AssertionFailedf("restore span entry has no files")
	}
	// Note that the last file in entry.Files will be the most recent.
	uri, err := url.Parse(entry.Files[len(entry.Files)-1].Dir.URI)
	if err != nil {
		return "", err
	}
	return uri.Query().Get("COCKROACH_LOCALITY"), nil
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
