// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// InstanceUnavailabilityTimeout is the duration after which the job will be
// marked as permanently failed if required SQL instances remain unavailable.
var InstanceUnavailabilityTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkmerge.instance_unavailability.timeout",
	"duration to wait for unavailable SQL instances before permanently failing the job",
	30*time.Minute,
)

// MergeOptions contains configuration for the distributed merge operation.
type MergeOptions struct {
	// Iteration is the current merge iteration (1-based).
	Iteration int

	// MaxIterations is the total number of merge iterations planned.
	// When Iteration == MaxIterations, data is written directly to KV
	// instead of producing SSTs in external storage.
	MaxIterations int

	// WriteTimestamp is the MVCC timestamp to use when the final iteration
	// writes to KV. If nil, the current cluster time is used.
	WriteTimestamp *hlc.Timestamp

	// EnforceUniqueness enables duplicate key detection during the merge.
	// When true:
	// - Cross-SST duplicates within the same merge raise DuplicateKeyError.
	// - Consecutive duplicates within an SST batch raise DuplicateKeyError.
	// - Keys conflicting with pre-existing KV data raise KeyCollisionError.
	//
	// Should be true when building unique indexes. Callers are responsible for
	// wrapping errors into user-friendly messages.
	EnforceUniqueness bool

	// MemoryMonitor selects which parent memory monitor the merge processor uses.
	MemoryMonitor execinfrapb.BulkMergeSpec_MemoryMonitor

	// OnProgress is called when progress metadata is received from the
	// distributed merge flow. This allows the caller to track task completion
	// during merge iterations.
	OnProgress func(context.Context, *execinfrapb.ProducerMetadata) error
}

// MergeResult contains the output of a distributed merge operation.
type MergeResult struct {
	// SSTs contains output SSTs from intermediate merge iterations.
	// Empty for the final iteration which writes directly to KV.
	SSTs []execinfrapb.BulkMergeSpec_SST

	// IngestSummary contains the BulkOpSummary from the final merge
	// iteration's KV ingest. It reflects the actual row count written
	// to KV (excluding skipped identical duplicates from
	// checkpoint-and-resume overlap). Only populated for the final
	// iteration.
	IngestSummary kvpb.BulkOpSummary
}

// Merge creates and waits on a DistSQL flow that merges the provided SSTs into
// the ranges defined by the input splits.
func Merge(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ssts []execinfrapb.BulkMergeSpec_SST,
	spans []roachpb.Span,
	genOutputURIAndRecordPrefix func(sqlInstance base.SQLInstanceID) (string, error),
	opts MergeOptions,
) (MergeResult, error) {
	logMergeInputs(ctx, ssts, opts.Iteration, opts.MaxIterations)

	// Proactive plan-coverage gate: retry until SetupAllNodesPlanning
	// returns an instance set that covers every node owning an input
	// SST. Avoids silent empty merge output when the planner's health
	// view briefly drops a required node.
	sv := &execCtx.ExecCfg().Settings.SV
	retryOpts := retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     1 * time.Minute,
		Multiplier:     2,
		MaxDuration:    InstanceUnavailabilityTimeout.Get(sv),
	}
	planCtx, sqlInstanceIDs, err := waitForViablePlan(ctx, ssts, retryOpts,
		func(ctx context.Context) (*sql.PlanningCtx, []base.SQLInstanceID, error) {
			return execCtx.DistSQLPlanner().SetupAllNodesPlanning(
				ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg())
		})
	if err != nil {
		return MergeResult{}, err
	}

	plan, err := newBulkMergePlan(
		ctx, execCtx, planCtx, sqlInstanceIDs, ssts, spans, genOutputURIAndRecordPrefix, opts)
	if err != nil {
		return MergeResult{}, err
	}

	var mergeResult MergeResult
	var result execinfrapb.BulkMergeSpec_Output
	rowWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		return protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &result)
	})

	// Wrap the caller's OnProgress callback to intercept the KV ingest
	// summary emitted by the merge processor at the end of the final
	// iteration.
	onProgress := func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			mergeResult.IngestSummary.Add(meta.BulkProcessorProgress.BulkSummary)
		}
		if opts.OnProgress != nil {
			return opts.OnProgress(ctx, meta)
		}
		return nil
	}

	sqlReceiver := makeMergeReceiver(ctx, execCtx, rowWriter, onProgress)
	defer sqlReceiver.Release()

	execCtx.DistSQLPlanner().Run(
		ctx,
		planCtx,
		nil,
		plan,
		sqlReceiver,
		&execCtx.ExtendedEvalContext().Context,
		nil,
	)

	if err := rowWriter.Err(); err != nil {
		return MergeResult{}, err
	}

	if opts.Iteration == opts.MaxIterations {
		// Final iteration writes directly to KV; no SST outputs expected.
		return mergeResult, nil
	}

	// Sort the SSTs by their range start key. Ingest requires that SSTs are
	// sorted an non-overlapping. The output of merge is not sorted because SSTs
	// are emitted as their task is completed.
	slices.SortFunc(result.SSTs, func(i, j execinfrapb.BulkMergeSpec_SST) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})

	mergeResult.SSTs = result.SSTs
	return mergeResult, nil
}

// makeMergeReceiver creates a DistSQLReceiver for the merge flow. If an
// onProgress callback is provided, the receiver is wrapped to invoke the
// callback on metadata messages (used for tracking task completion).
//
// Note: The two MakeDistSQLReceiver calls cannot be unified because the
// rowResultWriter interface (which both *CallbackResultWriter and
// *MetadataCallbackWriter implement) is unexported from the sql package.
// We cannot declare a variable of that interface type here to conditionally
// assign either writer type.
func makeMergeReceiver(
	ctx context.Context,
	execCtx sql.JobExecContext,
	rowWriter *sql.CallbackResultWriter,
	onProgress func(context.Context, *execinfrapb.ProducerMetadata) error,
) *sql.DistSQLReceiver {
	execCfg := execCtx.ExecCfg()
	if onProgress != nil {
		return sql.MakeDistSQLReceiver(
			ctx,
			sql.NewMetadataCallbackWriter(rowWriter, onProgress),
			tree.Rows,
			execCfg.RangeDescriptorCache,
			nil,
			nil,
			execCtx.ExtendedEvalContext().Tracing)
	}
	return sql.MakeDistSQLReceiver(
		ctx,
		rowWriter,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil,
		nil,
		execCtx.ExtendedEvalContext().Tracing)
}

// logMergeInputs logs the input SSTs for the current merge iteration.
// The main iteration message is always logged; detailed per-SST logging
// is opt-in via log.V(2).
func logMergeInputs(
	ctx context.Context, ssts []execinfrapb.BulkMergeSpec_SST, iteration int, maxIterations int,
) {
	iterType := "local"
	if iteration == maxIterations {
		iterType = "final"
	}

	// Always log the phase transition.
	log.Dev.Infof(ctx, "distributed merge: starting iteration %d of %d (%s) with %d input SSTs",
		iteration, maxIterations, iterType, len(ssts))

	// Detailed per-SST logging is verbose (opt-in).
	if !log.V(2) {
		return
	}

	var totalInputKeys uint64
	for i, sst := range ssts {
		totalInputKeys += sst.KeyCount
		log.Dev.Infof(ctx, "  input SST[%d]: %d keys, span=[%s, %s), uri=%s",
			i, sst.KeyCount, sst.StartKey, sst.EndKey, sst.URI)
	}
	log.Dev.Infof(ctx, "  total input keys: %d", totalInputKeys)
}

// planFn returns the planning context and planned SQL instance set for a
// single attempt of the merge planner. Implementations should call
// SetupAllNodesPlanning (or equivalent) and surface its outputs without
// building the full physical plan, so the retry loop can validate
// coverage before paying for plan construction.
type planFn func(ctx context.Context) (*sql.PlanningCtx, []base.SQLInstanceID, error)

// waitForViablePlan retries plan() until its instance set covers every
// SQL instance referenced by an input SST URI, or the retry budget is
// exhausted. On success it returns the planning context and instance
// set from the satisfying attempt. On timeout it returns the
// InstanceUnavailableError from the final attempt; on context
// cancellation it returns ctx.Err().
//
// The retry loop tolerates transient gaps where the planner briefly
// excludes a node that owns input SSTs (e.g., during a health-check
// blip). Without coverage, no merge task touches the SSTs on the
// excluded node and the merge silently produces empty output.
func waitForViablePlan(
	ctx context.Context, ssts []execinfrapb.BulkMergeSpec_SST, retryOpts retry.Options, plan planFn,
) (*sql.PlanningCtx, []base.SQLInstanceID, error) {
	var lastErr error
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		planCtx, instances, err := plan(ctx)
		if err != nil {
			return nil, nil, err
		}
		lastErr = CheckRequiredInstancesAvailable(ssts, instances)
		if lastErr == nil {
			return planCtx, instances, nil
		}
		log.Dev.Warningf(ctx,
			"distributed merge waiting for planner to cover required SQL instances "+
				"(attempt %d, timeout %s): %v",
			r.CurrentAttempt(), retryOpts.MaxDuration, lastErr,
		)
	}

	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	return nil, nil, errors.Wrapf(lastErr,
		"distributed merge failed: required SQL instances remain uncovered after %s",
		retryOpts.MaxDuration,
	)
}

func init() {
	// Register an adapter that receives individual parameters and constructs
	// MergeOptions internally. This avoids duplicating the MergeOptions type
	// in the sql package (which would cause an import cycle).
	sql.RegisterBulkMerge(func(
		ctx context.Context,
		execCtx sql.JobExecContext,
		ssts []execinfrapb.BulkMergeSpec_SST,
		spans []roachpb.Span,
		genOutputURIAndRecordPrefix func(base.SQLInstanceID) (string, error),
		iteration int,
		maxIterations int,
		writeTimestamp *hlc.Timestamp,
		enforceUniqueness bool,
		onProgress func(context.Context, *execinfrapb.ProducerMetadata) error,
		memoryMonitor execinfrapb.BulkMergeSpec_MemoryMonitor,
	) ([]execinfrapb.BulkMergeSpec_SST, error) {
		result, err := Merge(ctx, execCtx, ssts, spans, genOutputURIAndRecordPrefix, MergeOptions{
			Iteration:         iteration,
			MaxIterations:     maxIterations,
			WriteTimestamp:    writeTimestamp,
			EnforceUniqueness: enforceUniqueness,
			MemoryMonitor:     memoryMonitor,
			OnProgress:        onProgress,
		})
		return result.SSTs, err
	})
}
