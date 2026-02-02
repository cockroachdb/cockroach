// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

	// EnforceUniqueness enables duplicate key detection during the final merge
	// iteration. When true:
	// - Consecutive duplicates within an SST batch raise DuplicateKeyError.
	// - Keys conflicting with pre-existing KV data raise KeyCollisionError.
	// TODO(161447): cross-SST duplicates within the same merge are not yet
	// detected by this mechanism.
	//
	// Should be true when building unique indexes. Callers are responsible for
	// wrapping errors into user-friendly messages.
	EnforceUniqueness bool

	// OnProgress is called when progress metadata is received from the
	// distributed merge flow. This allows the caller to track task completion
	// during merge iterations.
	OnProgress func(context.Context, *execinfrapb.ProducerMetadata) error
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
) ([]execinfrapb.BulkMergeSpec_SST, error) {
	logMergeInputs(ctx, ssts, opts.Iteration, opts.MaxIterations)

	plan, planCtx, err := newBulkMergePlan(ctx, execCtx, ssts, spans, genOutputURIAndRecordPrefix, opts)
	if err != nil {
		return nil, err
	}

	var result execinfrapb.BulkMergeSpec_Output
	rowWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		return protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &result)
	})

	sqlReceiver := makeMergeReceiver(ctx, execCtx, rowWriter, opts.OnProgress)
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
		return nil, err
	}

	if opts.Iteration == opts.MaxIterations {
		// Final iteration writes directly to KV; no SST outputs expected.
		return nil, nil
	}

	// Sort the SSTs by their range start key. Ingest requires that SSTs are
	// sorted an non-overlapping. The output of merge is not sorted because SSTs
	// are emitted as their task is completed.
	slices.SortFunc(result.SSTs, func(i, j execinfrapb.BulkMergeSpec_SST) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})

	return result.SSTs, nil
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
// All logging is opt-in via log.V(2) for detailed iteration tracking.
func logMergeInputs(
	ctx context.Context, ssts []execinfrapb.BulkMergeSpec_SST, iteration int, maxIterations int,
) {
	// All iteration logging is verbose (opt-in).
	if !log.V(2) {
		return
	}

	iterType := "local"
	if iteration == maxIterations {
		iterType = "final"
	}
	log.Dev.Infof(ctx, "Distributed merge iteration %d (%s) starting with %d input SSTs",
		iteration, iterType, len(ssts))

	var totalInputKeys uint64
	for i, sst := range ssts {
		totalInputKeys += sst.KeyCount
		log.Dev.Infof(ctx, "  input SST[%d]: %d keys, span=[%s, %s), uri=%s",
			i, sst.KeyCount, sst.StartKey, sst.EndKey, sst.URI)
	}
	log.Dev.Infof(ctx, "  Total input keys: %d", totalInputKeys)
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
	) ([]execinfrapb.BulkMergeSpec_SST, error) {
		return Merge(ctx, execCtx, ssts, spans, genOutputURIAndRecordPrefix, MergeOptions{
			Iteration:         iteration,
			MaxIterations:     maxIterations,
			WriteTimestamp:    writeTimestamp,
			EnforceUniqueness: enforceUniqueness,
			OnProgress:        onProgress,
		})
	})
}
