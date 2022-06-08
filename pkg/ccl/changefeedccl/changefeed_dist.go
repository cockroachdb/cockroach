// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeeddist"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	rowexec.NewChangeAggregatorProcessor = newChangeAggregatorProcessor
	rowexec.NewChangeFrontierProcessor = newChangeFrontierProcessor
}

const (
	changeAggregatorProcName = `changeagg`
	changeFrontierProcName   = `changefntr`
)

// distChangefeedFlow plans and runs a distributed changefeed.
//
// One or more ChangeAggregator processors watch table data for changes. These
// transform the changed kvs into changed rows and either emit them to a sink
// (such as kafka) or, if there is no sink, forward them in columns 1,2,3 (where
// they will be eventually returned directly via pgwire). In either case,
// periodically a span will become resolved as of some timestamp, meaning that
// no new rows will ever be emitted at or below that timestamp. These span-level
// resolved timestamps are emitted as a marshaled `jobspb.ResolvedSpan` proto in
// column 0.
//
// The flow will always have exactly one ChangeFrontier processor which all the
// ChangeAggregators feed into. It collects all span-level resolved timestamps
// and aggregates them into a changefeed-level resolved timestamp, which is the
// minimum of the span-level resolved timestamps. This changefeed-level resolved
// timestamp is emitted into the changefeed sink (or returned to the gateway if
// there is no sink) whenever it advances. ChangeFrontier also updates the
// progress of the changefeed's corresponding system job.
func distChangefeedFlow(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
) error {
	var err error
	details, err = validateDetails(details)
	if err != nil {
		return err
	}

	// NB: A non-empty high water indicates that we have checkpointed a resolved
	// timestamp. Skipping the initial scan is equivalent to starting the
	// changefeed from a checkpoint at its start time. Initialize the progress
	// based on whether we should perform an initial scan.
	{
		h := progress.GetHighWater()
		noHighWater := (h == nil || h.IsEmpty())
		// We want to set the highWater and thus avoid an initial scan if either
		// this is a cursor and there was no request for one, or we don't have a
		// cursor but we have a request to not have an initial scan.
		initialScanType, err := initialScanTypeFromOpts(details.Opts)
		if err != nil {
			return err
		}
		if noHighWater && initialScanType == changefeedbase.NoInitialScan {
			// If there is a cursor, the statement time has already been set to it.
			progress.Progress = &jobspb.Progress_HighWater{HighWater: &details.StatementTime}
		}
	}

	execCfg := execCtx.ExecCfg()
	var initialHighWater hlc.Timestamp
	var trackedSpans []roachpb.Span
	{
		spansTS := details.StatementTime
		if h := progress.GetHighWater(); h != nil && !h.IsEmpty() {
			initialHighWater = *h
			// If we have a high-water set, use it to compute the spans, since the
			// ones at the statement time may have been garbage collected by now.
			spansTS = initialHighWater
		}

		// We want to fetch the target spans as of the timestamp following the
		// highwater unless the highwater corresponds to a timestamp of an initial
		// scan. This logic is irritatingly complex but extremely important. Namely,
		// we may be here because the schema changed at the current resolved
		// timestamp. However, an initial scan should be performed at exactly the
		// timestamp specified; initial scans can be created at the timestamp of a
		// schema change and thus should see the side-effect of the schema change.
		isRestartAfterCheckpointOrNoInitialScan := progress.GetHighWater() != nil
		if isRestartAfterCheckpointOrNoInitialScan {
			spansTS = spansTS.Next()
		}
		var err error
		var tableDescs []catalog.TableDescriptor
		tableDescs, err = fetchTableDescriptors(ctx, execCfg, AllTargets(details), spansTS)
		if err != nil {
			return err
		}

		if filterExpr, isSet := details.Opts[changefeedbase.OptPrimaryKeyFilter]; isSet {
			if len(tableDescs) > 1 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"option %s can only be used with 1 changefeed target (found %d)",
					changefeedbase.OptPrimaryKeyFilter, len(tableDescs),
				)
			}
			trackedSpans, err = constrainSpansByExpression(ctx, execCtx, filterExpr, tableDescs[0])
			if err != nil {
				return err
			}
		} else {
			for _, d := range tableDescs {
				trackedSpans = append(trackedSpans, d.PrimaryIndexSpan(execCfg.Codec))
			}
		}
	}

	var checkpoint jobspb.ChangefeedProgress_Checkpoint
	if cf := progress.GetChangefeed(); cf != nil && cf.Checkpoint != nil {
		checkpoint = *cf.Checkpoint
	}

	var distflowKnobs changefeeddist.TestingKnobs
	if knobs, ok := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok && knobs != nil {
		distflowKnobs = knobs.DistflowKnobs
	}

	return changefeeddist.StartDistChangefeed(
		ctx, execCtx, jobID, details, trackedSpans, initialHighWater, checkpoint, resultsCh, distflowKnobs)
}

func fetchTableDescriptors(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	targets []jobspb.ChangefeedTargetSpecification,
	ts hlc.Timestamp,
) ([]catalog.TableDescriptor, error) {
	var targetDescs []catalog.TableDescriptor

	fetchSpans := func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		targetDescs = nil
		if err := txn.SetFixedTimestamp(ctx, ts); err != nil {
			return err
		}
		seen := make(map[descpb.ID]struct{}, len(targets))
		// Note that all targets are currently guaranteed to have a Table ID
		// and lie within the primary index span. Deduplication is important
		// here as requesting the same span twice will deadlock.
		for _, table := range targets {
			if _, dup := seen[table.TableID]; dup {
				continue
			}
			seen[table.TableID] = struct{}{}
			flags := tree.ObjectLookupFlagsWithRequired()
			flags.AvoidLeased = true
			tableDesc, err := descriptors.GetImmutableTableByID(ctx, txn, table.TableID, flags)
			if err != nil {
				return err
			}
			targetDescs = append(targetDescs, tableDesc)
		}
		return nil
	}
	if err := sql.DescsTxn(ctx, execCfg, fetchSpans); err != nil {
		return nil, err
	}
	return targetDescs, nil
}

func constrainSpansByExpression(
	ctx context.Context, execCtx sql.JobExecContext, filterStr string, descr catalog.TableDescriptor,
) ([]roachpb.Span, error) {
	if filterStr == "" {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"option %s must not be empty", changefeedbase.OptPrimaryKeyFilter,
		)
	}

	filterExpr, err := parser.ParseExpr(filterStr)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"filter expression %s must be a valid SQL expression", changefeedbase.OptPrimaryKeyFilter)
	}

	semaCtx := tree.MakeSemaContext()
	spans, _, err := execCtx.ConstrainPrimaryIndexSpanByExpr(
		ctx, sql.MustFullyConstrain, nil, descr, &execCtx.ExtendedEvalContext().Context, &semaCtx, filterExpr)
	return spans, err
}
