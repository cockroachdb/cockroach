// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcprogresspb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/tableset"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	pbtypes "github.com/gogo/protobuf/types"
)

// featureChangefeedEnabled is used to enable and disable the CHANGEFEED feature.
var featureChangefeedEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.changefeed.enabled",
	"set to true to enable changefeeds, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
	settings.WithPublic)

func init() {
	sql.AddPlanHook("changefeed", changefeedPlanHook, changefeedTypeCheck)
	jobs.RegisterConstructor(
		jobspb.TypeChangefeed,
		func(job *jobs.Job, s *cluster.Settings) jobs.Resumer {
			r := &changefeedResumer{job: job, sv: &s.SV}
			r.mu.perNodeAggregatorStats = make(bulk.ComponentAggregatorStats)
			return r
		},
		jobs.UsesTenantCostControl,
	)
}

type annotatedChangefeedStatement struct {
	*tree.CreateChangefeed
	originalSpecs       map[tree.ChangefeedTableTarget]jobspb.ChangefeedTargetSpecification
	alterChangefeedAsOf hlc.Timestamp
	CreatedByInfo       *jobs.CreatedByInfo
}

func getChangefeedStatement(stmt tree.Statement) *annotatedChangefeedStatement {
	switch changefeed := stmt.(type) {
	case *annotatedChangefeedStatement:
		return changefeed
	case *tree.CreateChangefeed:
		return &annotatedChangefeedStatement{CreateChangefeed: changefeed}
	default:
		return nil
	}
}

var (
	sinklessHeader = colinfo.ResultColumns{
		{Name: "table", Typ: types.String},
		{Name: "key", Typ: types.Bytes},
		{Name: "value", Typ: types.Bytes},
	}
	withSinkHeader = colinfo.ResultColumns{
		{Name: "job_id", Typ: types.Int},
	}
)

func changefeedTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	changefeedStmt := getChangefeedStatement(stmt)
	if changefeedStmt == nil {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(ctx, `CREATE CHANGEFEED`, p.SemaCtx(),
		exprutil.Strings{changefeedStmt.SinkURI},
		&exprutil.KVOptions{
			KVOptions:  changefeedStmt.Options,
			Validation: changefeedvalidators.CreateOptionValidations,
		},
	); err != nil {
		return false, nil, err
	}
	unspecifiedSink := changefeedStmt.SinkURI == nil
	if unspecifiedSink {
		return true, sinklessHeader, nil
	}
	return true, withSinkHeader, nil
}

func maybeShowCursorAgeWarning(
	ctx context.Context, p sql.PlanHookState, opts changefeedbase.StatementOptions,
) error {
	st, err := opts.GetInitialScanType()
	if err != nil {
		return err
	}

	if !opts.HasStartCursor() || st == changefeedbase.OnlyInitialScan {
		return nil
	}
	statementTS := p.ExtendedEvalContext().GetStmtTimestamp().UnixNano()
	cursorTS, err := evalCursor(ctx, p, hlc.Timestamp{WallTime: statementTS}, opts.GetCursor())
	if err != nil {
		return err
	}

	warningAge := int64(5 * time.Hour)
	cursorAge := func() int64 {
		knobs, _ := p.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)
		if knobs != nil && knobs.OverrideCursorAge != nil {
			return knobs.OverrideCursorAge()
		}
		return statementTS - cursorTS.WallTime
	}()

	if cursorAge > warningAge {
		warningMsg := fmt.Sprintf("the provided cursor is %d hours old; "+
			"older cursors can result in increased changefeed latency", cursorAge/int64(time.Hour))
		log.Changefeed.Warningf(ctx, "%s", warningMsg)
		if err := p.SendClientNotice(ctx, pgnotice.Newf("%s", warningMsg), true); err != nil {
			return err
		}
	}

	return nil
}

// changefeedPlanHook implements sql.planHookFn.
func changefeedPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	changefeedStmt := getChangefeedStatement(stmt)
	if changefeedStmt == nil {
		return nil, nil, false, nil
	}

	exprEval := p.ExprEvaluator("CREATE CHANGEFEED")
	var sinkURI string
	unspecifiedSink := changefeedStmt.SinkURI == nil
	var avoidBuffering bool
	var header colinfo.ResultColumns
	if unspecifiedSink {
		// An unspecified sink triggers a fairly radical change in behavior.
		// Instead of setting up a system.job to emit to a sink in the
		// background and returning immediately with the job ID, the `CREATE
		// CHANGEFEED` blocks forever and returns all changes as rows directly
		// over pgwire. The types of these rows are `(topic STRING, key BYTES,
		// value BYTES)` and they correspond exactly to what would be emitted to
		// a sink.
		avoidBuffering = true
		header = sinklessHeader
	} else {
		var err error
		sinkURI, err = exprEval.String(ctx, changefeedStmt.SinkURI)
		if err != nil {
			return nil, nil, false, changefeedbase.MarkTaggedError(err, changefeedbase.UserInput)
		}
		if sinkURI == `` {
			// Error if someone specifies an INTO with the empty string.
			return nil, nil, false, errors.New(`omit the SINK clause for inline results`)
		}
		header = withSinkHeader
	}

	rawOpts, err := exprEval.KVOptions(
		ctx, changefeedStmt.Options, changefeedvalidators.CreateOptionValidations,
	)
	if err != nil {
		return nil, nil, false, err
	}

	if changefeedStmt.Level == tree.ChangefeedLevelDatabase {
		// Treat all tables inside a database as if "split_column_families" is set.
		rawOpts[changefeedbase.OptSplitColumnFamilies] = `yes`

		// The default behavior for a database-level changefeed is
		// NOT to perform an initial scan, unlike table-level changefeeds.
		_, initialScanSet := rawOpts[changefeedbase.OptInitialScan]
		_, initialScanOnlySet := rawOpts[changefeedbase.OptInitialScanOnly]
		_, noInitialScanSet := rawOpts[changefeedbase.OptNoInitialScan]
		if !initialScanOnlySet && !noInitialScanSet && !initialScanSet {
			rawOpts[changefeedbase.OptInitialScan] = `no`
		}
	}
	opts := changefeedbase.MakeStatementOptions(rawOpts)

	description, err := makeChangefeedDescription(ctx, changefeedStmt.CreateChangefeed, sinkURI, opts)
	if err != nil {
		return nil, nil, false, err
	}

	// rowFn implements sql.PlanHookRowFn.
	rowFn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()
		st, err := opts.GetInitialScanType()
		if err != nil {
			return err
		}
		if err := validateSettings(ctx, st != changefeedbase.OnlyInitialScan, p.ExecCfg()); err != nil {
			return err
		}

		jr, targets, err := createChangefeedJobRecord(
			ctx,
			p,
			changefeedStmt,
			description,
			sinkURI,
			opts,
			jobspb.InvalidJobID,
			`changefeed.create`,
		)
		if err != nil {
			return changefeedbase.MarkTaggedError(err, changefeedbase.UserInput)
		}

		details := jr.Details.(jobspb.ChangefeedDetails)
		progress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{},
			},
		}

		if details.SinkURI == `` {
			// If this is a sinkless changefeed, then we should not hold on to the
			// descriptor leases accessed to plan the changefeed. If changes happen
			// to descriptors, they will be addressed during the execution.
			// Failing to release the leases would result in preventing any schema
			// changes on the relevant descriptors (including, potentially,
			// system.role_membership, if the privileges to access the table were
			// granted via an inherited role).
			p.ExtendedEvalContext().Descs.ReleaseAll(ctx)

			telemetry.Count(`changefeed.create.core`)
			shouldMigrate := log.ShouldMigrateEvent(p.ExecCfg().SV())
			logCreateChangefeedTelemetry(ctx, jr, changefeedStmt.Select != nil, targets.Size, shouldMigrate)
			if err := maybeShowCursorAgeWarning(ctx, p, opts); err != nil {
				return err
			}

			err := coreChangefeed(ctx, p, details, description, progress, resultsCh, targets)
			// TODO(yevgeniy): This seems wrong -- core changefeeds always terminate
			// with an error.  Perhaps rename this telemetry to indicate number of
			// completed feeds.
			telemetry.Count(`changefeed.core.error`)
			return err
		}

		// The below block creates the job and protects the data required for the
		// changefeed to function from being garbage collected even if the
		// changefeed lags behind the gcttl. We protect the data here rather than in
		// Resume to shorten the window that data may be GC'd. The protected
		// timestamps are updated to the highwater mark periodically during the
		// execution of the changefeed by the changeFrontier. Protected timestamps
		// are removed in OnFailOrCancel. See
		// changeFrontier.manageProtectedTimestamps for more details on the handling
		// of protected timestamps.
		var sj *jobs.StartableJob
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		jr.JobID = jobID
		{
			metrics := p.ExecCfg().JobRegistry.MetricsStruct().Changefeed.(*Metrics)
			scope, _ := opts.GetMetricScope()
			sliMetrics, err := metrics.getSLIMetrics(scope)
			if err != nil {
				return err
			}

			recordPTSMetricsTime := sliMetrics.Timers.PTSCreate.Start()
			// ptr is the feed-level protected timestamp record which exists when per-table protected
			// timestamps are disabled.
			var ptr *ptpb.Record
			// perTablePTSRecords is the per-table protected timestamp records which exist when
			// per-table protected timestamps are enabled.
			var perTablePTSRecords []*ptpb.Record
			// systemTablesPTSRecord is the system tables protected timestamp record which exists when
			// per-table protected timestamps are enabled.
			var systemTablesPTSRecord *ptpb.Record
			// ptsRecords is the protected timestamp records object containing all per-table protected
			// timestamp records. Its format matches what will be persisted to the job info table.
			var ptsRecords *cdcprogresspb.ProtectedTimestampRecords

			// We do not yet have the progress config here, so we need to check the settings directly.
			perTableTrackingEnabled := changefeedbase.TrackPerTableProgress.Get(&p.ExecCfg().Settings.SV)
			// TODO(#158779): Re-add per table protected timestamps setting and
			// fetch this value from that cluster setting.
			perTableProtectedTimestampsEnabled := false
			usingPerTablePTS := perTableTrackingEnabled && perTableProtectedTimestampsEnabled
			if usingPerTablePTS {
				protectedTimestampRecords := make(map[descpb.ID]uuid.UUID)
				if err := targets.EachTarget(func(target changefeedbase.Target) error {
					// TODO(#155957): We are likely leaking PTS records here in
					// the column families case.
					ptsTargets := changefeedbase.Targets{}
					ptsTargets.Add(target)
					ptsRecord := createUserTablesProtectedTimestampRecord(
						ctx,
						jobID,
						ptsTargets,
						details.StatementTime,
					)
					perTablePTSRecords = append(perTablePTSRecords, ptsRecord)
					uuid := ptsRecord.ID.GetUUID()
					protectedTimestampRecords[target.DescID] = uuid
					return nil
				}); err != nil {
					return err
				}
				systemTablesPTSRecord = createSystemTablesProtectedTimestampRecord(
					ctx,
					jobID,
					details.StatementTime,
				)
				ptsRecords = &cdcprogresspb.ProtectedTimestampRecords{
					UserTables:   protectedTimestampRecords,
					SystemTables: systemTablesPTSRecord.ID.GetUUID(),
				}
			} else {
				ptr = createCombinedProtectedTimestampRecord(
					ctx,
					jobID,
					targets,
					details.StatementTime,
				)
				progress.GetChangefeed().ProtectedTimestampRecord = ptr.ID.GetUUID()
			}
			jr.Progress = *progress.GetChangefeed()

			if changefeedStmt.CreatedByInfo != nil {
				// We protect the PTS records that we created earlier. There should either be a
				// feed-level PTS record or per-table PTS record, but not both.
				if ptr != nil {
					pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(p.InternalSQLTxn())
					if err := pts.Protect(ctx, ptr); err != nil {
						return err
					}
				}
				if usingPerTablePTS {
					pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(p.InternalSQLTxn())
					for _, perTableRecord := range perTablePTSRecords {
						if err := pts.Protect(ctx, perTableRecord); err != nil {
							return err
						}
					}
					if err := pts.Protect(ctx, systemTablesPTSRecord); err != nil {
						return err
					}
					if err := writeChangefeedJobInfo(
						ctx, perTableProtectedTimestampsFilename, ptsRecords, p.InternalSQLTxn(), jobID,
					); err != nil {
						return err
					}
				}

				// This changefeed statement invoked by the scheduler.  As such, the scheduler
				// must have specified transaction to use, and is responsible for committing
				// transaction.

				_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, *jr, jr.JobID, p.InternalSQLTxn())
				if err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case resultsCh <- tree.Datums{
					tree.NewDInt(tree.DInt(jobID)),
				}:
					return nil
				}
			}

			if err := p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				// We protect the PTS records that we created earlier. There should either be a
				// feed-level PTS record or per-table PTS record, but not both.
				if ptr != nil {
					err := p.ExecCfg().ProtectedTimestampProvider.WithTxn(txn).Protect(ctx, ptr)
					if err != nil {
						return err
					}
				}
				if usingPerTablePTS {
					pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(txn)
					for _, perTableRecord := range perTablePTSRecords {
						if err := pts.Protect(ctx, perTableRecord); err != nil {
							return err
						}
					}
					if err := pts.Protect(ctx, systemTablesPTSRecord); err != nil {
						return err
					}
					if err := writeChangefeedJobInfo(
						ctx, perTableProtectedTimestampsFilename, ptsRecords, txn, jobID,
					); err != nil {
						return err
					}
				}
				if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jr.JobID, txn, *jr); err != nil {
					return err
				}
				return nil
			}); err != nil {
				if sj != nil {
					if err := sj.CleanupOnRollback(ctx); err != nil {
						log.Changefeed.Warningf(ctx, "failed to cleanup aborted job: %v", err)
					}
				}
				return err
			}
			recordPTSMetricsTime.End()
		}

		// Start the job.
		if err := sj.Start(ctx); err != nil {
			return err
		}

		if err := maybeShowCursorAgeWarning(ctx, p, opts); err != nil {
			return err
		}
		shouldMigrate := log.ShouldMigrateEvent(p.ExecCfg().SV())
		logCreateChangefeedTelemetry(ctx, jr, changefeedStmt.Select != nil, targets.Size, shouldMigrate)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(jobID)),
		}:
			return nil
		}
	}

	rowFnLogErrors := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		err := rowFn(ctx, resultsCh)
		if err != nil {

			shouldMigrate := log.ShouldMigrateEvent(p.ExecCfg().SV())
			logChangefeedFailedTelemetryDuringStartup(ctx, description, failureTypeForStartupError(err), shouldMigrate)
			var e *kvpb.BatchTimestampBeforeGCError
			if errors.As(err, &e) && opts.HasStartCursor() {
				err = errors.Wrapf(err,
					"could not create changefeed: cursor %s is older than the GC threshold %d",
					opts.GetCursor(), e.Threshold.WallTime)
				err = errors.WithHint(err,
					"use a more recent cursor")
			}
		}
		return err
	}
	return rowFnLogErrors, header, avoidBuffering, nil
}

func coreChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	details jobspb.ChangefeedDetails,
	description string,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
	targets changefeedbase.Targets,
) error {
	knobs, _ := p.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)
	var prevResult flowResult

	maxBackoff := changefeedbase.MaxRetryBackoff.Get(&p.ExecCfg().Settings.SV)
	backoffReset := changefeedbase.RetryBackoffReset.Get(&p.ExecCfg().Settings.SV)
	for r := getRetry(ctx, maxBackoff, backoffReset); ; {
		if !r.Next() {
			// Retry loop exits when context is canceled.
			log.Changefeed.Infof(ctx, "core changefeed retry loop exiting: %s", ctx.Err())
			return ctx.Err()
		}

		if knobs != nil && knobs.BeforeDistChangefeed != nil {
			knobs.BeforeDistChangefeed()
		}

		initialHighWater, schemaTS, err := computeDistChangefeedTimestamps(ctx, p, details, progress)
		if err != nil {
			return err
		}
		if knobs != nil && knobs.AfterComputeDistChangefeedTimestamps != nil {
			knobs.AfterComputeDistChangefeedTimestamps(ctx)
		}
		result, err := startDistChangefeed(ctx, p, 0 /* jobID */, schemaTS, details, description,
			initialHighWater, progress, prevResult, resultsCh, nil, targets)
		if err == nil {
			log.Changefeed.Infof(ctx, "core changefeed completed with no error")
			return nil
		}

		// Update progress from the flow result for the next retry.
		if result.coreProgress != nil {
			progress = result.coreProgress.progress
		}
		prevResult = result

		if knobs != nil && knobs.HandleDistChangefeedError != nil {
			err = knobs.HandleDistChangefeedError(err)
		}

		if err := changefeedbase.AsTerminalError(ctx, p.ExecCfg().LeaseManager, err); err != nil {
			log.Changefeed.Infof(ctx, "core changefeed failed due to error: %s", err)
			return err
		}

		log.Changefeed.Infof(ctx, "core changefeed retrying due to transient error: %s", err)
	}
}

func evalCursor(
	ctx context.Context, p sql.PlanHookState, statementTime hlc.Timestamp, timeString string,
) (hlc.Timestamp, error) {
	if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok {
		if knobs != nil && knobs.OverrideCursor != nil {
			timeString = knobs.OverrideCursor(&statementTime)
		}
	}
	asOfClause := tree.AsOfClause{Expr: tree.NewStrVal(timeString)}
	asOf, err := p.EvalAsOfTimestamp(ctx, asOfClause)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return asOf.Timestamp, nil
}

func getTargetList(changefeedStmt *annotatedChangefeedStatement) (*tree.BackupTargetList, error) {
	targetList := tree.BackupTargetList{}
	switch changefeedStmt.Level {
	case tree.ChangefeedLevelTable:
		for _, t := range changefeedStmt.TableTargets {
			targetList.Tables.TablePatterns = append(targetList.Tables.TablePatterns, t.TableName)
		}
	case tree.ChangefeedLevelDatabase:
		targetList.Databases = tree.NameList{tree.Name(changefeedStmt.DatabaseTarget)}
	default:
		return nil, errors.AssertionFailedf("unknown changefeed level: %s", changefeedStmt.Level.String())
	}
	return &targetList, nil
}

func createChangefeedJobRecord(
	ctx context.Context,
	p sql.PlanHookState,
	changefeedStmt *annotatedChangefeedStatement,
	description string,
	sinkURI string,
	opts changefeedbase.StatementOptions,
	jobID jobspb.JobID,
	telemetryPath string,
) (*jobs.Record, changefeedbase.Targets, error) {
	unspecifiedSink := changefeedStmt.SinkURI == nil

	for _, warning := range opts.DeprecationWarnings() {
		p.BufferClientNotice(ctx, pgnotice.Newf("%s", warning))
	}

	statementTime := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}
	var initialHighWater hlc.Timestamp

	if opts.HasStartCursor() {
		var err error
		initialHighWater, err = evalCursor(ctx, p, statementTime, opts.GetCursor())
		if err != nil {
			return nil, changefeedbase.Targets{}, err
		}
		statementTime = initialHighWater
	}

	checkPrivs := true
	if !changefeedStmt.alterChangefeedAsOf.IsEmpty() {
		statementTime = changefeedStmt.alterChangefeedAsOf
		// When altering a changefeed, we generate target descriptors below
		// based on a timestamp in the past. For example, this may be the
		// last highwater timestamp of a paused changefeed.
		// This is a problem because any privilege checks done on these
		// descriptors will be out of date.
		// To solve this problem, we validate the descriptors
		// in the alterChangefeedPlanHook at the statement time.
		// Thus, we can skip the check here.
		checkPrivs = false
	}

	endTime := hlc.Timestamp{}

	if opts.HasEndTime() {
		asOfClause := tree.AsOfClause{Expr: tree.NewStrVal(opts.GetEndTime())}
		asOf, err := asof.Eval(ctx, asOfClause, p.SemaCtx(), &p.ExtendedEvalContext().Context, asof.OptionAllowFutureTimestamp)
		if err != nil {
			return nil, changefeedbase.Targets{}, err
		}
		endTime = asOf.Timestamp
	}

	{
		initialScanType, err := opts.GetInitialScanType()
		if err != nil {
			return nil, changefeedbase.Targets{}, err
		}
		// TODO (zinger): Should we error or take the minimum
		// if endTime is already set?
		if initialScanType == changefeedbase.OnlyInitialScan {
			endTime = statementTime
		}
	}

	targetList, err := getTargetList(changefeedStmt)
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}

	var details jobspb.ChangefeedDetails

	tableNameToDescriptor, targetDatabaseDescs, tableAndParentDescs, err := getTargetDescriptors(
		ctx,
		p,
		targetList,
		statementTime,
		initialHighWater,
	)

	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}
	if len(tableAndParentDescs) == 0 {
		return nil, changefeedbase.Targets{}, errors.AssertionFailedf("expected at least one descriptor")
	}
	if len(targetDatabaseDescs) > 1 {
		return nil, changefeedbase.Targets{}, errors.AssertionFailedf("expected at most one database descriptor, got %d", len(targetDatabaseDescs))
	}

	for _, t := range tableNameToDescriptor {
		if tbl, ok := t.(catalog.TableDescriptor); ok && tbl.ExternalRowData() != nil {
			if tbl.ExternalRowData().TenantID.IsSet() {
				return nil, changefeedbase.Targets{}, errors.UnimplementedError(errors.IssueLink{}, "changefeeds on a replication target are not supported")
			}
			return nil, changefeedbase.Targets{}, errors.UnimplementedError(errors.IssueLink{}, "changefeeds on external tables are not supported")
		}
	}
	// This grabs table descriptors once to get their ids.
	tableTargets, tables, err := getTargetsAndTables(ctx, p, tableNameToDescriptor, changefeedStmt.TableTargets,
		changefeedStmt.originalSpecs, opts.ShouldUseFullStatementTimeName())

	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}

	sd := p.SessionData().Clone()
	// Add non-local session data state (localization, etc).
	sessiondata.MarshalNonLocal(p.SessionData(), &sd.SessionData)
	switch changefeedStmt.Level {
	case tree.ChangefeedLevelTable:
		for _, t := range tableNameToDescriptor {
			if tbl, ok := t.(catalog.TableDescriptor); ok && tbl.ExternalRowData() != nil {
				if tbl.ExternalRowData().TenantID.IsSet() {
					return nil, changefeedbase.Targets{}, errors.UnimplementedError(errors.IssueLink{}, "changefeeds on a replication target are not supported")
				}
				return nil, changefeedbase.Targets{}, errors.UnimplementedError(errors.IssueLink{}, "changefeeds on external tables are not supported")
			}
		}

		details = jobspb.ChangefeedDetails{
			Tables:               tables,
			SinkURI:              sinkURI,
			StatementTime:        statementTime,
			EndTime:              endTime,
			TargetSpecifications: tableTargets,
			SessionData:          &sd.SessionData,
		}
	case tree.ChangefeedLevelDatabase:
		if len(targetDatabaseDescs) == 0 || len(targetDatabaseDescs) > 1 {
			return nil, changefeedbase.Targets{}, errors.Errorf("changefeed only supports one database target")
		}
		targetDatabaseDesc := targetDatabaseDescs[0]
		if targetDatabaseDesc.GetID() == keys.SystemDatabaseID {
			return nil, changefeedbase.Targets{}, errors.Errorf("changefeed cannot target the system database")
		}
		fqTableNames, err := getFullyQualifiedTableNames(
			targetDatabaseDesc.GetName(), changefeedStmt.FilterOption.Tables,
		)
		if err != nil {
			return nil, changefeedbase.Targets{}, err
		}
		changefeedStmt.FilterOption.Tables = fqTableNames

		targetSpec := getDatabaseTargetSpec(targetDatabaseDesc, changefeedStmt.FilterOption)
		details = jobspb.ChangefeedDetails{
			TargetSpecifications: []jobspb.ChangefeedTargetSpecification{targetSpec},
			SinkURI:              sinkURI,
			StatementTime:        statementTime,
			EndTime:              endTime,
			SessionData:          &sd.SessionData,
		}
	default:
		return nil, changefeedbase.Targets{}, errors.AssertionFailedf("unknown changefeed level: %s", changefeedStmt.Level)
	}

	targets, err := AllTargets(ctx, details, p.ExecCfg(), statementTime)
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}

	hasSelectPrivOnAllTables := true
	hasChangefeedPrivOnAllTables := true
	tolerances := opts.GetCanHandle()
	// Core changefeed:
	//	- Table-level changefeeds require the user to have SELECT privileges
	// 	  on all target tables
	//	- DB-level feeds are not supported
	// Enterprise changefeed:
	//  - Table-level feeds require the CHANGEFEED privilege on all target tables
	//  - DB-level feeds require the CHANGEFEED privilege on the target database
	switch changefeedStmt.Level {
	case tree.ChangefeedLevelTable:
		_, tableToDatabaseLookup := buildTableToDatabaseAndSchemaLookup(tableAndParentDescs)
		for _, desc := range tableNameToDescriptor {
			if table, isTable := desc.(catalog.TableDescriptor); isTable {
				if err := changefeedvalidators.ValidateTable(targets, table, tolerances, false /* allowOfflineDescriptor */); err != nil {
					return nil, changefeedbase.Targets{}, err
				}
				for _, warning := range changefeedvalidators.WarningsForTable(table, tolerances) {
					p.BufferClientNotice(ctx, pgnotice.Newf("%s", warning))
				}

				hasSelect, hasChangefeed, err := checkPrivilegesForDescriptor(ctx, p, desc)
				if err != nil {
					return nil, changefeedbase.Targets{}, err
				}

				databaseDesc, ok := tableToDatabaseLookup[table.GetID()]
				if !ok {
					return nil, changefeedbase.Targets{}, errors.AssertionFailedf("expected to find a database descriptor for table %s", table.GetName())
				}
				if !hasChangefeed {
					_, hasChangefeed, err = checkPrivilegesForDescriptor(ctx, p, databaseDesc)
					if err != nil {
						return nil, changefeedbase.Targets{}, err
					}
				}

				hasSelectPrivOnAllTables = hasSelectPrivOnAllTables && hasSelect
				hasChangefeedPrivOnAllTables = hasChangefeedPrivOnAllTables && hasChangefeed
			}
		}
	case tree.ChangefeedLevelDatabase:
		_, hasDatabaseChangefeedPriv, err := checkPrivilegesForDescriptor(ctx, p, targetDatabaseDescs[0])
		if err != nil {
			return nil, changefeedbase.Targets{}, err
		}
		hasChangefeedPrivOnAllTables = hasDatabaseChangefeedPriv
	default:
		return nil, changefeedbase.Targets{}, errors.AssertionFailedf("unknown changefeed level: %s", changefeedStmt.Level)
	}

	if checkPrivs {
		if err := authorizeUserToCreateChangefeed(
			ctx,
			p,
			sinkURI,
			hasSelectPrivOnAllTables,
			hasChangefeedPrivOnAllTables,
			changefeedStmt.Level,
			opts.GetConfluentSchemaRegistry(),
		); err != nil {
			return nil, changefeedbase.Targets{}, err
		}
	}

	if changefeedStmt.Select != nil {
		// Serialize changefeed expression.
		normalized, withDiff, err := validateAndNormalizeChangefeedExpression(
			ctx, p, opts, changefeedStmt.Select, tableNameToDescriptor, tableTargets, statementTime,
		)
		if err != nil {
			return nil, changefeedbase.Targets{}, err
		}
		if withDiff {
			opts.ForceDiff()
		} else if opts.IsSet(changefeedbase.OptDiff) {
			// Expression didn't reference cdc_prev, but the diff option was specified.
			// This only makes sense if we have wrapped envelope.
			encopts, err := opts.GetEncodingOptions()
			if err != nil {
				return nil, changefeedbase.Targets{}, err
			}
			if encopts.Envelope != changefeedbase.OptEnvelopeWrapped {
				opts.ClearDiff()
				p.BufferClientNotice(ctx, pgnotice.Newf(
					"turning off unused %s option (expression <%s> does not use cdc_prev)",
					changefeedbase.OptDiff, tree.AsString(normalized)))
			}
		}

		// TODO: Set the default envelope to row here when using a sink and format
		// that support it.
		opts.SetDefaultEnvelope(changefeedbase.OptEnvelopeBare)
		details.Select = cdceval.AsStringUnredacted(normalized)
	}

	// TODO(dan): In an attempt to present the most helpful error message to the
	// user, the ordering requirements between all these usage validations have
	// become extremely fragile and non-obvious.
	//
	// - `validateDetails` has to run first to fill in defaults for `envelope`
	//   and `format` if the user didn't specify them.
	// - Then `getEncoder` is run to return any configuration errors.
	// - Then the changefeed is opted in to `OptKeyInValue` for any cloud
	//   storage sink or webhook sink. Kafka etc have a key and value field in
	//   each message but cloud storage sinks and webhook sinks don't have
	//   anywhere to put the key. So if the key is not in the value, then for
	//   DELETEs there is no way to recover which key was deleted. We could make
	//   the user explicitly pass this option for every cloud storage sink/
	//   webhook sink and error if they don't, but that seems user-hostile for
	//   insufficient reason. We can't do this any earlier, because we might
	//   return errors about `key_in_value` being incompatible which is
	//   confusing when the user didn't type that option.
	//   This is the same for the topic and webhook sink, which uses
	//   `topic_in_value` to embed the topic in the value by default, since it
	//   has no other avenue to express the topic.
	// - Finally, we create a "canary" sink to test sink configuration and
	//   connectivity. This has to go last because it is strange to return sink
	//   connectivity errors before we've finished validating all the other
	//   options. We should probably split sink configuration checking and sink
	//   connectivity checking into separate methods.
	//
	// The only upside in all this nonsense is the tests are decent. I've tuned
	// this particular order simply by rearranging stuff until the changefeedccl
	// tests all pass.
	parsedSink, err := url.Parse(sinkURI)
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}
	if newScheme, ok := changefeedbase.NoLongerExperimental[parsedSink.Scheme]; ok {
		parsedSink.Scheme = newScheme // This gets munged anyway when building the sink
		p.BufferClientNotice(ctx, pgnotice.Newf(`%[1]s is no longer experimental, use %[1]s://`,
			newScheme),
		)
	}

	if err = validateDetailsAndOptions(details, opts, p.ExecCfg().Settings); err != nil {
		return nil, changefeedbase.Targets{}, err
	}

	// Validate the encoder. We can pass an empty slimetrics struct and source provider
	// here since the encoder will not be used.
	encodingOpts, err := opts.GetEncodingOptions()
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}
	sourceProvider, err := newEnrichedSourceProvider(encodingOpts, enrichedSourceData{})
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}
	if _, err := getEncoder(ctx, encodingOpts, targets, details.Select != "",
		makeExternalConnectionProvider(ctx, p.ExecCfg().InternalDB), nil, sourceProvider); err != nil {
		return nil, changefeedbase.Targets{}, err
	}

	if !unspecifiedSink && p.ExecCfg().ExternalIODirConfig.DisableOutbound {
		return nil, changefeedbase.Targets{}, errors.Errorf("Outbound IO is disabled by configuration, cannot create changefeed into %s", parsedSink.Scheme)
	}

	if telemetryPath != `` {
		// Feature telemetry
		telemetrySink := parsedSink.Scheme
		if telemetrySink == `` {
			telemetrySink = `sinkless`
		}
		telemetry.Count(telemetryPath + `.sink.` + telemetrySink)
		telemetry.Count(telemetryPath + `.format.` + string(encodingOpts.Format))
		telemetry.CountBucketed(telemetryPath+`.num_tables`, int64(len(tables)))
	}

	if scope, ok := opts.GetMetricScope(); ok {
		if scope == defaultSLIScope {
			return nil, changefeedbase.Targets{}, pgerror.Newf(pgcode.InvalidParameterValue,
				"%[1]q=%[2]q is the default metrics scope which keeps track of statistics "+
					"across all changefeeds without explicit label.  "+
					"If this is an intended behavior, please re-run the statement "+
					"without specifying %[1]q parameter.  "+
					"Otherwise, please re-run with a different %[1]q value.",
				changefeedbase.OptMetricsScope, defaultSLIScope)
		}

		if !status.ChildMetricsEnabled.Get(&p.ExecCfg().Settings.SV) {
			p.BufferClientNotice(ctx, pgnotice.Newf(
				"%s is set to false, metrics will only be published to the '%s' label when it is set to true",
				status.ChildMetricsEnabled.Name(),
				scope,
			))
		}
	}

	if details.SinkURI == `` {
		details.Opts = opts.AsMap()
		// Jobs should not be created for sinkless changefeeds. However, note that
		// we create and return a job record for sinkless changefeeds below. This is
		// because we need the job description and details to create our sinkless changefeed.
		// After this job record is returned, we create our forever running sinkless
		// changefeed, thus ensuring that no job is created for this changefeed as
		// desired.
		sinklessRecord := &jobs.Record{
			Description: description,
			Details:     details,
		}
		return sinklessRecord, targets, nil
	}

	if telemetryPath != `` {
		telemetry.Count(telemetryPath + `.enterprise`)
	}

	// TODO (zinger): validateSink shouldn't need details, remove that so we only
	// need to have this line once.
	details.Opts = opts.AsMap()

	// In the case where a user is executing a CREATE CHANGEFEED and is still
	// waiting for the statement to return, we take the opportunity to ensure
	// that the user has not made any obvious errors when specifying the sink in
	// the CREATE CHANGEFEED statement. To do this, we create a "canary" sink,
	// which will be immediately closed, only to check for errors.
	// We also check here for any options that have passed previous validations
	// but are inappropriate for the provided sink.
	// TODO: Ideally those option validations would happen in validateDetails()
	// earlier, like the others.
	err = validateSink(ctx, p, jobID, details, opts, targets)
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}
	details.Opts = opts.AsMap()

	if locFilter := details.Opts[changefeedbase.OptExecutionLocality]; locFilter != "" {
		var executionLocality roachpb.Locality
		if err := executionLocality.Set(locFilter); err != nil {
			return nil, changefeedbase.Targets{}, err
		}
		if _, err := p.DistSQLPlanner().GetAllInstancesByLocality(ctx, executionLocality); err != nil {
			return nil, changefeedbase.Targets{}, err
		}
	}
	resolvedOpt, emit, err := opts.GetResolvedTimestampInterval()
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}
	var resolved time.Duration
	resolvedStr := " by default"
	if resolvedOpt != nil {
		resolved = *resolvedOpt
		resolvedStr = ""
	}
	freqOpt, err := opts.GetMinCheckpointFrequency()
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}
	freq := changefeedbase.DefaultMinCheckpointFrequency
	freqStr := "default"
	if freqOpt != nil {
		freq = *freqOpt
		freqStr = "configured"
	}
	if emit && (resolved < freq) {
		p.BufferClientNotice(ctx, pgnotice.Newf("resolved (%s%s) messages will not be emitted "+
			"more frequently than the %s min_checkpoint_frequency (%s), but may be emitted "+
			"less frequently", resolved, resolvedStr, freqStr, freq))
	}

	const minRecommendedFrequency = 500 * time.Millisecond

	if emit && resolvedOpt != nil && *resolvedOpt < minRecommendedFrequency {
		p.BufferClientNotice(ctx, pgnotice.Newf(
			"the 'resolved' timestamp interval (%s) is very low; consider increasing it to at least %s",
			resolvedOpt, minRecommendedFrequency))
	}

	if freqOpt != nil && *freqOpt < minRecommendedFrequency {
		p.BufferClientNotice(ctx, pgnotice.Newf(
			"the 'min_checkpoint_frequency' timestamp interval (%s) is very low; consider increasing it to at least %s",
			freqOpt, minRecommendedFrequency))
	}

	ptsExpiration, err := opts.GetPTSExpiration()
	if err != nil {
		return nil, changefeedbase.Targets{}, err
	}

	useDefaultExpiration := ptsExpiration == 0
	if useDefaultExpiration {
		ptsExpiration = changefeedbase.MaxProtectedTimestampAge.Get(&p.ExecCfg().Settings.SV)
	}

	if ptsExpiration > 0 && ptsExpiration < time.Hour {
		// This threshold is rather arbitrary.  But we want to warn users about
		// the potential impact of keeping this setting too low.
		const explainer = `Having a low protected timestamp expiration value should not have adverse effect
as long as changefeed is running. However, should the changefeed be paused, it
will need to be resumed before expiration time. The value of this setting should
reflect how much time he changefeed may remain paused, before it is canceled.
Few hours to a few days range are appropriate values for this option.`
		if useDefaultExpiration {
			p.BufferClientNotice(ctx, pgnotice.Newf(
				`the value of %s for changefeed.protect_timestamp.max_age setting might be too low. %s`,
				ptsExpiration, changefeedbase.OptExpirePTSAfter, explainer))
		} else {
			p.BufferClientNotice(ctx, pgnotice.Newf(
				`the value of %s for changefeed option %s might be too low. %s`,
				ptsExpiration, changefeedbase.OptExpirePTSAfter, explainer))
		}
	}

	descriptorIDs := func() (sqlDescIDs []descpb.ID) {
		if changefeedStmt.Level == tree.ChangefeedLevelDatabase {
			sqlDescIDs = append(sqlDescIDs, targetDatabaseDescs[0].GetID())
		} else {
			for _, desc := range tableNameToDescriptor {
				sqlDescIDs = append(sqlDescIDs, desc.GetID())
			}
		}
		return sqlDescIDs
	}()

	jr := &jobs.Record{
		Description:   description,
		Username:      p.User(),
		DescriptorIDs: descriptorIDs,
		Details:       details,
		CreatedBy:     changefeedStmt.CreatedByInfo,
		MaximumPTSAge: ptsExpiration,
	}

	return jr, targets, nil
}

func validateSettings(ctx context.Context, needsRangeFeed bool, execCfg *sql.ExecutorConfig) error {
	if err := featureflag.CheckEnabled(
		ctx,
		execCfg,
		featureChangefeedEnabled,
		"CHANGEFEED",
	); err != nil {
		return err
	}

	// Changefeeds are based on the Rangefeed abstraction, which
	// requires the `kv.rangefeed.enabled` setting to be true.
	if needsRangeFeed && !kvserver.RangefeedEnabled.Get(&execCfg.Settings.SV) {
		return errors.Errorf("rangefeeds require the kv.rangefeed.enabled setting. See %s",
			docs.URL(`create-and-configure-changefeeds.html#enable-rangefeeds`))
	}

	return nil
}

func getTargetDescriptors(
	ctx context.Context,
	p sql.PlanHookState,
	targets *tree.BackupTargetList,
	statementTime hlc.Timestamp,
	initialHighWater hlc.Timestamp,
) (
	tableNameToDescriptor map[tree.TablePattern]catalog.Descriptor,
	databaseDescs []catalog.DatabaseDescriptor,
	tableAndParentDescs []catalog.Descriptor,
	err error,
) {
	if len(targets.Databases) > 0 && len(targets.Tables.TablePatterns) > 0 {
		return nil, nil, nil, errors.Errorf(`CHANGEFEED cannot target both databases and tables`)
	}

	for _, t := range targets.Tables.TablePatterns {
		p, err := t.NormalizeTablePattern()
		if err != nil {
			return nil, nil, nil, err
		}
		if _, ok := p.(*tree.TableName); !ok {
			return nil, nil, nil, errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(t))
		}
	}
	// targetTableDescs is empty if the targets are not tables, targetDatabaseDescs is empty if the targets are not databases
	targetAndParentDescs, _, targetDatabaseDescs, targetTableDescs, err := backupresolver.ResolveTargets(ctx, p, statementTime, targets)
	if err != nil {
		var m *backupresolver.MissingTableErr
		if errors.As(err, &m) {
			err = errors.Wrapf(m.Unwrap(), "table %q does not exist", m.GetTableName())
		}
		err = errors.Wrap(err, "failed to resolve targets in the CHANGEFEED stmt")
		if !initialHighWater.IsEmpty() {
			// We specified cursor -- it is possible the targets do not exist at that time.
			// Give a bit more context in the error message.
			err = errors.WithHintf(err,
				"do the targets exist at the specified cursor time %s?", initialHighWater)
		}
	}
	return targetTableDescs, targetDatabaseDescs, targetAndParentDescs, err
}

func getTargetsAndTables(
	ctx context.Context,
	p sql.PlanHookState,
	targetDescs map[tree.TablePattern]catalog.Descriptor,
	rawTargets tree.ChangefeedTableTargets,
	originalSpecs map[tree.ChangefeedTableTarget]jobspb.ChangefeedTargetSpecification,
	fullTableName bool,
) ([]jobspb.ChangefeedTargetSpecification, jobspb.ChangefeedTargets, error) {
	tables := make(jobspb.ChangefeedTargets, len(targetDescs))
	targets := make([]jobspb.ChangefeedTargetSpecification, len(rawTargets))
	seen := make(map[jobspb.ChangefeedTargetSpecification]tree.ChangefeedTableTarget)

	for i, ct := range rawTargets {
		desc, ok := targetDescs[ct.TableName]
		if !ok {
			return nil, nil, errors.Newf("could not match %v to a fetched descriptor. Fetched were %v", ct.TableName, targetDescs)
		}
		td, ok := desc.(catalog.TableDescriptor)
		if !ok {
			return nil, nil, errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(&ct))
		}

		if spec, ok := originalSpecs[ct]; ok {
			targets[i] = spec
			if table, ok := tables[td.GetID()]; ok {
				if table.StatementTimeName != spec.StatementTimeName {
					return nil, nil, errors.Errorf(
						`table with id %d is referenced with multiple statement time names: %q and %q`, td.GetID(),
						table.StatementTimeName, spec.StatementTimeName)
				}
			} else {
				tables[td.GetID()] = jobspb.ChangefeedTargetTable{
					StatementTimeName: spec.StatementTimeName,
				}
			}
		} else {

			name, err := getChangefeedTargetName(ctx, td, p.ExecCfg(), p.Txn(), fullTableName)

			if err != nil {
				return nil, nil, err
			}

			tables[td.GetID()] = jobspb.ChangefeedTargetTable{
				StatementTimeName: name,
			}
			typ := jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY
			if ct.FamilyName != "" {
				typ = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
			} else {
				if td.NumFamilies() > 1 {
					typ = jobspb.ChangefeedTargetSpecification_EACH_FAMILY
				}
			}
			targets[i] = jobspb.ChangefeedTargetSpecification{
				Type:              typ,
				DescID:            td.GetID(),
				FamilyName:        string(ct.FamilyName),
				StatementTimeName: tables[td.GetID()].StatementTimeName,
			}
		}
		if dup, isDup := seen[targets[i]]; isDup {
			return nil, nil, errors.Errorf(
				"CHANGEFEED targets %s and %s are duplicates",
				tree.AsString(&dup), tree.AsString(&ct),
			)
		}
		seen[targets[i]] = ct
	}

	return targets, tables, nil
}

func getDatabaseTargetSpec(
	targetDatabaseDesc catalog.DatabaseDescriptor, filterOpt tree.ChangefeedFilterOption,
) jobspb.ChangefeedTargetSpecification {
	target := jobspb.ChangefeedTargetSpecification{
		DescID:            targetDatabaseDesc.GetID(),
		Type:              jobspb.ChangefeedTargetSpecification_DATABASE,
		StatementTimeName: targetDatabaseDesc.GetName(),
	}
	filterTables := make(map[string]pbtypes.Empty)
	for _, table := range filterOpt.Tables {
		filterTables[table.FQString()] = pbtypes.Empty{}
	}
	target.FilterList = &jobspb.FilterList{
		FilterType: filterOpt.FilterType,
		Tables:     filterTables,
	}
	return target
}

func getFullyQualifiedTableNames(
	targetDatabase string, tableNames tree.TableNames,
) (tree.TableNames, error) {
	var fqTableNames tree.TableNames

	for _, tableName := range tableNames {
		if tableName.SchemaName == "" {
			// The table name is non-qualified e.g. foo. This will resolve to <targetDatabase>.public.foo.
			tableName.SchemaName = catconstants.PublicSchemaName
			tableName.CatalogName = tree.Name(targetDatabase)
		} else if tableName.CatalogName == "" {
			// The table name is partially qualified e.g. foo.bar. This will resolve to
			// <targetDatabase>.foo.bar.
			tableName.CatalogName = tree.Name(targetDatabase)
		} else {
			// Table name is fully qualfied e.g. foo.bar.fizz. This will resolve to
			// foo.bar.fizz unless foo != <targetDatabase>, in which case it would fail.
			if tableName.CatalogName != tree.Name(targetDatabase) {
				return nil, errors.AssertionFailedf(
					"table %q must be in target database %q", tableName.FQString(), targetDatabase,
				)
			}
		}
		fqTableNames = append(fqTableNames, tableName)
	}
	return fqTableNames, nil
}

func validateSink(
	ctx context.Context,
	p sql.PlanHookState,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	opts changefeedbase.StatementOptions,
	targets changefeedbase.Targets,
) error {
	metrics := p.ExecCfg().JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	scope, _ := opts.GetMetricScope()
	sli, err := metrics.getSLIMetrics(scope)
	if err != nil {
		return err
	}
	u, err := url.Parse(details.SinkURI)
	if err != nil {
		return err
	}

	ambiguousSchemes := map[string][2]string{
		changefeedbase.DeprecatedSinkSchemeHTTP:  {changefeedbase.SinkSchemeCloudStorageHTTP, changefeedbase.SinkSchemeWebhookHTTP},
		changefeedbase.DeprecatedSinkSchemeHTTPS: {changefeedbase.SinkSchemeCloudStorageHTTPS, changefeedbase.SinkSchemeWebhookHTTPS},
	}

	if disambiguations, isAmbiguous := ambiguousSchemes[u.Scheme]; isAmbiguous {
		p.BufferClientNotice(ctx, pgnotice.Newf(
			`Interpreting deprecated URI scheme %s as %s. For webhook semantics, use %s.`,
			u.Scheme,
			disambiguations[0],
			disambiguations[1],
		))
	}

	var nilOracle timestampLowerBoundOracle
	canarySink, err := getAndDialSink(ctx, &p.ExecCfg().DistSQLSrv.ServerConfig, details,
		nilOracle, p.User(), jobID, sli, targets, true /* initialValidation */)
	if err != nil {
		return err
	}
	sinkTy := canarySink.getConcreteType()
	if err := canarySink.Close(); err != nil {
		return err
	}

	// envelope=enriched is only allowed for non-query feeds and certain sinks.
	if details.Opts[changefeedbase.OptEnvelope] == string(changefeedbase.OptEnvelopeEnriched) {
		if details.Select != `` {
			return errors.Newf("envelope=%s is incompatible with SELECT statement", changefeedbase.OptEnvelopeEnriched)
		}
		allowedSinkTypes := map[sinkType]struct{}{
			sinkTypeNull:           {},
			sinkTypePubsub:         {},
			sinkTypeKafka:          {},
			sinkTypeWebhook:        {},
			sinkTypeSinklessBuffer: {},
			sinkTypeCloudstorage:   {},
		}
		if _, ok := allowedSinkTypes[sinkTy]; !ok {
			return errors.Newf("envelope=%s is incompatible with %s sink", changefeedbase.OptEnvelopeEnriched, sinkTy)
		}
	}

	// If there's no projection we may need to force some options to ensure messages
	// have enough information.
	if details.Select == `` {
		if requiresKeyInValue(canarySink) {
			if err = opts.ForceKeyInValue(); err != nil {
				return err
			}
		}
		if requiresTopicInValue(canarySink) {
			if err = opts.ForceTopicInValue(); err != nil {
				return err
			}
		}
	}
	if sink, ok := canarySink.(SinkWithTopics); ok {
		if opts.IsSet(changefeedbase.OptResolvedTimestamps) &&
			opts.IsSet(changefeedbase.OptSplitColumnFamilies) {
			return errors.Newf("Resolved timestamps are not currently supported with %s for this sink"+
				" as the set of topics to fan them out to may change. Instead, use TABLE tablename FAMILY familyname"+
				" to specify individual families to watch.", changefeedbase.OptSplitColumnFamilies)
		}

		topics := sink.Topics()
		for _, topic := range topics {
			p.BufferClientNotice(ctx, pgnotice.Newf(`changefeed will emit to topic %s`, topic))
		}
		opts.SetTopics(topics)
	}
	return nil
}

func requiresKeyInValue(s Sink) bool {
	switch s.getConcreteType() {
	case sinkTypeCloudstorage, sinkTypeWebhook:
		return true
	default:
		return false
	}
}

func requiresTopicInValue(s Sink) bool {
	return s.getConcreteType() == sinkTypeWebhook
}

// buildDatabaseWatcherFilterFromSpec constructs a tableset watcher filter from a
// database-level changefeed target specification.
func buildDatabaseWatcherFilterFromSpec(
	spec jobspb.ChangefeedTargetSpecification,
) (tableset.Filter, error) {
	filter := tableset.Filter{
		DatabaseID: spec.DescID,
	}
	if spec.FilterList != nil && len(spec.FilterList.Tables) > 0 {
		switch spec.FilterList.FilterType {
		case tree.IncludeFilter:
			filter.IncludeTables = make(map[string]struct{})
			for fqTableName := range spec.FilterList.Tables {
				// TODO(#156859): use fully qualified names once watcher supports it.
				// Extract just the table name from the fully qualified name (e.g., "db.public.table" -> "table")
				tn, err := parser.ParseQualifiedTableName(fqTableName)
				if err != nil {
					return tableset.Filter{}, errors.Wrapf(err, "failed to parse name in filter list: %s", fqTableName)
				}
				filter.IncludeTables[tn.Object()] = struct{}{}
			}
		case tree.ExcludeFilter:
			filter.ExcludeTables = make(map[string]struct{})
			for fqTableName := range spec.FilterList.Tables {
				// TODO(#156859): use fully qualified names once watcher supports it.
				// Extract just the table name from the fully qualified name (e.g., "db.public.table" -> "table")
				tn, err := parser.ParseQualifiedTableName(fqTableName)
				if err != nil {
					return tableset.Filter{}, errors.Wrapf(err, "failed to parse name in filter list: %s", fqTableName)
				}
				filter.ExcludeTables[tn.Object()] = struct{}{}
			}
		}
	}
	return filter, nil
}

var errDoneWatching = errors.New("done watching")

func makeChangefeedDescription(
	ctx context.Context,
	changefeed *tree.CreateChangefeed,
	sinkURI string,
	opts changefeedbase.StatementOptions,
) (string, error) {
	c := &tree.CreateChangefeed{
		Select: changefeed.Select,
		Level:  changefeed.Level,
	}
	if changefeed.Level == tree.ChangefeedLevelDatabase {
		c.DatabaseTarget = changefeed.DatabaseTarget
	} else {
		c.TableTargets = changefeed.TableTargets
	}

	if sinkURI != "" {
		// Redacts user sensitive information from description.
		cleanedSinkURI, err := cloud.SanitizeExternalStorageURI(sinkURI, nil)
		if err != nil {
			return "", err
		}

		cleanedSinkURI, err = changefeedbase.RedactUserFromURI(cleanedSinkURI)
		if err != nil {
			return "", err
		}

		logSanitizedChangefeedDestination(ctx, cleanedSinkURI)

		c.SinkURI = tree.NewDString(cleanedSinkURI)
	}

	if err := opts.ForEachWithRedaction(func(k string, v string) {
		opt := tree.KVOption{Key: tree.Name(k)}
		if len(v) > 0 {
			opt.Value = tree.NewDString(v)
		}
		c.Options = append(c.Options, opt)
	}); err != nil {
		return "", err
	}

	sort.Slice(c.Options, func(i, j int) bool { return c.Options[i].Key < c.Options[j].Key })

	return tree.AsStringWithFlags(c, tree.FmtShowFullURIs), nil
}

func logSanitizedChangefeedDestination(ctx context.Context, destination string) {
	log.Ops.Infof(ctx, "changefeed planning to connect to destination %v", redact.Safe(destination))
}

func validateDetailsAndOptions(
	details jobspb.ChangefeedDetails,
	opts changefeedbase.StatementOptions,
	settings *cluster.Settings,
) error {
	if err := opts.ValidateForCreateChangefeed(details.Select != ""); err != nil {
		return err
	}
	if isDBLevelChangefeed(details) {
		scanType, err := opts.GetInitialScanType()
		if err != nil {
			return err
		}
		if scanType == changefeedbase.OnlyInitialScan {
			return errors.Errorf(
				`cannot specify %s on a database level changefeed`, changefeedbase.OptInitialScanOnly)
		}
	} else {
		if opts.IsSet(changefeedbase.OptHibernationPollingFrequency) {
			return errors.Errorf(
				"%s is only supported for database-level changefeeds",
				changefeedbase.OptHibernationPollingFrequency)
		}
	}
	if opts.HasEndTime() {
		scanType, err := opts.GetInitialScanType()
		if err != nil {
			return err
		}
		if scanType == changefeedbase.OnlyInitialScan {
			return errors.Errorf(
				`cannot specify both %s and %s`, changefeedbase.OptInitialScanOnly,
				changefeedbase.OptEndTime)
		}

		if details.EndTime.Less(details.StatementTime) {
			return errors.Errorf(
				`specified end time %s cannot be less than statement time %s`,
				details.EndTime.AsOfSystemTime(),
				details.StatementTime.AsOfSystemTime(),
			)
		}
	}

	{
		if details.Select != "" {
			if len(details.TargetSpecifications) != 1 {
				return errors.Errorf(
					"CREATE CHANGEFEED ... AS SELECT ... is not supported for more than 1 table")
			}
		}
	}

	if opts.IsSet(changefeedbase.OptPartitionAlg) {
		if !changefeedbase.PartitionAlgEnabled.Get(&settings.SV) {
			return errors.Newf(
				"option %q requires cluster setting %q to be enabled",
				changefeedbase.OptPartitionAlg,
				changefeedbase.PartitionAlgEnabled.Name(),
			)
		}
	}

	return nil
}

// validateAndNormalizeChangefeedExpression validates and normalizes changefeed expressions.
// This method modifies passed in select clause to reflect normalization step.
// TODO(yevgeniy): Add virtual column support.
func validateAndNormalizeChangefeedExpression(
	ctx context.Context,
	execCtx sql.PlanHookState,
	opts changefeedbase.StatementOptions,
	sc *tree.SelectClause,
	descriptors map[tree.TablePattern]catalog.Descriptor,
	targets []jobspb.ChangefeedTargetSpecification,
	statementTime hlc.Timestamp,
) (*cdceval.NormalizedSelectClause, bool, error) {
	if len(descriptors) != 1 || len(targets) != 1 {
		return nil, false, pgerror.Newf(pgcode.InvalidParameterValue, "CDC expressions require single table")
	}
	var tableDescr catalog.TableDescriptor
	for _, d := range descriptors {
		tableDescr = d.(catalog.TableDescriptor)
	}
	splitColFams := opts.IsSet(changefeedbase.OptSplitColumnFamilies)
	norm, withDiff, err := cdceval.NormalizeExpression(ctx, execCtx,
		tableDescr, statementTime, targets[0], sc, splitColFams)
	if err != nil {
		return nil, false, err
	}
	return norm, withDiff, nil
}

type changefeedResumer struct {
	job *jobs.Job
	sv  *settings.Values

	mu struct {
		syncutil.Mutex
		// perNodeAggregatorStats is a per component running aggregate of trace
		// driven AggregatorStats emitted by the processors.
		perNodeAggregatorStats bulk.ComponentAggregatorStats
	}
}

// DumpTraceAfterRun implements jobs.TraceableJob.
func (b *changefeedResumer) DumpTraceAfterRun() bool {
	return true
}

// ForceRealSpan implements jobs.TraceableJob.
func (b *changefeedResumer) ForceRealSpan() bool {
	return true
}

var _ jobs.TraceableJob = &changefeedResumer{}

func (b *changefeedResumer) setJobStatusMessage(
	ctx context.Context, lastUpdate time.Time, fmtOrMsg string, args ...interface{},
) time.Time {
	if timeutil.Since(lastUpdate) < runStatusUpdateFrequency {
		return lastUpdate
	}

	status := jobs.StatusMessage(fmt.Sprintf(fmtOrMsg, args...))
	if err := b.job.NoTxn().UpdateStatusMessage(ctx, status); err != nil {
		log.Changefeed.Warningf(ctx, "failed to set status: %v", err)
	}

	return timeutil.Now()
}

// Resume is part of the jobs.Resumer interface.
func (b *changefeedResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()
	jobID := b.job.ID()
	details := b.job.Details().(jobspb.ChangefeedDetails)
	progress := b.job.Progress()
	description := b.job.Payload().Description

	if err := b.ensureClusterIDMatches(ctx, jobExec.ExtendedEvalContext().ClusterID); err != nil {
		return err
	}

	err := b.resumeWithRetries(ctx, jobExec, jobID, details, description, progress, execCfg)
	if err != nil {
		return b.handleChangefeedError(ctx, err, details, jobExec)
	}
	return nil
}

// ensureClusterIDMatches verifies that this job record matches
// the cluster ID of this cluster.
// This check ensures that if the job has been restored from the
// full backup, or from streaming replication, then we will fail
// this changefeed since resuming changefeed, from potentially
// long "sleep", and attempting to write to existing bucket/topic,
// is more undesirable and dangerous than just failing this job.
func (b *changefeedResumer) ensureClusterIDMatches(ctx context.Context, clusterID uuid.UUID) error {
	if createdBy := b.job.Payload().CreationClusterID; createdBy == uuid.Nil {
		// This cluster was upgraded from a version that did not set clusterID
		// in the job record -- rectify this issue.
		if err := b.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			md.Payload.CreationClusterID = clusterID
			ju.UpdatePayload(md.Payload)
			return nil
		}); err != nil {
			return jobs.MarkAsRetryJobError(err)
		}
	} else if clusterID != createdBy {
		return errors.Newf("this changefeed was originally created by cluster %s; "+
			"it must be recreated on this cluster if this cluster is now expected "+
			"to emit to the same destination", createdBy)
	}
	return nil
}

// TODO(dt): remove this copy pasta from backupResumer in favor of a shared func
// somewhere in pkg/jobs or the job registry.
func (b *changefeedResumer) maybeRelocateJobExecution(
	ctx context.Context, p sql.JobExecContext, locality roachpb.Locality,
) error {
	if locality.NonEmpty() {
		current, err := p.DistSQLPlanner().GetSQLInstanceInfo(ctx, p.ExecCfg().JobRegistry.ID())
		if err != nil {
			return err
		}
		if ok, missedTier := current.Locality.Matches(locality); !ok {
			log.Changefeed.Infof(ctx,
				"CHANGEFEED job %d initially adopted on instance %d but it does not match locality filter %s, finding a new coordinator",
				b.job.ID(), current.GetInstanceID(), missedTier.String(),
			)

			instancesInRegion, err := p.DistSQLPlanner().GetAllInstancesByLocality(ctx, locality)
			if err != nil {
				return err
			}
			rng, _ := randutil.NewPseudoRand()
			dest := instancesInRegion[rng.Intn(len(instancesInRegion))]

			var res error
			if err := p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				var err error
				res, err = p.ExecCfg().JobRegistry.RelocateLease(ctx, txn, b.job.ID(), dest.InstanceID, dest.SessionID)
				return err
			}); err != nil {
				return errors.Wrapf(err, "failed to relocate job coordinator to %d", dest.InstanceID)
			}
			return res
		}
	}
	return nil
}

func (b *changefeedResumer) handleChangefeedError(
	ctx context.Context,
	changefeedErr error,
	details jobspb.ChangefeedDetails,
	jobExec sql.JobExecContext,
) error {
	// Execution relocation errors just get returned immediately, as they indicate
	// another node has taken over execution and this execution should end now.
	if jobs.IsLeaseRelocationError(changefeedErr) {
		log.Changefeed.Warningf(ctx, "job lease relocated (%v)", changefeedErr)
		return changefeedErr
	}
	opts := changefeedbase.MakeStatementOptions(details.Opts)
	onError, errErr := opts.GetOnError()
	if errErr != nil {
		log.Changefeed.Warningf(ctx, "job failed (%v) but was unable to get on error option (%v)", changefeedErr, errErr)
		return errors.CombineErrors(changefeedErr, errErr)
	}
	switch onError {
	// default behavior
	case changefeedbase.OptOnErrorFail:
		log.Changefeed.Warningf(ctx, "job failed (%v)", changefeedErr)
		return changefeedErr
	// pause instead of failing
	case changefeedbase.OptOnErrorPause:
		// note: we only want the job to pause here if a failure happens, not a
		// user-initiated cancellation. if the job has been canceled, the ctx
		// will handle it and the pause will return an error.
		const errorFmt = "job failed (%v) but is being paused because of %s=%s"
		errorMessage := fmt.Sprintf(errorFmt, changefeedErr,
			changefeedbase.OptOnError, changefeedbase.OptOnErrorPause)
		return b.job.NoTxn().PauseRequestedWithFunc(ctx, func(ctx context.Context, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			// directly update running status to avoid the running/reverted job status check
			md.Progress.StatusMessage = errorMessage
			ju.UpdateProgress(md.Progress)
			log.Changefeed.Warningf(ctx, errorFmt, changefeedErr, changefeedbase.OptOnError, changefeedbase.OptOnErrorPause)
			return nil
		}, errorMessage)
	default:
		log.Changefeed.Warningf(ctx, "job failed (%v) but has unrecognized option value %s=%s", changefeedErr, changefeedbase.OptOnError, details.Opts[changefeedbase.OptOnError])
		return errors.Wrapf(changefeedErr, "unrecognized option value: %s=%s for handling error",
			changefeedbase.OptOnError, details.Opts[changefeedbase.OptOnError])
	}
}

var replanErr = errors.New("replan due to detail change")

func (b *changefeedResumer) resumeWithRetries(
	ctx context.Context,
	jobExec sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	description string,
	initialProgress jobspb.Progress,
	execCfg *sql.ExecutorConfig,
) error {
	// If execution needs to be and is relocated, the resulting error should be
	// returned without retry, as it indicates _this_ execution should cease now
	// that execution is elsewhere, so check this before the retry loop.
	if filter := details.Opts[changefeedbase.OptExecutionLocality]; filter != "" {
		var loc roachpb.Locality
		if err := loc.Set(filter); err != nil {
			return err
		}
		if err := b.maybeRelocateJobExecution(ctx, jobExec, loc); err != nil {
			return err
		}
	}

	// Grab a "reference" to this node's job registry in order to make sure
	// this resumer has enough time to persist up to date checkpoint in case
	// of node drain.
	drainCh, cleanup := execCfg.JobRegistry.OnDrain()
	defer cleanup()

	progress := initialProgress
	var prevResult flowResult
	knobs, _ := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)

	resolvedDest, err := resolveDest(ctx, execCfg, details.SinkURI)
	if err != nil {
		log.Changefeed.Warningf(ctx, "failed to resolve destination details for change monitoring: %v", err)
	}

	onTracingEvent := func(ctx context.Context, meta *execinfrapb.TracingAggregatorEvents) {
		componentID := execinfrapb.ComponentID{
			FlowID:        meta.FlowID,
			SQLInstanceID: meta.SQLInstanceID,
		}

		b.mu.Lock()
		defer b.mu.Unlock()
		b.mu.perNodeAggregatorStats[componentID] = *meta
	}

	// Create memory monitor for tableset watcher. Similar to how processors create
	// their monitors at function start, we create this unconditionally.
	watcherMemMonitor := execinfra.NewMonitor(ctx, execCfg.DistSQLSrv.ChangefeedMonitor, mon.MakeName("changefeed-tableset-watcher-mem"))
	defer watcherMemMonitor.Stop(ctx)

	// We'd like to avoid failing a changefeed unnecessarily, so when an error
	// bubbles up to this level, we'd like to "retry" the flow if possible. This
	// could be because the sink is down or because a cockroach node has crashed
	// or for many other reasons.
	maxBackoff := changefeedbase.MaxRetryBackoff.Get(&execCfg.Settings.SV)
	backoffReset := changefeedbase.RetryBackoffReset.Get(&execCfg.Settings.SV)

	// lastRunStatusUpdate is used to track the last time the job status was updated
	// to avoid updating the job status too frequently.
	var lastRunStatusUpdate time.Time

	for r := getRetry(ctx, maxBackoff, backoffReset); r.Next(); {
		flowErr := maybeUpgradePreProductionReadyExpression(ctx, jobID, details, jobExec)

		if flowErr == nil {
			// startedCh is normally used to signal back to the creator of the job that
			// the job has started; however, in this case nothing will ever receive
			// on the channel, causing the changefeed flow to block. Replace it with
			// a dummy channel.
			startedCh := make(chan tree.Datums, 1)
			if knobs != nil && knobs.BeforeDistChangefeed != nil {
				knobs.BeforeDistChangefeed()
			}

			changefeedDoneCh := make(chan struct{})
			g := ctxgroup.WithContext(ctx)
			initialHighWater, schemaTS, err := computeDistChangefeedTimestamps(ctx, jobExec, details, progress)
			if err != nil {
				return err
			}
			maybeCfKnobs, haveKnobs := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)
			if haveKnobs && maybeCfKnobs.AfterComputeDistChangefeedTimestamps != nil {
				maybeCfKnobs.AfterComputeDistChangefeedTimestamps(ctx)
			}
			targets, err := AllTargets(ctx, details, execCfg, schemaTS)
			if err != nil {
				return err
			}

			// This watcher is only used for db-level changefeeds with no watched tables.
			var watcher *tableset.Watcher
			var cancelWatcher context.CancelCauseFunc
			var watcherCtx context.Context
			waitForTables := isDBLevelChangefeed(details) && targets.NumUniqueTables() == 0
			if waitForTables {
				// Create a watcher for the database.
				filter, err := buildDatabaseWatcherFilterFromSpec(details.TargetSpecifications[0])
				if err != nil {
					return err
				}

				watcher = tableset.NewWatcher(filter, execCfg, watcherMemMonitor, int64(jobID))
				g.GoCtx(func(ctx context.Context) error {
					// This method runs the watcher until its context is canceled.
					// The watcher context is canceled when diffs are sent to the
					// watcherChan or when the the parent group context is canceled.
					watcherCtx, cancelWatcher = context.WithCancelCause(ctx)
					defer cancelWatcher(nil)
					err := watcher.Start(watcherCtx, schemaTS)
					if err != nil {
						if errors.Is(context.Cause(watcherCtx), errDoneWatching) {
							return nil
						}
						return err
					}
					return nil
				})
			}

			// Run the changefeed until it completes or is shut down.
			// If the changefeed's target tableset is empty, it will need to block
			// until a diff in the tableset is found from the watcher.
			g.GoCtx(func(ctx context.Context) error {
				defer close(changefeedDoneCh)
				if waitForTables {
					opts := changefeedbase.MakeStatementOptions(details.Opts)
					frequency, err := opts.GetHibernationPollingFrequency()
					if err != nil {
						return err
					}
				watchLoop:
					for ts := schemaTS; ctx.Err() == nil; ts = ts.AddDuration(*frequency) {
						unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, ts)
						if err != nil {
							return err
						}
						if !unchanged {
							// todo(#156874): once watcher gives us only adds,
							// we can safely take the first diff.
							for _, diff := range diffs {
								if diff.Added.ID != 0 {
									schemaTS = diff.AsOf
									initialHighWater = diff.AsOf
									targets, err = AllTargets(ctx, details, execCfg, schemaTS)
									if err != nil {
										return err
									}
									cancelWatcher(errDoneWatching)
									break watchLoop
								}
							}
						}
					}
				}
				var err error
				prevResult, err = startDistChangefeed(ctx, jobExec, jobID, schemaTS, details, description,
					initialHighWater, progress, prevResult, startedCh, onTracingEvent, targets)
				return err
			})

			// Poll for updated configuration or new database tables if hibernating.
			g.GoCtx(func(ctx context.Context) error {
				t := time.NewTicker(15 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-changefeedDoneCh:
						return nil
					case <-t.C:
						newDest, err := reloadDest(ctx, jobID, execCfg)
						if err != nil {
							log.Changefeed.Warningf(ctx, "failed to check for updated configuration: %v", err)
						} else if newDest != resolvedDest {
							resolvedDest = newDest
							return replanErr
						}
					}
				}
			})

			flowErr = g.Wait()

			if flowErr == nil {
				return nil // Changefeed completed -- e.g. due to initial_scan=only mode.
			}

			if errors.Is(flowErr, replanErr) {
				log.Changefeed.Infof(ctx, "restarting changefeed due to updated configuration (approx backoff: %s)", r.NextBackoff())
				continue
			}

			if knobs != nil && knobs.HandleDistChangefeedError != nil {
				flowErr = knobs.HandleDistChangefeedError(flowErr)
			}
		}

		// Terminate changefeed if needed.
		if err := changefeedbase.AsTerminalError(ctx, jobExec.ExecCfg().LeaseManager, flowErr); err != nil {
			log.Changefeed.Infof(ctx, "CHANGEFEED %d shutting down (cause: %v)", jobID, err)
			// Best effort -- update job status to make it clear why changefeed shut down.
			// This won't always work if this node is being shutdown/drained.
			if ctx.Err() == nil {
				b.setJobStatusMessage(ctx, time.Time{}, "shutdown due to %s", err)
			}
			return err
		}

		// All other errors retry.
		log.Changefeed.Warningf(ctx, `Changefeed job %d encountered transient error: %v (attempt %d) (approx backoff: %s)`,
			jobID, flowErr, 1+r.CurrentAttempt(), r.NextBackoff())
		lastRunStatusUpdate = b.setJobStatusMessage(ctx, lastRunStatusUpdate, "transient error: %s", flowErr)

		if metrics, ok := execCfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics); ok {
			sli, err := metrics.getSLIMetrics(details.Opts[changefeedbase.OptMetricsScope])
			if err != nil {
				return err
			}
			sli.ErrorRetries.Inc(1)
		}

		var err error
		progress, err = reconcileJobProgress(ctx, jobID, prevResult, execCfg)
		if err != nil {
			// Any errors during reconciliation are retry-able.
			// When retry-able error propagates to jobs registry, it will clear out
			// claim information, and will restart this job somewhere else (though,
			// it's possible that the job gets restarted on this node).
			return jobs.MarkAsRetryJobError(err)
		}

		if errors.Is(flowErr, changefeedbase.ErrNodeDraining) {
			select {
			case <-drainCh:
				// If this node is draining, there is no point in retrying.
				return jobs.MarkAsRetryJobError(changefeedbase.ErrNodeDraining)
			default:
				// We know that some node (other than this one) is draining.
				// When we retry, the planner ought to take into account
				// this information.  However, there is a bit of a race here
				// between draining node propagating information to this node,
				// and this node restarting changefeed before this happens.
				// We could come up with a mechanism to provide additional
				// information to dist sql planner.  Or... we could just wait a bit.
				log.Changefeed.Warningf(ctx,
					"Changefeed %d delaying restart due to %d node(s) (%v) draining (approx backoff: %s)",
					jobID, len(prevResult.drainingNodes), prevResult.drainingNodes, r.NextBackoff())
				r.Next() // default config: ~5 sec delay, plus 10 sec on the retry loop.
			}
		}
	}

	return errors.Wrap(ctx.Err(), `ran out of retries`)
}

func resolveDest(ctx context.Context, execCfg *sql.ExecutorConfig, sinkURI string) (string, error) {
	u, err := url.Parse(sinkURI)
	if err != nil {
		return "", err
	}
	if u.Scheme != changefeedbase.SinkSchemeExternalConnection {
		return sinkURI, nil
	}
	resolved := ""
	err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		conn, err := externalconn.LoadExternalConnection(ctx, u.Host, txn)
		if err != nil {
			return err
		}
		resolved = conn.UnredactedConnectionStatement()
		return nil
	})
	return resolved, err
}

func reloadDest(ctx context.Context, id jobspb.JobID, execCfg *sql.ExecutorConfig) (string, error) {
	reloadedJob, err := execCfg.JobRegistry.LoadJob(ctx, id)
	if err != nil {
		return "", err
	}
	newDetails := reloadedJob.Details().(jobspb.ChangefeedDetails)
	return resolveDest(ctx, execCfg, newDetails.SinkURI)
}

// reconcileJobProgress reloads job progress from the job table and applies
// any frontier updates from the flow result. If the frontier advanced or a
// span-level checkpoint was produced, the updated progress is persisted back
// to the job table. The returned progress is used for the next retry iteration.
func reconcileJobProgress(
	ctx context.Context, jobID jobspb.JobID, result flowResult, execCfg *sql.ExecutorConfig,
) (jobspb.Progress, error) {
	// Re-load the job in order to get the latest progress, which may have
	// been updated by the changeFrontier processor since the flow started.
	reloadedJob, reloadErr := execCfg.JobRegistry.LoadClaimedJob(ctx, jobID)
	if reloadErr != nil {
		log.Changefeed.Warningf(ctx, `CHANGEFEED job %d could not reload job progress (%s); `+
			`job should be retried later`, jobID, reloadErr)
		return jobspb.Progress{}, reloadErr
	}
	knobs, _ := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)
	if knobs != nil && knobs.LoadJobErr != nil {
		if err := knobs.LoadJobErr(); err != nil {
			return jobspb.Progress{}, err
		}
	}

	progress := reloadedJob.Progress()

	// The flow result contains up-to-date frontier information transmitted by
	// aggregators when the flow was terminated. To be safe, we don't blindly
	// trust it; instead, this is applied to the reloaded job progress, and
	// the resulting progress record persisted back to the jobs table.
	var highWater hlc.Timestamp
	if hw := progress.GetHighWater(); hw != nil {
		highWater = *hw
	}

	// Build frontier based on tracked spans.
	sf, err := span.MakeFrontierAt(highWater, result.trackedSpans...)
	if err != nil {
		return jobspb.Progress{}, err
	}
	// Advance frontier based on the information received from the aggregators.
	for sp, ts := range result.aggregatorFrontierSpans() {
		_, err := sf.Forward(sp, ts)
		if err != nil {
			return jobspb.Progress{}, err
		}
	}

	maxBytes := changefeedbase.SpanCheckpointMaxBytes.Get(&execCfg.Settings.SV)
	cp := checkpoint.Make(
		sf.Frontier(),
		result.aggregatorFrontierSpans(),
		maxBytes,
		nil, /* metrics */
	)

	// Update checkpoint.
	updateHW := highWater.Less(sf.Frontier())
	updateSpanCheckpoint := !cp.IsEmpty()

	if updateHW || updateSpanCheckpoint {
		if updateHW {
			frontier := sf.Frontier()
			progress.Progress = &jobspb.Progress_HighWater{HighWater: &frontier}
		}
		progress.Details.(*jobspb.Progress_Changefeed).Changefeed.SpanLevelCheckpoint = cp
		if log.V(1) {
			log.Changefeed.Infof(ctx, "Applying checkpoint to job record:  hw=%v, cf=%v",
				progress.GetHighWater(), progress.GetChangefeed())
		}
		if err := reloadedJob.NoTxn().Update(ctx,
			func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				if err := md.CheckRunningOrReverting(); err != nil {
					return err
				}
				ju.UpdateProgress(&progress)
				return nil
			},
		); err != nil {
			return jobspb.Progress{}, err
		}
	}

	return progress, nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *changefeedResumer) OnFailOrCancel(
	ctx context.Context, jobExec interface{}, _ error,
) error {
	exec := jobExec.(sql.JobExecContext)
	execCfg := exec.ExecCfg()
	progress := b.job.Progress()

	maybeCleanUpProtectedTimestamp := func(ptsID uuid.UUID) {
		b.maybeCleanUpProtectedTimestamp(
			ctx,
			execCfg.InternalDB,
			execCfg.ProtectedTimestampProvider,
			ptsID,
		)
	}

	maybeCleanUpProtectedTimestamp(progress.GetChangefeed().ProtectedTimestampRecord)
	// We clean up the per-table protected timestamps (and their accompanying
	// system tables protected timestamp record) in a transaction since we need
	// to read from the job info.
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptsEntries := cdcprogresspb.ProtectedTimestampRecords{}
		if err := readChangefeedJobInfo(
			ctx, perTableProtectedTimestampsFilename, &ptsEntries, txn, b.job.ID(),
		); err != nil {
			return err
		}
		// In the event that the changefeed is not using per-table protected
		// timestamps, the ptsEntries populated from the job info table
		// (in the file perTableProtectedTimestampsFilename) will be empty.
		// There is nothing to clean up, so we can safely return here.
		if len(ptsEntries.UserTables) == 0 && ptsEntries.SystemTables == uuid.Nil {
			return nil
		}
		for _, record := range ptsEntries.UserTables {
			maybeCleanUpProtectedTimestamp(record)
		}
		maybeCleanUpProtectedTimestamp(ptsEntries.SystemTables)
		return deleteChangefeedJobInfo(ctx, perTableProtectedTimestampsFilename, txn, b.job.ID())
	}); err != nil {
		return err
	}

	var numTargets uint
	if b.job != nil {
		targetsTS := progress.GetHighWater()
		if targetsTS == nil || targetsTS.IsEmpty() {
			details := b.job.Details().(jobspb.ChangefeedDetails)
			targetsTS = &details.StatementTime
		}
		targets, err := AllTargets(ctx, b.job.Details().(jobspb.ChangefeedDetails), execCfg, *targetsTS)
		if err != nil {
			return err
		}
		numTargets = targets.Size
	}
	shouldMigrate := log.ShouldMigrateEvent(b.sv)
	// If this job has failed (not canceled), increment the counter.
	if jobs.HasErrJobCanceled(
		errors.DecodeError(ctx, *b.job.Payload().FinalResumeError),
	) {
		telemetry.Count(`changefeed.enterprise.cancel`)
		logChangefeedCanceledTelemetry(ctx, b.job, numTargets, shouldMigrate)
	} else {
		telemetry.Count(`changefeed.enterprise.fail`)
		exec.ExecCfg().JobRegistry.MetricsStruct().Changefeed.(*Metrics).Failures.Inc(1)
		logChangefeedFailedTelemetry(ctx, b.job, changefeedbase.UnknownError, numTargets, shouldMigrate)
	}
	return nil
}

// CollectProfile is part of the jobs.Resumer interface.
func (b *changefeedResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	var aggStatsCopy bulk.ComponentAggregatorStats
	func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		aggStatsCopy = b.mu.perNodeAggregatorStats.DeepCopy()
	}()

	return bulk.FlushTracingAggregatorStats(ctx, b.job.ID(),
		p.ExecCfg().InternalDB, aggStatsCopy)
}

// Try to clean up a protected timestamp created by the changefeed.
func (b *changefeedResumer) maybeCleanUpProtectedTimestamp(
	ctx context.Context, db isql.DB, pts protectedts.Manager, ptsID uuid.UUID,
) {
	if ptsID == uuid.Nil {
		return
	}
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pts.WithTxn(txn).Release(ctx, ptsID)
	}); err != nil && !errors.Is(err, protectedts.ErrNotExists) {
		// NB: The record should get cleaned up by the reconciliation loop.
		// No good reason to cause more trouble by returning an error here.
		// Log and move on.
		log.Changefeed.Warningf(ctx, "failed to remove protected timestamp record %v: %v", ptsID, err)
	}
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor.
func getQualifiedTableName(
	ctx context.Context, execCfg *sql.ExecutorConfig, txn *kv.Txn, desc catalog.TableDescriptor,
) (string, error) {
	tbName, err := getQualifiedTableNameObj(ctx, execCfg, txn, desc)
	if err != nil {
		return "", err
	}
	if changefeedbase.UseBareTableNames.Get(&execCfg.Settings.SV) {
		return tree.AsStringWithFlags(&tbName, tree.FmtBareIdentifiers), nil
	}
	return tbName.String(), nil
}

// getQualifiedTableNameObj returns the database-qualified name of the table
// or view represented by the provided descriptor.
func getQualifiedTableNameObj(
	ctx context.Context, execCfg *sql.ExecutorConfig, txn *kv.Txn, desc catalog.TableDescriptor,
) (tree.TableName, error) {
	col := execCfg.CollectionFactory.NewCollection(ctx)
	db, err := col.ByIDWithoutLeased(txn).Get().Database(ctx, desc.GetParentID())
	if err != nil {
		return tree.TableName{}, err
	}
	sc, err := col.ByIDWithoutLeased(txn).Get().Schema(ctx, desc.GetParentSchemaID())
	if err != nil {
		return tree.TableName{}, err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(db.GetName()),
		tree.Name(sc.GetName()),
		tree.Name(desc.GetName()),
	)
	return tbName, nil
}

func isDBLevelChangefeed(details jobspb.ChangefeedDetails) bool {
	targetSpecs := details.TargetSpecifications
	if len(targetSpecs) == 0 {
		return false
	}
	return targetSpecs[0].Type == jobspb.ChangefeedTargetSpecification_DATABASE
}

// getChangefeedTargetName gets a table name with or without the dots
func getChangefeedTargetName(
	ctx context.Context,
	desc catalog.TableDescriptor,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	qualified bool,
) (string, error) {
	if qualified {
		return getQualifiedTableName(ctx, execCfg, txn, desc)
	}
	return desc.GetName(), nil
}

func logCreateChangefeedTelemetry(
	ctx context.Context, jr *jobs.Record, isTransformation bool, numTargets uint, migrateEvent bool,
) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if jr != nil {
		changefeedDetails := jr.Details.(jobspb.ChangefeedDetails)
		changefeedEventDetails = makeCommonChangefeedEventDetails(ctx, changefeedDetails, jr.Description, jr.JobID, numTargets)
	}

	createChangefeedEvent := &eventpb.CreateChangefeed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		Transformation:               isTransformation,
	}

	getChangefeedEventMigrator(migrateEvent).StructuredEvent(ctx, severity.INFO, createChangefeedEvent)
}

func logAlterChangefeedTelemetry(
	ctx context.Context, job *jobs.Job, prevDescription string, numTargets uint, migrateEvent bool,
) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if job != nil {
		changefeedDetails := job.Details().(jobspb.ChangefeedDetails)
		changefeedEventDetails = makeCommonChangefeedEventDetails(
			ctx, changefeedDetails, job.Payload().Description, job.ID(), numTargets)
	}

	alterChangefeedEvent := &eventpb.AlterChangefeed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		PreviousDescription:          prevDescription,
	}

	getChangefeedEventMigrator(migrateEvent).StructuredEvent(ctx, severity.INFO, alterChangefeedEvent)
}

func logChangefeedFailedTelemetry(
	ctx context.Context,
	job *jobs.Job,
	failureType changefeedbase.FailureType,
	numTargets uint,
	migrateEvent bool,
) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if job != nil {
		changefeedDetails := job.Details().(jobspb.ChangefeedDetails)
		changefeedEventDetails = makeCommonChangefeedEventDetails(ctx, changefeedDetails, job.Payload().Description, job.ID(), numTargets)
	}

	changefeedFailedEvent := &eventpb.ChangefeedFailed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		FailureType:                  failureType,
	}

	getChangefeedEventMigrator(migrateEvent).StructuredEvent(ctx, severity.INFO, changefeedFailedEvent)
}

func logChangefeedFailedTelemetryDuringStartup(
	ctx context.Context,
	description string,
	failureType changefeedbase.FailureType,
	migrateEvent bool,
) {
	changefeedFailedEvent := &eventpb.ChangefeedFailed{
		CommonChangefeedEventDetails: eventpb.CommonChangefeedEventDetails{Description: description},
		FailureType:                  failureType,
	}

	getChangefeedEventMigrator(migrateEvent).StructuredEvent(ctx, severity.INFO, changefeedFailedEvent)
}

func logChangefeedCanceledTelemetry(
	ctx context.Context, job *jobs.Job, numTargets uint, migrateEvent bool,
) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if job != nil {
		changefeedDetails := job.Details().(jobspb.ChangefeedDetails)
		changefeedEventDetails = makeCommonChangefeedEventDetails(ctx, changefeedDetails, job.Payload().Description, job.ID(), numTargets)
	}

	changefeedCanceled := &eventpb.ChangefeedCanceled{
		CommonChangefeedEventDetails: changefeedEventDetails,
	}
	getChangefeedEventMigrator(migrateEvent).StructuredEvent(ctx, severity.INFO, changefeedCanceled)
}

func makeCommonChangefeedEventDetails(
	ctx context.Context,
	details jobspb.ChangefeedDetails,
	description string,
	jobID jobspb.JobID,
	numTargets uint,
) eventpb.CommonChangefeedEventDetails {
	opts := details.Opts

	sinkType := "core"
	if details.SinkURI != `` {
		parsedSink, err := url.Parse(details.SinkURI)
		if err != nil {
			log.Changefeed.Warningf(ctx, "failed to parse sink for telemetry logging: %v", err)
		}
		sinkType = parsedSink.Scheme
	}

	initialScanType, initialScanSet := opts[changefeedbase.OptInitialScan]
	_, initialScanOnlySet := opts[changefeedbase.OptInitialScanOnly]
	_, noInitialScanSet := opts[changefeedbase.OptNoInitialScan]
	initialScan := `yes`
	if initialScanSet && initialScanType != `` {
		initialScan = initialScanType
	} else if initialScanOnlySet {
		initialScan = `only`
	} else if noInitialScanSet {
		initialScan = `no`
	}

	var resolved string
	resolvedValue, resolvedSet := opts[changefeedbase.OptResolvedTimestamps]
	if !resolvedSet {
		resolved = "no"
	} else if resolvedValue == `` {
		resolved = "yes"
	} else {
		resolved = resolvedValue
	}

	changefeedEventDetails := eventpb.CommonChangefeedEventDetails{
		Description: description,
		SinkType:    sinkType,
		// TODO: Rename this field to NumTargets.
		NumTables:   int32(numTargets),
		Resolved:    resolved,
		Format:      opts[changefeedbase.OptFormat],
		InitialScan: initialScan,
		JobId:       int64(jobID),
	}

	return changefeedEventDetails
}

func failureTypeForStartupError(err error) changefeedbase.FailureType {
	if errors.Is(err, context.Canceled) { // Occurs for sinkless changefeeds
		return changefeedbase.ConnectionClosed
	} else if isTagged, tag := changefeedbase.IsTaggedError(err); isTagged {
		return tag
	}
	return changefeedbase.OnStartup
}

// maybeUpgradePreProductionReadyExpression updates job record for the
// changefeed using CDC transformation, created prior to 23.1. The update
// happens once cluster version finalized.
// Returns nil when nothing needs to be done.
// Returns fatal error message, causing changefeed to fail, if automatic upgrade
// cannot for some reason. Returns a transient error to cause job retry/reload
// when expression was upgraded.
func maybeUpgradePreProductionReadyExpression(
	ctx context.Context,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	jobExec sql.JobExecContext,
) error {
	if details.Select == "" {
		// Not an expression based changefeed.  Nothing to do.
		return nil
	}

	if details.SessionData != nil {
		// Already production ready. Nothing to do.
		return nil
	}

	// Expressions prior to 23.1 were rewritten to fully qualify all
	// columns/types. Furthermore, those expressions couldn't use any functions
	// that depend on session data. Thus, it is safe to use minimal session data.
	sd := sessiondatapb.SessionData{
		Database:   "",
		UserProto:  jobExec.User().EncodeProto(),
		Internal:   true,
		SearchPath: sessiondata.DefaultSearchPathForUser(jobExec.User()).GetPathArray(),
	}
	details.SessionData = &sd

	const errUpgradeErrMsg = "error rewriting changefeed expression.  Please recreate failed changefeed manually."
	oldExpression, err := cdceval.ParseChangefeedExpression(details.Select)
	if err != nil {
		// That's mighty surprising; There is nothing we can do.  Make sure
		// we fail changefeed in this case.
		return changefeedbase.WithTerminalError(errors.WithHint(err, errUpgradeErrMsg))
	}
	newExpression, err := cdceval.RewritePreviewExpression(oldExpression)
	if err != nil {
		// That's mighty surprising; There is nothing we can do.  Make sure
		// we fail changefeed in this case.
		return changefeedbase.WithTerminalError(errors.WithHint(err, errUpgradeErrMsg))
	}
	details.Select = cdceval.AsStringUnredacted(newExpression)

	if err := jobExec.ExecCfg().JobRegistry.UpdateJobWithTxn(ctx, jobID, nil, /* txn */
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			payload := md.Payload
			payload.Details = jobspb.WrapPayloadDetails(details)
			ju.UpdatePayload(payload)
			return nil
		},
	); err != nil {
		// Failed to update job record; try again.
		return err
	}

	// Job record upgraded.  Return transient error to reload job record and retry.
	if newExpression == oldExpression {
		return errors.New("changefeed expression updated")
	}

	return errors.Newf("changefeed expression %s rewritten as %s. "+
		"Note: changefeed expression accesses the previous state of the row via deprecated cdc_prev() "+
		"function. The rewritten expression should continue to work, but is likely to be inefficient. "+
		"Existing changefeed needs to be recreated using new syntax. "+
		"Please see CDC documentation on the use of new cdc_prev tuple.",
		tree.AsString(oldExpression), tree.AsString(newExpression))
}

func getChangefeedEventMigrator(migrateEvent bool) log.StructuredEventMigrator {
	return log.NewStructuredEventMigrator(
		func() bool {
			return migrateEvent
		},
		channel.TELEMETRY,
	)
}

func buildTableToDatabaseAndSchemaLookup(
	targetAndParentDescs []catalog.Descriptor,
) (
	tableToSchema map[descpb.ID]catalog.SchemaDescriptor,
	tableToDatabase map[descpb.ID]catalog.DatabaseDescriptor,
) {
	// getSchema maps schema IDs to its schema descriptor.
	getSchema := make(map[descpb.ID]catalog.SchemaDescriptor)
	// getDatabase maps database IDs to its database descriptor.
	getDatabase := make(map[descpb.ID]catalog.DatabaseDescriptor)
	for _, desc := range targetAndParentDescs {
		switch desc.DescriptorType() {
		case catalog.Schema:
			getSchema[desc.GetID()] = desc.(catalog.SchemaDescriptor)
		case catalog.Database:
			getDatabase[desc.GetID()] = desc.(catalog.DatabaseDescriptor)
		}
	}
	// tableToSchema maps table IDs to its parent schema descriptor.
	tableToSchema = make(map[descpb.ID]catalog.SchemaDescriptor)
	// tableToDatabase maps table IDs to its parent database descriptor.
	tableToDatabase = make(map[descpb.ID]catalog.DatabaseDescriptor)
	for _, desc := range targetAndParentDescs {
		if desc.DescriptorType() == catalog.Table {
			tableToDatabase[desc.GetID()] = getDatabase[desc.GetParentID()]
			tableToSchema[desc.GetID()] = getSchema[desc.GetParentSchemaID()]
		}
	}
	return tableToSchema, tableToDatabase
}
