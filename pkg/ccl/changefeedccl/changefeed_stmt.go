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
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// featureChangefeedEnabled is used to enable and disable the CHANGEFEED feature.
var featureChangefeedEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"feature.changefeed.enabled",
	"set to true to enable changefeeds, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

func init() {
	sql.AddPlanHook("changefeed", changefeedPlanHook, changefeedTypeCheck)
	jobs.RegisterConstructor(
		jobspb.TypeChangefeed,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &changefeedResumer{job: job}
		},
		jobs.UsesTenantCostControl,
	)
}

type annotatedChangefeedStatement struct {
	*tree.CreateChangefeed
	originalSpecs       map[tree.ChangefeedTarget]jobspb.ChangefeedTargetSpecification
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

// changefeedPlanHook implements sql.PlanHookFn.
func changefeedPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	changefeedStmt := getChangefeedStatement(stmt)
	if changefeedStmt == nil {
		return nil, nil, nil, false, nil
	}

	exprEval := p.ExprEvaluator("CREATE CHANGEFEED")
	var sinkURI string
	unspecifiedSink := changefeedStmt.SinkURI == nil
	avoidBuffering := unspecifiedSink
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
			return nil, nil, nil, false, changefeedbase.MarkTaggedError(err, changefeedbase.UserInput)
		}
		header = withSinkHeader
	}

	rawOpts, err := exprEval.KVOptions(
		ctx, changefeedStmt.Options, changefeedvalidators.CreateOptionValidations,
	)
	if err != nil {
		return nil, nil, nil, false, err
	}

	// rowFn impements sql.PlanHookRowFn
	rowFn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if err := validateSettings(ctx, p); err != nil {
			return err
		}

		if !unspecifiedSink && sinkURI == `` {
			// Error if someone specifies an INTO with the empty string. We've
			// already sent the wrong result column headers.
			return errors.New(`omit the SINK clause for inline results`)
		}

		opts := changefeedbase.MakeStatementOptions(rawOpts)

		jr, err := createChangefeedJobRecord(
			ctx,
			p,
			changefeedStmt,
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
			p.ExtendedEvalContext().ChangefeedState = &coreChangefeedProgress{
				progress: progress,
			}

			// If this is a sinkless changefeed, then we should not hold on to the
			// descriptor leases accessed to plan the changefeed. If changes happen
			// to descriptors, they will be addressed during the execution.
			// Failing to release the leases would result in preventing any schema
			// changes on the relevant descriptors (including, potentially,
			// system.role_membership, if the privileges to access the table were
			// granted via an inherited role).
			p.ExtendedEvalContext().Descs.ReleaseAll(ctx)

			telemetry.Count(`changefeed.create.core`)
			logChangefeedCreateTelemetry(ctx, jr, changefeedStmt.Select != nil)

			var err error
			for r := getRetry(ctx); r.Next(); {
				if err = distChangefeedFlow(ctx, p, 0 /* jobID */, details, progress, resultsCh); err == nil {
					return nil
				}

				if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok {
					if knobs != nil && knobs.HandleDistChangefeedError != nil {
						err = knobs.HandleDistChangefeedError(err)
					}
				}

				if err = changefeedbase.AsTerminalError(ctx, p.ExecCfg().LeaseManager, err); err != nil {
					break
				}

				// All other errors retry.
				progress = p.ExtendedEvalContext().ChangefeedState.(*coreChangefeedProgress).progress
			}
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
		{
			var ptr *ptpb.Record
			codec := p.ExecCfg().Codec
			ptr = createProtectedTimestampRecord(
				ctx,
				codec,
				jobID,
				AllTargets(details),
				details.StatementTime,
				progress.GetChangefeed(),
			)

			jr.Progress = *progress.GetChangefeed()

			if changefeedStmt.CreatedByInfo != nil {
				// This changefeed statement invoked by the scheduler.  As such, the scheduler
				// must have specified transaction to use, and is responsible for committing
				// transaction.

				_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, *jr, jobID, p.InternalSQLTxn())
				if err != nil {
					return err
				}

				if ptr != nil {
					pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(p.InternalSQLTxn())
					if err := pts.Protect(ctx, ptr); err != nil {
						return err
					}
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
				if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, *jr); err != nil {
					return err
				}
				if ptr != nil {
					return p.ExecCfg().ProtectedTimestampProvider.WithTxn(txn).Protect(ctx, ptr)
				}
				return nil
			}); err != nil {
				if sj != nil {
					if err := sj.CleanupOnRollback(ctx); err != nil {
						log.Warningf(ctx, "failed to cleanup aborted job: %v", err)
					}
				}
				return err
			}
		}

		// Start the job.
		if err := sj.Start(ctx); err != nil {
			return err
		}

		logChangefeedCreateTelemetry(ctx, jr, changefeedStmt.Select != nil)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(jobID)),
		}:
			return nil
		}
	}

	rowFnLogErrors := func(ctx context.Context, pn []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := rowFn(ctx, pn, resultsCh)
		if err != nil {
			logChangefeedFailedTelemetry(ctx, nil, failureTypeForStartupError(err))
		}
		return err
	}
	return rowFnLogErrors, header, nil, avoidBuffering, nil
}

func createChangefeedJobRecord(
	ctx context.Context,
	p sql.PlanHookState,
	changefeedStmt *annotatedChangefeedStatement,
	sinkURI string,
	opts changefeedbase.StatementOptions,
	jobID jobspb.JobID,
	telemetryPath string,
) (*jobs.Record, error) {
	unspecifiedSink := changefeedStmt.SinkURI == nil

	for _, warning := range opts.DeprecationWarnings() {
		p.BufferClientNotice(ctx, pgnotice.Newf("%s", warning))
	}

	jobDescription, err := changefeedJobDescription(ctx, changefeedStmt.CreateChangefeed, sinkURI, opts)
	if err != nil {
		return nil, err
	}

	statementTime := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}
	var initialHighWater hlc.Timestamp
	evalTimestamp := func(s string) (hlc.Timestamp, error) {
		if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok {
			if knobs != nil && knobs.OverrideCursor != nil {
				s = knobs.OverrideCursor(&statementTime)
			}
		}
		asOfClause := tree.AsOfClause{Expr: tree.NewStrVal(s)}
		asOf, err := p.EvalAsOfTimestamp(ctx, asOfClause)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		return asOf.Timestamp, nil
	}
	if opts.HasStartCursor() {
		initialHighWater, err = evalTimestamp(opts.GetCursor())
		if err != nil {
			return nil, err
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
		asOf, err := asof.Eval(ctx, asOfClause, p.SemaCtx(), &p.ExtendedEvalContext().Context)
		if err != nil {
			return nil, err
		}
		endTime = asOf.Timestamp
	}

	{
		initialScanType, err := opts.GetInitialScanType()
		if err != nil {
			return nil, err
		}
		// TODO (zinger): Should we error or take the minimum
		// if endTime is already set?
		if initialScanType == changefeedbase.OnlyInitialScan {
			endTime = statementTime
		}
	}

	tableOnlyTargetList := tree.BackupTargetList{}
	for _, t := range changefeedStmt.Targets {
		tableOnlyTargetList.Tables.TablePatterns = append(tableOnlyTargetList.Tables.TablePatterns, t.TableName)
	}

	// This grabs table descriptors once to get their ids.
	targetDescs, err := getTableDescriptors(ctx, p, &tableOnlyTargetList, statementTime, initialHighWater)
	if err != nil {
		return nil, err
	}

	targets, tables, err := getTargetsAndTables(ctx, p, targetDescs, changefeedStmt.Targets,
		changefeedStmt.originalSpecs, opts.ShouldUseFullStatementTimeName(), sinkURI)

	if err != nil {
		return nil, err
	}
	tolerances := opts.GetCanHandle()
	sd := p.SessionData().Clone()
	// Add non-local session data state (localization, etc).
	sessiondata.MarshalNonLocal(p.SessionData(), &sd.SessionData)
	details := jobspb.ChangefeedDetails{
		Tables:               tables,
		SinkURI:              sinkURI,
		StatementTime:        statementTime,
		EndTime:              endTime,
		TargetSpecifications: targets,
		SessionData:          &sd.SessionData,
	}

	specs := AllTargets(details)
	hasSelectPrivOnAllTables := true
	hasChangefeedPrivOnAllTables := true
	for _, desc := range targetDescs {
		if table, isTable := desc.(catalog.TableDescriptor); isTable {
			if err := changefeedvalidators.ValidateTable(specs, table, tolerances); err != nil {
				return nil, err
			}
			for _, warning := range changefeedvalidators.WarningsForTable(table, tolerances) {
				p.BufferClientNotice(ctx, pgnotice.Newf("%s", warning))
			}

			hasSelect, hasChangefeed, err := checkPrivilegesForDescriptor(ctx, p, desc)
			if err != nil {
				return nil, err
			}
			hasSelectPrivOnAllTables = hasSelectPrivOnAllTables && hasSelect
			hasChangefeedPrivOnAllTables = hasChangefeedPrivOnAllTables && hasChangefeed
		}
	}
	if checkPrivs {
		if err := authorizeUserToCreateChangefeed(ctx, p, sinkURI, hasSelectPrivOnAllTables, hasChangefeedPrivOnAllTables); err != nil {
			return nil, err
		}
	}

	if changefeedStmt.Select != nil {
		// Serialize changefeed expression.
		normalized, withDiff, err := validateAndNormalizeChangefeedExpression(
			ctx, p, opts, changefeedStmt.Select, targetDescs, targets, statementTime,
		)
		if err != nil {
			return nil, err
		}
		if withDiff {
			if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V23_1_ChangefeedExpressionProductionReady) {
				return nil,
					pgerror.Newf(
						pgcode.FeatureNotSupported,
						"cannot create new changefeed with CDC expression <%s>, "+
							"which requires access to cdc_prev until cluster upgrade to %s finalized.",
						tree.AsString(normalized),
						clusterversion.V23_1_ChangefeedExpressionProductionReady.String,
					)
			}
			opts.ForceDiff()
		} else if opts.IsSet(changefeedbase.OptDiff) {
			opts.ClearDiff()
			p.BufferClientNotice(ctx, pgnotice.Newf(
				"turning off unused %s option (expression <%s> does not use cdc_prev)",
				changefeedbase.OptDiff, tree.AsString(normalized)))
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
		return nil, err
	}
	if newScheme, ok := changefeedbase.NoLongerExperimental[parsedSink.Scheme]; ok {
		parsedSink.Scheme = newScheme // This gets munged anyway when building the sink
		p.BufferClientNotice(ctx, pgnotice.Newf(`%[1]s is no longer experimental, use %[1]s://`,
			newScheme),
		)
	}

	if err = validateDetailsAndOptions(details, opts); err != nil {
		return nil, err
	}

	encodingOpts, err := opts.GetEncodingOptions()
	if err != nil {
		return nil, err
	}
	if _, err := getEncoder(encodingOpts, AllTargets(details)); err != nil {
		return nil, err
	}

	if !unspecifiedSink && p.ExecCfg().ExternalIODirConfig.DisableOutbound {
		return nil, errors.Errorf("Outbound IO is disabled by configuration, cannot create changefeed into %s", parsedSink.Scheme)
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
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), "CHANGEFEED",
		); err != nil {
			return nil, errors.Wrapf(err,
				"use of %q option requires enterprise license.", changefeedbase.OptMetricsScope)
		}

		if scope == defaultSLIScope {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
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
				status.ChildMetricsEnabled.Key(),
				scope,
			))
		}
	}

	if details.SinkURI == `` {
		details.Opts = opts.AsMap()
		// Jobs should not be created for sinkless changefeeds. However, note that
		// we create and return a job record for sinkless changefeeds below. This is
		// because we need the details field to create our sinkless changefeed.
		// After this job record is returned, we create our forever running sinkless
		// changefeed, thus ensuring that no job is created for this changefeed as
		// desired.
		sinklessRecord := &jobs.Record{
			Details: details,
		}
		return sinklessRecord, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), "CHANGEFEED",
	); err != nil {
		return nil, err
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
	err = validateSink(ctx, p, jobID, details, opts)

	details.Opts = opts.AsMap()

	jr := &jobs.Record{
		Description: jobDescription,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
			for _, desc := range targetDescs {
				sqlDescIDs = append(sqlDescIDs, desc.GetID())
			}
			return sqlDescIDs
		}(),
		Details:   details,
		CreatedBy: changefeedStmt.CreatedByInfo,
	}

	return jr, err
}

func validateSettings(ctx context.Context, p sql.PlanHookState) error {
	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureChangefeedEnabled,
		"CHANGEFEED",
	); err != nil {
		return err
	}

	// Changefeeds are based on the Rangefeed abstraction, which
	// requires the `kv.rangefeed.enabled` setting to be true.
	if !kvserver.RangefeedEnabled.Get(&p.ExecCfg().Settings.SV) {
		return errors.Errorf("rangefeeds require the kv.rangefeed.enabled setting. See %s",
			docs.URL(`change-data-capture.html#enable-rangefeeds-to-reduce-latency`))
	}

	return nil
}

func getTableDescriptors(
	ctx context.Context,
	p sql.PlanHookState,
	targets *tree.BackupTargetList,
	statementTime hlc.Timestamp,
	initialHighWater hlc.Timestamp,
) (map[tree.TablePattern]catalog.Descriptor, error) {
	// For now, disallow targeting a database or wildcard table selection.
	// Getting it right as tables enter and leave the set over time is
	// tricky.
	if len(targets.Databases) > 0 {
		return nil, errors.Errorf(`CHANGEFEED cannot target %s`,
			tree.AsString(targets))
	}
	for _, t := range targets.Tables.TablePatterns {
		p, err := t.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		if _, ok := p.(*tree.TableName); !ok {
			return nil, errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(t))
		}
	}

	_, _, _, targetDescs, err := backupresolver.ResolveTargetsToDescriptors(ctx, p, statementTime, targets)
	if err != nil {
		var m *backupresolver.MissingTableErr
		if errors.As(err, &m) {
			tableName := m.GetTableName()
			err = errors.Errorf("table %q does not exist", tableName)
		}
		err = errors.Wrap(err, "failed to resolve targets in the CHANGEFEED stmt")
		if !initialHighWater.IsEmpty() {
			// We specified cursor -- it is possible the targets do not exist at that time.
			// Give a bit more context in the error message.
			err = errors.WithHintf(err,
				"do the targets exist at the specified cursor time %s?", initialHighWater)
		}
	}
	return targetDescs, err
}

func getTargetsAndTables(
	ctx context.Context,
	p sql.PlanHookState,
	targetDescs map[tree.TablePattern]catalog.Descriptor,
	rawTargets tree.ChangefeedTargets,
	originalSpecs map[tree.ChangefeedTarget]jobspb.ChangefeedTargetSpecification,
	fullTableName bool,
	sinkURI string,
) ([]jobspb.ChangefeedTargetSpecification, jobspb.ChangefeedTargets, error) {
	tables := make(jobspb.ChangefeedTargets, len(targetDescs))
	targets := make([]jobspb.ChangefeedTargetSpecification, len(rawTargets))
	seen := make(map[jobspb.ChangefeedTargetSpecification]tree.ChangefeedTarget)

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
				TableID:           td.GetID(),
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

func validateSink(
	ctx context.Context,
	p sql.PlanHookState,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	opts changefeedbase.StatementOptions,
) error {
	metrics := p.ExecCfg().JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	scope, _ := opts.GetMetricScope()
	sli, err := metrics.getSLIMetrics(scope)
	if err != nil {
		return err
	}
	var nilOracle timestampLowerBoundOracle
	canarySink, err := getAndDialSink(ctx, &p.ExecCfg().DistSQLSrv.ServerConfig, details,
		nilOracle, p.User(), jobID, sli)
	if err != nil {
		return err
	}
	if err := canarySink.Close(); err != nil {
		return err
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

func changefeedJobDescription(
	ctx context.Context,
	changefeed *tree.CreateChangefeed,
	sinkURI string,
	opts changefeedbase.StatementOptions,
) (string, error) {
	cleanedSinkURI, err := cloud.SanitizeExternalStorageURI(sinkURI, []string{
		changefeedbase.SinkParamSASLPassword,
		changefeedbase.SinkParamCACert,
		changefeedbase.SinkParamClientCert,
	})
	if err != nil {
		return "", err
	}

	cleanedSinkURI, err = changefeedbase.RedactUserFromURI(cleanedSinkURI)
	if err != nil {
		return "", err
	}

	logSanitizedChangefeedDestination(ctx, cleanedSinkURI)

	c := &tree.CreateChangefeed{
		Targets: changefeed.Targets,
		SinkURI: tree.NewDString(cleanedSinkURI),
		Select:  changefeed.Select,
	}
	if err = opts.ForEachWithRedaction(func(k string, v string) {
		opt := tree.KVOption{Key: tree.Name(k)}
		if len(v) > 0 {
			opt.Value = tree.NewDString(v)
		}
		c.Options = append(c.Options, opt)
	}); err != nil {
		return "", err
	}
	sort.Slice(c.Options, func(i, j int) bool { return c.Options[i].Key < c.Options[j].Key })
	return tree.AsString(c), nil
}

func logSanitizedChangefeedDestination(ctx context.Context, destination string) {
	log.Ops.Infof(ctx, "changefeed planning to connect to destination %v", redact.Safe(destination))
}

func validateDetailsAndOptions(
	details jobspb.ChangefeedDetails, opts changefeedbase.StatementOptions,
) error {
	if err := opts.ValidateForCreateChangefeed(details.Select != ""); err != nil {
		return err
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
}

func (b *changefeedResumer) setJobRunningStatus(
	ctx context.Context, lastUpdate time.Time, fmtOrMsg string, args ...interface{},
) time.Time {
	if timeutil.Since(lastUpdate) < runStatusUpdateFrequency {
		return lastUpdate
	}

	status := jobs.RunningStatus(fmt.Sprintf(fmtOrMsg, args...))
	if err := b.job.NoTxn().RunningStatus(ctx,
		func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return status, nil
		},
	); err != nil {
		log.Warningf(ctx, "failed to set running status: %v", err)
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

	err := b.resumeWithRetries(ctx, jobExec, jobID, details, progress, execCfg)
	if err != nil {
		return b.handleChangefeedError(ctx, err, details, jobExec)
	}
	return nil
}

func (b *changefeedResumer) handleChangefeedError(
	ctx context.Context,
	changefeedErr error,
	details jobspb.ChangefeedDetails,
	jobExec sql.JobExecContext,
) error {
	opts := changefeedbase.MakeStatementOptions(details.Opts)
	onError, errErr := opts.GetOnError()
	if errErr != nil {
		return errors.CombineErrors(changefeedErr, errErr)
	}
	switch onError {
	// default behavior
	case changefeedbase.OptOnErrorFail:
		return changefeedErr
	// pause instead of failing
	case changefeedbase.OptOnErrorPause:
		// note: we only want the job to pause here if a failure happens, not a
		// user-initiated cancellation. if the job has been canceled, the ctx
		// will handle it and the pause will return an error.
		const errorFmt = "job failed (%v) but is being paused because of %s=%s"
		errorMessage := fmt.Sprintf(errorFmt, changefeedErr,
			changefeedbase.OptOnError, changefeedbase.OptOnErrorPause)
		return b.job.NoTxn().PauseRequestedWithFunc(ctx, func(ctx context.Context,
			planHookState interface{}, txn isql.Txn, progress *jobspb.Progress) error {
			err := b.OnPauseRequest(ctx, jobExec, txn, progress)
			if err != nil {
				return err
			}
			// directly update running status to avoid the running/reverted job status check
			progress.RunningStatus = errorMessage
			log.Warningf(ctx, errorFmt, changefeedErr, changefeedbase.OptOnError, changefeedbase.OptOnErrorPause)
			return nil
		}, errorMessage)
	default:
		return errors.Wrapf(changefeedErr, "unrecognized option value: %s=%s for handling error",
			changefeedbase.OptOnError, details.Opts[changefeedbase.OptOnError])
	}
}

func (b *changefeedResumer) resumeWithRetries(
	ctx context.Context,
	jobExec sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	execCfg *sql.ExecutorConfig,
) error {
	// We'd like to avoid failing a changefeed unnecessarily, so when an error
	// bubbles up to this level, we'd like to "retry" the flow if possible. This
	// could be because the sink is down or because a cockroach node has crashed
	// or for many other reasons.
	var lastRunStatusUpdate time.Time

	for r := getRetry(ctx); r.Next(); {
		err := maybeUpgradePreProductionReadyExpression(ctx, jobID, details, jobExec)

		if err == nil {
			// startedCh is normally used to signal back to the creator of the job that
			// the job has started; however, in this case nothing will ever receive
			// on the channel, causing the changefeed flow to block. Replace it with
			// a dummy channel.
			startedCh := make(chan tree.Datums, 1)
			err = distChangefeedFlow(ctx, jobExec, jobID, details, progress, startedCh)
			if err == nil {
				return nil // Changefeed completed -- e.g. due to initial_scan=only mode.
			}

			if knobs, ok := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok {
				if knobs != nil && knobs.HandleDistChangefeedError != nil {
					err = knobs.HandleDistChangefeedError(err)
				}
			}
		}

		// Terminate changefeed if needed.
		if err := changefeedbase.AsTerminalError(ctx, jobExec.ExecCfg().LeaseManager, err); err != nil {
			log.Infof(ctx, "CHANGEFEED %d shutting down (cause: %v)", jobID, err)
			// Best effort -- update job status to make it clear why changefeed shut down.
			// This won't always work if this node is being shutdown/drained.
			b.setJobRunningStatus(ctx, time.Time{}, "shutdown due to %s", err)
			return err
		}

		// All other errors retry.
		log.Warningf(ctx, `WARNING: CHANGEFEED job %d encountered retryable error: %v`, jobID, err)
		lastRunStatusUpdate = b.setJobRunningStatus(ctx, lastRunStatusUpdate, "retryable error: %s", err)
		if metrics, ok := execCfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics); ok {
			sli, err := metrics.getSLIMetrics(details.Opts[changefeedbase.OptMetricsScope])
			if err != nil {
				return err
			}
			sli.ErrorRetries.Inc(1)
		}
		// Re-load the job in order to update our progress object, which may have
		// been updated by the changeFrontier processor since the flow started.
		reloadedJob, reloadErr := execCfg.JobRegistry.LoadClaimedJob(ctx, jobID)
		if reloadErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Warningf(ctx, `CHANGEFEED job %d could not reload job progress; `+
				`continuing from last known high-water of %s: %v`,
				jobID, progress.GetHighWater(), reloadErr)
		} else {
			progress = reloadedJob.Progress()
			details = reloadedJob.Details().(jobspb.ChangefeedDetails)
		}
	}
	return errors.Wrap(ctx.Err(), `ran out of retries`)
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *changefeedResumer) OnFailOrCancel(
	ctx context.Context, jobExec interface{}, _ error,
) error {
	exec := jobExec.(sql.JobExecContext)
	execCfg := exec.ExecCfg()
	progress := b.job.Progress()
	b.maybeCleanUpProtectedTimestamp(
		ctx,
		execCfg.InternalDB,
		execCfg.ProtectedTimestampProvider,
		progress.GetChangefeed().ProtectedTimestampRecord,
	)

	// If this job has failed (not canceled), increment the counter.
	if jobs.HasErrJobCanceled(
		errors.DecodeError(ctx, *b.job.Payload().FinalResumeError),
	) {
		telemetry.Count(`changefeed.enterprise.cancel`)
	} else {
		telemetry.Count(`changefeed.enterprise.fail`)
		exec.ExecCfg().JobRegistry.MetricsStruct().Changefeed.(*Metrics).Failures.Inc(1)
		logChangefeedFailedTelemetry(ctx, b.job, changefeedbase.UnknownError)
	}
	return nil
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
		log.Warningf(ctx, "failed to remove protected timestamp record %v: %v", ptsID, err)
	}
}

var _ jobs.PauseRequester = (*changefeedResumer)(nil)

// OnPauseRequest implements jobs.PauseRequester. If this changefeed is being
// paused, we may want to clear the protected timestamp record.
func (b *changefeedResumer) OnPauseRequest(
	ctx context.Context, jobExec interface{}, txn isql.Txn, progress *jobspb.Progress,
) error {
	details := b.job.Details().(jobspb.ChangefeedDetails)

	cp := progress.GetChangefeed()
	execCfg := jobExec.(sql.JobExecContext).ExecCfg()

	if _, shouldProtect := details.Opts[changefeedbase.OptProtectDataFromGCOnPause]; !shouldProtect {
		// Release existing pts record to avoid a single changefeed left on pause
		// resulting in storage issues
		if cp.ProtectedTimestampRecord != uuid.Nil {
			pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
			if err := pts.Release(ctx, cp.ProtectedTimestampRecord); err != nil {
				log.Warningf(ctx, "failed to release protected timestamp %v: %v", cp.ProtectedTimestampRecord, err)
			} else {
				cp.ProtectedTimestampRecord = uuid.Nil
			}
		}
		return nil
	}

	if cp.ProtectedTimestampRecord == uuid.Nil {
		resolved := progress.GetHighWater()
		if resolved == nil {
			return nil
		}
		pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
		ptr := createProtectedTimestampRecord(ctx, execCfg.Codec, b.job.ID(), AllTargets(details), *resolved, cp)
		return pts.Protect(ctx, ptr)
	}

	return nil
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
	return tbName.String(), nil
}

// getQualifiedTableNameObj returns the database-qualified name of the table
// or view represented by the provided descriptor.
func getQualifiedTableNameObj(
	ctx context.Context, execCfg *sql.ExecutorConfig, txn *kv.Txn, desc catalog.TableDescriptor,
) (tree.TableName, error) {
	col := execCfg.CollectionFactory.NewCollection(ctx)
	db, err := col.ByID(txn).Get().Database(ctx, desc.GetParentID())
	if err != nil {
		return tree.TableName{}, err
	}
	sc, err := col.ByID(txn).Get().Schema(ctx, desc.GetParentSchemaID())
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

func logChangefeedCreateTelemetry(ctx context.Context, jr *jobs.Record, isTransformation bool) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if jr != nil {
		changefeedDetails := jr.Details.(jobspb.ChangefeedDetails)
		changefeedEventDetails = getCommonChangefeedEventDetails(ctx, changefeedDetails, jr.Description)
	}

	createChangefeedEvent := &eventpb.CreateChangefeed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		Transformation:               isTransformation,
	}

	log.StructuredEvent(ctx, createChangefeedEvent)
}

func logChangefeedFailedTelemetry(
	ctx context.Context, job *jobs.Job, failureType changefeedbase.FailureType,
) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if job != nil {
		changefeedDetails := job.Details().(jobspb.ChangefeedDetails)
		changefeedEventDetails = getCommonChangefeedEventDetails(ctx, changefeedDetails, job.Payload().Description)
	}

	changefeedFailedEvent := &eventpb.ChangefeedFailed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		FailureType:                  failureType,
	}

	log.StructuredEvent(ctx, changefeedFailedEvent)
}

func getCommonChangefeedEventDetails(
	ctx context.Context, details jobspb.ChangefeedDetails, description string,
) eventpb.CommonChangefeedEventDetails {
	opts := details.Opts

	sinkType := "core"
	if details.SinkURI != `` {
		parsedSink, err := url.Parse(details.SinkURI)
		if err != nil {
			log.Warningf(ctx, "failed to parse sink for telemetry logging: %v", err)
		}
		sinkType = parsedSink.Scheme
	}

	var initialScan string
	initialScanType, initialScanSet := opts[changefeedbase.OptInitialScan]
	_, initialScanOnlySet := opts[changefeedbase.OptInitialScanOnly]
	_, noInitialScanSet := opts[changefeedbase.OptNoInitialScan]
	if initialScanSet && initialScanType == `` {
		initialScan = `yes`
	} else if initialScanSet && initialScanType != `` {
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
		NumTables:   int32(AllTargets(details).Size),
		Resolved:    resolved,
		Format:      opts[changefeedbase.OptFormat],
		InitialScan: initialScan,
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
// changefeed using CDC transformation, created prior to
// clusterversion.V23_1_ChangefeedExpressionProductionReady. The update happens
// once cluster version finalized.
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

	if !jobExec.ExecCfg().Settings.Version.IsActive(
		ctx, clusterversion.V23_1_ChangefeedExpressionProductionReady,
	) {
		// Can't upgrade job record yet -- wait until upgrade finalized.
		return nil
	}

	// Expressions prior to
	// clusterversion.V23_1_ChangefeedExpressionProductionReady were rewritten to
	// fully qualify all columns/types.  Furthermore, those expressions couldn't
	// use any functions that depend on session data.  Thus, it is safe to use
	// minimal session data.
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

	const useReadLock = false
	if err := jobExec.ExecCfg().JobRegistry.UpdateJobWithTxn(ctx, jobID, nil, useReadLock,
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
