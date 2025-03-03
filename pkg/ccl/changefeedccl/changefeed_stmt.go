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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

		jr, err := createChangefeedJobRecord(
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
			logChangefeedCreateTelemetry(ctx, jr, changefeedStmt.Select != nil)

			err := coreChangefeed(ctx, p, details, description, progress, resultsCh)
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
			var ptr *ptpb.Record
			codec := p.ExecCfg().Codec
			ptr = createProtectedTimestampRecord(
				ctx,
				codec,
				jobID,
				AllTargets(details),
				details.StatementTime,
			)
			progress.GetChangefeed().ProtectedTimestampRecord = ptr.ID.GetUUID()

			jr.Progress = *progress.GetChangefeed()

			if changefeedStmt.CreatedByInfo != nil {
				// This changefeed statement invoked by the scheduler.  As such, the scheduler
				// must have specified transaction to use, and is responsible for committing
				// transaction.

				_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, *jr, jr.JobID, p.InternalSQLTxn())
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
				if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jr.JobID, txn, *jr); err != nil {
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

	rowFnLogErrors := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		err := rowFn(ctx, resultsCh)
		if err != nil {
			logChangefeedFailedTelemetryDuringStartup(ctx, description, failureTypeForStartupError(err))
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
) error {
	localState := &cachedState{progress: progress}
	p.ExtendedEvalContext().ChangefeedState = localState
	knobs, _ := p.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)

	for r := getRetry(ctx); ; {
		if !r.Next() {
			// Retry loop exits when context is canceled.
			log.Infof(ctx, "core changefeed retry loop exiting: %s", ctx.Err())
			return ctx.Err()
		}

		if knobs != nil && knobs.BeforeDistChangefeed != nil {
			knobs.BeforeDistChangefeed()
		}

		err := distChangefeedFlow(ctx, p, 0 /* jobID */, details, description, localState, resultsCh)
		if err == nil {
			log.Infof(ctx, "core changefeed completed with no error")
			return nil
		}

		if knobs != nil && knobs.HandleDistChangefeedError != nil {
			err = knobs.HandleDistChangefeedError(err)
		}

		if err := changefeedbase.AsTerminalError(ctx, p.ExecCfg().LeaseManager, err); err != nil {
			log.Infof(ctx, "core changefeed failed due to error: %s", err)
			return err
		}

		// All other errors retry; but we'll use an up-to-date progress
		// information which is saved in the localState.
		log.Infof(ctx, "core changefeed retrying due to transient error: %s", err)
	}
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
) (*jobs.Record, error) {
	unspecifiedSink := changefeedStmt.SinkURI == nil

	for _, warning := range opts.DeprecationWarnings() {
		p.BufferClientNotice(ctx, pgnotice.Newf("%s", warning))
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
		var err error
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

	for _, t := range targetDescs {
		if tbl, ok := t.(catalog.TableDescriptor); ok && tbl.ExternalRowData() != nil {
			if tbl.ExternalRowData().TenantID.IsSet() {
				return nil, errors.UnimplementedError(errors.IssueLink{}, "changefeeds on a replication target are not supported")
			}
			return nil, errors.UnimplementedError(errors.IssueLink{}, "changefeeds on external tables are not supported")
		}
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
		if err := authorizeUserToCreateChangefeed(ctx, p, sinkURI, hasSelectPrivOnAllTables, hasChangefeedPrivOnAllTables, opts.GetConfluentSchemaRegistry()); err != nil {
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
			opts.ForceDiff()
		} else if opts.IsSet(changefeedbase.OptDiff) {
			// Expression didn't reference cdc_prev, but the diff option was specified.
			// This only makes sense if we have wrapped envelope.
			encopts, err := opts.GetEncodingOptions()
			if err != nil {
				return nil, err
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

	// Validate the encoder. We can pass an empty slimetrics struct here since the encoder will not be used.
	encodingOpts, err := opts.GetEncodingOptions()
	if err != nil {
		return nil, err
	}
	sourceProvider := newEnrichedSourceProvider(encodingOpts, enrichedSourceData{jobId: jobID.String()})
	if _, err := getEncoder(ctx, encodingOpts, AllTargets(details), details.Select != "",
		makeExternalConnectionProvider(ctx, p.ExecCfg().InternalDB), nil, sourceProvider); err != nil {
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
			p.ExecCfg().Settings, "CHANGEFEED",
		); err != nil {
			return nil, errors.Wrapf(err,
				"use of %q option requires an enterprise license.", changefeedbase.OptMetricsScope)
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
				status.ChildMetricsEnabled.Name(),
				scope,
			))
		}
	}

	if details.SinkURI == `` {

		if details.Select != `` {
			if err := utilccl.CheckEnterpriseEnabled(
				p.ExecCfg().Settings, "CHANGEFEED",
			); err != nil {
				return nil, errors.Wrap(err, "use of AS SELECT requires an enterprise license.")
			}
		}

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
		return sinklessRecord, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, "CHANGEFEED",
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
	if err != nil {
		return nil, err
	}
	details.Opts = opts.AsMap()

	if locFilter := details.Opts[changefeedbase.OptExecutionLocality]; locFilter != "" {
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, changefeedbase.OptExecutionLocality,
		); err != nil {
			return nil, err
		}

		var executionLocality roachpb.Locality
		if err := executionLocality.Set(locFilter); err != nil {
			return nil, err
		}
		if _, err := p.DistSQLPlanner().GetAllInstancesByLocality(ctx, executionLocality); err != nil {
			return nil, err
		}
	}
	resolvedOpt, emit, err := opts.GetResolvedTimestampInterval()
	if err != nil {
		return nil, err
	}
	var resolved time.Duration
	resolvedStr := " by default"
	if resolvedOpt != nil {
		resolved = *resolvedOpt
		resolvedStr = ""
	}
	freqOpt, err := opts.GetMinCheckpointFrequency()
	if err != nil {
		return nil, err
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

	ptsExpiration, err := opts.GetPTSExpiration()
	if err != nil {
		return nil, err
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

	jr := &jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
			for _, desc := range targetDescs {
				sqlDescIDs = append(sqlDescIDs, desc.GetID())
			}
			return sqlDescIDs
		}(),
		Details:       details,
		CreatedBy:     changefeedStmt.CreatedByInfo,
		MaximumPTSAge: ptsExpiration,
	}

	return jr, nil
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
		nilOracle, p.User(), jobID, sli)
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

func makeChangefeedDescription(
	ctx context.Context,
	changefeed *tree.CreateChangefeed,
	sinkURI string,
	opts changefeedbase.StatementOptions,
) (string, error) {
	c := &tree.CreateChangefeed{
		Targets: changefeed.Targets,
		Select:  changefeed.Select,
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

func (b *changefeedResumer) setJobStatusMessage(
	ctx context.Context, lastUpdate time.Time, fmtOrMsg string, args ...interface{},
) time.Time {
	if timeutil.Since(lastUpdate) < runStatusUpdateFrequency {
		return lastUpdate
	}

	status := jobs.StatusMessage(fmt.Sprintf(fmtOrMsg, args...))
	if err := b.job.NoTxn().UpdateStatusMessage(ctx, status); err != nil {
		log.Warningf(ctx, "failed to set status: %v", err)
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
		current, err := p.DistSQLPlanner().GetSQLInstanceInfo(p.ExecCfg().JobRegistry.ID())
		if err != nil {
			return err
		}
		if ok, missedTier := current.Locality.Matches(locality); !ok {
			log.Infof(ctx,
				"CHANGEFEED job %d initially adopted on instance %d but it does not match locality filter %s, finding a new coordinator",
				b.job.ID(), current.NodeID, missedTier.String(),
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
		log.Warningf(ctx, "job lease relocated (%v)", changefeedErr)
		return changefeedErr
	}
	opts := changefeedbase.MakeStatementOptions(details.Opts)
	onError, errErr := opts.GetOnError()
	if errErr != nil {
		log.Warningf(ctx, "job failed (%v) but was unable to get on error option (%v)", changefeedErr, errErr)
		return errors.CombineErrors(changefeedErr, errErr)
	}
	switch onError {
	// default behavior
	case changefeedbase.OptOnErrorFail:
		log.Warningf(ctx, "job failed (%v)", changefeedErr)
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
			log.Warningf(ctx, errorFmt, changefeedErr, changefeedbase.OptOnError, changefeedbase.OptOnErrorPause)
			return nil
		}, errorMessage)
	default:
		log.Warningf(ctx, "job failed (%v) but has unrecognized option value %s=%s", changefeedErr, changefeedbase.OptOnError, details.Opts[changefeedbase.OptOnError])
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

	// Grab a "reference" to this nodes job registry in order to make sure
	// this resumer has enough time to persist up to date checkpoint in case
	// of node drain.
	drainCh, cleanup := execCfg.JobRegistry.OnDrain()
	defer cleanup()

	// We'd like to avoid failing a changefeed unnecessarily, so when an error
	// bubbles up to this level, we'd like to "retry" the flow if possible. This
	// could be because the sink is down or because a cockroach node has crashed
	// or for many other reasons.
	var lastRunStatusUpdate time.Time

	// Setup local state information.
	// This information is used by dist flow process to communicate back
	// the up-to-date checkpoint and node health information in case
	// changefeed encounters transient error.
	localState := &cachedState{progress: initialProgress}
	jobExec.ExtendedEvalContext().ChangefeedState = localState
	knobs, _ := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)

	resolvedDest, err := resolveDest(ctx, execCfg, details.SinkURI)
	if err != nil {
		log.Warningf(ctx, "failed to resolve destination details for change monitoring: %v", err)
	}

	for r := getRetry(ctx); r.Next(); {
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

			confPoller := make(chan struct{})
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				defer close(confPoller)
				return distChangefeedFlow(ctx, jobExec, jobID, details, description, localState, startedCh)
			})
			g.GoCtx(func(ctx context.Context) error {
				t := time.NewTicker(15 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-confPoller:
						return nil
					case <-t.C:
						newDest, err := reloadDest(ctx, jobID, execCfg)
						if err != nil {
							log.Warningf(ctx, "failed to check for updated configuration: %v", err)
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
				log.Infof(ctx, "restarting changefeed due to updated configuration")
				continue
			}

			if knobs != nil && knobs.HandleDistChangefeedError != nil {
				flowErr = knobs.HandleDistChangefeedError(flowErr)
			}
		}

		// Terminate changefeed if needed.
		if err := changefeedbase.AsTerminalError(ctx, jobExec.ExecCfg().LeaseManager, flowErr); err != nil {
			log.Infof(ctx, "CHANGEFEED %d shutting down (cause: %v)", jobID, err)
			// Best effort -- update job status to make it clear why changefeed shut down.
			// This won't always work if this node is being shutdown/drained.
			if ctx.Err() == nil {
				b.setJobStatusMessage(ctx, time.Time{}, "shutdown due to %s", err)
			}
			return err
		}

		// All other errors retry.
		log.Warningf(ctx, `Changefeed job %d encountered transient error: %v (attempt %d)`,
			jobID, flowErr, 1+r.CurrentAttempt())
		lastRunStatusUpdate = b.setJobStatusMessage(ctx, lastRunStatusUpdate, "transient error: %s", flowErr)

		if metrics, ok := execCfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics); ok {
			sli, err := metrics.getSLIMetrics(details.Opts[changefeedbase.OptMetricsScope])
			if err != nil {
				return err
			}
			sli.ErrorRetries.Inc(1)
		}

		if err := reconcileJobStateWithLocalState(ctx, jobID, localState, execCfg); err != nil {
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
				log.Warningf(ctx, "Changefeed %d delaying restart due to %d node(s) (%v) draining",
					jobID, len(localState.drainingNodes), localState.drainingNodes)
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

// reconcileJobStateWithLocalState ensures that the job progress information
// is consistent with the state present in the local state.
func reconcileJobStateWithLocalState(
	ctx context.Context, jobID jobspb.JobID, localState *cachedState, execCfg *sql.ExecutorConfig,
) error {
	// Re-load the job in order to update our progress object, which may have
	// been updated by the changeFrontier processor since the flow started.
	reloadedJob, reloadErr := execCfg.JobRegistry.LoadClaimedJob(ctx, jobID)
	if reloadErr != nil {
		log.Warningf(ctx, `CHANGEFEED job %d could not reload job progress (%s); `+
			`job should be retried later`, jobID, reloadErr)
		return reloadErr
	}
	knobs, _ := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs)
	if knobs != nil && knobs.LoadJobErr != nil {
		if err := knobs.LoadJobErr(); err != nil {
			return err
		}
	}

	localState.progress = reloadedJob.Progress()

	// localState contains an up-to-date checkpoint information transmitted by
	// aggregator when flow was terminated. To be safe, we don't blindly trust
	// local state; instead, this checkpoint is applied to the reloaded job
	// progress, and the resulting progress record persisted back to the jobs
	// table.
	var highWater hlc.Timestamp
	if hw := localState.progress.GetHighWater(); hw != nil {
		highWater = *hw
	}

	// Build frontier based on tracked spans.
	sf, err := span.MakeFrontierAt(highWater, localState.trackedSpans...)
	if err != nil {
		return err
	}
	// Advance frontier based on the information received from the aggregators.
	for _, s := range localState.aggregatorFrontier {
		_, err := sf.Forward(s.Span, s.Timestamp)
		if err != nil {
			return err
		}
	}

	maxBytes := changefeedbase.SpanCheckpointMaxBytes.Get(&execCfg.Settings.SV)
	checkpoint := checkpoint.Make(
		sf.Frontier(),
		func(forEachSpan span.Operation) {
			for _, fs := range localState.aggregatorFrontier {
				forEachSpan(fs.Span, fs.Timestamp)
			}
		},
		maxBytes,
		nil, /* metrics */
	)

	// Update checkpoint.
	updateHW := highWater.Less(sf.Frontier())
	updateSpanCheckpoint := !checkpoint.IsEmpty()

	if updateHW || updateSpanCheckpoint {
		if updateHW {
			localState.SetHighwater(sf.Frontier())
		}
		localState.SetCheckpoint(checkpoint)
		if log.V(1) {
			log.Infof(ctx, "Applying checkpoint to job record:  hw=%v, cf=%v",
				localState.progress.GetHighWater(), localState.progress.GetChangefeed())
		}
		return reloadedJob.NoTxn().Update(ctx,
			func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				if err := md.CheckRunningOrReverting(); err != nil {
					return err
				}
				ju.UpdateProgress(&localState.progress)
				return nil
			},
		)
	}

	return nil
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
		logChangefeedCanceledTelemetry(ctx, b.job)
	} else {
		telemetry.Count(`changefeed.enterprise.fail`)
		exec.ExecCfg().JobRegistry.MetricsStruct().Changefeed.(*Metrics).Failures.Inc(1)
		logChangefeedFailedTelemetry(ctx, b.job, changefeedbase.UnknownError)
	}
	return nil
}

// CollectProfile is part of the jobs.Resumer interface.
func (b *changefeedResumer) CollectProfile(_ context.Context, _ interface{}) error {
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
		changefeedEventDetails = makeCommonChangefeedEventDetails(ctx, changefeedDetails, jr.Description, jr.JobID)
	}

	createChangefeedEvent := &eventpb.CreateChangefeed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		Transformation:               isTransformation,
	}

	log.StructuredEvent(ctx, severity.INFO, createChangefeedEvent)
}

func logChangefeedFailedTelemetry(
	ctx context.Context, job *jobs.Job, failureType changefeedbase.FailureType,
) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if job != nil {
		changefeedDetails := job.Details().(jobspb.ChangefeedDetails)
		changefeedEventDetails = makeCommonChangefeedEventDetails(ctx, changefeedDetails, job.Payload().Description, job.ID())
	}

	changefeedFailedEvent := &eventpb.ChangefeedFailed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		FailureType:                  failureType,
	}

	log.StructuredEvent(ctx, severity.INFO, changefeedFailedEvent)
}

func logChangefeedFailedTelemetryDuringStartup(
	ctx context.Context, description string, failureType changefeedbase.FailureType,
) {
	changefeedFailedEvent := &eventpb.ChangefeedFailed{
		CommonChangefeedEventDetails: eventpb.CommonChangefeedEventDetails{Description: description},
		FailureType:                  failureType,
	}

	log.StructuredEvent(ctx, severity.INFO, changefeedFailedEvent)
}

func logChangefeedCanceledTelemetry(ctx context.Context, job *jobs.Job) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if job != nil {
		changefeedDetails := job.Details().(jobspb.ChangefeedDetails)
		changefeedEventDetails = makeCommonChangefeedEventDetails(ctx, changefeedDetails, job.Payload().Description, job.ID())
	}

	changefeedCanceled := &eventpb.ChangefeedCanceled{
		CommonChangefeedEventDetails: changefeedEventDetails,
	}
	log.StructuredEvent(ctx, severity.INFO, changefeedCanceled)
}

func makeCommonChangefeedEventDetails(
	ctx context.Context, details jobspb.ChangefeedDetails, description string, jobID jobspb.JobID,
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
		NumTables:   int32(AllTargets(details).Size),
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
