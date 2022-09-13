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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var changefeedRetryOptions = retry.Options{
	InitialBackoff: 5 * time.Millisecond,
	Multiplier:     2,
	MaxBackoff:     10 * time.Second,
}

// featureChangefeedEnabled is used to enable and disable the CHANGEFEED feature.
var featureChangefeedEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"feature.changefeed.enabled",
	"set to true to enable changefeeds, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

func init() {
	sql.AddPlanHook("changefeed", changefeedPlanHook)
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
	originalSpecs map[tree.ChangefeedTarget]jobspb.ChangefeedTargetSpecification
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

// changefeedPlanHook implements sql.PlanHookFn.
func changefeedPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	changefeedStmt := getChangefeedStatement(stmt)
	if changefeedStmt == nil {
		return nil, nil, nil, false, nil
	}

	var sinkURIFn func() (string, error)
	var header colinfo.ResultColumns
	unspecifiedSink := changefeedStmt.SinkURI == nil
	avoidBuffering := false

	if unspecifiedSink {
		// An unspecified sink triggers a fairly radical change in behavior.
		// Instead of setting up a system.job to emit to a sink in the
		// background and returning immediately with the job ID, the `CREATE
		// CHANGEFEED` blocks forever and returns all changes as rows directly
		// over pgwire. The types of these rows are `(topic STRING, key BYTES,
		// value BYTES)` and they correspond exactly to what would be emitted to
		// a sink.
		sinkURIFn = func() (string, error) { return ``, nil }
		header = colinfo.ResultColumns{
			{Name: "table", Typ: types.String},
			{Name: "key", Typ: types.Bytes},
			{Name: "value", Typ: types.Bytes},
		}
		avoidBuffering = true
	} else {
		var err error
		sinkURIFn, err = p.TypeAsString(ctx, changefeedStmt.SinkURI, `CREATE CHANGEFEED`)
		if err != nil {
			return nil, nil, nil, false, err
		}
		header = colinfo.ResultColumns{
			{Name: "job_id", Typ: types.Int},
		}
	}

	optsFn, err := p.TypeAsStringOpts(ctx, changefeedStmt.Options, changefeedvalidators.CreateOptionValidations)
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

		sinkURI, err := sinkURIFn()
		if err != nil {
			return changefeedbase.MarkTaggedError(err, changefeedbase.UserInput)
		}

		if !unspecifiedSink && sinkURI == `` {
			// Error if someone specifies an INTO with the empty string. We've
			// already sent the wrong result column headers.
			return errors.New(`omit the SINK clause for inline results`)
		}

		rawOpts, err := optsFn()
		if err != nil {
			return err
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
			logChangefeedCreateTelemetry(ctx, jr)

			var err error
			for r := retry.StartWithCtx(ctx, changefeedRetryOptions); r.Next(); {
				if err = distChangefeedFlow(ctx, p, 0 /* jobID */, details, progress, resultsCh); err == nil {
					return nil
				}

				if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok {
					if knobs != nil && knobs.HandleDistChangefeedError != nil {
						err = knobs.HandleDistChangefeedError(err)
					}
				}

				if !changefeedbase.IsRetryableError(err) {
					log.Warningf(ctx, `CHANGEFEED returning with error: %+v`, err)
					return err
				}

				progress = p.ExtendedEvalContext().ChangefeedState.(*coreChangefeedProgress).progress
			}
			telemetry.Count(`changefeed.core.error`)
			return changefeedbase.MaybeStripRetryableErrorMarker(err)
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

			activeTimestampProtection := changefeedbase.ActiveProtectedTimestampsEnabled.Get(&p.ExecCfg().Settings.SV)
			initialScanType, err := opts.GetInitialScanType()
			if err != nil {
				return err
			}
			shouldProtectTimestamp := activeTimestampProtection || (initialScanType != changefeedbase.NoInitialScan)
			if shouldProtectTimestamp {
				ptr = createProtectedTimestampRecord(ctx, codec, jobID, AllTargets(details), details.StatementTime, progress.GetChangefeed())
			}

			jr.Progress = *progress.GetChangefeed()

			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, *jr); err != nil {
					return err
				}
				if ptr != nil {
					return p.ExecCfg().ProtectedTimestampProvider.Protect(ctx, txn, ptr)
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

		logChangefeedCreateTelemetry(ctx, jr)

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

	jobDescription, err := changefeedJobDescription(changefeedStmt.CreateChangefeed, sinkURI, opts)
	if err != nil {
		return nil, err
	}

	statementTime := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}
	var initialHighWater hlc.Timestamp
	evalTimestamp := func(s string) (hlc.Timestamp, error) {
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

	targets, tables, err := getTargetsAndTables(ctx, p, targetDescs, changefeedStmt.Targets, changefeedStmt.originalSpecs, opts.ShouldUseFullStatementTimeName())
	if err != nil {
		return nil, err
	}
	tolerances := opts.GetCanHandle()
	details := jobspb.ChangefeedDetails{
		Tables:               tables,
		SinkURI:              sinkURI,
		StatementTime:        statementTime,
		EndTime:              endTime,
		TargetSpecifications: targets,
	}
	specs := AllTargets(details)
	for _, desc := range targetDescs {
		if table, isTable := desc.(catalog.TableDescriptor); isTable {
			if err := changefeedvalidators.ValidateTable(specs, table, tolerances); err != nil {
				return nil, err
			}
			for _, warning := range changefeedvalidators.WarningsForTable(table, tolerances) {
				p.BufferClientNotice(ctx, pgnotice.Newf("%s", warning))
			}
		}
	}

	if changefeedStmt.Select != nil {
		// Serialize changefeed expression.
		normalized, _, err := validateAndNormalizeChangefeedExpression(
			ctx, p, changefeedStmt.Select, targetDescs, targets, opts.IncludeVirtual(), opts.IsSet(changefeedbase.OptSplitColumnFamilies),
		)
		if err != nil {
			return nil, err
		}
		needDiff, err := cdceval.SelectClauseRequiresPrev(ctx, *p.SemaCtx(), normalized)
		if err != nil {
			return nil, err
		}
		if needDiff {
			opts.ForceDiff()
		}
		// TODO: Set the default envelope to row here when using a sink and format
		// that support it.
		details.Select = cdceval.AsStringUnredacted(normalized.Clause())

		// TODO(#85143): do not enforce schema_change_policy='stop' for changefeed expressions.
		schemachangeOptions, err := opts.GetSchemaChangeHandlingOptions()
		if err != nil {
			return nil, err
		}
		if schemachangeOptions.Policy != changefeedbase.OptSchemaChangePolicyStop {
			return nil, errors.Errorf(`using "AS SELECT" requires option schema_change_policy='stop'`)
		}
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

	//	 The changefeed is opted in to `OptKeyInValue` for any cloud
	//   storage sink or webhook sink. Kafka etc have a key and value field in
	//   each message but cloud storage sinks and webhook sinks don't have
	//   anywhere to put the key. So if the key is not in the value, then for
	//   DELETEs there is no way to recover which key was deleted. We could make
	//   the user explicitly pass this option for every cloud storage sink/
	//   webhook sink and error if they don't, but that seems user-hostile for
	//   insufficient reason.
	//   This is the same for the topic and webhook sink, which uses
	//   `topic_in_value` to embed the topic in the value by default, since it
	//   has no other avenue to express the topic.
	//   If this results in an overall invalid encoding, we need to override
	//   the default error to avoid claiming the user set an option they didn't
	//   explicitly set. Fortunately we know the only way to cause this is to
	//   set envelope.
	if isCloudStorageSink(parsedSink) || isWebhookSink(parsedSink) {
		if err = opts.ForceKeyInValue(); err != nil {
			return nil, errors.Errorf(`this sink is incompatible with envelope=%s`, encodingOpts.Envelope)
		}
	}
	if isWebhookSink(parsedSink) {
		if err = opts.ForceTopicInValue(); err != nil {
			return nil, errors.Errorf(`this sink is incompatible with envelope=%s`, encodingOpts.Envelope)
		}
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
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), p.ExecCfg().Organization(), "CHANGEFEED",
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
	}

	details.Opts = opts.AsMap()

	if details.SinkURI == `` {
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
		p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), p.ExecCfg().Organization(), "CHANGEFEED",
	); err != nil {
		return nil, err
	}

	if telemetryPath != `` {
		telemetry.Count(telemetryPath + `.enterprise`)
	}

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

	jr := &jobs.Record{
		Description: jobDescription,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
			for _, desc := range targetDescs {
				sqlDescIDs = append(sqlDescIDs, desc.GetID())
			}
			return sqlDescIDs
		}(),
		Details: details,
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
) ([]jobspb.ChangefeedTargetSpecification, jobspb.ChangefeedTargets, error) {
	tables := make(jobspb.ChangefeedTargets, len(targetDescs))
	targets := make([]jobspb.ChangefeedTargetSpecification, len(rawTargets))
	seen := make(map[jobspb.ChangefeedTargetSpecification]tree.ChangefeedTarget)

	hasControlChangefeed, err := p.HasRoleOption(ctx, roleoption.CONTROLCHANGEFEED)
	if err != nil {
		return nil, nil, err
	}

	var requiredPrivilegePerTable privilege.Kind
	if hasControlChangefeed {
		requiredPrivilegePerTable = privilege.SELECT
	} else {
		requiredPrivilegePerTable = privilege.CHANGEFEED
	}

	for i, ct := range rawTargets {
		desc, ok := targetDescs[ct.TableName]
		if !ok {
			return nil, nil, errors.Newf("could not match %v to a fetched descriptor. Fetched were %v", ct.TableName, targetDescs)
		}
		td, ok := desc.(catalog.TableDescriptor)
		if !ok {
			return nil, nil, errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(&ct))
		}

		if err := p.CheckPrivilege(ctx, desc, requiredPrivilegePerTable); err != nil {
			return nil, nil, errors.WithHint(err, `Users with CONTROLCHANGEFEED need SELECT, other users need CHANGEFEED.`)
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
	canarySink, err := getSink(ctx, &p.ExecCfg().DistSQLSrv.ServerConfig, details,
		nilOracle, p.User(), jobID, sli)
	if err != nil {
		return changefeedbase.MaybeStripRetryableErrorMarker(err)
	}
	if err := canarySink.Close(); err != nil {
		return err
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
		details.Opts[changefeedbase.Topics] = strings.Join(topics, ",")
	}
	return nil
}

func changefeedJobDescription(
	changefeed *tree.CreateChangefeed, sinkURI string, opts changefeedbase.StatementOptions,
) (string, error) {
	cleanedSinkURI, err := cloud.SanitizeExternalStorageURI(sinkURI, []string{
		changefeedbase.SinkParamSASLPassword,
		changefeedbase.SinkParamCACert,
		changefeedbase.SinkParamClientCert,
	})

	if err != nil {
		return "", err
	}

	cleanedSinkURI = redactUser(cleanedSinkURI)

	c := &tree.CreateChangefeed{
		Targets: changefeed.Targets,
		SinkURI: tree.NewDString(cleanedSinkURI),
		Select:  changefeed.Select,
	}
	opts.ForEachWithRedaction(func(k string, v string) {
		opt := tree.KVOption{Key: tree.Name(k)}
		if len(v) > 0 {
			opt.Value = tree.NewDString(v)
		}
		c.Options = append(c.Options, opt)
	})
	sort.Slice(c.Options, func(i, j int) bool { return c.Options[i].Key < c.Options[j].Key })
	return tree.AsString(c), nil
}

func redactUser(uri string) string {
	u, _ := url.Parse(uri)
	if u.User != nil {
		u.User = url.User(`redacted`)
	}
	return u.String()
}

func validateDetailsAndOptions(
	details jobspb.ChangefeedDetails, opts changefeedbase.StatementOptions,
) error {
	if err := opts.ValidateForCreateChangefeed(); err != nil {
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
func validateAndNormalizeChangefeedExpression(
	ctx context.Context,
	execCtx sql.JobExecContext,
	sc *tree.SelectClause,
	descriptors map[tree.TablePattern]catalog.Descriptor,
	targets []jobspb.ChangefeedTargetSpecification,
	includeVirtual bool,
	splitColFams bool,
) (n cdceval.NormalizedSelectClause, target jobspb.ChangefeedTargetSpecification, _ error) {
	if len(descriptors) != 1 || len(targets) != 1 {
		return n, target, pgerror.Newf(pgcode.InvalidParameterValue, "CDC expressions require single table")
	}
	var tableDescr catalog.TableDescriptor
	for _, d := range descriptors {
		tableDescr = d.(catalog.TableDescriptor)
	}
	return cdceval.NormalizeAndValidateSelectForTarget(
		ctx, execCtx, tableDescr, targets[0], sc, includeVirtual, splitColFams)
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
	if err := b.job.RunningStatus(ctx, nil,
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
		return b.job.PauseRequested(ctx, jobExec.Txn(), func(ctx context.Context,
			planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress) error {
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
	var err error
	var lastRunStatusUpdate time.Time

	for r := retry.StartWithCtx(ctx, changefeedRetryOptions); r.Next(); {
		// startedCh is normally used to signal back to the creator of the job that
		// the job has started; however, in this case nothing will ever receive
		// on the channel, causing the changefeed flow to block. Replace it with
		// a dummy channel.
		startedCh := make(chan tree.Datums, 1)

		if err = distChangefeedFlow(ctx, jobExec, jobID, details, progress, startedCh); err == nil {
			return nil
		}

		if knobs, ok := execCfg.DistSQLSrv.TestingKnobs.Changefeed.(*TestingKnobs); ok {
			if knobs != nil && knobs.HandleDistChangefeedError != nil {
				err = knobs.HandleDistChangefeedError(err)
			}
		}

		// Retry changefeed if error is retryable.  In addition, we want to handle
		// context cancellation as retryable, but only if the resumer context has not been cancelled.
		// (resumer context is canceled by the jobs framework -- so we should respect it).
		isRetryableErr := changefeedbase.IsRetryableError(err) ||
			(ctx.Err() == nil && errors.Is(err, context.Canceled))
		if !isRetryableErr {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if flowinfra.IsFlowRetryableError(err) {
				// We don't want to retry flowinfra retryable error in the retry loop above.
				// This error currently indicates that this node is being drained.  As such,
				// retries will not help.
				// Instead, we want to make sure that the changefeed job is not marked failed
				// due to a transient, retryable error.
				err = jobs.MarkAsRetryJobError(err)
				_ = b.setJobRunningStatus(ctx, lastRunStatusUpdate, "retryable flow error: %s", err)
			}

			log.Warningf(ctx, `CHANGEFEED job %d returning with error: %+v`, jobID, err)
			return err
		}

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
		}
	}
	return errors.Wrap(err, `ran out of retries`)
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *changefeedResumer) OnFailOrCancel(
	ctx context.Context, jobExec interface{}, _ error,
) error {
	exec := jobExec.(sql.JobExecContext)
	execCfg := exec.ExecCfg()
	progress := b.job.Progress()
	b.maybeCleanUpProtectedTimestamp(ctx, execCfg.DB, execCfg.ProtectedTimestampProvider,
		progress.GetChangefeed().ProtectedTimestampRecord)

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
	ctx context.Context, db *kv.DB, pts protectedts.Storage, ptsID uuid.UUID,
) {
	if ptsID == uuid.Nil {
		return
	}
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return pts.Release(ctx, txn, ptsID)
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
	ctx context.Context, jobExec interface{}, txn *kv.Txn, progress *jobspb.Progress,
) error {
	details := b.job.Details().(jobspb.ChangefeedDetails)

	cp := progress.GetChangefeed()
	execCfg := jobExec.(sql.JobExecContext).ExecCfg()

	if _, shouldProtect := details.Opts[changefeedbase.OptProtectDataFromGCOnPause]; !shouldProtect {
		// Release existing pts record to avoid a single changefeed left on pause
		// resulting in storage issues
		if cp.ProtectedTimestampRecord != uuid.Nil {
			if err := execCfg.ProtectedTimestampProvider.Release(ctx, txn, cp.ProtectedTimestampRecord); err != nil {
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
		pts := execCfg.ProtectedTimestampProvider
		ptr := createProtectedTimestampRecord(ctx, execCfg.Codec, b.job.ID(), AllTargets(details), *resolved, cp)
		return pts.Protect(ctx, txn, ptr)
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
	col := execCfg.CollectionFactory.NewCollection(ctx, nil /* TemporarySchemaProvider */, nil /* monitor */)
	dbDesc, err := col.Direct().MustGetDatabaseDescByID(ctx, txn, desc.GetParentID())
	if err != nil {
		return tree.TableName{}, err
	}
	schemaID := desc.GetParentSchemaID()
	schemaName, err := resolver.ResolveSchemaNameByID(ctx, txn, execCfg.Codec, dbDesc, schemaID)
	if err != nil {
		return tree.TableName{}, err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		tree.Name(schemaName),
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

func logChangefeedCreateTelemetry(ctx context.Context, jr *jobs.Record) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if jr != nil {
		changefeedDetails := jr.Details.(jobspb.ChangefeedDetails)
		changefeedEventDetails = getCommonChangefeedEventDetails(ctx, changefeedDetails, jr.Description)
	}

	createChangefeedEvent := &eventpb.CreateChangefeed{
		CommonChangefeedEventDetails: changefeedEventDetails,
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
