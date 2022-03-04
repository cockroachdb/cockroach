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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

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
	)
}

// changefeedPlanHook implements sql.PlanHookFn.
func changefeedPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	changefeedStmt, ok := stmt.(*tree.CreateChangefeed)
	if !ok {
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

	//TODO: We're passing around the full output of optsFn a lot, make it a type.
	optsFn, err := p.TypeAsStringOpts(ctx, changefeedStmt.Options, changefeedbase.ChangefeedOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if err := validateSettings(ctx, p); err != nil {
			return err
		}

		sinkURI, err := sinkURIFn()
		if err != nil {
			return err
		}

		if !unspecifiedSink && sinkURI == `` {
			// Error if someone specifies an INTO with the empty string. We've
			// already sent the wrong result column headers.
			return errors.New(`omit the SINK clause for inline results`)
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

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
			return err
		}

		details := jr.Details.(jobspb.ChangefeedDetails)
		progress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{},
			},
		}

		if details.SinkURI == `` {
			telemetry.Count(`changefeed.create.core`)
			err := distChangefeedFlow(ctx, p, 0 /* jobID */, details, progress, resultsCh)
			if err != nil {
				telemetry.Count(`changefeed.core.error`)
			}
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
			var protectedTimestampID uuid.UUID
			codec := p.ExecCfg().Codec
			if shouldProtectTimestamps(codec) {
				ptr = createProtectedTimestampRecord(ctx, codec, jobID, AllTargets(details), details.StatementTime, progress.GetChangefeed())
				protectedTimestampID = ptr.ID.GetUUID()
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
			// If we created a protected timestamp for an initial scan, verify it.
			// Doing this synchronously here rather than asynchronously later provides
			// a nice UX win in the case that the data isn't actually available.
			if protectedTimestampID != uuid.Nil {
				if err := p.ExecCfg().ProtectedTimestampProvider.Verify(ctx, protectedTimestampID); err != nil {
					if cancelErr := sj.Cancel(ctx); cancelErr != nil {
						if ctx.Err() == nil {
							log.Warningf(ctx, "failed to cancel job: %v", cancelErr)
						}
					}
					return err
				}
			}
		}

		// Start the job.
		if err := sj.Start(ctx); err != nil {
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
	return fn, header, nil, avoidBuffering, nil
}

func createChangefeedJobRecord(
	ctx context.Context,
	p sql.PlanHookState,
	changefeedStmt *tree.CreateChangefeed,
	sinkURI string,
	opts map[string]string,
	jobID jobspb.JobID,
	telemetryPath string,
) (*jobs.Record, error) {
	unspecifiedSink := changefeedStmt.SinkURI == nil

	for key, value := range opts {
		// if option is case insensitive then convert its value to lower case
		if _, ok := changefeedbase.CaseInsensitiveOpts[key]; ok {
			opts[key] = strings.ToLower(value)
		}
	}

	if newFormat, ok := changefeedbase.NoLongerExperimental[opts[changefeedbase.OptFormat]]; ok {
		p.BufferClientNotice(ctx, pgnotice.Newf(
			`%[1]s is no longer experimental, use %[2]s=%[1]s`,
			newFormat, changefeedbase.OptFormat),
		)
		// Still serialize the experimental_ form for backwards compatibility
	}

	jobDescription, err := changefeedJobDescription(p, changefeedStmt, sinkURI, opts)
	if err != nil {
		return nil, err
	}

	statementTime := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}
	var initialHighWater hlc.Timestamp
	if cursor, ok := opts[changefeedbase.OptCursor]; ok {
		asOfClause := tree.AsOfClause{Expr: tree.NewStrVal(cursor)}
		var err error
		asOf, err := p.EvalAsOfTimestamp(ctx, asOfClause)
		if err != nil {
			return nil, err
		}
		initialHighWater = asOf.Timestamp
		statementTime = initialHighWater
	}

	// This grabs table descriptors once to get their ids.
	targetDescs, err := getTableDescriptors(ctx, p, &changefeedStmt.Targets, statementTime, initialHighWater)
	if err != nil {
		return nil, err
	}

	targets, tables, err := getTargetsAndTables(ctx, p, targetDescs, opts)
	if err != nil {
		return nil, err
	}

	details := jobspb.ChangefeedDetails{
		Tables:               tables,
		Opts:                 opts,
		SinkURI:              sinkURI,
		StatementTime:        statementTime,
		TargetSpecifications: targets,
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

	if details, err = validateDetails(details); err != nil {
		return nil, err
	}

	if _, err := getEncoder(details.Opts, AllTargets(details)); err != nil {
		return nil, err
	}

	if isCloudStorageSink(parsedSink) || isWebhookSink(parsedSink) {
		details.Opts[changefeedbase.OptKeyInValue] = ``
	}
	if isWebhookSink(parsedSink) {
		details.Opts[changefeedbase.OptTopicInValue] = ``
	}

	if !unspecifiedSink && p.ExecCfg().ExternalIODirConfig.DisableOutbound {
		return nil, errors.Errorf("Outbound IO is disabled by configuration, cannot create changefeed into %s", parsedSink.Scheme)
	}

	if _, shouldProtect := details.Opts[changefeedbase.OptProtectDataFromGCOnPause]; shouldProtect && !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, errorutil.UnsupportedWithMultiTenancy(67271)
	}

	if telemetryPath != `` {
		// Feature telemetry
		telemetrySink := parsedSink.Scheme
		if telemetrySink == `` {
			telemetrySink = `sinkless`
		}
		telemetry.Count(telemetryPath + `.sink.` + telemetrySink)
		telemetry.Count(telemetryPath + `.format.` + details.Opts[changefeedbase.OptFormat])
		telemetry.CountBucketed(telemetryPath+`.num_tables`, int64(len(tables)))
	}

	if scope, ok := opts[changefeedbase.OptMetricsScope]; ok {
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "CHANGEFEED",
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
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "CHANGEFEED",
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

	ok, err := p.HasRoleOption(ctx, roleoption.CONTROLCHANGEFEED)
	if err != nil {
		return err
	}
	if !ok {
		return pgerror.New(pgcode.InsufficientPrivilege, "current user must have a role WITH CONTROLCHANGEFEED")
	}

	return nil
}

func getTableDescriptors(
	ctx context.Context,
	p sql.PlanHookState,
	targets *tree.TargetList,
	statementTime hlc.Timestamp,
	initialHighWater hlc.Timestamp,
) ([]catalog.Descriptor, error) {
	// For now, disallow targeting a database or wildcard table selection.
	// Getting it right as tables enter and leave the set over time is
	// tricky.
	if len(targets.Databases) > 0 {
		return nil, errors.Errorf(`CHANGEFEED cannot target %s`,
			tree.AsString(targets))
	}
	for _, t := range targets.Tables {
		p, err := t.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		if _, ok := p.(*tree.TableName); !ok {
			return nil, errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(t))
		}
	}

	// This grabs table descriptors once to get their ids.
	targetDescs, _, err := backupresolver.ResolveTargetsToDescriptors(
		ctx, p, statementTime, targets)
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
	targetDescs []catalog.Descriptor,
	opts map[string]string,
) ([]jobspb.ChangefeedTargetSpecification, jobspb.ChangefeedTargets, error) {
	tables := make(jobspb.ChangefeedTargets, len(targetDescs))
	targets := make([]jobspb.ChangefeedTargetSpecification, len(targetDescs))
	for _, desc := range targetDescs {
		if table, isTable := desc.(catalog.TableDescriptor); isTable {
			if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
				return nil, nil, err
			}
			_, qualified := opts[changefeedbase.OptFullTableName]
			name, err := getChangefeedTargetName(ctx, table, p.ExecCfg(), p.ExtendedEvalContext().Txn, qualified)
			if err != nil {
				return nil, nil, err
			}
			tables[table.GetID()] = jobspb.ChangefeedTargetTable{
				StatementTimeName: name,
			}
			typ := jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY
			if len(table.GetFamilies()) > 1 {
				typ = jobspb.ChangefeedTargetSpecification_EACH_FAMILY
			}
			ts := jobspb.ChangefeedTargetSpecification{
				Type:    typ,
				TableID: table.GetID(),
			}
			targets = append(targets, ts)
			if err := changefeedbase.ValidateTable(targets, table, opts); err != nil {
				return nil, nil, err
			}
			for _, warning := range changefeedbase.WarningsForTable(tables, table, opts) {
				p.BufferClientNotice(ctx, pgnotice.Newf("%s", warning))
			}
		}
	}
	return targets, tables, nil
}

func validateSink(
	ctx context.Context,
	p sql.PlanHookState,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	opts map[string]string,
) error {
	metrics := p.ExecCfg().JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	sli, err := metrics.getSLIMetrics(opts[changefeedbase.OptMetricsScope])
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
		topics := sink.Topics()
		for _, topic := range topics {
			p.BufferClientNotice(ctx, pgnotice.Newf(`changefeed will emit to topic %s`, topic))
		}
		details.Opts[changefeedbase.Topics] = strings.Join(topics, ",")
	}
	return nil
}

func changefeedJobDescription(
	p sql.PlanHookState, changefeed *tree.CreateChangefeed, sinkURI string, opts map[string]string,
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
	}
	for k, v := range opts {
		if k == changefeedbase.OptWebhookAuthHeader {
			v = redactWebhookAuthHeader(v)
		}
		opt := tree.KVOption{Key: tree.Name(k)}
		if len(v) > 0 {
			opt.Value = tree.NewDString(v)
		}
		c.Options = append(c.Options, opt)
	}
	sort.Slice(c.Options, func(i, j int) bool { return c.Options[i].Key < c.Options[j].Key })
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(c, ann), nil
}

func redactUser(uri string) string {
	u, _ := url.Parse(uri)
	if u.User != nil {
		u.User = url.User(`redacted`)
	}
	return u.String()
}

// validateNonNegativeDuration returns a nil error if optValue can be
// parsed as a duration and is non-negative; otherwise, an error is
// returned.
func validateNonNegativeDuration(optName string, optValue string) error {
	if d, err := time.ParseDuration(optValue); err != nil {
		return err
	} else if d < 0 {
		return errors.Errorf("negative durations are not accepted: %s='%s'", optName, optValue)
	}
	return nil
}

func validateDetails(details jobspb.ChangefeedDetails) (jobspb.ChangefeedDetails, error) {
	if details.Opts == nil {
		// The proto MarshalTo method omits the Opts field if the map is empty.
		// So, if no options were specified by the user, Opts will be nil when
		// the job gets restarted.
		details.Opts = map[string]string{}
	}
	{
		const opt = changefeedbase.OptResolvedTimestamps
		if o, ok := details.Opts[opt]; ok && o != `` {
			if err := validateNonNegativeDuration(opt, o); err != nil {
				return jobspb.ChangefeedDetails{}, err
			}
		}
	}
	{
		const opt = changefeedbase.OptMinCheckpointFrequency
		if o, ok := details.Opts[opt]; ok && o != `` {
			if err := validateNonNegativeDuration(opt, o); err != nil {
				return jobspb.ChangefeedDetails{}, err
			}
		}
	}
	{
		const opt = changefeedbase.OptSchemaChangeEvents
		switch v := changefeedbase.SchemaChangeEventClass(details.Opts[opt]); v {
		case ``, changefeedbase.OptSchemaChangeEventClassDefault:
			details.Opts[opt] = string(changefeedbase.OptSchemaChangeEventClassDefault)
		case changefeedbase.OptSchemaChangeEventClassColumnChange:
			// No-op
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}
	{
		const opt = changefeedbase.OptSchemaChangePolicy
		switch v := changefeedbase.SchemaChangePolicy(details.Opts[opt]); v {
		case ``, changefeedbase.OptSchemaChangePolicyBackfill:
			details.Opts[opt] = string(changefeedbase.OptSchemaChangePolicyBackfill)
		case changefeedbase.OptSchemaChangePolicyNoBackfill:
			// No-op
		case changefeedbase.OptSchemaChangePolicyStop:
			// No-op
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}
	{
		_, withInitialScan := details.Opts[changefeedbase.OptInitialScan]
		_, noInitialScan := details.Opts[changefeedbase.OptNoInitialScan]
		if withInitialScan && noInitialScan {
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`cannot specify both %s and %s`, changefeedbase.OptInitialScan,
				changefeedbase.OptNoInitialScan)
		}
	}
	{
		const opt = changefeedbase.OptEnvelope
		switch v := changefeedbase.EnvelopeType(details.Opts[opt]); v {
		case changefeedbase.OptEnvelopeRow, changefeedbase.OptEnvelopeDeprecatedRow:
			details.Opts[opt] = string(changefeedbase.OptEnvelopeRow)
		case changefeedbase.OptEnvelopeKeyOnly:
			details.Opts[opt] = string(changefeedbase.OptEnvelopeKeyOnly)
		case ``, changefeedbase.OptEnvelopeWrapped:
			details.Opts[opt] = string(changefeedbase.OptEnvelopeWrapped)
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}
	{
		const opt = changefeedbase.OptFormat
		switch v := changefeedbase.FormatType(details.Opts[opt]); v {
		case ``, changefeedbase.OptFormatJSON:
			details.Opts[opt] = string(changefeedbase.OptFormatJSON)
		case changefeedbase.OptFormatAvro, changefeedbase.DeprecatedOptFormatAvro:
			// No-op.
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}
	{
		const opt = changefeedbase.OptOnError
		switch v := changefeedbase.OnErrorType(details.Opts[opt]); v {
		case ``, changefeedbase.OptOnErrorFail:
			details.Opts[opt] = string(changefeedbase.OptOnErrorFail)
		case changefeedbase.OptOnErrorPause:
			// No-op.
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s, valid values are '%s' and '%s'`, opt, v,
				changefeedbase.OptOnErrorPause,
				changefeedbase.OptOnErrorFail)
		}
	}
	{
		const opt = changefeedbase.OptVirtualColumns
		switch v := changefeedbase.VirtualColumnVisibility(details.Opts[opt]); v {
		case ``, changefeedbase.OptVirtualColumnsOmitted:
			details.Opts[opt] = string(changefeedbase.OptVirtualColumnsOmitted)
		case changefeedbase.OptVirtualColumnsNull:
			details.Opts[opt] = string(changefeedbase.OptVirtualColumnsNull)
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}
	return details, nil
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
	switch onError := changefeedbase.OnErrorType(details.Opts[changefeedbase.OptOnError]); onError {
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
		return b.job.PauseRequested(ctx, jobExec.ExtendedEvalContext().Txn, func(ctx context.Context,
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
		return errors.Errorf("unrecognized option value: %s=%s",
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
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
	}
	var err error
	var lastRunStatusUpdate time.Time

	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
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

		if !changefeedbase.IsRetryableError(err) {
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
				lastRunStatusUpdate = b.setJobRunningStatus(ctx, lastRunStatusUpdate, "retryable flow error: %s", err)
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
func (b *changefeedResumer) OnFailOrCancel(ctx context.Context, jobExec interface{}) error {
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
	}
	return nil
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor.
func getQualifiedTableName(
	ctx context.Context, execCfg *sql.ExecutorConfig, txn *kv.Txn, desc catalog.TableDescriptor,
) (string, error) {
	col := execCfg.CollectionFactory.MakeCollection(ctx, nil /* TemporarySchemaProvider */)
	dbDesc, err := col.Direct().MustGetDatabaseDescByID(ctx, txn, desc.GetParentID())
	if err != nil {
		return "", err
	}
	schemaID := desc.GetParentSchemaID()
	schemaName, err := resolver.ResolveSchemaNameByID(ctx, txn, execCfg.Codec, dbDesc, schemaID, execCfg.Settings.Version)
	if err != nil {
		return "", err
	}
	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		tree.Name(schemaName),
		tree.Name(desc.GetName()),
	)
	return tbName.String(), nil
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

// AllTargets gets all the targets listed in a ChangefeedDetails,
// from the statement time name map in old protos
// or the TargetSpecifications in new ones.
func AllTargets(cd jobspb.ChangefeedDetails) (targets []jobspb.ChangefeedTargetSpecification) {
	//TODO: Use a version gate for this once we have CDC version gates
	if len(cd.TargetSpecifications) > 0 {
		for _, ts := range cd.TargetSpecifications {
			if ts.TableID > 0 {
				ts.StatementTimeName = cd.Tables[ts.TableID].StatementTimeName
				targets = append(targets, ts)
			}
		}
	} else {
		for id, t := range cd.Tables {
			ct := jobspb.ChangefeedTargetSpecification{
				Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				TableID:           id,
				StatementTimeName: t.StatementTimeName,
			}
			targets = append(targets, ct)
		}
	}
	return
}
