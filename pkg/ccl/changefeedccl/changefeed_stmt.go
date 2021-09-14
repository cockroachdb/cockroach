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
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// featureChangefeedEnabled is used to enable and disable the CHANGEFEED feature.
var featureChangefeedEnabled = settings.RegisterBoolSetting(
	"feature.changefeed.enabled",
	"set to true to enable changefeeds, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

func init() {
	sql.AddPlanHook(changefeedPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeChangefeed,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &changefeedResumer{job: job}
		},
	)
}

// annotatedChangefeedStatement is a tree.CreateChangefeed used for export, optionally
// annotated with the scheduling information.
type annotatedChangefeedStatement struct {
	*tree.CreateChangefeed
	*jobs.CreatedByInfo
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

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureChangefeedEnabled,
		"CHANGEFEED",
	); err != nil {
		return nil, nil, nil, false, err
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

	optsFn, err := p.TypeAsStringOpts(ctx, changefeedStmt.Options, changefeedbase.ChangefeedOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		ok, err := p.HasRoleOption(ctx, roleoption.CONTROLCHANGEFEED)
		if err != nil {
			return err
		}
		if !ok {
			return pgerror.New(pgcode.InsufficientPrivilege, "permission denied to create changefeed")
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

		jobDescription, err := changefeedJobDescription(p, changefeedStmt.CreateChangefeed, sinkURI, opts)
		if err != nil {
			return err
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
				return err
			}
			initialHighWater = asOf.Timestamp
			statementTime = initialHighWater
		}

		if err := verifyChangefeedTargets(changefeedStmt.Targets); err != nil {
			return err
		}

		// Resolve statement targets to the ChangefeedTargets.
		// This grabs table descriptors once to get their ids.
		_, wantFullyQualified := opts[changefeedbase.OptFullTableName]
		targets := make(jobspb.ChangefeedTargets)
		var targetDescIDs []descpb.ID
		if err := resolveChangefeedTargets(
			ctx, p, changefeedStmt.Targets, statementTime,
			func(table catalog.TableDescriptor) error {
				name := table.GetName()
				if wantFullyQualified {
					qualName, err := getQualifiedTableName(ctx, *p.ExecCfg(), p.ExtendedEvalContext().Txn, table)
					if err != nil {
						return err
					}
					name = qualName.String()
				}

				targets[table.GetID()] = jobspb.ChangefeedTarget{
					StatementTimeName: name,
				}
				targetDescIDs = append(targetDescIDs, table.GetID())
				return nil
			}); err != nil {
			err = errors.Wrap(err, "failed to resolve targets in the CHANGEFEED stmt")
			if !initialHighWater.IsEmpty() {
				// We specified cursor -- it is possible the targets do not exist at that time.
				// Give a bit more context in the error message.
				err = errors.WithHintf(err,
					"do the targets exist at the specified cursor time %s?", initialHighWater)
			}
			return err
		}

		// TODO(yevgeniy): Validate options when running in export mode; most of the
		// options dealing with cursor/initial scan, etc are not applicable.
		isRunningExport := false
		if export := changefeedStmt.ExportSpec; export != nil {
			isRunningExport = true

			if export.AsOf.Expr != nil {
				// If asOf clause specified, evaluate that and use that timestamp
				// as the statement time.  This will cause the table to be backfilled,
				// up to that time.
				var err error
				asOf, err := p.EvalAsOfTimestamp(ctx, export.AsOf)
				if err != nil {
					return err
				}
				statementTime = asOf.Timestamp
			}
		}

		details := jobspb.ChangefeedDetails{
			Targets:       targets,
			Opts:          opts,
			SinkURI:       sinkURI,
			StatementTime: statementTime,
			ExportMode:    isRunningExport,
		}
		progress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{},
			},
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
			return err
		}
		if newScheme, ok := changefeedbase.NoLongerExperimental[parsedSink.Scheme]; ok {
			parsedSink.Scheme = newScheme // This gets munged anyway when building the sink
			p.BufferClientNotice(ctx, pgnotice.Newf(`%[1]s is no longer experimental, use %[1]s://`,
				newScheme),
			)
		}

		if details, err = validateDetails(details); err != nil {
			return err
		}

		if _, err := getEncoder(ctx, details.Opts, details.Targets); err != nil {
			return err
		}
		if isCloudStorageSink(parsedSink) || isWebhookSink(parsedSink) {
			details.Opts[changefeedbase.OptKeyInValue] = ``
		}
		if isWebhookSink(parsedSink) {
			details.Opts[changefeedbase.OptTopicInValue] = ``
		}

		if !unspecifiedSink && p.ExecCfg().ExternalIODirConfig.DisableOutbound {
			return errors.Errorf("Outbound IO is disabled by configuration, cannot create changefeed into %s", parsedSink.Scheme)
		}

		if _, shouldProtect := details.Opts[changefeedbase.OptProtectDataFromGCOnPause]; shouldProtect && !p.ExecCfg().Codec.ForSystemTenant() {
			return errorutil.UnsupportedWithMultiTenancy(67271)
		}

		// Feature telemetry
		telemetrySink := parsedSink.Scheme
		if telemetrySink == `` {
			telemetrySink = `sinkless`
		}
		telemetry.Count(`changefeed.create.sink.` + telemetrySink)
		telemetry.Count(`changefeed.create.format.` + details.Opts[changefeedbase.OptFormat])
		telemetry.CountBucketed(`changefeed.create.num_tables`, int64(len(targets)))

		if details.SinkURI == `` {
			telemetry.Count(`changefeed.create.core`)
			err := distChangefeedFlow(ctx, p, 0 /* jobID */, details, progress, resultsCh)
			if err != nil {
				telemetry.Count(`changefeed.core.error`)
			}
			return changefeedbase.MaybeStripRetryableErrorMarker(err)
		}

		// Changefeeds are based on the Rangefeed abstraction, which requires the
		// `kv.rangefeed.enabled` setting to be true.
		if !kvserver.RangefeedEnabled.Get(&p.ExecCfg().Settings.SV) {
			return errors.Errorf("rangefeeds require the kv.rangefeed.enabled setting. See %s",
				docs.URL(`change-data-capture.html#enable-rangefeeds-to-reduce-latency`))
		}
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "CHANGEFEED",
		); err != nil {
			return err
		}

		telemetry.Count(`changefeed.create.enterprise`)

		// In the case where a user is executing a CREATE CHANGEFEED and is still
		// waiting for the statement to return, we take the opportunity to ensure
		// that the user has not made any obvious errors when specifying the sink in
		// the CREATE CHANGEFEED statement. To do this, we create a "canary" sink,
		// which will be immediately closed, only to check for errors.
		{
			var nilOracle timestampLowerBoundOracle
			canarySink, err := getSink(
				ctx, &p.ExecCfg().DistSQLSrv.ServerConfig, details, nilOracle, p.User(), jobspb.InvalidJobID,
			)
			if err != nil {
				return changefeedbase.MaybeStripRetryableErrorMarker(err)
			}
			if err := canarySink.Close(); err != nil {
				return err
			}
		}

		// The below block creates the job and if there's an initial scan, protects
		// the data required for that scan. We protect the data here rather than in
		// Resume to shorten the window that data may be GC'd. The protected
		// timestamps are removed and created during the execution of the changefeed
		// by the changeFrontier when checkpointing progress. Additionally protected
		// timestamps are removed in OnFailOrCancel. See the comment on
		// changeFrontier.manageProtectedTimestamps for more details on the handling of
		// protected timestamps.
		var sj *jobs.StartableJob
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		{

			var protectedTimestampID uuid.UUID
			var spansToProtect []roachpb.Span
			var ptr *ptpb.Record

			shouldProtectTimestamp := initialScanFromOptions(details.Opts) && p.ExecCfg().Codec.ForSystemTenant()
			if shouldProtectTimestamp {
				protectedTimestampID = uuid.MakeV4()
				spansToProtect = makeSpansToProtect(p.ExecCfg().Codec, details.Targets)
				progress.GetChangefeed().ProtectedTimestampRecord = protectedTimestampID
				ptr = jobsprotectedts.MakeRecord(protectedTimestampID, int64(jobID), statementTime,
					spansToProtect, jobsprotectedts.Jobs)
			}

			jr := jobs.Record{
				Description:   jobDescription,
				Username:      p.User(),
				DescriptorIDs: targetDescIDs,
				Details:       details,
				Progress:      *progress.GetChangefeed(),
				CreatedBy:     changefeedStmt.CreatedByInfo,
			}

			if changefeedStmt.CreatedByInfo != nil {
				// This changefeed statement invoked by the scheduler.  As such, the scheduler
				// must have specified transaction to use, and is responsible for committing
				// transaction.
				txn := p.ExtendedEvalContext().Txn
				_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, jr, jobID, txn)
				if err != nil {
					return err
				}

				if ptr != nil {
					if err := p.ExecCfg().ProtectedTimestampProvider.Protect(ctx, txn, ptr); err != nil {
						return err
					}
				}
				resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
				return nil
			}

			// TODO(yevgeniy): Use planner context instead of DB.Txn().
			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, jr); err != nil {
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

func changefeedJobDescription(
	p sql.PlanHookState, changefeed *tree.CreateChangefeed, sinkURI string, opts map[string]string,
) (string, error) {
	cleanedSinkURI, err := cloud.SanitizeExternalStorageURI(sinkURI, []string{changefeedbase.SinkParamSASLPassword})
	if err != nil {
		return "", err
	}
	c := &tree.CreateChangefeed{
		Targets:    changefeed.Targets,
		SinkURI:    tree.NewDString(cleanedSinkURI),
		ExportSpec: changefeed.ExportSpec,
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
	return details, nil
}

type changefeedResumer struct {
	job *jobs.Job
}

// generateChangefeedSessionID generates a unique string that is used to
// prevent overwriting of output files by the cloudStorageSink.
func generateChangefeedSessionID() string {
	// We read exactly 8 random bytes. 8 bytes should be enough because:
	// Consider that each new session for a changefeed job can occur at the
	// same highWater timestamp for its catch up scan. This session ID is
	// used to ensure that a session emitting files with the same timestamp
	// as the session before doesn't clobber existing files. Let's assume that
	// each of these runs for 0 seconds. Our node liveness duration is currently
	// 9 seconds, but let's go with a conservative duration of 1 second.
	// With 8 bytes using the rough approximation for the birthday problem
	// https://en.wikipedia.org/wiki/Birthday_problem#Square_approximation, we
	// will have a 50% chance of a single collision after sqrt(2^64) = 2^32
	// sessions. So if we start a new job every second, we get a coin flip chance of
	// single collision after 136 years. With this same approximation, we get
	// something like 220 days to have a 0.001% chance of a collision. In practice,
	// jobs are likely to run for longer and it's likely to take longer for
	// job adoption, so we should be good with 8 bytes. Similarly, it's clear that
	// 16 would be way overkill. 4 bytes gives us a 50% chance of collision after
	// 65K sessions at the same timestamp.
	const size = 8
	p := make([]byte, size)
	buf := make([]byte, hex.EncodedLen(size))
	rand.Read(p)
	hex.Encode(buf, p)
	return string(buf)
}

func (b *changefeedResumer) setJobRunningStatus(
	ctx context.Context, fmtOrMsg string, args ...interface{},
) {
	status := jobs.RunningStatus(fmt.Sprintf(fmtOrMsg, args...))
	if err := b.job.RunningStatus(ctx, nil,
		func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return status, nil
		},
	); err != nil {
		log.Warningf(ctx, "failed to set running status: %v", err)
	}
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

	return b.maybeNotifyScheduledJobCompletion(
		ctx, jobs.StatusSucceeded, execCtx.(sql.JobExecContext).ExecCfg())
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
				b.setJobRunningStatus(ctx, "retryable flow error: %s", err)
			}

			log.Warningf(ctx, `CHANGEFEED job %d returning with error: %+v`, jobID, err)
			return err
		}

		log.Warningf(ctx, `CHANGEFEED job %d encountered retryable error: %v`, jobID, err)
		b.setJobRunningStatus(ctx, "retryable error: %s", err)
		if metrics, ok := execCfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics); ok {
			metrics.ErrorRetries.Inc(1)
		}
		// Re-load the job in order to update our progress object, which may have
		// been updated by the changeFrontier processor since the flow started.
		reloadedJob, reloadErr := execCfg.JobRegistry.LoadJob(ctx, jobID)
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

func (b *changefeedResumer) maybeNotifyScheduledJobCompletion(
	ctx context.Context, jobStatus jobs.Status, exec *sql.ExecutorConfig,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := exec.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	err := exec.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// We cannot rely on b.job containing created_by_id because on job
		// resumption the registry does not populate the resumer's CreatedByInfo.
		datums, err := exec.InternalExecutor.QueryRowEx(
			ctx,
			"lookup-schedule-info",
			txn,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			fmt.Sprintf(
				"SELECT created_by_id FROM %s WHERE id=$1 AND created_by_type=$2",
				env.SystemJobsTableName()),
			b.job.ID(), jobs.CreatedByScheduledJobs)

		if err != nil {
			return errors.Wrap(err, "schedule info lookup")
		}
		if datums == nil {
			// Not a scheduled backup.
			return nil
		}

		scheduleID := int64(tree.MustBeDInt(datums[0]))
		if err := jobs.NotifyJobTermination(
			ctx, env, b.job.ID(), jobStatus, b.job.Details(), scheduleID, exec.InternalExecutor, txn); err != nil {
			return errors.Wrapf(err,
				"failed to notify schedule %d of completion of job %d", scheduleID, b.job.ID())
		}
		return nil
	})
	return err
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

	// This should never return an error unless resolving the schedule that the
	// job is being run under fails. This could happen if the schedule is dropped
	// while the job is executing.
	if err := b.maybeNotifyScheduledJobCompletion(ctx, jobs.StatusFailed,
		jobExec.(sql.JobExecContext).ExecCfg()); err != nil {
		log.Errorf(ctx, "failed to notify job %d on completion of OnFailOrCancel: %+v",
			b.job.ID(), err)
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
// paused, we want to install a protected timestamp at the most recent high
// watermark if there isn't already one.
func (b *changefeedResumer) OnPauseRequest(
	ctx context.Context, jobExec interface{}, txn *kv.Txn, progress *jobspb.Progress,
) error {
	details := b.job.Details().(jobspb.ChangefeedDetails)
	if _, shouldProtect := details.Opts[changefeedbase.OptProtectDataFromGCOnPause]; !shouldProtect {
		return nil
	}

	cp := progress.GetChangefeed()

	// If we already have a protected timestamp record, keep it where it is.
	if cp.ProtectedTimestampRecord != uuid.Nil {
		return nil
	}

	resolved := progress.GetHighWater()
	if resolved == nil {
		// This should only happen if the job was created in a version that did not
		// use protected timestamps but has yet to checkpoint its high water.
		// Changefeeds from older versions didn't get protected timestamps so it's
		// fine to not protect this one. In newer versions changefeeds which perform
		// an initial scan at the statement time (and don't have an initial high
		// water) will have a protected timestamp.
		return nil
	}

	execCfg := jobExec.(sql.JobExecContext).ExecCfg()
	pts := execCfg.ProtectedTimestampProvider
	return createProtectedTimestampRecord(ctx, execCfg.Codec, pts, txn, b.job.ID(),
		details.Targets, *resolved, cp)
}
