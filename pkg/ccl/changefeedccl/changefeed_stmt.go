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
	"math/rand"
	"net/url"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func init() {
	sql.AddPlanHook(changefeedPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeChangefeed,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &changefeedResumer{job: job}
		},
	)
}

// changefeedPlanHook implements sql.PlanHookFn.
func changefeedPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	changefeedStmt, ok := stmt.(*tree.CreateChangefeed)
	if !ok {
		return nil, nil, nil, false, nil
	}

	var sinkURIFn func() (string, error)
	var header sqlbase.ResultColumns
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
		header = sqlbase.ResultColumns{
			{Name: "table", Typ: types.String},
			{Name: "key", Typ: types.Bytes},
			{Name: "value", Typ: types.Bytes},
		}
		avoidBuffering = true
	} else {
		var err error
		sinkURIFn, err = p.TypeAsString(changefeedStmt.SinkURI, `CREATE CHANGEFEED`)
		if err != nil {
			return nil, nil, nil, false, err
		}
		header = sqlbase.ResultColumns{
			{Name: "job_id", Typ: types.Int},
		}
	}

	optsFn, err := p.TypeAsStringOpts(changefeedStmt.Options, changefeedbase.ChangefeedOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := p.RequireAdminRole(ctx, "CREATE CHANGEFEED"); err != nil {
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

		jobDescription, err := changefeedJobDescription(p, changefeedStmt, sinkURI, opts)
		if err != nil {
			return err
		}

		statementTime := hlc.Timestamp{
			WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
		}
		var initialHighWater hlc.Timestamp
		if cursor, ok := opts[changefeedbase.OptCursor]; ok {
			asOf := tree.AsOfClause{Expr: tree.NewStrVal(cursor)}
			var err error
			if initialHighWater, err = p.EvalAsOfTimestamp(asOf); err != nil {
				return err
			}
			statementTime = initialHighWater
		}

		// For now, disallow targeting a database or wildcard table selection.
		// Getting it right as tables enter and leave the set over time is
		// tricky.
		if len(changefeedStmt.Targets.Databases) > 0 {
			return errors.Errorf(`CHANGEFEED cannot target %s`,
				tree.AsString(&changefeedStmt.Targets))
		}
		for _, t := range changefeedStmt.Targets.Tables {
			p, err := t.NormalizeTablePattern()
			if err != nil {
				return err
			}
			if _, ok := p.(*tree.TableName); !ok {
				return errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(t))
			}
		}

		// This grabs table descriptors once to get their ids.
		targetDescs, _, err := backupccl.ResolveTargetsToDescriptors(
			ctx, p, statementTime, changefeedStmt.Targets, tree.RequestedDescriptors)
		if err != nil {
			return err
		}
		targets := make(jobspb.ChangefeedTargets, len(targetDescs))
		for _, desc := range targetDescs {
			if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
				targets[tableDesc.ID] = jobspb.ChangefeedTarget{
					StatementTimeName: tableDesc.Name,
				}
				if err := validateChangefeedTable(targets, tableDesc); err != nil {
					return err
				}
			}
		}

		details := jobspb.ChangefeedDetails{
			Targets:       targets,
			Opts:          opts,
			SinkURI:       sinkURI,
			StatementTime: statementTime,
		}
		progress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{HighWater: &initialHighWater},
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
		//   storage sink. Kafka etc have a key and value field in each message but
		//   cloud storage sinks don't have anywhere to put the key. So if the key
		//   is not in the value, then for DELETEs there is no way to recover which
		//   key was deleted. We could make the user explicitly pass this option for
		//   every cloud storage sink and error if they don't, but that seems
		//   user-hostile for insufficient reason. We can't do this any earlier,
		//   because we might return errors about `key_in_value` being incompatible
		//   which is confusing when the user didn't type that option.
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
		if details, err = validateDetails(details); err != nil {
			return err
		}

		if _, err := getEncoder(details.Opts); err != nil {
			return err
		}
		if isCloudStorageSink(parsedSink) {
			details.Opts[changefeedbase.OptKeyInValue] = ``
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
			err := distChangefeedFlow(ctx, p, 0 /* jobID */, details, progress, resultsCh)
			return MaybeStripRetryableErrorMarker(err)
		}

		settings := p.ExecCfg().Settings
		if err := utilccl.CheckEnterpriseEnabled(
			settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "CHANGEFEED",
		); err != nil {
			return err
		}

		// In the case where a user is executing a CREATE CHANGEFEED and is still
		// waiting for the statement to return, we take the opportunity to ensure
		// that the user has not made any obvious errors when specifying the sink in
		// the CREATE CHANGEFEED statement. To do this, we create a "canary" sink,
		// which will be immediately closed, only to check for errors.
		{
			nodeID := p.ExtendedEvalContext().NodeID
			var nilOracle timestampLowerBoundOracle
			canarySink, err := getSink(
				ctx, details.SinkURI, nodeID, details.Opts, details.Targets,
				settings, nilOracle, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI,
			)
			if err != nil {
				return MaybeStripRetryableErrorMarker(err)
			}
			if err := canarySink.Close(); err != nil {
				return err
			}
		}

		// Make a channel for runChangefeedFlow to signal once everything has
		// been setup okay. This intentionally abuses what would normally be
		// hooked up to resultsCh to avoid a bunch of extra plumbing.
		startedCh := make(chan tree.Datums)
		job, errCh, err := p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, startedCh, jobs.Record{
			Description: jobDescription,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, desc := range targetDescs {
					sqlDescIDs = append(sqlDescIDs, desc.GetID())
				}
				return sqlDescIDs
			}(),
			Details:  details,
			Progress: *progress.GetChangefeed(),
		})
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-startedCh:
			// The feed set up without error, return control to the user.
		}

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
		}
		return nil
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
		Targets: changefeed.Targets,
		SinkURI: tree.NewDString(cleanedSinkURI),
	}
	for k, v := range opts {
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

func validateDetails(details jobspb.ChangefeedDetails) (jobspb.ChangefeedDetails, error) {
	if details.Opts == nil {
		// The proto MarshalTo method omits the Opts field if the map is empty.
		// So, if no options were specified by the user, Opts will be nil when
		// the job gets restarted.
		details.Opts = map[string]string{}
	}

	if r, ok := details.Opts[changefeedbase.OptResolvedTimestamps]; ok && r != `` {
		if d, err := time.ParseDuration(r); err != nil {
			return jobspb.ChangefeedDetails{}, err
		} else if d < 0 {
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`negative durations are not accepted: %s='%s'`,
				changefeedbase.OptResolvedTimestamps, details.Opts[changefeedbase.OptResolvedTimestamps])
		}
	}

	switch changefeedbase.EnvelopeType(details.Opts[changefeedbase.OptEnvelope]) {
	case changefeedbase.OptEnvelopeRow, changefeedbase.OptEnvelopeDeprecatedRow:
		details.Opts[changefeedbase.OptEnvelope] = string(changefeedbase.OptEnvelopeRow)
	case changefeedbase.OptEnvelopeKeyOnly:
		details.Opts[changefeedbase.OptEnvelope] = string(changefeedbase.OptEnvelopeKeyOnly)
	case ``, changefeedbase.OptEnvelopeWrapped:
		details.Opts[changefeedbase.OptEnvelope] = string(changefeedbase.OptEnvelopeWrapped)
	default:
		return jobspb.ChangefeedDetails{}, errors.Errorf(
			`unknown %s: %s`, changefeedbase.OptEnvelope, details.Opts[changefeedbase.OptEnvelope])
	}

	switch changefeedbase.FormatType(details.Opts[changefeedbase.OptFormat]) {
	case ``, changefeedbase.OptFormatJSON:
		details.Opts[changefeedbase.OptFormat] = string(changefeedbase.OptFormatJSON)
	case changefeedbase.OptFormatAvro:
		// No-op.
	default:
		return jobspb.ChangefeedDetails{}, errors.Errorf(
			`unknown %s: %s`, changefeedbase.OptFormat, details.Opts[changefeedbase.OptFormat])
	}

	return details, nil
}

func validateChangefeedTable(
	targets jobspb.ChangefeedTargets, tableDesc *sqlbase.TableDescriptor,
) error {
	t, ok := targets[tableDesc.ID]
	if !ok {
		return errors.Errorf(`unwatched table: %s`, tableDesc.Name)
	}

	// Technically, the only non-user table known not to work is system.jobs
	// (which creates a cycle since the resolved timestamp high-water mark is
	// saved in it), but there are subtle differences in the way many of them
	// work and this will be under-tested, so disallow them all until demand
	// dictates.
	if tableDesc.ID < keys.MinUserDescID {
		return errors.Errorf(`CHANGEFEEDs are not supported on system tables`)
	}
	if tableDesc.IsView() {
		return errors.Errorf(`CHANGEFEED cannot target views: %s`, tableDesc.Name)
	}
	if tableDesc.IsVirtualTable() {
		return errors.Errorf(`CHANGEFEED cannot target virtual tables: %s`, tableDesc.Name)
	}
	if tableDesc.IsSequence() {
		return errors.Errorf(`CHANGEFEED cannot target sequences: %s`, tableDesc.Name)
	}
	if len(tableDesc.Families) != 1 {
		return errors.Errorf(
			`CHANGEFEEDs are currently supported on tables with exactly 1 column family: %s has %d`,
			tableDesc.Name, len(tableDesc.Families))
	}

	if tableDesc.State == sqlbase.TableDescriptor_DROP {
		return errors.Errorf(`"%s" was dropped or truncated`, t.StatementTimeName)
	}
	if tableDesc.Name != t.StatementTimeName {
		return errors.Errorf(`"%s" was renamed to "%s"`, t.StatementTimeName, tableDesc.Name)
	}

	// TODO(mrtracy): re-enable this when allow-backfill option is added.
	// if tableDesc.HasColumnBackfillMutation() {
	// 	return errors.Errorf(`CHANGEFEEDs cannot operate on tables being backfilled`)
	// }

	return nil
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

// Resume is part of the jobs.Resumer interface.
func (b *changefeedResumer) Resume(
	ctx context.Context, planHookState interface{}, startedCh chan<- tree.Datums,
) error {
	phs := planHookState.(sql.PlanHookState)
	execCfg := phs.ExecCfg()
	jobID := *b.job.ID()
	details := b.job.Details().(jobspb.ChangefeedDetails)
	progress := b.job.Progress()

	// TODO(dan): This is a workaround for not being able to set an initial
	// progress high-water when creating a job (currently only the progress
	// details can be set). I didn't want to pick off the refactor to get this
	// fix in, but it'd be nice to remove this hack.
	if _, ok := details.Opts[changefeedbase.OptCursor]; ok {
		if h := progress.GetHighWater(); h == nil || *h == (hlc.Timestamp{}) {
			progress.Progress = &jobspb.Progress_HighWater{HighWater: &details.StatementTime}
		}
	}

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
		if err = distChangefeedFlow(ctx, phs, jobID, details, progress, startedCh); err == nil {
			return nil
		}
		if !IsRetryableError(err) {
			log.Warningf(ctx, `CHANGEFEED job %d returning with error: %+v`, jobID, err)
			return err
		}

		log.Warningf(ctx, `CHANGEFEED job %d encountered retryable error: %v`, jobID, err)
		if metrics, ok := execCfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics); ok {
			metrics.ErrorRetries.Inc(1)
		}
		// Re-load the job in order to update our progress object, which may have
		// been updated by the changeFrontier processor since the flow started.
		reloadedJob, reloadErr := execCfg.JobRegistry.LoadJob(ctx, jobID)
		if reloadErr != nil {
			log.Warningf(ctx, `CHANGEFEED job %d could not reload job progress; `+
				`continuing from last known high-water of %s: %v`,
				jobID, progress.GetHighWater(), reloadErr)
		} else {
			progress = reloadedJob.Progress()
		}

		// startedCh is normally used to signal back to the creator of the job that
		// the job has started; however, in this case nothing will ever receive
		// on the channel, causing the changefeed flow to block. Replace it with
		// a dummy channel.
		startedCh = make(chan tree.Datums, 1)
	}
	// We only hit this if `r.Next()` returns false, which right now only happens
	// on context cancellation.
	return errors.Wrap(err, `ran out of retries`)
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *changefeedResumer) OnFailOrCancel(context.Context, interface{}) error { return nil }

// OnSuccess is part of the jobs.Resumer interface.
func (b *changefeedResumer) OnSuccess(context.Context, *client.Txn) error { return nil }

// OnTerminal is part of the jobs.Resumer interface.
func (b *changefeedResumer) OnTerminal(context.Context, jobs.Status, chan<- tree.Datums) {}
