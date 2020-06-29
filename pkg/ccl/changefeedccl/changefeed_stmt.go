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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	crdberrors "github.com/cockroachdb/errors"
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
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
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
		sinkURIFn, err = p.TypeAsString(ctx, changefeedStmt.SinkURI, `CREATE CHANGEFEED`)
		if err != nil {
			return nil, nil, nil, false, err
		}
		header = sqlbase.ResultColumns{
			{Name: "job_id", Typ: types.Int},
		}
	}

	optsFn, err := p.TypeAsStringOpts(ctx, changefeedStmt.Options, changefeedbase.ChangefeedOptionExpectValues)
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
			if initialHighWater, err = p.EvalAsOfTimestamp(ctx, asOf); err != nil {
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
			nodeID, err := p.ExtendedEvalContext().NodeID.OptionalNodeIDErr(48274)
			if err != nil {
				return err
			}
			var nilOracle timestampLowerBoundOracle
			canarySink, err := getSink(
				ctx, details.SinkURI, nodeID, details.Opts, details.Targets,
				settings, nilOracle, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User(),
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

		// The below block creates the job and if there's an initial scan, protects
		// the data required for that scan. We protect the data here rather than in
		// Resume to shorten the window that data may be GC'd. The protected
		// timestamps are removed and created during the execution of the changefeed
		// by the changeFrontier when checkpointing progress. Additionally protected
		// timestamps are removed in OnFailOrCancel. See the comment on
		// changeFrontier.manageProtectedTimestamps for more details on the handling of
		// protected timestamps.
		var sj *jobs.StartableJob
		{
			var protectedTimestampID uuid.UUID
			var spansToProtect []roachpb.Span
			if hasInitialScan := initialScanFromOptions(details.Opts); hasInitialScan {
				protectedTimestampID = uuid.MakeV4()
				spansToProtect = makeSpansToProtect(details.Targets)
				progress.GetChangefeed().ProtectedTimestampRecord = protectedTimestampID
			}

			jr := jobs.Record{
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
			}
			createJobAndProtectedTS := func(ctx context.Context, txn *kv.Txn) (err error) {
				sj, err = p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, jr, txn, startedCh)
				if err != nil {
					return err
				}
				if protectedTimestampID == uuid.Nil {
					return nil
				}
				ptr := jobsprotectedts.MakeRecord(protectedTimestampID, *sj.ID(),
					statementTime, spansToProtect)
				return p.ExecCfg().ProtectedTimestampProvider.Protect(ctx, txn, ptr)
			}
			if err := p.ExecCfg().DB.Txn(ctx, createJobAndProtectedTS); err != nil {
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

		// Start the job and wait for it to signal on startedCh.
		errCh, err := sj.Start(ctx)
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
			tree.NewDInt(tree.DInt(*sj.ID())),
		}
		return nil
	}
	return fn, header, nil, avoidBuffering, nil
}

func changefeedJobDescription(
	p sql.PlanHookState, changefeed *tree.CreateChangefeed, sinkURI string, opts map[string]string,
) (string, error) {
	cleanedSinkURI, err := cloudimpl.SanitizeExternalStorageURI(sinkURI, []string{changefeedbase.SinkParamSASLPassword})
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
	{
		const opt = changefeedbase.OptResolvedTimestamps
		if o, ok := details.Opts[opt]; ok && o != `` {
			if d, err := time.ParseDuration(o); err != nil {
				return jobspb.ChangefeedDetails{}, err
			} else if d < 0 {
				return jobspb.ChangefeedDetails{}, errors.Errorf(
					`negative durations are not accepted: %s='%s'`, opt, o)
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
		case changefeedbase.OptFormatAvro:
			// No-op.
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
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
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if flowinfra.IsFlowRetryableError(err) {
				// We don't want to retry flowinfra retryable error in the retry loop above.
				// This error currently indicates that this node is being drained.  As such,
				// retries will not help.
				// Instead, we want to make sure that the changefeed job is not marked failed
				// due to a transient, retryable error.
				err = jobs.NewRetryJobError(fmt.Sprintf("retryable flow error: %+v", err))
			}

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
			if ctx.Err() != nil {
				return ctx.Err()
			}
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
func (b *changefeedResumer) OnFailOrCancel(ctx context.Context, planHookState interface{}) error {
	phs := planHookState.(sql.PlanHookState)
	execCfg := phs.ExecCfg()
	progress := b.job.Progress()
	b.maybeCleanUpProtectedTimestamp(ctx, execCfg.DB, execCfg.ProtectedTimestampProvider,
		progress.GetChangefeed().ProtectedTimestampRecord)
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
	}); err != nil && !crdberrors.Is(err, protectedts.ErrNotExists) {
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
	ctx context.Context, planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress,
) error {
	details := b.job.Details().(jobspb.ChangefeedDetails)
	if _, shouldPause := details.Opts[changefeedbase.OptProtectDataFromGCOnPause]; !shouldPause {
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

	pts := planHookState.(sql.PlanHookState).ExecCfg().ProtectedTimestampProvider
	return createProtectedTimestampRecord(ctx, pts, txn, *b.job.ID(),
		details.Targets, *resolved, cp)
}
