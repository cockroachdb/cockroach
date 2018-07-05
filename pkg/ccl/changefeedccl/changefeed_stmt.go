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

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func init() {
	sql.AddPlanHook(changefeedPlanHook)
	jobs.AddResumeHook(changefeedResumeHook)
}

type envelopeType string

const (
	optCursor     = `cursor`
	optEnvelope   = `envelope`
	optTimestamps = `timestamps`

	optEnvelopeKeyOnly envelopeType = `key_only`
	optEnvelopeRow     envelopeType = `row`

	sinkSchemeChannel    = ``
	sinkSchemeKafka      = `kafka`
	sinkParamTopicPrefix = `topic_prefix`
)

var changefeedOptionExpectValues = map[string]bool{
	optCursor:     true,
	optEnvelope:   true,
	optTimestamps: false,
}

// changefeedPlanHook implements sql.PlanHookFn.
func changefeedPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, error) {
	changefeedStmt, ok := stmt.(*tree.CreateChangefeed)
	if !ok {
		return nil, nil, nil, nil
	}

	var sinkURIFn func() (string, error)
	var header sqlbase.ResultColumns
	unspecifiedSink := changefeedStmt.SinkURI == nil
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
	} else {
		var err error
		sinkURIFn, err = p.TypeAsString(changefeedStmt.SinkURI, `CREATE CHANGEFEED`)
		if err != nil {
			return nil, nil, nil, err
		}
		header = sqlbase.ResultColumns{
			{Name: "job_id", Typ: types.Int},
		}
	}

	optsFn, err := p.TypeAsStringOpts(changefeedStmt.Options, changefeedOptionExpectValues)
	if err != nil {
		return nil, nil, nil, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

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

		now := p.ExecCfg().Clock.Now()
		var highwater hlc.Timestamp
		if cursor, ok := opts[optCursor]; ok {
			asOf := tree.AsOfClause{Expr: tree.NewStrVal(cursor)}
			var err error
			if highwater, err = p.EvalAsOfTimestamp(asOf, now); err != nil {
				return err
			}
		}

		// TODO(dan): This grabs table descriptors once, but uses them to
		// interpret kvs written later. This both doesn't handle any schema
		// changes and breaks the table leasing.
		descriptorTime := now
		if highwater != (hlc.Timestamp{}) {
			descriptorTime = highwater
		}
		targetDescs, _, err := backupccl.ResolveTargetsToDescriptors(
			ctx, p, descriptorTime, changefeedStmt.Targets)
		if err != nil {
			return err
		}
		var tableDescs []sqlbase.TableDescriptor
		for _, desc := range targetDescs {
			if tableDesc := desc.GetTable(); tableDesc != nil {
				tableDescs = append(tableDescs, *tableDesc)
			}
		}

		details := jobspb.ChangefeedDetails{
			TableDescs: tableDescs,
			Opts:       opts,
			SinkURI:    sinkURI,
		}
		progress := jobspb.ChangefeedProgress{
			Highwater: highwater,
		}

		if details.SinkURI == `` {
			return runChangefeedFlow(
				ctx, p.ExecCfg(), details, progress, resultsCh, nil, /* progressedFn */
			)
		}

		// Make a channel for runChangefeedFlow to signal once everything has
		// been setup okay. This intentionally abuses what would normally be
		// hooked up to resultsCh to avoid a bunch of extra plumbing.
		startedCh := make(chan tree.Datums)
		job, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, startedCh, jobs.Record{
			Description: changefeedJobDescription(changefeedStmt),
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, desc := range targetDescs {
					sqlDescIDs = append(sqlDescIDs, desc.GetID())
				}
				return sqlDescIDs
			}(),
			Details:  details,
			Progress: progress,
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
	return fn, header, nil, nil
}

func changefeedJobDescription(changefeed *tree.CreateChangefeed) string {
	return tree.AsStringWithFlags(changefeed, tree.FmtAlwaysQualifyTableNames)
}

func validateChangefeed(details jobspb.ChangefeedDetails) (jobspb.ChangefeedDetails, error) {
	if details.Opts == nil {
		// The proto MarshalTo method omits the Opts field if the map is empty.
		// So, if no options were specified by the user, Opts will be nil when
		// the job gets restarted.
		details.Opts = map[string]string{}
	}

	switch envelopeType(details.Opts[optEnvelope]) {
	case ``, optEnvelopeRow:
		details.Opts[optEnvelope] = string(optEnvelopeRow)
	case optEnvelopeKeyOnly:
		details.Opts[optEnvelope] = string(optEnvelopeKeyOnly)
	default:
		return jobspb.ChangefeedDetails{}, errors.Errorf(
			`unknown %s: %s`, optEnvelope, details.Opts[optEnvelope])
	}

	for _, tableDesc := range details.TableDescs {
		if len(tableDesc.Families) != 1 {
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`only tables with 1 column family are currently supported: %s has %d`,
				tableDesc.Name, len(tableDesc.Families))
		}
	}

	return details, nil
}

type changefeedResumer struct{}

func (b *changefeedResumer) Resume(
	ctx context.Context, job *jobs.Job, planHookState interface{}, startedCh chan<- tree.Datums,
) error {
	execCfg := planHookState.(sql.PlanHookState).ExecCfg()
	details := job.Details().(jobspb.ChangefeedDetails)
	progress := job.Progress().Details.(*jobspb.Progress_Changefeed).Changefeed
	return runChangefeedFlow(ctx, execCfg, details, *progress, startedCh, job.Progressed)
}
func (b *changefeedResumer) OnFailOrCancel(context.Context, *client.Txn, *jobs.Job) error { return nil }
func (b *changefeedResumer) OnSuccess(context.Context, *client.Txn, *jobs.Job) error      { return nil }
func (b *changefeedResumer) OnTerminal(
	context.Context, *jobs.Job, jobs.Status, chan<- tree.Datums,
) {
}

func changefeedResumeHook(typ jobspb.Type, _ *cluster.Settings) jobs.Resumer {
	if typ != jobspb.TypeChangefeed {
		return nil
	}
	return &changefeedResumer{}
}
