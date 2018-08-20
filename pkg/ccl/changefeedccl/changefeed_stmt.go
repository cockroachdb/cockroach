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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

func init() {
	sql.AddPlanHook(changefeedPlanHook)
	jobs.AddResumeHook(changefeedResumeHook)
}

type envelopeType string

const (
	optCursor             = `cursor`
	optEnvelope           = `envelope`
	optResolvedTimestamps = `resolved`
	optUpdatedTimestamps  = `updated`

	optEnvelopeKeyOnly envelopeType = `key_only`
	optEnvelopeRow     envelopeType = `row`

	sinkSchemeBuffer     = ``
	sinkSchemeKafka      = `kafka`
	sinkParamTopicPrefix = `topic_prefix`
)

var changefeedOptionExpectValues = map[string]bool{
	optCursor:             true,
	optEnvelope:           true,
	optResolvedTimestamps: false,
	optUpdatedTimestamps:  false,
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

		if !p.ExecCfg().Settings.Version.IsMinSupported(cluster.VersionCreateChangefeed) {
			return errors.Errorf(`CREATE CHANGEFEED requires all nodes to be upgraded to %s`,
				cluster.VersionByKey(cluster.VersionCreateChangefeed),
			)
		}

		if err := p.RequireSuperUser(ctx, "CREATE CHANGEFEED"); err != nil {
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

		now := p.ExecCfg().Clock.Now()
		var highWater hlc.Timestamp
		if cursor, ok := opts[optCursor]; ok {
			asOf := tree.AsOfClause{Expr: tree.NewStrVal(cursor)}
			var err error
			if highWater, err = p.EvalAsOfTimestamp(asOf, now); err != nil {
				return err
			}
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
		descriptorTime := now
		if highWater != (hlc.Timestamp{}) {
			descriptorTime = highWater
		}
		targetDescs, _, err := backupccl.ResolveTargetsToDescriptors(
			ctx, p, descriptorTime, changefeedStmt.Targets)
		if err != nil {
			return err
		}
		targets := make(map[sqlbase.ID]string, len(targetDescs))
		for _, desc := range targetDescs {
			if tableDesc := desc.GetTable(); tableDesc != nil {
				if err := validateChangefeedTable(tableDesc); err != nil {
					return err
				}
				targets[tableDesc.ID] = tableDesc.Name
			}
		}

		details := jobspb.ChangefeedDetails{
			Targets: targets,
			Opts:    opts,
			SinkURI: sinkURI,
		}
		progress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{HighWater: &highWater},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{},
			},
		}

		if details.SinkURI == `` {
			return distChangefeedFlow(ctx, p, 0 /* jobID */, details, progress, resultsCh)
		}

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "CHANGEFEED",
		); err != nil {
			return err
		}

		// Make a channel for runChangefeedFlow to signal once everything has
		// been setup okay. This intentionally abuses what would normally be
		// hooked up to resultsCh to avoid a bunch of extra plumbing.
		startedCh := make(chan tree.Datums)
		job, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, startedCh, jobs.Record{
			Description: changefeedJobDescription(changefeedStmt, sinkURI, opts),
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
	return fn, header, nil, nil
}

func changefeedJobDescription(
	changefeed *tree.CreateChangefeed, sinkURI string, opts map[string]string,
) string {
	c := &tree.CreateChangefeed{
		Targets: changefeed.Targets,
		// If/when we start accepting export storage uris (or ones with
		// secrets), we'll need to sanitize sinkURI.
		SinkURI: tree.NewDString(sinkURI),
	}
	for k, v := range opts {
		opt := tree.KVOption{Key: tree.Name(k)}
		if changefeedOptionExpectValues[k] {
			opt.Value = tree.NewDString(v)
		}
		c.Options = append(c.Options, opt)
	}
	sort.Slice(c.Options, func(i, j int) bool { return c.Options[i].Key < c.Options[j].Key })
	return tree.AsStringWithFlags(c, tree.FmtAlwaysQualifyTableNames)
}

func validateDetails(details jobspb.ChangefeedDetails) (jobspb.ChangefeedDetails, error) {
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

	return details, nil
}

func validateChangefeedTable(tableDesc *sqlbase.TableDescriptor) error {
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
	return nil
}

type changefeedResumer struct{}

func (b *changefeedResumer) Resume(
	ctx context.Context, job *jobs.Job, planHookState interface{}, startedCh chan<- tree.Datums,
) error {
	phs := planHookState.(sql.PlanHookState)
	details := job.Details().(jobspb.ChangefeedDetails)
	progress := job.Progress()
	err := distChangefeedFlow(ctx, phs, *job.ID(), details, progress, startedCh)
	if err != nil {
		log.Infof(ctx, `CHANGEFEED job %d returning with error: %v`, *job.ID(), err)
	}
	return err
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
