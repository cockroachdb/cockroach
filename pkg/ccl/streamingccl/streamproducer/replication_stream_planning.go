// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeeddist"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// replicationStreamEval is a representation of tree.ReplicationStream, prepared
// for evaluation
type replicationStreamEval struct {
	*tree.ReplicationStream
	sinkURI func() (string, error)
}

const createStreamOp = "CREATE REPLICATION STREAM"

func makeReplicationStreamEval(
	ctx context.Context, p sql.PlanHookState, stream *tree.ReplicationStream,
) (*replicationStreamEval, error) {
	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(),
		p.ExecCfg().Organization(), createStreamOp); err != nil {
		return nil, err
	}

	eval := &replicationStreamEval{ReplicationStream: stream}
	if eval.SinkURI == nil {
		eval.sinkURI = func() (string, error) { return "", nil }
	} else {
		var err error
		eval.sinkURI, err = p.TypeAsString(ctx, stream.SinkURI, createStreamOp)
		if err != nil {
			return nil, err
		}
	}

	return eval, nil
}

func telemetrySinkName(sink string) string {
	// TODO(yevgeniy): Support sinks.
	return "sinkless"
}

func streamKVs(
	ctx context.Context,
	p sql.PlanHookState,
	startTS hlc.Timestamp,
	spans []roachpb.Span,
	resultsCh chan<- tree.Datums,
) error {
	// Statement time is used by changefeed aggregator as a high watermark.
	// So, if the cursor (startTS) is specified, then use that.  Otherwise,
	// set the statement time to the current time.
	statementTime := startTS
	if statementTime.IsEmpty() {
		statementTime = hlc.Timestamp{
			WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
		}
	}

	cfOpts := map[string]string{
		changefeedbase.OptSchemaChangePolicy: string(changefeedbase.OptSchemaChangePolicyIgnore),
		changefeedbase.OptFormat:             string(changefeedbase.OptFormatNative),
		changefeedbase.OptResolvedTimestamps: changefeedbase.OptEmitAllResolvedTimestamps,
	}

	details := jobspb.ChangefeedDetails{
		Targets:       nil, // Not interested in schema changes
		Opts:          cfOpts,
		SinkURI:       "", // TODO(yevgeniy): Support sinks
		StatementTime: statementTime,
	}

	telemetry.Count(`replication.create.sink.` + telemetrySinkName(details.SinkURI))
	telemetry.Count(`replication.create.ok`)
	var checkpoint jobspb.ChangefeedProgress_Checkpoint
	if err := changefeeddist.StartDistChangefeed(
		ctx, p, 0, details, spans, startTS, checkpoint, resultsCh,
	); err != nil {
		telemetry.Count("replication.done.fail")
		return err
	}
	telemetry.Count(`replication.done.ok`)
	return nil
}

// doCreateReplicationStream is a plan hook implementation responsible for
// creation of replication stream.
func doCreateReplicationStream(
	ctx context.Context,
	p sql.PlanHookState,
	eval *replicationStreamEval,
	resultsCh chan<- tree.Datums,
) error {
	if err := p.RequireAdminRole(ctx, createStreamOp); err != nil {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "only the admin can backup other tenants")
	}

	if !p.ExecCfg().Codec.ForSystemTenant() {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can backup other tenants")
	}

	sinkURI, err := eval.sinkURI()
	if err != nil {
		return err
	}

	if sinkURI != "" {
		// TODO(yevgeniy): Support replication stream sinks.
		return pgerror.New(pgcode.FeatureNotSupported, "replication streaming into sink not supported")
	}

	var scanStart hlc.Timestamp
	if eval.Options.Cursor != nil {
		if scanStart, err = p.EvalAsOfTimestamp(ctx, tree.AsOfClause{Expr: eval.Options.Cursor}); err != nil {
			return err
		}
	}

	var spans []roachpb.Span
	if eval.Targets.Tenant == (roachpb.TenantID{}) {
		// TODO(yevgeniy): Only tenant streaming supported now; Support granular streaming.
		return pgerror.New(pgcode.FeatureNotSupported, "granular replication streaming not supported")
	}

	telemetry.Count(`replication.create.tenant`)
	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(eval.Targets.Tenant.ToUint64()))
	spans = append(spans, roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	})

	// TODO(yevgeniy): Implement and use replication job to stream results into sink.
	return streamKVs(ctx, p, scanStart, spans, resultsCh)
}

// replicationStreamHeader is the header for "CREATE REPLICATION STREAM..." statements results.
// This must match results returned by "CREATE CHANGEFEED"
var replicationStreamHeader = colinfo.ResultColumns{
	{Name: "_", Typ: types.String},
	{Name: "key", Typ: types.Bytes},
	{Name: "value", Typ: types.Bytes},
}

// createReplicationStreamHook is a plan hook responsible for creating replication stream.
func createReplicationStreamHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	stream, ok := stmt.(*tree.ReplicationStream)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if !p.SessionData().EnableStreamReplication {
		return nil, nil, nil, false, errors.WithTelemetry(
			pgerror.WithCandidateCode(
				errors.WithHint(
					errors.Newf("stream replication is only supported experimentally"),
					"You can enable stream replication by running `SET enable_experimental_stream_replication = true`.",
				),
				pgcode.FeatureNotSupported,
			),
			"replication.create.disabled",
		)
	}

	eval, err := makeReplicationStreamEval(ctx, p, stream)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doCreateReplicationStream(ctx, p, eval, resultsCh)
		if err != nil {
			telemetry.Count("replication.create.failed")
			return err
		}

		return nil
	}
	avoidBuffering := stream.SinkURI == nil
	return fn, replicationStreamHeader, nil, avoidBuffering, nil
}

func init() {
	sql.AddPlanHook(createReplicationStreamHook)
}
