// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupccl

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
	var err error
	eval.sinkURI, err = p.TypeAsString(ctx, stream.SinkURI, createStreamOp)
	if err != nil {
		return nil, err
	}
	return eval, nil
}

// This is a placeholder implementation for KV event sink.
// It simply emits the key of the KV to the user connected channel.
// TODO(yevgeniy): This implementaion needs to be replace with real one.
type emitKeysSink struct {
	resultsCh chan<- tree.Datums
}

var _ kvfeed.EventBufferWriter = &emitKeysSink{}

func (s *emitKeysSink) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	// TODO(yevgeniy): This is silly.   We need to buffer, etc
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.resultsCh <- tree.Datums{
		tree.NewDString(kv.Key.String()),
		tree.DNull,
	}:
		return nil
	}
}

func (s *emitKeysSink) AddResolved(
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, boundaryReached bool,
) error {
	// TODO(yevgeniy): This is silly.   We need to buffer, etc
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.resultsCh <- tree.Datums{
		tree.NewDString(span.Key.String()),
		tree.TimestampToDecimalDatum(ts),
	}:
		return nil
	}
}

func (s *emitKeysSink) Close(ctx context.Context) {

}

func streamKVs(
	ctx context.Context,
	p sql.PlanHookState,
	startTS hlc.Timestamp,
	spans []roachpb.Span,
	resultsCh chan<- tree.Datums,
) error {
	metrics := kvfeed.MakeMetrics("replication", base.DefaultHistogramWindowInterval())
	// TODO(yevgeniy): Use correct memory monitor.
	monitor := mon.NewUnlimitedMonitor(
		context.Background(), "replication", mon.MemoryResource,
		nil /* curCount */, nil /* maxHist */, math.MaxInt64, p.ExecCfg().Settings)

	cfg := kvfeed.Config{
		Settings:           p.ExecCfg().Settings,
		DB:                 p.ExecCfg().DB,
		Codec:              p.ExecCfg().Codec,
		Clock:              p.ExecCfg().Clock,
		Gossip:             p.ExecCfg().Gossip,
		Spans:              spans,
		Targets:            nil,
		Sink:               &emitKeysSink{resultsCh: resultsCh},
		LeaseMgr:           p.ExecCfg().LeaseManager,
		Metrics:            &metrics,
		MM:                 monitor,
		WithDiff:           false,
		NeedsInitialScan:   true,
		InitialHighWater:   startTS,
		SchemaChangePolicy: changefeedbase.OptSchemaChangePolicyIgnore,
	}
	return kvfeed.Run(ctx, cfg)
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
		return err
	}

	sinkURI, err := eval.sinkURI()
	if err != nil {
		return err
	}

	if sinkURI != "" {
		return errors.AssertionFailedf("replication streaming into sink not supported")
	}

	scanStart := hlc.Timestamp{
		WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
	}
	if eval.Options.Cursor != nil {
		if scanStart, err = p.EvalAsOfTimestamp(ctx, tree.AsOfClause{Expr: eval.Options.Cursor}); err != nil {
			return err
		}
	}

	var spans []roachpb.Span
	if eval.Targets.Tenant == (roachpb.TenantID{}) {
		// TODO(yevgeniy): Only tenant streaming supported now; Support granular streaming.
		return errors.AssertionFailedf("granular replication streaming not supported")
	}

	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(eval.Targets.Tenant.ToUint64()))
	spans = append(spans, roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	})

	// TODO(yevgeniy): Implement and use replication job to stream results into sink.
	return streamKVs(ctx, p, scanStart, spans, resultsCh)
}

// replicationStreamHeader is the header for "CREATE REPLICATION STREAM..." statements results.
var replicationStreamHeader = colinfo.ResultColumns{
	{Name: "kv", Typ: types.String},
	{Name: "ts", Typ: types.Decimal},
}

// createReplicationStreamHook is a plan hook responsible for creating replication stream.
func createReplicationStreamHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	stream, ok := stmt.(*tree.ReplicationStream)
	if !ok {
		return nil, nil, nil, false, nil
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
