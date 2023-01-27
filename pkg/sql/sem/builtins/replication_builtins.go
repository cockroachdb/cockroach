// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func init() {
	// Add all replicationBuiltins to the builtins map after a sanity check.
	for k, v := range replicationBuiltins {
		registerBuiltin(k, v)
	}
}

// replication builtins contains the cluster to cluster replication built-in functions indexed by name.
//
// For use in other packages, see AllBuiltinNames and GetBuiltinProperties().
var replicationBuiltins = map[string]builtinDefinition{
	// Stream ingestion functions starts here.
	"crdb_internal.complete_stream_ingestion_job": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "job_id", Typ: types.Int},
				{Name: "cutover_ts", Typ: types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetStreamIngestManager(ctx)
				if err != nil {
					return nil, err
				}

				ingestionJobID := jobspb.JobID(*args[0].(*tree.DInt))
				cutoverTime := args[1].(*tree.DTimestampTZ).Time
				cutoverTimestamp := hlc.Timestamp{WallTime: cutoverTime.UnixNano()}
				err = mgr.CompleteStreamIngestion(ctx, ingestionJobID, cutoverTimestamp)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(ingestionJobID)), err
			},
			Info: "This function can be used to signal a running stream ingestion job to complete. " +
				"The job will eventually stop ingesting, revert to the specified timestamp and leave the " +
				"cluster in a consistent state. The specified timestamp can only be specified up to the " +
				"microsecond. " +
				"This function does not wait for the job to reach a terminal state, " +
				"but instead returns the job id as soon as it has signaled the job to complete. " +
				"This builtin can be used in conjunction with `SHOW JOBS WHEN COMPLETE` to ensure that the " +
				"job has left the cluster in a consistent state.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.stream_ingestion_stats_json": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist: true,
		},

		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "job_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// Keeping this builtin as 'unimplemented' in order to reserve the oid.
				return tree.DNull, errors.New("unimplemented")
			},
			Info:       "DEPRECATED, consider using `SHOW TENANT name WITH REPLICATION STATUS`",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.stream_ingestion_stats_pb": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist: true,
		},

		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "job_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// Keeping this builtin as 'unimplemented' in order to reserve the oid.
				return tree.DNull, errors.New("unimplemented")
			},
			Info:       "DEPRECATED, consider using `SHOW TENANT name WITH REPLICATION STATUS`",
			Volatility: volatility.Volatile,
		},
	),

	// Stream production functions starts here.
	"crdb_internal.start_replication_stream": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "tenant_name", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}
				tenantName := string(tree.MustBeDString(args[0]))
				replicationProducerSpec, err := mgr.StartReplicationStream(ctx, roachpb.TenantName(tenantName))
				if err != nil {
					return nil, err
				}
				rawReplicationProducerSpec, err := protoutil.Marshal(&replicationProducerSpec)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(rawReplicationProducerSpec)), err
			},
			Info: "This function can be used on the producer side to start a replication stream for " +
				"the specified tenant. The returned stream ID uniquely identifies created stream. " +
				"The caller must periodically invoke crdb_internal.heartbeat_stream() function to " +
				"notify that the replication is still ongoing.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.replication_stream_progress": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "stream_id", Typ: types.Int},
				{Name: "frontier_ts", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, errors.New("stream_id or frontier_ts cannot be specified with null argument")
				}
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}
				frontier, err := hlc.ParseTimestamp(string(tree.MustBeDString(args[1])))
				if err != nil {
					return nil, err
				}
				streamID := streampb.StreamID(int(tree.MustBeDInt(args[0])))
				sps, err := mgr.HeartbeatReplicationStream(ctx, streamID, frontier)
				if err != nil {
					return nil, err
				}
				rawStatus, err := protoutil.Marshal(&sps)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(rawStatus)), nil
			},
			Info: "This function can be used on the consumer side to heartbeat its replication progress to " +
				"a replication stream in the source cluster. The returns a StreamReplicationStatus message " +
				"that indicates stream status (`ACTIVE`, `PAUSED`, `INACTIVE`, or `STATUS_UNKNOWN_RETRY`).",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.stream_partition": makeBuiltin(
		tree.FunctionProperties{
			Category:           builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist:   false,
			VectorizeStreaming: true,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "stream_id", Typ: types.Int},
				{Name: "partition_spec", Typ: types.Bytes},
			},
			types.MakeLabeledTuple(
				[]*types.T{types.Bytes},
				[]string{"stream_event"},
			),
			func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}
				return mgr.StreamPartition(
					streampb.StreamID(tree.MustBeDInt(args[0])),
					[]byte(tree.MustBeDBytes(args[1])),
				)
			},
			"Stream partition data",
			volatility.Volatile,
		),
	),

	"crdb_internal.replication_stream_spec": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "stream_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}

				streamID := int64(tree.MustBeDInt(args[0]))
				spec, err := mgr.GetReplicationStreamSpec(ctx, streampb.StreamID(streamID))
				if err != nil {
					return nil, err
				}
				rawSpec, err := protoutil.Marshal(spec)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(rawSpec)), err
			},
			Info: "This function can be used on the consumer side to get a replication stream specification " +
				"for the specified stream. The consumer will later call 'stream_partition' to a partition with " +
				"the spec to start streaming.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.complete_replication_stream": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "stream_id", Typ: types.Int},
				{Name: "successful_ingestion", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}

				streamID := int64(tree.MustBeDInt(args[0]))
				successfulIngestion := bool(tree.MustBeDBool(args[1]))
				if err := mgr.CompleteReplicationStream(
					ctx, streampb.StreamID(streamID), successfulIngestion,
				); err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(streamID)), err
			},
			Info: "This function can be used on the producer side to complete and clean up a replication stream." +
				"'successful_ingestion' indicates whether the stream ingestion finished successfully.",
			Volatility: volatility.Volatile,
		},
	),
}
