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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func initReplicationBuiltins() {
	// Add all replicationBuiltins to the Builtins map after a sanity check.
	for k, v := range replicationBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		builtins[k] = v
	}
}

// replication builtins contains the cluster to cluster replication built-in functions indexed by name.
//
// For use in other packages, see AllBuiltinNames and GetBuiltinProperties().
var replicationBuiltins = map[string]builtinDefinition{
	"crdb_internal.complete_stream_ingestion_job": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"job_id", types.Int},
				{"cutover_ts", types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				mgr, err := streaming.GetReplicationStreamManager()
				if err != nil {
					return nil, err
				}

				jobID := int(*args[0].(*tree.DInt))
				cutoverTime := args[1].(*tree.DTimestampTZ).Time
				cutoverTimestamp := hlc.Timestamp{WallTime: cutoverTime.UnixNano()}
				err = mgr.CompleteStreamIngestion(evalCtx, evalCtx.Txn, jobID, cutoverTimestamp)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(jobID)), err
			},
			Info: "This function can be used to signal a running stream ingestion job to complete. " +
				"The job will eventually stop ingesting, revert to the specified timestamp and leave the " +
				"cluster in a consistent state. The specified timestamp can only be specified up to the" +
				" microsecond. " +
				"This function does not wait for the job to reach a terminal state, " +
				"but instead returns the job id as soon as it has signaled the job to complete. " +
				"This builtin can be used in conjunction with SHOW JOBS WHEN COMPLETE to ensure that the" +
				" job has left the cluster in a consistent state.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.start_replication_stream": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"tenant_id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				mgr, err := streaming.GetReplicationStreamManager()
				if err != nil {
					return nil, err
				}

				tenantID := int(tree.MustBeDInt(args[0]))
				jobID, err := mgr.StartReplicationStream(evalCtx, evalCtx.Txn, uint64(tenantID))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(jobID)), err
			},
			Info: "This function can be used on the producer side to start a replication stream for " +
				"the specified tenant. The returned stream ID uniquely identifies created stream. " +
				"The caller must periodically invoke crdb_internal.heartbeat_stream() function to " +
				"notify that the replication is still ongoing.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.replication_stream_progress": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"stream_id", types.Int},
				{"frontier_ts", types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				mgr, err := streaming.GetReplicationStreamManager()
				if err != nil {
					return nil, err
				}
				frontier, err := hlc.ParseTimestamp(string(tree.MustBeDString(args[1])))
				if err != nil {
					return nil, err
				}
				streamID := streaming.StreamID(int(tree.MustBeDInt(args[0])))
				pts, err := mgr.UpdateReplicationStreamProgress(evalCtx, streamID, frontier, evalCtx.Txn)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(pts.String()), nil
			},
			Info: "This function can be used on the consumer side to heartbeat its replication progress to " +
				"a replication stream in the source cluster. The returns a StreamReplicationStatus message " +
				"that indicates stream status (RUNNING, PAUSED, or STOPPED).",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.replication_stream_spec": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"stream_id", types.Int},
				{"start_from", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				mgr, err := streaming.GetReplicationStreamManager()
				if err != nil {
					return nil, err
				}

				streamID := int64(tree.MustBeDInt(args[0]))
				initialTime, err := hlc.ParseTimestamp(string(tree.MustBeDString(args[1])))
				if err != nil {
					return nil, err
				}
				spec, err := mgr.GetReplicationStreamSpec(evalCtx, evalCtx.Txn, streaming.StreamID(streamID), initialTime)
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
				"for the specified stream starting from the specified 'start_from' timestamp. The consumer will " +
				"later call 'stream_partition' to a partition with the spec to start streaming.",
			Volatility: tree.VolatilityVolatile,
		},
	),
}
