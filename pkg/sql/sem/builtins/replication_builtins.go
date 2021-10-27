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
	"github.com/cockroachdb/errors"
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

func streamingAPI(
	apiName string,
) func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	return func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		if streaming.StreamAPIFactoryHook == nil {
			return nil, errors.New("invoking a streaming replication API requires a CCL binary")
		}
		return streaming.StreamAPIFactoryHook(apiName, evalCtx, args)
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
			Fn:         streamingAPI("complete_stream_ingestion_job"),
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

	"crdb_internal.init_stream": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryStreamReplication,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"tenant_id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn:         streamingAPI("init_stream"),
			Info: "This function can be used to start a stream replication job for the specified tenant " +
				"on the producer side. The job will periotically check liveness of the stream replication " +
				"and will kill the job if it has been inactive for a duration of time specified by the" +
				"cluster setting 'stream_replication.job_liveness_timeout'.",
			Volatility: tree.VolatilityVolatile,
		},
	),
}
