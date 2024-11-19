// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func init() {
	for k, v := range replicationBuiltins {
		// Most builtins in this file are of the Normal class, but there are a
		// couple of the Generator class.
		const enforceClass = false
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

// replication builtins contains the cluster to cluster replication built-in functions indexed by name.
//
// For use in other packages, see AllBuiltinNames and GetBuiltinProperties().
var replicationBuiltins = map[string]builtinDefinition{
	// Stream ingestion functions starts here.
	"crdb_internal.complete_stream_ingestion_job": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "job_id", Typ: types.Int},
				{Name: "cutover_ts", Typ: types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// Keeping this builtin as 'unimplemented' in order to reserve the oid.
				return tree.DNull, errors.New("unimplemented")
			},
			Info:       "DEPRECATED, consider using `ALTER VIRTUAL CLUSTER <TENANT> COMPLETE REPLICATION TO SYSTEM TIME <TIME>`",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.stream_ingestion_stats_json": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
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
			Info:       "DEPRECATED, consider using `SHOW VIRTUAL CLUSTER name WITH REPLICATION STATUS`",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.stream_ingestion_stats_pb": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
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
			Info:       "DEPRECATED, consider using `SHOW VIRTUAL CLUSTER name WITH REPLICATION STATUS`",
			Volatility: volatility.Volatile,
		},
	),

	// Stream production functions starts here.
	"crdb_internal.start_replication_stream": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
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
				replicationProducerSpec, err := mgr.StartReplicationStream(ctx, roachpb.TenantName(tenantName), streampb.ReplicationProducerRequest{})
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
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "tenant_name", Typ: types.String},
				{Name: "spec", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}
				tenantName := string(tree.MustBeDString(args[0]))
				reqBytes := []byte(tree.MustBeDBytes(args[1]))

				req := streampb.ReplicationProducerRequest{}
				if err := protoutil.Unmarshal(reqBytes, &req); err != nil {
					return nil, err
				}

				replicationProducerSpec, err := mgr.StartReplicationStream(ctx, roachpb.TenantName(tenantName), req)
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
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
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
			Category:           builtinconstants.CategoryClusterReplication,
			Undocumented:       true,
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
					ctx,
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
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
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
				spec, err := mgr.GetPhysicalReplicationStreamSpec(ctx, streampb.StreamID(streamID))
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
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
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
	"crdb_internal.setup_span_configs_stream": makeBuiltin(
		tree.FunctionProperties{
			Category:           builtinconstants.CategoryClusterReplication,
			Undocumented:       true,
			DistsqlBlocklist:   false,
			VectorizeStreaming: true,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "tenant_name", Typ: types.String},
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
				return mgr.SetupSpanConfigsStream(ctx, roachpb.TenantName(tree.MustBeDString(args[0])))
			},
			"Stream span config updates for specified tenant",
			volatility.Volatile,
		),
	),
	"crdb_internal.unsafe_revert_tenant_to_timestamp": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "tenant_name", Typ: types.String},
				{Name: "ts", Typ: types.Decimal},
			},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// NB: GetReplicationStreamManager does a permissions check for
				// ADMIN or MANAGEVIRTUALCLUSTER.
				if evalCtx.SessionData().SafeUpdates {
					err := errors.Newf("crdb_internal.unsafe_revert_tenant_to_timestamp causes irreversible data loss")
					err = errors.WithMessage(err, "rejected (via sql_safe_updates)")
					err = pgerror.WithCandidateCode(err, pgcode.Warning)
					return nil, err
				}

				tenantName := roachpb.TenantName(string(tree.MustBeDString(args[0])))

				tsDec := tree.MustBeDDecimal(args[1])
				revertTimestamp, err := hlc.DecimalToHLC(&tsDec.Decimal)
				if err != nil {
					return nil, err
				}

				mgr, err := evalCtx.StreamManagerFactory.GetStreamIngestManager(ctx)
				if err != nil {
					return nil, err
				}

				if err := mgr.RevertTenantToTimestamp(ctx, tenantName, revertTimestamp); err != nil {
					return nil, err
				}
				return &tsDec, err
			},
			Info:       "This function reverts the given tenant to a particular timestamp.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.split_at": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			Undocumented:     true,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "key", Typ: types.Bytes},
				{Name: "ttl", Typ: types.Interval},
			},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				key := roachpb.Key(tree.MustBeDBytes(args[0]))
				ttl := tree.MustBeDInterval(args[1])
				expiration := evalCtx.Txn.DB().Clock().Now().Add(ttl.Nanos(), 0)
				return tree.DVoidDatum, evalCtx.Txn.DB().AdminSplit(ctx, key, expiration)
			},
			Info:       "Splits at an *arbitrary* byte key.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.scatter": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			Undocumented:     true,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "key", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				key := roachpb.Key(tree.MustBeDBytes(args[0]))
				_, err := evalCtx.Txn.DB().AdminScatter(ctx, key, 0)
				return tree.DVoidDatum, err
			},
			Info:       "Scatters the passed arbitrary key",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				key := roachpb.Key(tree.MustBeDBytes(args[0]))
				endKey := roachpb.Key(tree.MustBeDBytes(args[1]))

				scatterReq := &kvpb.AdminScatterRequest{
					RequestHeader:   kvpb.RequestHeaderFromSpan(roachpb.Span{Key: key, EndKey: endKey}),
					RandomizeLeases: true,
				}
				_, pErr := kv.SendWrapped(ctx, evalCtx.Txn.DB().NonTransactionalSender(), scatterReq)
				if pErr != nil {
					return nil, pErr.GoError()
				}
				return tree.DVoidDatum, nil
			},
			Info:       "Scatters the passed arbitrary key",
			Volatility: volatility.Volatile,
		},
	),
	// TODO(ssd): These functions likely aren't the final API we
	// want.  Namely, boths should perhaps just be overloads of
	// existing functions.
	"crdb_internal.plan_logical_replication": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "req", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}
				reqBytes := []byte(tree.MustBeDBytes(args[0]))
				req := streampb.LogicalReplicationPlanRequest{}
				if err := protoutil.Unmarshal(reqBytes, &req); err != nil {
					return nil, err
				}

				spec, err := mgr.PlanLogicalReplication(ctx, req)
				if err != nil {
					return nil, err
				}
				rawSpec, err := protoutil.Marshal(spec)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(rawSpec)), err
			},
			Info:       "Returns a replication stream spec for the given logical replication plan request",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.start_replication_stream_for_tables": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "req", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}
				reqBytes := []byte(tree.MustBeDBytes(args[0]))
				req := streampb.ReplicationProducerRequest{}
				if err := protoutil.Unmarshal(reqBytes, &req); err != nil {
					return nil, err
				}

				spec, err := mgr.StartReplicationStreamForTables(ctx, req)
				if err != nil {
					return nil, err
				}

				rawSpec, err := protoutil.Marshal(&spec)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(rawSpec)), err
			},
			Info:       "TODO(ssd)",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.logical_replication_inject_failures": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryClusterReplication,
			Undocumented:     true,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "stream", Typ: types.Int},
				{Name: "proc", Typ: types.Int},
				{Name: "percent", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				mgr, err := evalCtx.StreamManagerFactory.GetReplicationStreamManager(ctx)
				if err != nil {
					return nil, err
				}
				stream := streampb.StreamID(int(tree.MustBeDInt(args[0])))
				proc := int32(int(tree.MustBeDInt(args[1])))
				percent := streampb.StreamID(int(tree.MustBeDInt(args[2])))

				if percent > 100 {
					return nil, errors.New("invalid percent")
				}

				found := false
				for _, i := range mgr.DebugGetLogicalConsumerStatuses(ctx) {
					if stream == 0 || stream == i.StreamID {
						if proc == 0 || proc == i.ProcessorID {
							found = true
							i.SetInjectedFailurePercent(uint32(percent))
						}
					}
				}
				if !found {
					return nil, errors.New("no matching processors found")
				}
				return tree.DVoidDatum, nil
			},
			Info:       "Debugging tool to force failures during application of LDR events",
			Volatility: volatility.Volatile,
		},
	),
}
