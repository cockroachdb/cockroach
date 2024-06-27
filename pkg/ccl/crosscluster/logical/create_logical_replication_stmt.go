// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/issuelink"
)

func init() {
	sql.AddPlanHook("create logical replication stream", createLogicalReplicationStreamPlanHook, createLogicalReplicationStreamTypeCheck)
}

var streamCreationHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
}

func createLogicalReplicationStreamPlanHook(
	ctx context.Context, untypedStmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	stmt, ok := untypedStmt.(*tree.CreateLogicalReplicationStream)
	if !ok {
		return nil, nil, nil, false, nil
	}

	exprEval := p.ExprEvaluator("LOGICAL REPLICATION STREAM")

	from, err := exprEval.String(ctx, stmt.PGURL)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) (err error) {
		defer func() {
			if err == nil {
				telemetry.Count("logical_replication_stream.started")
			}
		}()
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V24_2) {
			return pgerror.New(pgcode.FeatureNotSupported,
				"replication job not supported before V24.2")
		}

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings,
			"CREATE LOGICAL REPLICATION",
		); err != nil {
			return err
		}

		if !stmt.Options.IsDefault() {
			return errors.UnimplementedErrorf(issuelink.IssueLink{}, "logical replication stream options are not yet supported")
		}
		if stmt.On.Database != "" {
			return errors.UnimplementedErrorf(issuelink.IssueLink{}, "logical replication streams on databases are unsupported")
		}
		if len(stmt.On.Tables) != len(stmt.Into.Tables) {
			return pgerror.New(pgcode.InvalidParameterValue, "the same number of source and destination tables must be specified")
		}

		var targetsDescription string
		srcTableNames := make([]string, len(stmt.On.Tables))

		repPairs := make([]jobspb.LogicalReplicationDetails_ReplicationPair, len(stmt.Into.Tables))
		for i := range stmt.On.Tables {

			dstObjName, err := stmt.Into.Tables[i].ToUnresolvedObjectName(tree.NoAnnotation)
			if err != nil {
				return err
			}
			dstTableName := dstObjName.ToTableName()
			prefix, td, err := resolver.ResolveMutableExistingTableObject(ctx, p, &dstTableName, true, tree.ResolveRequireTableDesc)
			if err != nil {
				return err
			}

			repPairs[i].DstDescriptorID = int32(td.GetID())

			tbNameWithSchema := tree.MakeTableNameWithSchema(
				tree.Name(prefix.Database.GetName()),
				tree.Name(prefix.Schema.GetName()),
				tree.Name(td.GetName()),
			)

			srcTableNames[i] = stmt.On.Tables[i].String()

			if i == 0 {
				targetsDescription = tbNameWithSchema.FQString()
			} else {
				targetsDescription += ", " + tbNameWithSchema.FQString()
			}
		}

		streamAddress := crosscluster.StreamAddress(from)
		streamURL, err := streamAddress.URL()
		if err != nil {
			return err
		}
		streamAddress = crosscluster.StreamAddress(streamURL.String())

		client, err := streamclient.NewStreamClient(ctx, streamAddress, p.ExecCfg().InternalDB, streamclient.WithLogical())
		if err != nil {
			return err
		}
		defer func() {
			_ = client.Close(ctx)
		}()

		if err := client.Dial(ctx); err != nil {
			return err
		}

		spec, err := client.CreateForTables(ctx, &streampb.ReplicationProducerRequest{
			TableNames: srcTableNames,
		})
		if err != nil {
			return err
		}

		for i, name := range srcTableNames {
			repPairs[i].SrcDescriptorID = int32(spec.TableDescriptors[name].ID)
		}

		jr := jobs.Record{
			JobID:       p.ExecCfg().JobRegistry.MakeJobID(),
			Description: fmt.Sprintf("LOGICAL REPLICATION STREAM into %s from %s", targetsDescription, streamAddress),
			Username:    p.User(),
			Details: jobspb.LogicalReplicationDetails{
				StreamID:             uint64(spec.StreamID),
				SourceClusterID:      spec.SourceClusterID,
				ReplicationStartTime: spec.ReplicationStartTime,
				TargetClusterConnStr: string(streamAddress),
				ReplicationPairs:     repPairs,
				TableNames:           srcTableNames},
			Progress: jobspb.LogicalReplicationProgress{},
		}

		if _, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, p.InternalSQLTxn()); err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jr.JobID))}
		return nil
	}

	return fn, streamCreationHeader, nil, false, nil
}

func createLogicalReplicationStreamTypeCheck(
	ctx context.Context, untypedStmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	stmt, ok := untypedStmt.(*tree.CreateLogicalReplicationStream)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(ctx, "LOGICAL REPLICATION STREAM", p.SemaCtx(),
		exprutil.Strings{stmt.PGURL},
	); err != nil {
		return false, nil, err
	}

	return true, streamCreationHeader, nil
}
