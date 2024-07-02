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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

		// TODO(dt): the global priv is a big hammer; should we be checking just on
		// table(s) or database being replicated from and into?
		if err := p.CheckPrivilege(
			ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPLICATION,
		); err != nil {
			return err
		}
		if !stmt.Options.IsDefault() {
			// TODO: update handling of the the returned options
			_, err := evalLogicalReplicationOptions(ctx, stmt.Options, exprEval, &p.ExtendedEvalContext().Context, p.SemaCtx())
			if err != nil {
				return err
			}
			return errors.UnimplementedErrorf(issuelink.IssueLink{}, "logical replication stream options are not yet supported")
		}
		if stmt.From.Database != "" {
			return errors.UnimplementedErrorf(issuelink.IssueLink{}, "logical replication streams on databases are unsupported")
		}
		if len(stmt.From.Tables) != len(stmt.Into.Tables) {
			return pgerror.New(pgcode.InvalidParameterValue, "the same number of source and destination tables must be specified")
		}

		var targetsDescription string
		srcTableNames := make([]string, len(stmt.From.Tables))

		repPairs := make([]jobspb.LogicalReplicationDetails_ReplicationPair, len(stmt.Into.Tables))
		for i := range stmt.From.Tables {

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

			// TODO(dt): remove when we support this via KV metadata.
			var foundTSCol bool
			for _, col := range td.GetColumns() {
				if col.Name == originTimestampColumnName {
					foundTSCol = true
					if col.Type.Family() != types.DecimalFamily {
						return errors.Newf(
							"%s column must be type DECIMAL for use by logical replication", originTimestampColumnName,
						)
					}
					break
				}
			}
			if !foundTSCol {
				return errors.WithHintf(errors.Newf(
					"tables written to by logical replication currently require a %q DECIMAL column",
					originTimestampColumnName,
				), "try 'ALTER TABLE %s ADD COLUMN %s DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL",
					dstObjName.String(), originTimestampColumnName,
				)
			}

			tbNameWithSchema := tree.MakeTableNameWithSchema(
				tree.Name(prefix.Database.GetName()),
				tree.Name(prefix.Schema.GetName()),
				tree.Name(td.GetName()),
			)

			srcTableNames[i] = stmt.From.Tables[i].String()

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
	toTypeCheck := []exprutil.ToTypeCheck{
		exprutil.Strings{stmt.PGURL},
		exprutil.Strings{
			stmt.Options.Cursor,
			stmt.Options.DefaultFunction,
			stmt.Options.Mode,
		},
	}
	if err := exprutil.TypeCheck(ctx, "LOGICAL REPLICATION STREAM", p.SemaCtx(),
		toTypeCheck...,
	); err != nil {
		return false, nil, err
	}

	return true, streamCreationHeader, nil
}

type resolvedLogicalReplicationOptions struct {
	cursor          *hlc.Timestamp
	mode            *string
	defaultFunction *string
	userFunctions   map[tree.TableName]tree.RoutineName
}

func evalLogicalReplicationOptions(
	ctx context.Context,
	options tree.LogicalReplicationOptions,
	eval exprutil.Evaluator,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
) (*resolvedLogicalReplicationOptions, error) {
	r := &resolvedLogicalReplicationOptions{}
	if options.Mode != nil {
		mode, err := eval.String(ctx, options.Mode)
		if err != nil {
			return nil, err
		}
		r.mode = &mode
	}
	if options.Cursor != nil {
		cursor, err := eval.String(ctx, options.Cursor)
		if err != nil {
			return nil, err
		}
		asOfClause := tree.AsOfClause{Expr: tree.NewStrVal(cursor)}
		asOf, err := asof.Eval(ctx, asOfClause, semaCtx, evalCtx)
		if err != nil {
			return nil, err
		}
		r.cursor = &asOf.Timestamp
	}
	if options.DefaultFunction != nil {
		defaultFnc, err := eval.String(ctx, options.DefaultFunction)
		if err != nil {
			return nil, err
		}
		r.defaultFunction = &defaultFnc
	}
	if options.UserFunctions != nil {
		r.userFunctions = make(map[tree.TableName]tree.RoutineName)
		for tb, fnc := range options.UserFunctions {
			objName, err := tb.ToUnresolvedObjectName(tree.NoAnnotation)
			if err != nil {
				return nil, err
			}
			r.userFunctions[objName.ToTableName()] = fnc
		}
	}
	return r, nil
}

func (r *resolvedLogicalReplicationOptions) GetCursor() (hlc.Timestamp, bool) {
	if r == nil || r.cursor == nil {
		return hlc.Timestamp{}, false
	}
	return *r.cursor, true
}

func (r *resolvedLogicalReplicationOptions) GetMode() (string, bool) {
	if r == nil || r.mode == nil {
		return "", false
	}
	return *r.mode, true
}

func (r *resolvedLogicalReplicationOptions) GetDefaultFunction() (string, bool) {
	if r == nil || r.defaultFunction == nil {
		return "", false
	}
	return *r.defaultFunction, true
}

func (r *resolvedLogicalReplicationOptions) GetUserFunctions() (
	map[tree.TableName]tree.RoutineName,
	bool,
) {
	if r == nil || r.userFunctions == nil {
		return map[tree.TableName]tree.RoutineName{}, false
	}
	return r.userFunctions, true
}
