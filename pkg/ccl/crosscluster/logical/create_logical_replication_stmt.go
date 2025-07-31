// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) (retErr error) {
		defer func() {
			if retErr == nil {
				telemetry.Count("logical_replication_stream.started")
			}
		}()
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V24_3) {
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

		if stmt.From.Database != "" {
			return errors.UnimplementedErrorf(errors.IssueLink{}, "logical replication streams on databases are unsupported")
		}
		if len(stmt.From.Tables) != len(stmt.Into.Tables) {
			return pgerror.New(pgcode.InvalidParameterValue, "the same number of source and destination tables must be specified")
		}

		options, err := evalLogicalReplicationOptions(ctx, stmt.Options, exprEval, p)
		if err != nil {
			return err
		}

		hasUDF := len(options.userFunctions) > 0 || options.defaultFunction != nil && options.defaultFunction.FunctionId != 0

		mode := jobspb.LogicalReplicationDetails_Immediate
		if m, ok := options.GetMode(); ok {
			switch m {
			case "immediate":
				if hasUDF {
					return pgerror.Newf(pgcode.InvalidParameterValue, "MODE = 'immediate' cannot be used with user-defined functions")
				}
			case "validated":
				mode = jobspb.LogicalReplicationDetails_Validated
			default:
				return pgerror.Newf(pgcode.InvalidParameterValue, "unknown mode %q", m)
			}
		} else if hasUDF {
			// UDFs imply applying changes via SQL, which implies validation.
			mode = jobspb.LogicalReplicationDetails_Validated
		}

		discard := jobspb.LogicalReplicationDetails_DiscardNothing
		if m, ok := options.Discard(); ok {
			switch m {
			case "ttl-deletes":
				discard = jobspb.LogicalReplicationDetails_DiscardCDCIgnoredTTLDeletes
			case "all-deletes":
				discard = jobspb.LogicalReplicationDetails_DiscardAllDeletes
			default:
				return pgerror.Newf(pgcode.InvalidParameterValue, "unknown discard option %q", m)
			}
		}

		var (
			targetsDescription string
			srcTableNames      = make([]string, len(stmt.From.Tables))
			repPairs           = make([]jobspb.LogicalReplicationDetails_ReplicationPair, len(stmt.Into.Tables))
			srcTableDescs      = make([]*descpb.TableDescriptor, len(stmt.Into.Tables))
		)
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

			if mode != jobspb.LogicalReplicationDetails_Validated {
				if len(td.OutboundForeignKeys()) > 0 || len(td.InboundForeignKeys()) > 0 {
					return pgerror.Newf(pgcode.InvalidParameterValue, "foreign keys are only supported with MODE = 'validated'")
				}
			}
		}
		if !p.ExtendedEvalContext().TxnIsSingleStmt {
			return errors.New("cannot CREATE LOGICAL REPLICATION STREAM in a multi-statement transaction")
		}

		// Commit the planner txn because several operations below may take several
		// seconds, which we would like to conduct outside the scope of the planner
		// txn to prevent txn refresh errors.
		if err := p.Txn().Commit(ctx); err != nil {
			return err
		}
		// Release all descriptor leases here. We need to do this because we're
		// about run schema changes below to lock the replicating tables. Note that
		// we committed the underlying transaction above -- so we're not using any
		// leases anymore, but we might be holding some.
		//
		// This is all a bit of a hack to deal with the fact that the usual
		// machinery for releasing leases assumes that we do not close the planner
		// txn during statement execution.
		p.InternalSQLTxn().Descriptors().ReleaseAll(ctx)

		streamAddress := crosscluster.StreamAddress(from)
		streamURL, err := streamAddress.URL()
		if err != nil {
			return err
		}
		streamAddress = crosscluster.StreamAddress(streamURL.String())

		cleanedURI, err := cloud.SanitizeExternalStorageURI(from, nil)
		if err != nil {
			return err
		}

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
		defer func() {
			if retErr != nil {
				retErr = errors.CombineErrors(retErr, client.Complete(ctx, spec.StreamID, false))
			}
		}()

		sourceTypes := make([]*descpb.TypeDescriptor, len(spec.TypeDescriptors))
		for i, desc := range spec.TypeDescriptors {
			sourceTypes[i] = &desc
		}
		crossClusterResolver := crosscluster.MakeCrossClusterTypeResolver(sourceTypes)

		// If the user asked to ignore "ttl-deletes", make sure that at least one of
		// the source tables actually has a TTL job which sets the omit bit that
		// is used for filtering; if not, they probably forgot that step.
		throwNoTTLWithCDCIgnoreError := discard == jobspb.LogicalReplicationDetails_DiscardCDCIgnoredTTLDeletes

		for i, name := range srcTableNames {
			td := spec.TableDescriptors[name]
			cpy := tabledesc.NewBuilder(&td).BuildCreatedMutableTable()
			if err := typedesc.HydrateTypesInDescriptor(ctx, cpy, crossClusterResolver); err != nil {
				return err
			}
			srcTableDescs[i] = cpy.TableDesc()
			repPairs[i].SrcDescriptorID = int32(td.ID)
			if td.RowLevelTTL != nil && td.RowLevelTTL.DisableChangefeedReplication {
				throwNoTTLWithCDCIgnoreError = false
			}
		}

		if throwNoTTLWithCDCIgnoreError {
			return pgerror.Newf(pgcode.InvalidParameterValue, "DISCARD = 'ttl-deletes' specified but no tables have changefeed-excluded TTLs")
		}

		replicationStartTime := spec.ReplicationStartTime
		progress := jobspb.LogicalReplicationProgress{}
		if cursor, ok := options.GetCursor(); ok {
			replicationStartTime = cursor
			progress.ReplicatedTime = cursor
		}

		if uf, ok := options.GetUserFunctions(); ok {
			for i, name := range srcTableNames {
				repPairs[i].DstFunctionID = uf[name]
			}
		}
		// Default conflict resolution if not set will be LWW
		defaultConflictResolution := jobspb.LogicalReplicationDetails_DefaultConflictResolution{
			ConflictResolutionType: jobspb.LogicalReplicationDetails_DefaultConflictResolution_LWW,
		}
		if cr, ok := options.GetDefaultFunction(); ok {
			defaultConflictResolution = *cr
		}

		var jobID jobspb.JobID
		if err := p.ExecCfg().InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			dstTableDescs := make([]*tabledesc.Mutable, 0, len(srcTableDescs))
			for _, pair := range repPairs {
				dstTableDesc, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, catid.DescID(pair.DstDescriptorID))
				if err != nil {
					return err
				}
				dstTableDescs = append(dstTableDescs, dstTableDesc)
			}

			if buildutil.CrdbTestBuild {
				if len(srcTableDescs) != len(dstTableDescs) {
					panic("srcTableDescs and dstTableDescs should have the same length")
				}
			}
			for i := range srcTableDescs {
				err := tabledesc.CheckLogicalReplicationCompatibility(srcTableDescs[i], dstTableDescs[i].TableDesc(), options.SkipSchemaCheck())
				if err != nil {
					return err
				}
			}

			jr := jobs.Record{
				JobID:       p.ExecCfg().JobRegistry.MakeJobID(),
				Description: fmt.Sprintf("LOGICAL REPLICATION STREAM into %s from %s", targetsDescription, cleanedURI),
				Username:    p.User(),
				Details: jobspb.LogicalReplicationDetails{
					StreamID:                  uint64(spec.StreamID),
					SourceClusterID:           spec.SourceClusterID,
					ReplicationStartTime:      replicationStartTime,
					SourceClusterConnStr:      string(streamAddress),
					ReplicationPairs:          repPairs,
					TableNames:                srcTableNames,
					DefaultConflictResolution: defaultConflictResolution,
					Discard:                   discard,
					Mode:                      mode,
					MetricsLabel:              options.metricsLabel,
				},
				Progress: progress,
			}
			jobID = jr.JobID
			if err := replicationutils.LockLDRTables(ctx, txn, dstTableDescs, jr.JobID); err != nil {
				return err
			}
			if _, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn); err != nil {
				return err
			}
			return err
		}); err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
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
			stmt.Options.MetricsLabel,
			stmt.Options.Discard,
		},
		exprutil.Bools{
			stmt.Options.SkipSchemaCheck,
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
	cursor          hlc.Timestamp
	mode            string
	defaultFunction *jobspb.LogicalReplicationDetails_DefaultConflictResolution
	// Mapping of table name to function descriptor
	userFunctions   map[string]int32
	discard         string
	skipSchemaCheck bool
	metricsLabel    string
}

func evalLogicalReplicationOptions(
	ctx context.Context,
	options tree.LogicalReplicationOptions,
	eval exprutil.Evaluator,
	p sql.PlanHookState,
) (*resolvedLogicalReplicationOptions, error) {
	r := &resolvedLogicalReplicationOptions{}
	if options.Mode != nil {
		mode, err := eval.String(ctx, options.Mode)
		if err != nil {
			return nil, err
		}
		r.mode = mode
	}
	if options.MetricsLabel != nil {
		metricsLabel, err := eval.String(ctx, options.MetricsLabel)
		if err != nil {
			return nil, err
		}
		r.metricsLabel = metricsLabel
	}
	if options.Cursor != nil {
		cursor, err := eval.String(ctx, options.Cursor)
		if err != nil {
			return nil, err
		}
		asOfClause := tree.AsOfClause{Expr: tree.NewStrVal(cursor)}
		asOf, err := asof.Eval(ctx, asOfClause, p.SemaCtx(), &p.ExtendedEvalContext().Context)
		if err != nil {
			return nil, err
		}
		r.cursor = asOf.Timestamp
	}
	if options.DefaultFunction != nil {
		defaultResolution := &jobspb.LogicalReplicationDetails_DefaultConflictResolution{}
		defaultFnc, err := eval.String(ctx, options.DefaultFunction)
		if err != nil {
			return nil, err
		}
		switch strings.ToLower(defaultFnc) {
		case "lww":
			defaultResolution.ConflictResolutionType = jobspb.LogicalReplicationDetails_DefaultConflictResolution_LWW
		case "dlq":
			defaultResolution.ConflictResolutionType = jobspb.LogicalReplicationDetails_DefaultConflictResolution_DLQ
		// This case will assume that a function name was passed in
		// and we will try to resolve it.
		default:
			urn, err := parser.ParseFunctionName(defaultFnc)
			if err != nil {
				return nil, err
			}
			un := urn.ToUnresolvedName()
			descID, err := lookupFunctionID(ctx, p, *un)
			if err != nil {
				return nil, err
			}
			defaultResolution.ConflictResolutionType = jobspb.LogicalReplicationDetails_DefaultConflictResolution_UDF
			defaultResolution.FunctionId = descID
		}

		r.defaultFunction = defaultResolution
	}
	if options.UserFunctions != nil {
		r.userFunctions = make(map[string]int32)
		for tb, fnc := range options.UserFunctions {
			objName, err := tb.ToUnresolvedObjectName(tree.NoAnnotation)
			if err != nil {
				return nil, err
			}

			un := fnc.ToUnresolvedObjectName().ToUnresolvedName()
			descID, err := lookupFunctionID(ctx, p, *un)
			if err != nil {
				return nil, err
			}
			r.userFunctions[objName.String()] = descID
		}
	}

	if options.Discard != nil {
		discard, err := eval.String(ctx, options.Discard)
		if err != nil {
			return nil, err
		}
		r.discard = discard
	}
	if options.SkipSchemaCheck == tree.DBoolTrue {
		r.skipSchemaCheck = true
	}
	return r, nil
}

func lookupFunctionID(
	ctx context.Context, p sql.PlanHookState, u tree.UnresolvedName,
) (int32, error) {
	rf, err := p.ResolveFunction(ctx, tree.MakeUnresolvedFunctionName(&u), &p.SessionData().SearchPath)
	if err != nil {
		return 0, err
	}
	if len(rf.Overloads) > 1 {
		return 0, errors.Newf("function %q has more than 1 overload", u.String())
	}
	fnOID := rf.Overloads[0].Oid
	descID := typedesc.UserDefinedTypeOIDToID(fnOID)
	if descID == 0 {
		return 0, errors.Newf("function %q is not a user defined type", u.String())
	}
	return int32(descID), nil
}

func (r *resolvedLogicalReplicationOptions) GetCursor() (hlc.Timestamp, bool) {
	if r == nil || r.cursor.IsEmpty() {
		return hlc.Timestamp{}, false
	}
	return r.cursor, true
}

func (r *resolvedLogicalReplicationOptions) GetMode() (string, bool) {
	if r == nil || r.mode == "" {
		return "", false
	}
	return r.mode, true
}

func (r *resolvedLogicalReplicationOptions) GetDefaultFunction() (
	*jobspb.LogicalReplicationDetails_DefaultConflictResolution,
	bool,
) {
	if r == nil || r.defaultFunction == nil {
		return &jobspb.LogicalReplicationDetails_DefaultConflictResolution{}, false
	}
	return r.defaultFunction, true
}

func (r *resolvedLogicalReplicationOptions) GetUserFunctions() (map[string]int32, bool) {
	if r == nil || r.userFunctions == nil {
		return map[string]int32{}, false
	}
	return r.userFunctions, true
}

func (r *resolvedLogicalReplicationOptions) Discard() (string, bool) {
	if r == nil || r.discard == "" {
		return "", false
	}
	return r.discard, true
}

func (r *resolvedLogicalReplicationOptions) SkipSchemaCheck() bool {
	if r == nil {
		return false
	}
	return r.skipSchemaCheck
}
