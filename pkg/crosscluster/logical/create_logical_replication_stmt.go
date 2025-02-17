// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog/externalpb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

var checkJobWithSameParent = `
SELECT
	t.job_id
	FROM (
		SELECT
			id AS job_id,
			crdb_internal.pb_to_json(
				'cockroach.sql.jobs.jobspb.Payload',
				payload)->'logicalReplicationDetails'->>'parentId' AS parent_id 
		FROM crdb_internal.system_jobs 
		WHERE job_type = 'LOGICAL REPLICATION'
	) AS t
	WHERE t.parent_id = $1
`

func createLogicalReplicationStreamPlanHook(
	ctx context.Context, untypedStmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	stmt, ok := untypedStmt.(*tree.CreateLogicalReplicationStream)
	if !ok {
		return nil, nil, false, nil
	}

	exprEval := p.ExprEvaluator("LOGICAL REPLICATION STREAM")

	from, err := exprEval.String(ctx, stmt.PGURL)
	if err != nil {
		return nil, nil, false, err
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) (retErr error) {
		defer func() {
			if retErr == nil {
				telemetry.Count("logical_replication_stream.started")
			}
		}()
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

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

		options, err := evalLogicalReplicationOptions(ctx, stmt.Options, exprEval, p, stmt.CreateTable)
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

		resolvedDestObjects, err := resolveDestinationObjects(ctx, p, p.SessionData(), stmt.Into, stmt.CreateTable)
		if err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnIsSingleStmt {
			return errors.New("cannot CREATE LOGICAL REPLICATION STREAM in a multi-statement transaction")
		}

		if !p.ExecCfg().Settings.Version.ActiveVersion(ctx).AtLeast(clusterversion.V25_1.Version()) {
			return errors.New("cannot create ldr stream until finalizing on 25.1")
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

		if options.ParentID != 0 {
			row, err := p.ExecCfg().InternalDB.Executor().QueryRow(ctx, "check-parent-job", nil, checkJobWithSameParent, fmt.Sprintf("%d", options.ParentID))
			if err != nil {
				return err
			}
			if row != nil {
				// If a job already exists with the same parent ID, then this CREATE
				// LOGICAL stmt execution is a retry and the replication stream already
				// exists.
				jobID := int(*row[0].(*tree.DInt))
				resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
				return nil
			}
		}

		configUri, err := streamclient.ParseConfigUri(from)
		if err != nil {
			return err
		}
		if !configUri.IsExternalOrTestScheme() {
			return errors.New("uri must be an external connection")
		}

		clusterUri, err := configUri.AsClusterUri(ctx, p.ExecCfg().InternalDB)
		if err != nil {
			return err
		}

		cleanedURI, err := cloud.SanitizeExternalStorageURI(from, nil)
		if err != nil {
			return err
		}

		client, err := streamclient.NewStreamClient(ctx, clusterUri, p.ExecCfg().InternalDB, streamclient.WithLogical())
		if err != nil {
			return err
		}
		defer func() {
			_ = client.Close(ctx)
		}()

		srcTableNames := make([]string, len(stmt.From.Tables))
		for i, tb := range stmt.From.Tables {
			srcTableNames[i] = tb.String()
		}
		spec, err := client.CreateForTables(ctx, &streampb.ReplicationProducerRequest{
			TableNames:                  srcTableNames,
			AllowOffline:                options.ParentID != 0,
			UnvalidatedReverseStreamURI: options.BidirectionalURI(),
		})
		if err != nil {
			return err
		}
		defer func() {
			if retErr != nil {
				retErr = errors.CombineErrors(retErr, client.Complete(ctx, spec.StreamID, false))
			}
		}()

		sourceTypes := make([]*descpb.TypeDescriptor, len(spec.ExternalCatalog.Types))
		for i, desc := range spec.ExternalCatalog.Types {
			sourceTypes[i] = &desc
		}
		crossClusterResolver := crosscluster.MakeCrossClusterTypeResolver(sourceTypes)

		replicationStartTime := spec.ReplicationStartTime
		progress := jobspb.LogicalReplicationProgress{}
		if cursor, ok := options.GetCursor(); ok {
			replicationStartTime = cursor
			progress.ReplicatedTime = cursor
		}

		// If the user asked to ignore "ttl-deletes", make sure that at least one of
		// the source tables actually has a TTL job which sets the omit bit that
		// is used for filtering; if not, they probably forgot that step.
		throwNoTTLWithCDCIgnoreError := discard == jobspb.LogicalReplicationDetails_DiscardCDCIgnoredTTLDeletes

		// TODO: consider moving repPair construction into doLDRPlan after dest tables are created.
		repPairs := make([]jobspb.LogicalReplicationDetails_ReplicationPair, len(spec.ExternalCatalog.Tables))
		for i, td := range spec.ExternalCatalog.Tables {
			cpy := tabledesc.NewBuilder(&td).BuildCreatedMutableTable()
			if err := typedesc.HydrateTypesInDescriptor(ctx, cpy, crossClusterResolver); err != nil {
				return err
			}
			// TODO: i don't like this at all. this could be fixed if repPairs were
			// populated in doLDRPlan.
			spec.ExternalCatalog.Tables[i] = *cpy.TableDesc()

			if !stmt.CreateTable {
				repPairs[i].DstDescriptorID = int32(resolvedDestObjects.TableIDs[i])
			}
			repPairs[i].SrcDescriptorID = int32(td.ID)
			if td.RowLevelTTL != nil && td.RowLevelTTL.DisableChangefeedReplication {
				throwNoTTLWithCDCIgnoreError = false
			}
		}
		if uf, ok := options.GetUserFunctions(); ok {
			for i, name := range srcTableNames {
				repPairs[i].DstFunctionID = uf[name]
			}
		}
		if throwNoTTLWithCDCIgnoreError {
			return pgerror.Newf(pgcode.InvalidParameterValue, "DISCARD = 'ttl-deletes' specified but no tables have changefeed-excluded TTLs")
		}

		// Default conflict resolution if not set will be LWW
		defaultConflictResolution := jobspb.LogicalReplicationDetails_DefaultConflictResolution{
			ConflictResolutionType: jobspb.LogicalReplicationDetails_DefaultConflictResolution_LWW,
		}
		if cr, ok := options.GetDefaultFunction(); ok {
			defaultConflictResolution = *cr
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		var reverseStreamCmd string
		if stmt.CreateTable && options.BidirectionalURI() != "" {
			reverseStmt := *stmt
			reverseStmt.From, reverseStmt.Into = reverseStmt.Into, reverseStmt.From
			reverseStmt.CreateTable = false
			reverseStmt.Options.BidirectionalURI = nil
			reverseStmt.Options.ParentID = tree.NewStrVal(jobID.String())
			reverseStmt.PGURL = tree.NewStrVal(options.BidirectionalURI())
			reverseStmt.Options.Cursor = &tree.Placeholder{Idx: 0}
			reverseStreamCmd = reverseStmt.String()
		}

		jr := jobs.Record{
			JobID:       jobID,
			Description: fmt.Sprintf("LOGICAL REPLICATION STREAM into %s from %s", resolvedDestObjects.TargetDescription(), cleanedURI),
			Username:    p.User(),
			Details: jobspb.LogicalReplicationDetails{
				StreamID:                  uint64(spec.StreamID),
				SourceClusterID:           spec.SourceClusterID,
				ReplicationStartTime:      replicationStartTime,
				ReplicationPairs:          repPairs,
				SourceClusterConnUri:      configUri.Serialize(),
				TableNames:                srcTableNames,
				DefaultConflictResolution: defaultConflictResolution,
				Discard:                   discard,
				Mode:                      mode,
				MetricsLabel:              options.metricsLabel,
				CreateTable:               stmt.CreateTable,
				ReverseStreamCommand:      reverseStreamCmd,
				ParentID:                  int64(options.ParentID),
				Command:                   stmt.String(),
			},
			Progress: progress,
		}
		if err := doLDRPlan(ctx, p.User(), p.ExecCfg(), jr, spec.ExternalCatalog, resolvedDestObjects, options.SkipSchemaCheck()); err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jr.JobID))}
		return nil
	}

	return fn, streamCreationHeader, false, nil
}

type ResolvedDestObjects struct {
	TableIDs   []catid.DescID
	TableNames []tree.TableName

	// For CreateTable case
	ParentSchemaID   catid.DescID
	ParentDatabaseID catid.DescID
}

func (r *ResolvedDestObjects) TargetDescription() string {
	var targetDescription string
	for i := range r.TableNames {
		if i == 0 {
			targetDescription = r.TableNames[i].FQString()
		} else {
			targetDescription += ", " + r.TableNames[i].FQString()
		}
	}
	return targetDescription
}

func resolveDestinationObjects(
	ctx context.Context,
	r resolver.SchemaResolver,
	sessionData *sessiondata.SessionData,
	destResources tree.LogicalReplicationResources,
	createTable bool,
) (ResolvedDestObjects, error) {
	var resolved ResolvedDestObjects
	for i := range destResources.Tables {
		dstObjName, err := destResources.Tables[i].ToUnresolvedObjectName(tree.NoAnnotation)
		if err != nil {
			return resolved, err
		}
		dstTableName := dstObjName.ToTableName()
		if createTable {
			found, _, resPrefix, err := resolver.ResolveTarget(ctx,
				&dstObjName, r, sessionData.Database, sessionData.SearchPath)
			if err != nil {
				return resolved, errors.Newf("resolving target import name")
			}
			if !found {
				return resolved, errors.Newf("database or schema not found for destination table %s", destResources.Tables[i])
			}
			if resolved.ParentDatabaseID == 0 {
				resolved.ParentDatabaseID = resPrefix.Database.GetID()
				resolved.ParentSchemaID = resPrefix.Schema.GetID()
			} else if resolved.ParentDatabaseID != resPrefix.Database.GetID() {
				return resolved, errors.Newf("destination tables must all be in the same database")
			} else if resolved.ParentSchemaID != resPrefix.Schema.GetID() {
				return resolved, errors.Newf("destination tables must all be in the same schema")
			}
			if _, _, err := resolver.ResolveMutableExistingTableObject(ctx, r, &dstTableName, true, tree.ResolveRequireTableDesc); err == nil {
				return resolved, errors.Newf("destination table %s already exists", destResources.Tables[i])
			}
			tbNameWithSchema := tree.MakeTableNameWithSchema(
				tree.Name(resPrefix.Database.GetName()),
				tree.Name(resPrefix.Schema.GetName()),
				tree.Name(dstObjName.Object()),
			)
			resolved.TableNames = append(resolved.TableNames, tbNameWithSchema)
		} else {
			prefix, td, err := resolver.ResolveMutableExistingTableObject(ctx, r, &dstTableName, true, tree.ResolveRequireTableDesc)
			if err != nil {
				return resolved, errors.Wrapf(err, "failed to find existing destination table %s", destResources.Tables[i])
			}

			tbNameWithSchema := tree.MakeTableNameWithSchema(
				tree.Name(prefix.Database.GetName()),
				tree.Name(prefix.Schema.GetName()),
				tree.Name(td.GetName()),
			)
			resolved.TableNames = append(resolved.TableNames, tbNameWithSchema)
			resolved.TableIDs = append(resolved.TableIDs, td.GetID())
		}
	}
	return resolved, nil
}

func doLDRPlan(
	ctx context.Context,
	user username.SQLUsername,
	execCfg *sql.ExecutorConfig,
	jr jobs.Record,
	srcExternalCatalog externalpb.ExternalCatalog,
	resolvedDestObjects ResolvedDestObjects,
	skipSchemaCheck bool,
) error {
	details := jr.Details.(jobspb.LogicalReplicationDetails)
	return execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var (
			err             error
			ingestedCatalog externalpb.ExternalCatalog
		)
		if details.CreateTable {
			ingestedCatalog, err = externalcatalog.IngestExternalCatalog(ctx, execCfg, user, srcExternalCatalog, txn, txn.Descriptors(), resolvedDestObjects.ParentDatabaseID, resolvedDestObjects.ParentSchemaID, true /* setOffline */)
			if err != nil {
				return err
			}
			details.IngestedExternalCatalog = ingestedCatalog
			jr.Details = details
		}

		dstTableDescs := make([]*tabledesc.Mutable, 0, len(details.ReplicationPairs))
		for i := range details.ReplicationPairs {
			if details.CreateTable {
				// TODO: it's quite messy to set RepPairs for the CreateTable case here,
				// but we need to because we only get the table IDs after ingesting the
				// external catalog. It's also good to ingest the external catalog in
				// same txn as validation so it's automatically rolled back if theres an
				// error during validation.
				//
				// Instead, we could populate repPairs in this txn.
				details.ReplicationPairs[i].DstDescriptorID = int32(ingestedCatalog.Tables[i].GetID())
			}
			dstTableDesc, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, catid.DescID(details.ReplicationPairs[i].DstDescriptorID))
			if err != nil {
				return err
			}
			dstTableDescs = append(dstTableDescs, dstTableDesc)
		}

		if buildutil.CrdbTestBuild {
			if len(srcExternalCatalog.Tables) != len(dstTableDescs) {
				return errors.AssertionFailedf("srcTableDescs and dstTableDescs should have the same length")
			}
		}
		for i := range srcExternalCatalog.Tables {
			destTableDesc := dstTableDescs[i]
			if details.Mode != jobspb.LogicalReplicationDetails_Validated {
				if len(destTableDesc.OutboundForeignKeys()) > 0 || len(destTableDesc.InboundForeignKeys()) > 0 {
					return pgerror.Newf(pgcode.InvalidParameterValue, "foreign keys are only supported with MODE = 'validated'")
				}
			}

			err := tabledesc.CheckLogicalReplicationCompatibility(&srcExternalCatalog.Tables[i], destTableDesc.TableDesc(), skipSchemaCheck || details.CreateTable)
			if err != nil {
				return err
			}
		}

		if err := replicationutils.LockLDRTables(ctx, txn, dstTableDescs, jr.JobID); err != nil {
			return err
		}
		if _, err := execCfg.JobRegistry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn); err != nil {
			return err
		}
		return nil
	})
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
			stmt.Options.BidirectionalURI,
			stmt.Options.ParentID,
		},
		exprutil.Ints{stmt.Options.ParentID},
		exprutil.Bools{
			stmt.Options.SkipSchemaCheck,
			stmt.Options.Unidirectional,
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
	userFunctions    map[string]int32
	discard          string
	skipSchemaCheck  bool
	metricsLabel     string
	bidirectionalURI string
	ParentID         catpb.JobID
}

func evalLogicalReplicationOptions(
	ctx context.Context,
	options tree.LogicalReplicationOptions,
	eval exprutil.Evaluator,
	p sql.PlanHookState,
	createTable bool,
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
	if options.ParentID != nil {
		parentID, err := eval.Int(ctx, options.ParentID)
		if err != nil {
			return nil, err
		}
		r.ParentID = catpb.JobID(parentID)
	}
	unidirectional := options.Unidirectional == tree.DBoolTrue

	if options.BidirectionalURI != nil {
		uri, err := eval.String(ctx, options.BidirectionalURI)
		if err != nil {
			return nil, err
		}
		r.bidirectionalURI = uri
	}
	if createTable && unidirectional && r.bidirectionalURI != "" {
		return nil, errors.New("UNIDIRECTIONAL and BIDIRECTIONAL cannot be specified together")
	}
	if createTable && !unidirectional && r.bidirectionalURI == "" {
		return nil, errors.New("either BIDIRECTIONAL or UNIDRECTIONAL must be specified")
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
	if rf.UnsupportedWithIssue != 0 {
		return 0, rf.MakeUnsupportedError()
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

func (r *resolvedLogicalReplicationOptions) BidirectionalURI() string {
	if r == nil || r.bidirectionalURI == "" {
		return ""
	}
	return r.bidirectionalURI
}
