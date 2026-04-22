// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/embedding"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vectorizer"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// Vectorizer option names.
const (
	vectorizerOptModel     = "model"
	vectorizerOptTemplate  = "template"
	vectorizerOptSchedule  = "schedule"
	vectorizerOptBatchSize = "batch_size"
	vectorizerOptLoading   = "loading"
)

var vectorizerOptionValidation = exprutil.KVOptionValidationMap{
	vectorizerOptModel:     exprutil.KVStringOptRequireValue,
	vectorizerOptTemplate:  exprutil.KVStringOptRequireValue,
	vectorizerOptSchedule:  exprutil.KVStringOptRequireValue,
	vectorizerOptBatchSize: exprutil.KVStringOptRequireValue,
	vectorizerOptLoading:   exprutil.KVStringOptRequireValue,
}

type createVectorizerNode struct {
	zeroInputPlanNode
	n         *tree.CreateVectorizer
	tableDesc *tabledesc.Mutable
	tableName tree.TableName
}

// CreateVectorizer creates an automatic embedding generator on a table.
func (p *planner) CreateVectorizer(
	ctx context.Context, n *tree.CreateVectorizer,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx, p.ExecCfg(), "CREATE VECTORIZER",
	); err != nil {
		return nil, err
	}

	tn := n.TableName.ToTableName()
	_, tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tn, true /* required */, tree.ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}

	// Check ownership.
	hasOwnership, err := p.HasOwnership(ctx, tableDesc)
	if err != nil {
		return nil, err
	}
	if !hasOwnership {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of table %s", tree.Name(tableDesc.GetName()))
	}

	// Check that a vectorizer is not already configured.
	if tableDesc.Vectorizer != nil {
		return nil, pgerror.Newf(pgcode.DuplicateObject,
			"table %s already has a vectorizer configured",
			tree.Name(tableDesc.GetName()))
	}

	// Validate that the specified columns exist.
	for _, colName := range n.Columns {
		col := catalog.FindColumnByTreeName(tableDesc, colName)
		if col == nil {
			return nil, pgerror.Newf(pgcode.UndefinedColumn,
				"column %q does not exist in table %s",
				colName, tree.Name(tableDesc.GetName()))
		}
	}

	return &createVectorizerNode{
		n:         n,
		tableDesc: tableDesc,
		tableName: tn,
	}, nil
}

func (n *createVectorizerNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	// Evaluate WITH options.
	eval := p.ExprEvaluator("CREATE VECTORIZER")
	optMap, err := eval.KVOptions(ctx, n.n.Options, vectorizerOptionValidation)
	if err != nil {
		return err
	}

	model := "all-MiniLM-L6-v2"
	if v, ok := optMap[vectorizerOptModel]; ok {
		model = v
	}
	tmpl := ""
	if v, ok := optMap[vectorizerOptTemplate]; ok {
		tmpl = v
	}
	scheduleCron := "@every 5m"
	if v, ok := optMap[vectorizerOptSchedule]; ok {
		scheduleCron = v
	}
	var batchSize int64 = 64
	if v, ok := optMap[vectorizerOptBatchSize]; ok {
		batchSize, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"invalid batch_size %q: must be an integer", v)
		}
		if batchSize <= 0 {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"batch_size must be positive, got %d", batchSize)
		}
	}

	loadingMode := "column" // default: embed text directly from column values
	if v, ok := optMap[vectorizerOptLoading]; ok {
		switch v {
		case "column", "uri":
			loadingMode = v
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"invalid loading mode %q: must be 'column' or 'uri'", v)
		}
	}
	if loadingMode == "uri" && len(n.n.Columns) != 1 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"URI loading mode requires exactly one column, got %d", len(n.n.Columns))
	}
	if loadingMode == "uri" {
		col := catalog.FindColumnByTreeName(n.tableDesc, n.n.Columns[0])
		if col.GetType().Family() != types.StringFamily {
			return pgerror.Newf(pgcode.DatatypeMismatch,
				"URI loading mode requires a STRING column, got %s",
				col.GetType().SQLString())
		}
	}

	// Collect source column names.
	sourceColumns := make([]string, len(n.n.Columns))
	for i, c := range n.n.Columns {
		sourceColumns[i] = string(c)
	}

	// Collect primary key columns from the source table for the companion.
	pkIdx := n.tableDesc.GetPrimaryIndex()
	pkCols := make([]vectorizer.PKColumn, pkIdx.NumKeyColumns())
	for i := range pkCols {
		colName := pkIdx.GetKeyColumnName(i)
		col, err := catalog.MustFindColumnByName(n.tableDesc, colName)
		if err != nil {
			return errors.Wrap(err, "reading primary key column")
		}
		pkCols[i] = vectorizer.PKColumn{
			Name:    colName,
			TypeSQL: col.GetType().SQLString(),
		}
	}

	// Look up model info from the registry to determine dimensions.
	info, err := embedding.LookupModel(model)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"unknown embedding model %q", model)
	}
	dims := info.Dims

	// For remote models, validate that the external connection exists.
	var connectionName string
	provider, _ := embedding.ParseModelSpec(model)
	if provider != "" {
		connectionName = provider
		row, err := p.InternalSQLTxn().QueryRowEx(
			ctx, "create-vectorizer-check-connection", p.Txn(),
			sessiondata.NodeUserSessionDataOverride,
			"SELECT connection_name FROM system.external_connections WHERE connection_name = $1",
			connectionName,
		)
		if err != nil {
			return errors.Wrap(err, "checking external connection")
		}
		if row == nil {
			return pgerror.Newf(pgcode.UndefinedObject,
				"external connection %q not found; create it with: "+
					"CREATE EXTERNAL CONNECTION %s AS 'https://api.openai.com/v1?api_key=sk-...'",
				connectionName, connectionName)
		}
	}

	// Create the companion embeddings table.
	companionSQL := vectorizer.CreateCompanionTableSQL(n.tableName, pkCols, dims)
	if _, err := p.InternalSQLTxn().ExecEx(
		ctx, "create-vectorizer-companion-table", p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		companionSQL,
	); err != nil {
		return errors.Wrap(err, "creating companion embeddings table")
	}

	// Create the companion view.
	viewSQL := vectorizer.CreateCompanionViewSQL(n.tableName, pkCols)
	if _, err := p.InternalSQLTxn().ExecEx(
		ctx, "create-vectorizer-companion-view", p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		viewSQL,
	); err != nil {
		return errors.Wrap(err, "creating companion embeddings view")
	}

	// Resolve the companion table to get its descriptor ID.
	companionName := vectorizer.CompanionTableName(n.tableName)
	_, companionDesc, err := resolver.ResolveMutableExistingTableObject(
		ctx, p, &companionName, true /* required */, tree.ResolveRequireTableDesc,
	)
	if err != nil {
		return errors.Wrap(err, "resolving companion table descriptor")
	}

	// Create the scheduled job for periodic embedding generation.
	sj, err := createVectorizerSchedule(
		ctx, p, n.tableDesc, scheduleCron,
	)
	if err != nil {
		return errors.Wrap(err, "creating vectorizer schedule")
	}

	// Set the Vectorizer config on the source table descriptor.
	n.tableDesc.Vectorizer = &catpb.Vectorizer{
		SourceColumns:    sourceColumns,
		Template:         tmpl,
		EmbeddingTableID: companionDesc.GetID(),
		Model:            model,
		ScheduleCron:     scheduleCron,
		BatchSize:        batchSize,
		ScheduleID:       sj.ScheduleID(),
		LoadingMode:      loadingMode,
		ConnectionName:   connectionName,
	}

	return p.writeSchemaChange(
		ctx, n.tableDesc, descpb.InvalidMutationID,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

func (n *createVectorizerNode) Next(runParams) (bool, error) { return false, nil }
func (n *createVectorizerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createVectorizerNode) Close(context.Context)        {}

// createVectorizerSchedule creates a scheduled job that periodically spawns
// a vectorizer job to process pending rows.
func createVectorizerSchedule(
	ctx context.Context, p *planner, tblDesc *tabledesc.Mutable, cronExpr string,
) (*jobs.ScheduledJob, error) {
	env := jobs.JobSchedulerEnv(p.ExecCfg().JobsKnobs())
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(
		fmt.Sprintf("vectorizer-%s-%d", tblDesc.GetName(), tblDesc.GetID()),
	)
	sj.SetOwner(p.User())
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait:                   jobspb.ScheduleDetails_SKIP,
		OnError:                jobspb.ScheduleDetails_RETRY_SCHED,
		ClusterID:              p.ExecCfg().NodeInfo.LogicalClusterID(),
		CreationClusterVersion: p.ExecCfg().Settings.Version.ActiveVersion(ctx),
	})
	if err := sj.SetScheduleAndNextRun(cronExpr); err != nil {
		return nil, err
	}

	args := &catpb.ScheduledVectorizerArgs{
		TableID: tblDesc.GetID(),
	}
	anyArgs, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledVectorizerExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: anyArgs},
	)

	storage := jobs.ScheduledJobTxn(p.InternalSQLTxn())
	if err := storage.Create(ctx, sj); err != nil {
		return nil, err
	}
	return sj, nil
}
