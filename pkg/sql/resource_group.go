// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const resourceGroupOp = "resource-group"

// TODO(ssd): replace HasAdminRole with a finer-grained privilege when
// the resource manager goes GA.
func checkResourceGroupsEnabled(ctx context.Context, p *planner) error {
	// The system.resource_groups table and its sequence are only present
	// once the V26_3_AddResourceGroupsTable migration has run. Reject
	// statements until the cluster upgrade is finalized so callers see a
	// clear error rather than "relation does not exist".
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V26_3_AddResourceGroupsTable) {
		return pgerror.New(pgcode.FeatureNotSupported,
			"resource group SQL is not available until the cluster upgrade to v26.3 is finalized")
	}
	if !experimentalResourceGroupsEnabled.Get(&p.ExecCfg().Settings.SV) {
		return errors.WithHint(
			pgerror.New(pgcode.FeatureNotSupported,
				"resource group SQL is disabled by the "+
					"sql.experimental_resource_groups.enabled cluster setting"),
			"to enable, run: "+
				"SET CLUSTER SETTING sql.experimental_resource_groups.enabled = true")
	}
	return nil
}

func requireAdminForResourceGroups(ctx context.Context, p *planner) error {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if !hasAdmin {
		return pgerror.New(pgcode.InsufficientPrivilege,
			"only users with the admin role may use resource group statements")
	}
	return nil
}

const (
	resourceGroupOptCPUWeight = "cpu_weight"
	resourceGroupOptMaxCPU    = "max_cpu"
)

// applyResourceGroupOptions mutates cfg by applying each option in opts,
// leaving fields whose keys do not appear in opts unchanged.
func applyResourceGroupOptions(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	cfg *admissionpb.ResourceGroupConfig,
	opts tree.KVOptions,
) error {
	evalOpt := func(expr tree.Expr) (tree.Datum, error) {
		typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, types.AnyElement)
		if err != nil {
			return nil, err
		}
		return eval.Expr(ctx, evalCtx, typedExpr)
	}
	seen := make(map[string]struct{}, len(opts))
	for _, opt := range opts {
		key := string(opt.Key)
		if _, dup := seen[key]; dup {
			return pgerror.Newf(pgcode.Syntax, "%s specified multiple times", key)
		}
		switch key {
		case resourceGroupOptCPUWeight:
			datum, err := evalOpt(opt.Value)
			if err != nil {
				return errors.Wrapf(err, "evaluating %s", key)
			}
			v, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if v <= 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"%s must be a positive integer, got %d", key, v)
			}
			cfg.CPUWeight = v
		case resourceGroupOptMaxCPU:
			datum, err := evalOpt(opt.Value)
			if err != nil {
				return errors.Wrapf(err, "evaluating %s", key)
			}
			b, err := paramparse.DatumAsBool(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			cfg.MaxCPU = b
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"unknown resource group option %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

type createResourceGroupNode struct {
	zeroInputPlanNode
	n *tree.CreateResourceGroup
}

func (p *planner) CreateResourceGroup(
	_ context.Context, n *tree.CreateResourceGroup,
) (planNode, error) {
	return &createResourceGroupNode{n: n}, nil
}

func (c *createResourceGroupNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	if err := checkResourceGroupsEnabled(ctx, p); err != nil {
		return err
	}
	if err := requireAdminForResourceGroups(ctx, p); err != nil {
		return err
	}

	var cfg admissionpb.ResourceGroupConfig
	if err := applyResourceGroupOptions(ctx, p.SemaCtx(), p.EvalContext(), &cfg, c.n.Options); err != nil {
		return err
	}
	if cfg.CPUWeight <= 0 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"%s is required and must be a positive integer", resourceGroupOptCPUWeight)
	}
	configBytes, err := protoutil.Marshal(&cfg)
	if err != nil {
		return errors.Wrap(err, "marshaling resource group config")
	}

	row, err := p.InternalSQLTxn().QueryRowEx(
		ctx, resourceGroupOp, p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT nextval('system.resource_group_id_seq')`,
	)
	if err != nil {
		return errors.Wrap(err, "allocating resource group id")
	}
	id := int64(tree.MustBeDInt(row[0]))

	name := string(c.n.Name)
	if _, err := p.InternalSQLTxn().ExecEx(
		ctx, resourceGroupOp, p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.resource_groups (id, name, config) VALUES ($1, $2, $3)`,
		id, name, configBytes,
	); err != nil {
		// On duplicate-name collision, give a friendlier error or no-op
		// for IF NOT EXISTS.
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			if c.n.IfNotExists {
				return nil
			}
			return pgerror.Newf(pgcode.DuplicateObject,
				"resource group %q already exists", name)
		}
		return errors.Wrap(err, "creating resource group")
	}
	return nil
}

func (c *createResourceGroupNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *createResourceGroupNode) Values() tree.Datums            { return nil }
func (c *createResourceGroupNode) Close(_ context.Context)        {}

type alterResourceGroupNode struct {
	zeroInputPlanNode
	n *tree.AlterResourceGroup
}

func (p *planner) AlterResourceGroup(
	_ context.Context, n *tree.AlterResourceGroup,
) (planNode, error) {
	return &alterResourceGroupNode{n: n}, nil
}

func (a *alterResourceGroupNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	if err := checkResourceGroupsEnabled(ctx, p); err != nil {
		return err
	}
	if err := requireAdminForResourceGroups(ctx, p); err != nil {
		return err
	}

	name := string(a.n.Name)
	row, err := p.InternalSQLTxn().QueryRowEx(
		ctx, resourceGroupOp, p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT config FROM system.resource_groups WHERE name = $1`, name,
	)
	if err != nil {
		return errors.Wrap(err, "loading resource group")
	}
	if row == nil {
		if a.n.IfExists {
			return nil
		}
		return pgerror.Newf(pgcode.UndefinedObject,
			"resource group %q does not exist", name)
	}

	var cfg admissionpb.ResourceGroupConfig
	if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(row[0])), &cfg); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			"decoding resource group config for %q", name)
	}
	if err := applyResourceGroupOptions(ctx, p.SemaCtx(), p.EvalContext(), &cfg, a.n.Options); err != nil {
		return err
	}
	configBytes, err := protoutil.Marshal(&cfg)
	if err != nil {
		return errors.Wrap(err, "marshaling resource group config")
	}
	if _, err := p.InternalSQLTxn().ExecEx(
		ctx, resourceGroupOp, p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.resource_groups SET config = $1 WHERE name = $2`,
		configBytes, name,
	); err != nil {
		return errors.Wrap(err, "updating resource group")
	}
	return nil
}

func (a *alterResourceGroupNode) Next(_ runParams) (bool, error) { return false, nil }
func (a *alterResourceGroupNode) Values() tree.Datums            { return nil }
func (a *alterResourceGroupNode) Close(_ context.Context)        {}

type dropResourceGroupNode struct {
	zeroInputPlanNode
	n *tree.DropResourceGroup
}

func (p *planner) DropResourceGroup(
	_ context.Context, n *tree.DropResourceGroup,
) (planNode, error) {
	return &dropResourceGroupNode{n: n}, nil
}

func (d *dropResourceGroupNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	if err := checkResourceGroupsEnabled(ctx, p); err != nil {
		return err
	}
	if err := requireAdminForResourceGroups(ctx, p); err != nil {
		return err
	}

	name := string(d.n.Name)
	row, err := p.InternalSQLTxn().QueryRowEx(
		ctx, resourceGroupOp, p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.resource_groups WHERE name = $1 RETURNING id`, name,
	)
	if err != nil {
		return errors.Wrap(err, "dropping resource group")
	}
	if row == nil && !d.n.IfExists {
		return pgerror.Newf(pgcode.UndefinedObject,
			"resource group %q does not exist", name)
	}
	return nil
}

func (d *dropResourceGroupNode) Next(_ runParams) (bool, error) { return false, nil }
func (d *dropResourceGroupNode) Values() tree.Datums            { return nil }
func (d *dropResourceGroupNode) Close(_ context.Context)        {}

var showResourceGroupColumns = colinfo.ResultColumns{
	{Name: "id", Typ: types.Int},
	{Name: "name", Typ: types.String},
	{Name: "cpu_weight", Typ: types.Int},
	{Name: "max_cpu", Typ: types.Bool},
	{Name: "cpu_share_percent", Typ: types.Float},
}

// showResourceGroupsImpl backs both SHOW RESOURCE GROUP <name> and
// SHOW RESOURCE GROUPS. An empty name lists all groups.
//
// The cpu_share_percent column is derived: every row is loaded so
// admissionpb.Normalize can compute each group's share from the full
// set of weights, even when only one row is being returned.
func (p *planner) showResourceGroupsImpl(name string) (planNode, error) {
	planName := "SHOW RESOURCE GROUPS"
	if name != "" {
		planName = fmt.Sprintf("SHOW RESOURCE GROUP %s", name)
	}
	return &delayedNode{
		name:    planName,
		columns: showResourceGroupColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			if err := checkResourceGroupsEnabled(ctx, p); err != nil {
				return nil, err
			}
			if err := requireAdminForResourceGroups(ctx, p); err != nil {
				return nil, err
			}

			// Load every row, even when a single name was requested:
			// share is a function of the entire set of weights.
			it, err := p.InternalSQLTxn().QueryIteratorEx(
				ctx, resourceGroupOp, p.Txn(),
				sessiondata.NodeUserSessionDataOverride,
				`SELECT id, name, config FROM system.resource_groups ORDER BY id`,
			)
			if err != nil {
				return nil, errors.Wrap(err, "loading resource groups")
			}
			defer func() { _ = it.Close() }()

			type rgRow struct {
				id   tree.Datum
				name tree.Datum
				cfg  admissionpb.ResourceGroupConfig
			}
			var rows []rgRow
			for {
				ok, err := it.Next(ctx)
				if err != nil {
					return nil, err
				}
				if !ok {
					break
				}
				cur := it.Cur()
				var cfg admissionpb.ResourceGroupConfig
				if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(cur[2])), &cfg); err != nil {
					return nil, errors.NewAssertionErrorWithWrappedErrf(err,
						"decoding resource group config for row %v", cur[0])
				}
				rows = append(rows, rgRow{id: cur[0], name: cur[1], cfg: cfg})
			}

			cfgs := make([]admissionpb.ResourceGroupConfig, len(rows))
			for i, r := range rows {
				cfgs[i] = r.cfg
			}
			admissionpb.Normalize(cfgs)

			v := p.newContainerValuesNode(showResourceGroupColumns, 0)
			matched := false
			for i, r := range rows {
				if name != "" && string(tree.MustBeDString(r.name)) != name {
					continue
				}
				matched = true
				out := tree.Datums{
					r.id,
					r.name,
					tree.NewDInt(tree.DInt(cfgs[i].CPUWeight)),
					tree.MakeDBool(tree.DBool(cfgs[i].MaxCPU)),
					tree.NewDFloat(tree.DFloat(cfgs[i].BurstFrac * 100)),
				}
				if _, err := v.rows.AddRow(ctx, out); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			// SHOW RESOURCE GROUP <name> errors if the named group does not
			// exist, matching SHOW HISTOGRAM / SHOW CREATE TABLE. SHOW
			// RESOURCE GROUPS (no name) is allowed to return zero rows.
			if name != "" && !matched {
				v.Close(ctx)
				return nil, pgerror.Newf(pgcode.UndefinedObject,
					"resource group %q does not exist", name)
			}
			return v, nil
		},
	}, nil
}

func (p *planner) ShowResourceGroup(
	_ context.Context, n *tree.ShowResourceGroup,
) (planNode, error) {
	return p.showResourceGroupsImpl(string(n.Name))
}

func (p *planner) ShowResourceGroups(
	_ context.Context, n *tree.ShowResourceGroups,
) (planNode, error) {
	return p.showResourceGroupsImpl("")
}
