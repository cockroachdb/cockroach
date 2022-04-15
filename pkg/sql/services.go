// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type createServiceNode struct {
	n              *tree.CreateService
	options        []exec.KVOption
	sourcePlan     planNode
	numRowsWritten int
}

func (n *createServiceNode) startExec(params runParams) error {
	// TODO: Use a custom privilege.
	if err := params.p.RequireAdminRole(params.ctx, "CREATE SERVICE"); err != nil {
		return err
	}

	cols := planColumns(n.sourcePlan)
	evalCtx := params.p.EvalContext()

	type entry struct {
		path    string
		options *tree.DJSON
	}
	var rules []entry

	nRowsRead := 0
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return err
		}
		select {
		case <-params.ctx.Done():
			return params.ctx.Err()
		default:
		}
		nRowsRead++
		ok, err := n.sourcePlan.Next(params)
		if err != nil {
			return errors.Wrapf(err, "reading source row %d", nRowsRead)
		}
		if !ok {
			break
		}

		// TODO(knz): memory monitor.
		datums := n.sourcePlan.Values()
		builder := json.NewObjectBuilder(len(datums))
		var pathVal *string
		for i, resultCol := range cols {
			key := resultCol.Name
			val, err := tree.AsJSON(
				datums[i],
				evalCtx.SessionData().DataConversionConfig,
				evalCtx.GetLocation())
			if err != nil {
				return errors.Wrapf(err, "reading source row %d", nRowsRead)
			}
			switch key {
			case "path":
				if val.Type() != json.StringJSONType {
					return errors.Wrapf(
						errors.New("path is not a string"),
						"reading source row %d", nRowsRead)
				}
				pathVal, _ = val.AsText()
			default:
				builder.Add(key, val)
			}
		}
		if pathVal == nil {
			return errors.Wrapf(
				errors.Newf("missing path %+v", cols),
				"reading source row %d", nRowsRead)
		}

		path := *pathVal
		if !strings.HasPrefix(path, "/") {
			// Vhost matching goes via separate config option virtual_host.
			path = "/" + path
		}

		ruleOptions := tree.NewDJSON(builder.Build())
		rules = append(rules, entry{path, ruleOptions})
	}

	onConflictClause := ""
	if n.n.IfNotExists {
		onConflictClause = "ON CONFLICT (service_name) DO NOTHING"
	}

	row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		params.ctx,
		"create-service",
		params.p.txn,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"INSERT INTO system.services"+
			" (service_name, owner, default_options, rules_query)"+
			" VALUES ($1, $2, $3, $4) "+
			onConflictClause+
			" RETURNING service_id",
		string(n.n.Service),
		params.p.User().Normalized(),
		tree.NewDJSON(json.NullJSONValue /* FIXME */),
		tree.Serialize(n.n.Rules),
	)
	if err != nil {
		return err
	}
	if row == nil {
		// There was already a service with this name and IF NOT EXISTS was specified.
		// Do nothing.
		return nil
	}
	n.numRowsWritten++

	serviceID, ok := row[0].(*tree.DUuid)
	if !ok {
		return errors.AssertionFailedf("expected UUID service_id, got %T", row[0])
	}

	for _, e := range rules {
		if err := params.p.cancelChecker.Check(); err != nil {
			return err
		}
		select {
		case <-params.ctx.Done():
			return params.ctx.Err()
		default:
		}
		_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"insert-service-rule",
			params.p.txn,
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"INSERT INTO system.service_rules (service_id, route, options) VALUES ($1, $2, $3)",
			serviceID, e.path, e.options)
		if err != nil {
			return err
		}
		n.numRowsWritten++
	}

	return nil
}

func (n *createServiceNode) FastPathResults() (int, bool) {
	return n.numRowsWritten, true
}
func (*createServiceNode) Next(runParams) (bool, error) { return false, nil }
func (*createServiceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createServiceNode) Close(ctx context.Context) {
	n.sourcePlan.Close(ctx)
}

type alterServiceNode struct {
	n          *tree.AlterService
	options    []exec.KVOption
	sourcePlan planNode
}

func (n *alterServiceNode) startExec(params runParams) error {
	return errors.AssertionFailedf("unimplemented")
}

func (*alterServiceNode) Next(runParams) (bool, error) { return false, nil }
func (*alterServiceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterServiceNode) Close(ctx context.Context) {
	n.sourcePlan.Close(ctx)
}

func (p *planner) DropService(ctx context.Context, n *tree.DropService) (planNode, error) {
	return &dropServiceNode{
		n: n,
	}, nil
}

type dropServiceNode struct {
	n              *tree.DropService
	numRowsDeleted int
}

func (n *dropServiceNode) startExec(params runParams) error {
	// TODO: Check ownership of service using the owner information.
	if err := params.p.RequireAdminRole(params.ctx, "DROP SERVICE"); err != nil {
		return err
	}

	for _, name := range n.n.Names {
		// Retrieve the service ID for this name.
		row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
			params.ctx,
			"get-service-id",
			params.p.txn,
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"SELECT service_id FROM system.services WHERE service_name=$1", string(name))
		if err != nil {
			return err
		}
		if row == nil {
			if n.n.IfExists {
				continue
			} else {
				return errors.Newf("service %q does not exist", name)
			}
		}
		serviceID := row[0]

		rowsAffected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-service",
			params.p.txn,
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"DELETE FROM system.services WHERE service_id=$1",
			serviceID)
		if err != nil {
			return err
		}
		n.numRowsDeleted += rowsAffected
		rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-service-rules",
			params.p.txn,
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"DELETE FROM system.service_rules WHERE service_id=$1",
			serviceID)
		if err != nil {
			return err
		}
		n.numRowsDeleted += rowsAffected
	}

	return nil
}

func (n *dropServiceNode) FastPathResults() (int, bool) {
	return n.numRowsDeleted, true
}
func (*dropServiceNode) Next(runParams) (bool, error) { return false, nil }
func (*dropServiceNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropServiceNode) Close(context.Context)        {}
