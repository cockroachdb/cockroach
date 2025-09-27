// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type inspectNode struct {
	zeroInputPlanNode

	n *tree.Inspect
}

// Inspect checks the database.
// Privileges: INSPECT.
func (p *planner) Inspect(ctx context.Context, n *tree.Inspect) (planNode, error) {
	if !p.extendedEvalCtx.Settings.Version.IsActive(ctx, clusterversion.V25_4) {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "SHOW INSPECT ERRORS requires the cluster to be upgraded to v25.4")
	}

	if err := p.CheckGlobalPrivilegeOrRoleOption(ctx, privilege.INSPECT); err != nil {
		return nil, err
	}

	switch n.Typ {
	case tree.InspectTable:
		tableName := n.Table.ToTableName()
		_, _, err := p.ResolveMutableTableDescriptor(ctx, &tableName, true /* required */, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}
	case tree.InspectDatabase:
		_, err := p.Descriptors().ByName(p.txn).Get().Database(ctx, n.Database.ToUnresolvedName().String())
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", n.Typ)
	}

	return &inspectNode{n: n}, nil
}

func (n *inspectNode) startExec(params runParams) error {
	switch n.n.Typ {
	case tree.InspectTable:
		if !params.p.extendedEvalCtx.SessionData().EnableInspectCommand {
			return unimplemented.New("INSPECT TABLE", "INSPECT TABLE requires the enable_inspect_command setting to be enabled")
		}
	case tree.InspectDatabase:
		if !params.p.extendedEvalCtx.SessionData().EnableInspectCommand {
			return unimplemented.New("INSPECT DATABASE", "INSPECT DATABASE requires the enable_inspect_command setting to be enabled")
		}
	default:
		return errors.AssertionFailedf("unexpected INSPECT type received, got: %v", n.n.Typ)
	}

	return nil
}

func (n *inspectNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *inspectNode) Values() tree.Datums {
	return nil
}

func (n *inspectNode) Close(ctx context.Context) {
}
