// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/gogo/protobuf/proto"
)

type alterViewSetOptionsNode struct {
	zeroInputPlanNode
	n      *tree.AlterViewSetOptions
	desc   *tabledesc.Mutable
	prefix catalog.ResolvedObjectPrefix
}

// AlterViewSetOptions sets options on a view.
func (p *planner) AlterViewSetOptions(
	ctx context.Context, n *tree.AlterViewSetOptions,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER VIEW SET",
	); err != nil {
		return nil, err
	}

	if !p.execCfg.Settings.Version.IsActive(ctx, clusterversion.V26_2) &&
		n.Options != nil && n.Options.SecurityInvoker {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"security invoker views are not supported")
	}

	tn := n.Name.ToTableName()
	prefix, viewDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tn, !n.IfExists, tree.ResolveRequireViewDesc)
	if err != nil {
		return nil, err
	}
	if viewDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := checkViewMatchesMaterialized(viewDesc, true /* requireView */, false /* wantMaterialized */); err != nil {
		return nil, err
	}

	// todo (shadi): What's the correct privilege check here?
	if err := p.CheckPrivilege(ctx, viewDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &alterViewSetOptionsNode{
		n:      n,
		desc:   viewDesc,
		prefix: prefix,
	}, nil
}

func (n *alterViewSetOptionsNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra(
		"view", n.n.TelemetryName(),
	))

	n.desc.SecurityInvoker = proto.Bool(n.n.Options.SecurityInvoker)

	return params.p.writeSchemaChange(
		params.ctx, n.desc, descpb.InvalidMutationID,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

func (n *alterViewSetOptionsNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterViewSetOptionsNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterViewSetOptionsNode) Close(context.Context)        {}
