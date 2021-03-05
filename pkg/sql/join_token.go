// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// FeatureTLSAutoJoinEnabled is used to enable and disable the TLS auto-join
// feature.
var FeatureTLSAutoJoinEnabled = settings.RegisterBoolSetting(
	"feature.tls_auto_join.enabled",
	"set to true to enable tls auto join through join tokens, false to disable; default is false",
	false,
)

type createJoinTokenNode struct {
	optColumnsSlot

	status serverpb.NodesStatusServer
	token  string
	nexted bool
}

func (c *createJoinTokenNode) startExec(params runParams) error {
	token, err := c.status.GenerateJoinToken(params.ctx)
	if err != nil {
		return err
	}
	c.token = token
	return nil
}

func (c *createJoinTokenNode) Next(runParams) (bool, error) {
	if len(c.token) > 0 && !c.nexted {
		c.nexted = true
		return true, nil
	}
	return false, nil
}

func (c *createJoinTokenNode) Close(context.Context) {
	c.token = ""
	c.nexted = false
}

func (c *createJoinTokenNode) Values() tree.Datums {
	if len(c.token) == 0 {
		return tree.Datums{tree.DNull}
	}
	return tree.Datums{tree.NewDString(c.token)}
}

// CreateJoinToken creates a join token creation node.
func (p *planner) CreateJoinToken(ctx context.Context, _ *tree.CreateJoinToken) (planNode, error) {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	if !hasAdmin {
		return nil, pgerror.New(pgcode.InsufficientPrivilege, "must be superuser to create join token")
	}
	if err := featureflag.CheckEnabled(
		ctx, p.ExecCfg(), FeatureTLSAutoJoinEnabled, "TLS auto join"); err != nil {
		return nil, pgerror.New(
			pgcode.FeatureNotSupported,
			err.Error(),
		)
	}
	if n, err := p.extendedEvalCtx.NodesStatusServer.OptionalNodesStatusServer(47900); err == nil && n != nil {
		return &createJoinTokenNode{
			status: n,
		}, nil
	}
	return nil, pgerror.New(
		pgcode.FeatureNotSupported,
		"unsupported statement",
	)
}
