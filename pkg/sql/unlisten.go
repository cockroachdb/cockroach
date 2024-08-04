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

	"github.com/cockroachdb/cockroach/pkg/sql/notify"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type unlistenNode struct {
	n *tree.Unlisten
}

func (un *unlistenNode) Close(_ context.Context)             {}
func (un *unlistenNode) Next(params runParams) (bool, error) { return false, nil }
func (un *unlistenNode) Values() tree.Datums                 { return nil }
func (un *unlistenNode) startExec(params runParams) error {
	registry := params.p.execCfg.PGListenerRegistry
	sessionID := params.extendedEvalCtx.SessionID
	registry.RemoveListener(params.ctx, notify.ListenerID(sessionID), un.n.ChannelName.String())
	return nil
}

var _ planNode = &unlistenNode{}

func (p *planner) Unlisten(ctx context.Context, n *tree.Unlisten) (planNode, error) {
	return &unlistenNode{n: n}, nil
}
