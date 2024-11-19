// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	if un.n.Star {
		registry.RemoveAllListeners(params.ctx, notify.ListenerID(sessionID))
	} else {
		registry.RemoveListener(params.ctx, notify.ListenerID(sessionID), un.n.ChannelName.String())
	}
	return nil
}

var _ planNode = &unlistenNode{}

func (p *planner) Unlisten(ctx context.Context, n *tree.Unlisten) (planNode, error) {
	return &unlistenNode{n: n}, nil
}
