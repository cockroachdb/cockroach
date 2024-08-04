// Copyright 2024 The Cockroach Authors.
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

type listenNode struct {
	n *tree.Listen
}

func (ln *listenNode) Close(_ context.Context)             {}
func (ln *listenNode) Next(params runParams) (bool, error) { return false, nil }
func (ln *listenNode) Values() tree.Datums                 { return nil }
func (ln *listenNode) startExec(params runParams) error {
	registry := params.p.execCfg.PGListenerRegistry
	sessionID := params.extendedEvalCtx.SessionID
	registry.AddListener(params.ctx, notify.ListenerID(sessionID), ln.n.ChannelName.String(), params.p.notificationSender)
	return nil
}

var _ planNode = &listenNode{}

func (p *planner) Listen(ctx context.Context, n *tree.Listen) (planNode, error) {
	return &listenNode{n: n}, nil
}
