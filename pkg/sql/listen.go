// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type listenNode struct {
	n *tree.Listen
}

func (l listenNode) startExec(params runParams) error {
	chanName := string(l.n.ChanName)
	registry := params.p.execCfg.PgListenerRegistry
	sessionID := params.extendedEvalCtx.SessionID.Uint128
	if l.n.Unlisten {
		if l.n.UnlistenAll {
			registry.UnlistenAll(sessionID)
		} else {
			registry.Unlisten(sessionID, chanName)
		}
		return nil
	}
	registry.Listen(sessionID, chanName)
	return nil
}

func (l listenNode) Next(_ runParams) (bool, error) { return false, nil }
func (l listenNode) Values() tree.Datums            { return nil }
func (l listenNode) Close(_ context.Context)        {}

// Listen represents a LISTEN statement.
func (p *planner) Listen(ctx context.Context, n *tree.Listen) (planNode, error) {
	return &listenNode{n: n}, nil
}
