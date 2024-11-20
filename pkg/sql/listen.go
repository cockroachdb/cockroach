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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type listenNode struct {
	n *tree.Listen
}

func (ln *listenNode) Close(_ context.Context)             {}
func (ln *listenNode) Next(params runParams) (bool, error) { return false, nil }
func (ln *listenNode) Values() tree.Datums                 { return nil }
func (ln *listenNode) startExec(params runParams) error {
	// TODO: It's probably possible to support running LISTEN in a txn, I just don't know how to do it. Ditto for UNLISTEN.
	if !params.extendedEvalCtx.TxnImplicit {
		return unimplemented.New("listen", "cannot be used inside a transaction")
	}

	registry := params.p.execCfg.PGListenerRegistry
	sessionID := params.extendedEvalCtx.SessionID
	sessionData := params.SessionData().SessionData
	err := registry.StartListener(
		params.ctx,
		listenerID(sessionID),
		ln.n.ChannelName.Normalize(),
		params.p.notificationSender,
		&sessionData,
		hlc.Timestamp{WallTime: params.EvalContext().StmtTimestamp.UnixNano()}, // ?
		params.p,
	)
	if err != nil {
		return err
	}
	return nil
}

var _ planNode = &listenNode{}

func (p *planner) Listen(ctx context.Context, n *tree.Listen) (planNode, error) {
	return &listenNode{n: n}, nil
}
