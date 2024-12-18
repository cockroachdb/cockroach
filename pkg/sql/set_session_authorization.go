// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) SetSessionAuthorizationDefault() (planNode, error) {
	return &setSessionAuthorizationDefaultNode{}, nil
}

type setSessionAuthorizationDefaultNode struct {
	zeroInputPlanNode
}

func (n *setSessionAuthorizationDefaultNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setSessionAuthorizationDefaultNode) Values() tree.Datums            { return nil }
func (n *setSessionAuthorizationDefaultNode) Close(_ context.Context)        {}
func (n *setSessionAuthorizationDefaultNode) startExec(params runParams) error {
	// This is currently the same as `SET ROLE = DEFAULT`, which means that it
	// only changes the "current user." In Postgres, `SET SESSION AUTHORIZATION`
	// also changes the "session user," but since the session user cannot be
	// modified in CockroachDB (at the time of writing), we just need to change
	// the current user here.
	// NOTE: If in the future we do allow the session user to be modified, we
	// should still track the original logged-user, and use that for the audit
	// logs written by event_log.
	return params.p.setRole(params.ctx, false /* local */, params.p.SessionData().SessionUser())
}
