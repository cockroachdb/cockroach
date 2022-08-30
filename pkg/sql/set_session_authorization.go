// Copyright 2019 The Cockroach Authors.
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

func (p *planner) SetSessionAuthorizationDefault() (planNode, error) {
	return &setSessionAuthorizationDefaultNode{}, nil
}

type setSessionAuthorizationDefaultNode struct{}

func (n *setSessionAuthorizationDefaultNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setSessionAuthorizationDefaultNode) Values() tree.Datums            { return nil }
func (n *setSessionAuthorizationDefaultNode) Close(_ context.Context)        {}
func (n *setSessionAuthorizationDefaultNode) startExec(params runParams) error {
	// This is currently the same as `SET ROLE = DEFAULT`, which means that it
	// only changes the "current user." In Postgres, `SET SESSION AUTHORIZATION`
	// also changes the "session user," but since the session user cannot be
	// modified in CockroachDB (at the time of writing), we just need to change
	// the current user here.
	return params.p.setRole(params.ctx, false /* local */, params.p.SessionData().SessionUser())
}
