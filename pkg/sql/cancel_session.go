// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type cancelSessionNode struct {
	sessionID tree.TypedExpr
}

func (p *planner) CancelSession(ctx context.Context, n *tree.CancelSession) (planNode, error) {
	typedSessionID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		tree.IndexedVarHelper{},
		types.String,
		true, /* requireType */
		"CANCEL SESSION",
	)
	if err != nil {
		return nil, err
	}

	return &cancelSessionNode{
		sessionID: typedSessionID,
	}, nil
}

func (n *cancelSessionNode) Next(runParams) (bool, error) { return false, nil }
func (*cancelSessionNode) Values() tree.Datums            { return nil }
func (*cancelSessionNode) Close(context.Context)          {}
