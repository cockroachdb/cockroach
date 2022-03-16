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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) Lock(_ context.Context, _ *tree.Lock) (planNode, error) {
	return &lockNode{}, nil
}

type lockNode struct{}

func (l lockNode) startExec(params runParams) error {
	return nil
}

func (l lockNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (l lockNode) Values() tree.Datums {
	return nil
}

func (l lockNode) Close(ctx context.Context) {
}
