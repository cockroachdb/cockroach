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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type createServiceNode struct {
	n          *tree.CreateService
	options    []exec.KVOption
	sourcePlan planNode
}

func (n *createServiceNode) startExec(params runParams) error {
	return errors.AssertionFailedf("unimplemented")
}

func (*createServiceNode) Next(runParams) (bool, error) { return false, nil }
func (*createServiceNode) Values() tree.Datums          { return tree.Datums{} }
func (*createServiceNode) Close(context.Context)        {}

type alterServiceNode struct {
	n          *tree.AlterService
	options    []exec.KVOption
	sourcePlan planNode
}

func (n *alterServiceNode) startExec(params runParams) error {
	return errors.AssertionFailedf("unimplemented")
}

func (*alterServiceNode) Next(runParams) (bool, error) { return false, nil }
func (*alterServiceNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterServiceNode) Close(context.Context)        {}

func (p *planner) DropService(ctx context.Context, n *tree.DropService) (planNode, error) {
	return &dropServiceNode{
		n: n,
	}, nil
}

type dropServiceNode struct {
	n *tree.DropService
}

func (n *dropServiceNode) startExec(params runParams) error {
	return errors.AssertionFailedf("unimplemented")
}

func (*dropServiceNode) Next(runParams) (bool, error) { return false, nil }
func (*dropServiceNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropServiceNode) Close(context.Context)        {}
