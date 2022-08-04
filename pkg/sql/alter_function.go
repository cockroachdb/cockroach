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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type alterFunctionOptionsNode struct {
	n *tree.AlterFunctionOptions
}

type alterFunctionRenameNode struct {
	n *tree.AlterFunctionRename
}

type alterFunctionSetOwnerNode struct {
	n *tree.AlterFunctionSetOwner
}

type alterFunctionSetSchemaNode struct {
	n *tree.AlterFunctionSetSchema
}

type alterFunctionDepExtensionNode struct {
	n *tree.AlterFunctionDepExtension
}

// AlterFunctionOptions alters a function's options.
func (p *planner) AlterFunctionOptions(
	ctx context.Context, n *tree.AlterFunctionOptions,
) (planNode, error) {
	return &alterFunctionOptionsNode{n: n}, nil
}

func (n *alterFunctionOptionsNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function not supported")
}

func (n *alterFunctionOptionsNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionOptionsNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionOptionsNode) Close(ctx context.Context)           {}

// AlterFunctionRename renames a function.
func (p *planner) AlterFunctionRename(
	ctx context.Context, n *tree.AlterFunctionRename,
) (planNode, error) {
	return &alterFunctionRenameNode{n: n}, nil
}

func (n *alterFunctionRenameNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function rename to not supported")
}

func (n *alterFunctionRenameNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionRenameNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionRenameNode) Close(ctx context.Context)           {}

// AlterFunctionSetOwner sets a function's owner.
func (p *planner) AlterFunctionSetOwner(
	ctx context.Context, n *tree.AlterFunctionSetOwner,
) (planNode, error) {
	return &alterFunctionSetOwnerNode{n: n}, nil
}

func (n *alterFunctionSetOwnerNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function owner to not supported")
}

func (n *alterFunctionSetOwnerNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionSetOwnerNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionSetOwnerNode) Close(ctx context.Context)           {}

// AlterFunctionSetSchema moves a function to another schema.
func (p *planner) AlterFunctionSetSchema(
	ctx context.Context, n *tree.AlterFunctionSetSchema,
) (planNode, error) {
	return &alterFunctionSetSchemaNode{n: n}, nil
}

func (n *alterFunctionSetSchemaNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function set schema not supported")
}

func (n *alterFunctionSetSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionSetSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionSetSchemaNode) Close(ctx context.Context)           {}

// AlterFunctionDepExtension alters a function dependency on an extension.
func (p *planner) AlterFunctionDepExtension(
	ctx context.Context, n *tree.AlterFunctionDepExtension,
) (planNode, error) {
	return &alterFunctionDepExtensionNode{n: n}, nil
}

func (n *alterFunctionDepExtensionNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function depends on extension not supported")
}

func (n *alterFunctionDepExtensionNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionDepExtensionNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionDepExtensionNode) Close(ctx context.Context)           {}
