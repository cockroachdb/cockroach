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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type dropSchemaNode struct {
	n *tree.DropSchema
}

// Use to satisfy the linter.
var _ planNode = &dropSchemaNode{n: nil}

func (p *planner) DropSchema(ctx context.Context, n *tree.DropSchema) (planNode, error) {
	return nil, unimplemented.NewWithIssue(50884, "DROP SCHEMA")
}

func (n *dropSchemaNode) startExec(params runParams) error {
	return errors.AssertionFailedf("unimplemented")
}

func (n *dropSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropSchemaNode) Close(ctx context.Context)           {}
func (n *dropSchemaNode) ReadingOwnWrites()                   {}
