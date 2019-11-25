// Copyright 2017 The Cockroach Authors.
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

type createSchemaNode struct {
	n *tree.CreateSchema
}

func (n *createSchemaNode) startExec(params runParams) error {
	if !n.n.IfNotExists {
		return unimplemented.NewWithIssuef(26443,
			"new schemas are unsupported")
	}
	if n.n.Schema == tree.PublicSchema {
		return nil
	}
	for _, virtualSchema := range virtualSchemas {
		if n.n.Schema == virtualSchema.name {
			return nil
		}
	}
	return unimplemented.NewWithIssuef(26443,
		"new schemas are unsupported")
}

func (*createSchemaNode) Next(runParams) (bool, error) { return false, nil }
func (*createSchemaNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createSchemaNode) Close(ctx context.Context)  {}

// CreateSchema creates a schema. Currently only works in IF NOT EXISTS mode,
// for schemas that do in fact already exist.
func (p *planner) CreateSchema(ctx context.Context, n *tree.CreateSchema) (planNode, error) {
	return &createSchemaNode{
		n: n,
	}, nil
}
