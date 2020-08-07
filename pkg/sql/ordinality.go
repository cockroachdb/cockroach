// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ordinalityNode represents a node that adds an "ordinality" column
// to its child node which numbers the rows it produces. Used to
// support WITH ORDINALITY.
//
// Note that the ordinalityNode produces results that number the *full
// set of original values*, as defined by the upstream data source
// specification. In particular, applying a filter before or after
// an intermediate ordinalityNode will produce different results.
//
// It is inserted in the logical plan between the renderNode and its
// source node, thus earlier than the WHERE filters.
//
// In other words, *ordinalityNode establishes a barrier to many
// common SQL optimizations*. Its use should be limited in clients to
// situations where the corresponding performance cost is affordable.
type ordinalityNode struct {
	source      planNode
	columns     colinfo.ResultColumns
	reqOrdering ReqOrdering
}

func (o *ordinalityNode) startExec(runParams) error {
	panic("ordinalityNode can't be run in local mode")
}

func (o *ordinalityNode) Next(params runParams) (bool, error) {
	panic("ordinalityNode can't be run in local mode")
}

func (o *ordinalityNode) Values() tree.Datums {
	panic("ordinalityNode can't be run in local mode")
}

func (o *ordinalityNode) Close(ctx context.Context) { o.source.Close(ctx) }
