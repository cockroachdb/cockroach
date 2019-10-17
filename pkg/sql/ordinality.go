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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	columns     sqlbase.ResultColumns
	reqOrdering ReqOrdering

	run ordinalityRun
}

// ordinalityRun contains the run-time state of ordinalityNode during local execution.
type ordinalityRun struct {
	row    tree.Datums
	curCnt int64
}

func (o *ordinalityNode) startExec(runParams) error {
	return nil
}

func (o *ordinalityNode) Next(params runParams) (bool, error) {
	hasNext, err := o.source.Next(params)
	if !hasNext || err != nil {
		return hasNext, err
	}
	copy(o.run.row, o.source.Values())
	// o.run.row was allocated one spot larger than o.source.Values().
	// Store the ordinality value there.
	o.run.row[len(o.run.row)-1] = tree.NewDInt(tree.DInt(o.run.curCnt))
	o.run.curCnt++
	return true, nil
}

func (o *ordinalityNode) Values() tree.Datums       { return o.run.row }
func (o *ordinalityNode) Close(ctx context.Context) { o.source.Close(ctx) }
