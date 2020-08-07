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

// zeroNode is a planNode with no columns and no rows and is used for nodes that
// have no results. (e.g. a table for which the filtering condition has a
// contradiction).
type zeroNode struct {
	columns colinfo.ResultColumns
}

func newZeroNode(columns colinfo.ResultColumns) *zeroNode {
	return &zeroNode{columns: columns}
}

func (*zeroNode) startExec(runParams) error    { return nil }
func (*zeroNode) Next(runParams) (bool, error) { return false, nil }
func (*zeroNode) Values() tree.Datums          { return nil }
func (*zeroNode) Close(context.Context)        {}
