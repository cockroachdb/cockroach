// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type intermediateResultsLogger struct {
	OneInputNode
	NonExplainable

	componentID string
	typs        []types.T
	da          sqlbase.DatumAlloc
	row         tree.Datums
}

var _ Operator = &intermediateResultsLogger{}

func newIntermediateResultsLogger(input Operator, typs []types.T, componentID string) Operator {
	return &intermediateResultsLogger{
		OneInputNode: NewOneInputNode(input),
		componentID:  componentID,
		typs:         typs,
		row:          make(tree.Datums, len(typs)),
	}
}

func (n *intermediateResultsLogger) Init() {
	n.input.Init()
}

func (n *intermediateResultsLogger) Next(ctx context.Context) coldata.Batch {
	b := n.input.Next(ctx)
	if b.Length() > 0 {
		for rowIdx := uint16(0); rowIdx < b.Length(); rowIdx++ {
			for colIdx, colVec := range b.ColVecs() {
				n.row[colIdx] = PhysicalTypeColElemToDatum(colVec, rowIdx, n.da, &n.typs[colIdx])
			}
			log.Infof(ctx, "%s: %v", n.componentID, n.row.String())
		}
	}
	return b
}
