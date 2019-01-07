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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// rowSourceToPlanNode wraps a RowSource and presents it as a PlanNode. It must
// be constructed with Create(), after which it is a PlanNode and can be treated
// as such.
type rowSourceToPlanNode struct {
	source    distsqlrun.RowSource
	forwarder metadataForwarder

	originalPlanNode planNode

	planCols sqlbase.ResultColumns

	// Temporary variables
	row      sqlbase.EncDatumRow
	da       sqlbase.DatumAlloc
	datumRow tree.Datums
}

var _ planNode = &rowSourceToPlanNode{}

// makeRowSourceToPlanNode creates a new planNode that wraps a RowSource. It
// takes an optional metadataForwarder, which if non-nil is invoked for every
// piece of metadata this wrapper receives from the wrapped RowSource.
// It also takes an optional planNode, which is the planNode that the RowSource
// that this rowSourceToPlanNode is wrapping originally replaced. That planNode
// will be closed when this one is closed.
func makeRowSourceToPlanNode(
	s distsqlrun.RowSource,
	forwarder metadataForwarder,
	planCols sqlbase.ResultColumns,
	originalPlanNode planNode,
) *rowSourceToPlanNode {
	row := make(tree.Datums, len(planCols))

	return &rowSourceToPlanNode{
		source:           s,
		datumRow:         row,
		forwarder:        forwarder,
		planCols:         planCols,
		originalPlanNode: originalPlanNode,
	}
}

func (r *rowSourceToPlanNode) startExec(params runParams) error {
	r.source.Start(params.ctx)
	return nil
}

func (r *rowSourceToPlanNode) Next(params runParams) (bool, error) {
	for {
		var p *distsqlrun.ProducerMetadata
		r.row, p = r.source.Next()

		if p != nil {
			if p.Err != nil {
				return false, p.Err
			}
			if r.forwarder != nil {
				r.forwarder.forwardMetadata(p)
				continue
			}
			if p.TraceData != nil {
				// We drop trace metadata since we have no reasonable way to propagate
				// it in local SQL execution.
				continue
			}
			return false, fmt.Errorf("unexpected producer metadata: %+v", p)
		}

		if r.row == nil {
			return false, nil
		}

		idx := 0
		types := r.source.OutputTypes()
		for i, col := range r.planCols {
			if col.Omitted {
				r.datumRow[i] = tree.DNull
				continue
			}
			encDatum := r.row[idx]
			err := encDatum.EnsureDecoded(&types[i], &r.da)
			if err != nil {
				return false, err
			}
			r.datumRow[i] = encDatum.Datum
			idx++
		}

		return true, nil
	}
}

func (r *rowSourceToPlanNode) Values() tree.Datums {
	return r.datumRow
}

func (r *rowSourceToPlanNode) Close(ctx context.Context) {
	if r.source != nil {
		r.source.ConsumerClosed()
	}
	if r.originalPlanNode != nil {
		r.originalPlanNode.Close(ctx)
	}
}
