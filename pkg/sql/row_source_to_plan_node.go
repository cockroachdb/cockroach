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
func makeRowSourceToPlanNode(
	s distsqlrun.RowSource, forwarder metadataForwarder, planCols sqlbase.ResultColumns,
) *rowSourceToPlanNode {
	row := make(tree.Datums, len(s.OutputTypes()))

	return &rowSourceToPlanNode{
		source:    s,
		datumRow:  row,
		forwarder: forwarder,
		planCols:  planCols,
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
			if r.forwarder != nil {
				r.forwarder.forwardMetadata(p)
				continue
			}
			if p.Err != nil {
				return false, p.Err
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

		if err := sqlbase.EncDatumRowToDatums(r.source.OutputTypes(), r.datumRow, r.row, &r.da); err != nil {
			return false, err
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
}
