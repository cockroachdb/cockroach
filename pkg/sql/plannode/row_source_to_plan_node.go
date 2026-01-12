// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// rowSourceToPlanNode wraps an execinfra.RowSource and presents it as a
// planNode. It must be constructed via newRowSourceToPlanNode, after which it
// is a planNode and can be treated as such.
type RowSourceToPlanNode struct {
	source    execinfra.RowSource
	forwarder metadataForwarder

	// We use a zeroInputPlanNode to prevent traversal into the original
	// planNode since planNodeToRowSource on the other end of the adapter will
	// take care of propagating signals via its own traversal.
	zeroInputPlanNode

	// originalPlanNode is the original planNode that the wrapped RowSource got
	// planned for.
	originalPlanNode planNode

	// Columns contains the metadata for the results of this node.
	Columns colinfo.ResultColumns

	// Temporary variables
	row      rowenc.EncDatumRow
	da       tree.DatumAlloc
	datumRow tree.Datums
}

var _ planNode = &rowSourceToPlanNode{}

// newRowSourceToPlanNode creates a new planNode that wraps an
// execinfra.RowSource. It takes a metadataForwarder, which is invoked for every
// piece of metadata this wrapper receives from the wrapped RowSource.
//
// It also takes an optional planNode, which is the planNode that the RowSource
// that this rowSourceToPlanNode is wrapping originally replaced. That planNode
// will be closed when this one is closed.
//
// NOTE: it is not guaranteed the ConsumerClosed will be called on the provided
// RowSource by the returned rowSourceToPlanNode.
func NewRowSourceToPlanNode(
	s execinfra.RowSource,
	forwarder metadataForwarder,
	planCols colinfo.ResultColumns,
	originalPlanNode planNode,
) *RowSourceToPlanNode {
	row := make(tree.Datums, len(planCols))

	return &RowSourceToPlanNode{
		source:           s,
		datumRow:         row,
		forwarder:        forwarder,
		Columns:          planCols,
		originalPlanNode: originalPlanNode,
	}
}

func (r *RowSourceToPlanNode) StartExec(params runParams) error {
	r.source.Start(params.Ctx)
	return nil
}

func (r *RowSourceToPlanNode) Next(params runParams) (bool, error) {
	for {
		var meta *execinfrapb.ProducerMetadata
		r.row, meta = r.source.Next()

		if meta != nil {
			// Return errors immediately, all other metadata is "forwarded".
			if meta.Err != nil {
				return false, meta.Err
			}
			r.forwarder.ForwardMetadata(meta)
			continue
		}

		if r.row == nil {
			return false, nil
		}

		types := r.source.OutputTypes()
		for i := range r.Columns {
			encDatum := r.row[i]
			err := encDatum.EnsureDecoded(types[i], &r.da)
			if err != nil {
				return false, err
			}
			r.datumRow[i] = encDatum.Datum
		}

		return true, nil
	}
}

func (r *RowSourceToPlanNode) Values() tree.Datums {
	return r.datumRow
}

func (r *RowSourceToPlanNode) Close(ctx context.Context) {
	// Make sure to lose the reference to the source.
	//
	// Note that we do not call ConsumerClosed on it since it is not the
	// responsibility of this rowSourceToPlanNode (the responsibility belongs to
	// the corresponding planNodeToRowSource).
	r.source = nil
	if r.originalPlanNode != nil {
		r.originalPlanNode.Close(ctx)
		r.originalPlanNode = nil
	}
}


// Lowercase alias
type rowSourceToPlanNode = RowSourceToPlanNode
