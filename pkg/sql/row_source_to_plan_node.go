package sql

import (
	"context"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// RowSourceToPlanNode wraps a RowSource and presents it as a PlanNode. It must
// be constructed with Create(), after which it is a PlanNode and can be treated
// as such.
type RowSourceToPlanNode struct {
	source distsqlrun.RowSource

	// Temporary variables
	started bool
	row     sqlbase.EncDatumRow
	da      sqlbase.DatumAlloc
}

func CreateRowSourceToPlanNode(s distsqlrun.RowSource) RowSourceToPlanNode {
	return RowSourceToPlanNode{
		source: s,
	}
}

func (r *RowSourceToPlanNode) Next(params runParams) (bool, error) {
	if r.source == nil {
		return false, errors.Errorf("no valid rowsource initialized")
	}
	var p *distsqlrun.ProducerMetadata
	r.row, p = r.source.Next()

	log.Infof(context.Background(), "RowSourceToPlanNode received a %+v\n", r.row)

	if p != nil {
		if p.Err != nil {
			return false, p.Err
		} else {
			panic(fmt.Sprintf("todo(arjun): why am i getting producer metadata %+v?", p))
		}
	}

	if len(r.row) == 0 {
		r.source.ConsumerClosed()
		return false, nil
	}

	return true, nil
}

func (r *RowSourceToPlanNode) Values() tree.Datums {
	datums := make(tree.Datums, len(r.row))
	sqlbase.EncDatumRowToDatums(r.source.OutputTypes(), datums, r.row, &r.da)
	return datums
}

func (r *RowSourceToPlanNode) Close(ctx context.Context) {
	if r.source != nil {
		r.source.ConsumerClosed()
	}
}

var _ planNode = &RowSourceToPlanNode{}
