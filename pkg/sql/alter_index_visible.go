package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type alterIndexVisibleNode struct {
	n         *tree.AlterIndexVisible
	tableDesc *tabledesc.Mutable
	index     catalog.Index
}

func (p *planner) AlterIndexVisible(ctx context.Context, stmt *tree.AlterIndexVisible) (planNode, error) {
	return nil, unimplemented.Newf(
		"Not Visible Index",
		"altering an index to visible or not visible is not supported yet")
}

// TODO (wenyihu6): Double check on whether we should have ReadingOwnWrites. I
// don't think changing visibility performs multiple KV operations on descriptor
// and expects to see its own writes. But I'm not certain since renameIndexNode
// is also implementing this interface method.
// func (n *alterIndexVisibleNode) ReadingOwnWrites() {}

func (n *alterIndexVisibleNode) startExec(params runParams) error {
	return unimplemented.Newf(
		"Not Visible Index",
		"altering an index to visible or not visible is not supported yet")
}
func (n *alterIndexVisibleNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexVisibleNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexVisibleNode) Close(context.Context)        {}
