package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type alterIndexVisibleNode struct {
	n         *tree.AlterIndexVisible
	tableDesc *tabledesc.Mutable
	index     catalog.Index
}

func (p *planner) AlterIndexVisible(ctx context.Context, n *tree.AlterIndexVisible) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER INDEX VISIBILITY",
	); err != nil {
		return nil, err
	}

	// Check if the table actually exists. expandMutableIndexName returns the
	// underlying table. If the index name is specified with a table name, it
	// checks if the table exists. Otherwise, it searches the table using the
	// index name. If no table is found, 1. IfExists is specified, tableDesc
	// returned is nil. 2. No IfExists, error is returned.
	_, tableDesc, err := expandMutableIndexName(ctx, p, &n.Index, !n.IfExists /* requireTable */)
	if err != nil {
		// Error if no table is found and IfExists is false.
		return nil, err
	}

	if tableDesc == nil {
		// No error if no table but IfExists is true.
		return newZeroNode(nil /* columns */), nil
	}

	// Check if the index actually exists. FindIndexWithName returns the first
	// catalog.Index in tableDesc.AllIndexes(). It returns error if none was found
	// in the tableDesc. If the index name is specified with a table name, this is
	// where we check if the index actually exists in the table.
	idx, err := tableDesc.FindIndexWithName(string(n.Index.Index))
	if err != nil {
		if n.IfExists {
			// Nothing needed if no index exists and IfExists is true.
			return newZeroNode(nil /* columns */), nil
		}
		// Error if no index exists and IfExists is not specified.
		return nil, pgerror.WithCandidateCode(err, pgcode.UndefinedObject)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &alterIndexVisibleNode{n: n, tableDesc: tableDesc, index: idx}, nil
}

// TODO (wenyihu6): Double check on whether we should have ReadingOwnWrites. I
// don't think changing visibility performs multiple KV operations on descriptor
// and expects to see its own writes. But I'm not certain since renameIndexNode
// is also implementing this interface method.
// func (n *alterIndexVisibleNode) ReadingOwnWrites() {}

func (n *alterIndexVisibleNode) startExec(params runParams) error {
	if n.n.NotVisible && n.index.Primary() {
		return pgerror.Newf(pgcode.FeatureNotSupported, "primary index cannot be invisible")
	}

	if n.index.IsNotVisible() == n.n.NotVisible {
		// Nothing needed if the index is already what they want.
		str := ""
		if n.n.NotVisible {
			str = " not"
		}
		// TODO (wenyihu6): should we log notices if index is already what they want or is that too noisy?
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("index is already%s visible", str),
		)
		return nil
	}

	n.index.IndexDesc().NotVisible = n.n.NotVisible

	if err := validateDescriptor(params.ctx, params.p, n.tableDesc); err != nil {
		return err
	}

	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return params.p.logEvent(params.ctx,
		n.tableDesc.ID,
		&eventpb.AlterIndexVisible{
			TableName:  n.n.Index.Table.FQString(),
			IndexName:  n.index.GetName(),
			NotVisible: n.n.NotVisible,
		})
}
func (n *alterIndexVisibleNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexVisibleNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexVisibleNode) Close(context.Context)        {}
