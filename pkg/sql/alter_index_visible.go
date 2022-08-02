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
	"github.com/cockroachdb/errors"
)

type alterIndexVisibleNode struct {
	n         *tree.AlterIndexVisible
	tableDesc *tabledesc.Mutable
	index     catalog.Index
}

func (p *planner) AlterIndexVisible(ctx context.Context, stmt *tree.AlterIndexVisible) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER INDEX VISIBILITY",
	); err != nil {
		return nil, err
	}

	// Check if the table actually exists. expandMutableIndexName returns the
	// underlying table for the index. If no table is found and IfExists is false,
	// error is returned. Otherwise, tableDesc returned is nil.
	_, tableDesc, err := expandMutableIndexName(ctx, p, stmt.Index, !stmt.IfExists /* requireTable */)
	if err != nil {
		// Error if no table is found and IfExists is false.
		return nil, err
	}

	if tableDesc == nil {
		// Nothing needed if no table is found and IfExists is true.
		return newZeroNode(nil /* columns */), nil
	}

	// Now we know this table exists, check if the index exists. FindIndexWithName
	// returns error if none was found in the tableDesc.
	idx, err := tableDesc.FindIndexWithName(string(stmt.Index.Index))
	if err != nil {
		if stmt.IfExists {
			// Nothing needed if no index exists and IfExists is true.
			return newZeroNode(nil /* columns */), nil
		}
		// Error if no index exists and IfExists is not specified.
		return nil, pgerror.WithCandidateCode(err, pgcode.UndefinedObject)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &alterIndexVisibleNode{n: stmt, tableDesc: tableDesc, index: idx}, nil
}

// TODO (wenyihu6): Double check on whether we should have ReadingOwnWrites. I
// don't think changing visibility performs multiple KV operations on descriptor
// and expects to see its own writes. But I'm not certain since renameIndexNode
// is also implementing this interface method.
// func (n *alterIndexVisibleNode) ReadingOwnWrites() {}

func (n *alterIndexVisibleNode) startExec(params runParams) error {
	if n.n.NotVisible {
		if n.index.Primary() {
			return errors.WithHint(
				pgerror.Newf(pgcode.FeatureNotSupported, "primary index cannot be invisible"),
				"instead, use ALTER TABLE ... ALTER PRIMARY KEY or ADD CONSTRAINT ... PRIMARY KEY "+
					"and change the secondary index invisible",
			)
		}
	}

	if n.index.IsNotVisible() == n.n.NotVisible {
		// Nothing needed if the index is already what they want.
		str := ""
		if n.n.NotVisible {
			str = "not "
		}
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("index is already "+str+"visible"),
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

	// Warn against dropping an index if there exists a NotVisible index that may
	// be used for constraint check behind the scene.
	if notVisibleIndexNotice := tabledesc.ValidateNotVisibleIndexWithinTable(n.tableDesc); notVisibleIndexNotice != nil {
		params.p.BufferClientNotice(
			params.ctx,
			notVisibleIndexNotice,
		)
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
