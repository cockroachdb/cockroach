// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type createIndexNode struct {
	n         *tree.CreateIndex
	tableDesc *sqlbase.TableDescriptor
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(ctx context.Context, n *tree.CreateIndex) (planNode, error) {
	tn, err := n.Table.Normalize()
	if err != nil {
		return nil, err
	}

	var tableDesc *TableDescriptor
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{skipCache: true, allowAdding: true}, func() {
		tableDesc, err = ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createIndexNode{tableDesc: tableDesc, n: n}, nil
}

func (n *createIndexNode) startExec(params runParams) error {
	_, dropped, err := n.tableDesc.FindIndexByName(string(n.n.Name))
	if err == nil {
		if dropped {
			return fmt.Errorf("index %q being dropped, try again later", string(n.n.Name))
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	indexDesc := sqlbase.IndexDescriptor{
		Name:             string(n.n.Name),
		Unique:           n.n.Unique,
		StoreColumnNames: n.n.Storing.ToStrings(),
	}

	if n.n.Inverted {
		if n.n.Interleave != nil {
			return pgerror.NewError(pgerror.CodeInvalidSQLStatementNameError, "inverted indexes don't support interleaved tables")
		}

		if n.n.PartitionBy != nil {
			return pgerror.NewError(pgerror.CodeInvalidSQLStatementNameError, "inverted indexes don't support partitioning")
		}

		if len(indexDesc.StoreColumnNames) > 0 {
			return pgerror.NewError(pgerror.CodeInvalidSQLStatementNameError, "inverted indexes don't support stored columns")
		}

		if n.n.Unique {
			return pgerror.NewError(pgerror.CodeInvalidSQLStatementNameError, "inverted indexes can't be unique")
		}
		indexDesc.Type = sqlbase.IndexDescriptor_INVERTED
	}

	if err := indexDesc.FillColumns(n.n.Columns); err != nil {
		return err
	}
	if n.n.PartitionBy != nil {
		partitioning, err := CreatePartitioning(params.ctx, params.p.ExecCfg().Settings,
			params.EvalContext(), n.tableDesc, &indexDesc, n.n.PartitionBy)
		if err != nil {
			return err
		}
		indexDesc.Partitioning = partitioning
	}

	mutationIdx := len(n.tableDesc.Mutations)
	if err := n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	if n.n.Interleave != nil {
		index := n.tableDesc.Mutations[mutationIdx].GetIndex()
		if err := params.p.addInterleave(params.ctx, n.tableDesc, index, n.n.Interleave); err != nil {
			return err
		}
		if err := params.p.finalizeInterleave(params.ctx, n.tableDesc, *index); err != nil {
			return err
		}
	}

	mutationID, err := params.p.createSchemaChangeJob(params.ctx, n.tableDesc,
		tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames))
	if err != nil {
		return err
	}
	if err := params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
		return err
	}

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateIndex,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			TableName  string
			IndexName  string
			Statement  string
			User       string
			MutationID uint32
		}{
			n.n.Table.TableName().FQString(), n.n.Name.String(), n.n.String(),
			params.SessionData().User, uint32(mutationID),
		},
	); err != nil {
		return err
	}
	params.p.notifySchemaChange(n.tableDesc, mutationID)

	return nil
}

func (*createIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*createIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*createIndexNode) Close(context.Context)        {}
