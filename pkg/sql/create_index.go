// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type createIndexNode struct {
	n         *tree.CreateIndex
	tableDesc *sqlbase.MutableTableDescriptor
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(ctx context.Context, n *tree.CreateIndex) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createIndexNode{tableDesc: tableDesc, n: n}, nil
}

// MakeIndexDescMaybeCreateShard creates an index descriptor from a CreateIndex node and
// optionally adds a hidden computed shard column (along with its check constraint) in
// case the index is hash sharded.
func MakeIndexDescMaybeCreateShard(
	params runParams, n *tree.CreateIndex, tableDesc *sqlbase.MutableTableDescriptor,
) (*sqlbase.IndexDescriptor, error) {
	indexDesc := sqlbase.IndexDescriptor{
		Name:              string(n.Name),
		Unique:            n.Unique,
		StoreColumnNames:  n.Storing.ToStrings(),
		CreatedExplicitly: true,
	}

	if n.Inverted {
		if n.Interleave != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support interleaved tables")
		}

		if n.PartitionBy != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support partitioning")
		}

		if n.Sharded != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support hash sharding")
		}

		if len(indexDesc.StoreColumnNames) > 0 {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support stored columns")
		}

		if n.Unique {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes can't be unique")
		}
		indexDesc.Type = sqlbase.IndexDescriptor_INVERTED
	}

	if n.Sharded != nil {
		colNames := make([]string, 0, len(n.Columns))
		for _, c := range n.Columns {
			colNames = append(colNames, string(c.Column))
		}
		buckets, err := sqlbase.EvalShardBucketCount(n.Sharded.ShardBuckets)
		if err != nil {
			return nil, err
		}
		shardCol, err := maybeCreateAndAddShardCol(int(buckets), tableDesc,
			colNames, false /* isNewTable */)
		if err != nil {
			return nil, err
		}
		shardIdxElem := tree.IndexElem{
			Column:    tree.Name(shardCol.Name),
			Direction: tree.Ascending,
		}
		n.Columns = append(tree.IndexElemList{shardIdxElem}, n.Columns...)
		sqlbase.AddShardToIndexDesc(&indexDesc, shardCol.Name, colNames, buckets)

		// Assign an ID to the newly-added shard column.
		if err := tableDesc.AllocateIDs(); err != nil {
			return nil, err
		}

		ckDef, err := makeShardCheckConstraintDef(tableDesc, int(buckets), shardCol)
		if err != nil {
			return nil, err
		}
		info, err := tableDesc.GetConstraintInfo(params.ctx, nil)
		if err != nil {
			return nil, err
		}

		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}

		ckName, err := generateMaybeDuplicateNameForCheckConstraint(tableDesc, ckDef.Expr)
		if err != nil {
			return nil, err
		}
		// Avoid creating duplicate check constraints.
		if _, ok := inuseNames[ckName]; !ok {
			ck, err := MakeCheckConstraint(params.ctx, tableDesc, ckDef, inuseNames,
				&params.p.semaCtx, params.p.tableName)
			if err != nil {
				return nil, err
			}
			ck.Validity = sqlbase.ConstraintValidity_Validating
			tableDesc.AddCheckMutation(ck, sqlbase.DescriptorMutation_ADD)
		}
	}

	if err := indexDesc.FillColumns(n.Columns); err != nil {
		return nil, err
	}
	return &indexDesc, nil
}

// maybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation)
// to `desc`, if one doesn't already exist for the given index column set.
func maybeCreateAndAddShardCol(
	shardBuckets int, desc *sqlbase.MutableTableDescriptor, colNames []string, isNewTable bool,
) (*sqlbase.ColumnDescriptor, error) {
	shardCol, err := makeShardColumnDesc(colNames, shardBuckets, false /* primaryKey */)
	if err != nil {
		return nil, err
	}
	if !hasColumn(*desc, shardCol.Name) {
		if isNewTable {
			desc.AddColumn(shardCol)
		} else {
			desc.AddColumnMutation(shardCol, sqlbase.DescriptorMutation_ADD)
		}
	}
	return shardCol, nil
}

func (n *createIndexNode) startExec(params runParams) error {
	_, dropped, err := n.tableDesc.FindIndexByName(string(n.n.Name))
	if err == nil {
		if dropped {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"index %q being dropped, try again later", string(n.n.Name))
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	// Guard against creating a non-partitioned index on a partitioned table,
	// which is undesirable in most cases.
	if params.SessionData().SafeUpdates && n.n.PartitionBy == nil &&
		n.tableDesc.PrimaryIndex.Partitioning.NumColumns > 0 {
		return pgerror.DangerousStatementf("non-partitioned index on partitioned table")
	}
	indexDesc, err := MakeIndexDescMaybeCreateShard(params, n.n, n.tableDesc)
	if err != nil {
		return err
	}

	// If all nodes in the cluster know how to handle secondary indexes with column families,
	// write the new version into the index descriptor.
	encodingVersion := sqlbase.BaseIndexFormatVersion
	if cluster.Version.IsActive(params.ctx, params.p.EvalContext().Settings, cluster.VersionSecondaryIndexColumnFamilies) {
		encodingVersion = sqlbase.SecondaryIndexFamilyFormatVersion
	}
	indexDesc.Version = encodingVersion

	if n.n.PartitionBy != nil {
		partitioning, err := CreatePartitioning(params.ctx, params.p.ExecCfg().Settings,
			params.EvalContext(), n.tableDesc, indexDesc, n.n.PartitionBy)
		if err != nil {
			return err
		}
		indexDesc.Partitioning = partitioning
	}

	mutationIdx := len(n.tableDesc.Mutations)
	if err := n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := n.tableDesc.AssignFamiliesToShardColumns(); err != nil {
		return err
	}
	// Assign the correct column ids to the updated column family.
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}
	// The index name may have changed as a result of
	// AllocateIDs(). Retrieve it for the event log below.
	index := n.tableDesc.Mutations[mutationIdx].GetIndex()
	indexName := index.Name

	if n.n.Interleave != nil {
		if err := params.p.addInterleave(params.ctx, n.tableDesc, index, n.n.Interleave); err != nil {
			return err
		}
		if err := params.p.finalizeInterleave(params.ctx, n.tableDesc, index); err != nil {
			return err
		}
	}

	mutationID, err := params.p.createOrUpdateSchemaChangeJob(
		params.ctx, n.tableDesc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
	if err != nil {
		return err
	}
	if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
		return err
	}

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
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
			n.n.Table.FQString(), indexName, n.n.String(),
			params.SessionData().User, uint32(mutationID),
		},
	)
}

func (*createIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*createIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*createIndexNode) Close(context.Context)        {}
