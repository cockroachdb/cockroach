// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

type mutationDescGetter struct {
	descs        *descs.Collection
	txn          *kv.Txn
	retrieved    catalog.DescriptorIDSet
	drainedNames map[descpb.ID][]descpb.NameInfo
	executor     sqlutil.InternalExecutor
	settings     *cluster.Settings
	evalCtx      *tree.EvalContext
}

func newMutationDescGetter(
	descs *descs.Collection,
	txn *kv.Txn,
	executor sqlutil.InternalExecutor,
	settings *cluster.Settings,
	evalCtx *tree.EvalContext,
) *mutationDescGetter {
	return &mutationDescGetter{
		descs:        descs,
		txn:          txn,
		drainedNames: make(map[descpb.ID][]descpb.NameInfo),
		executor:     executor,
		settings:     settings,
		evalCtx:      evalCtx,
	}
}

func (m *mutationDescGetter) GetAnyDescriptorByID(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	desc, err := m.descs.GetMutableDescriptorByID(ctx, id, m.txn)
	if err != nil {
		return nil, err
	}
	desc.MaybeIncrementVersion()
	m.retrieved.Add(desc.GetID())
	return desc, nil
}

func (m *mutationDescGetter) GetMutableTypeByID(
	ctx context.Context, id descpb.ID,
) (*typedesc.Mutable, error) {
	typeDesc, err := m.descs.GetMutableTypeVersionByID(ctx, m.txn, id)
	if err != nil {
		return nil, err
	}
	typeDesc.MaybeIncrementVersion()
	m.retrieved.Add(typeDesc.GetID())
	return typeDesc, nil
}

func (m *mutationDescGetter) GetImmutableDatabaseByID(
	ctx context.Context, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	_, dbDesc, err := m.descs.GetImmutableDatabaseByID(ctx, m.txn, id, tree.DatabaseLookupFlags{Required: true})
	return dbDesc, err
}

func (m *mutationDescGetter) GetMutableTableByID(
	ctx context.Context, id descpb.ID,
) (*tabledesc.Mutable, error) {
	table, err := m.descs.GetMutableTableVersionByID(ctx, id, m.txn)
	if err != nil {
		return nil, err
	}
	table.MaybeIncrementVersion()
	m.retrieved.Add(table.GetID())
	return table, nil
}

func (m *mutationDescGetter) AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo) {
	if _, ok := m.drainedNames[id]; !ok {
		m.drainedNames[id] = []descpb.NameInfo{nameInfo}
	} else {
		m.drainedNames[id] = append(m.drainedNames[id], nameInfo)
	}
}

func (m *mutationDescGetter) SubmitDrainedNames(
	ctx context.Context, codec keys.SQLCodec, ba *kv.Batch,
) error {
	for _, drainedNames := range m.drainedNames {
		for _, drain := range drainedNames {
			ba.Del(catalogkeys.EncodeNameKey(codec, drain))
		}
	}
	return nil
}

func (m *mutationDescGetter) RemoveObjectComments(ctx context.Context, id descpb.ID) error {
	_, err := m.executor.ExecEx(
		ctx,
		"delete-table-comments",
		m.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE object_id=$1",
		id)
	if err != nil {
		return err
	}
	return err
}

func (m *mutationDescGetter) AddPartitioning(
	tableDesc *tabledesc.Mutable,
	indexDesc *descpb.IndexDescriptor,
	partitionFields []string,
	listPartition []*scpb.ListPartition,
	rangePartition []*scpb.RangePartitions,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) error {
	ctx := context.Background()
	// Deserialize back into tree based types
	partitionBy := &tree.PartitionBy{}
	partitionBy.List = make([]tree.ListPartition, 0, len(listPartition))
	partitionBy.Range = make([]tree.RangePartition, 0, len(rangePartition))
	for _, partition := range listPartition {
		exprs, err := parser.ParseExprs(partition.Expr)
		if err != nil {
			return err
		}
		partitionBy.List = append(partitionBy.List,
			tree.ListPartition{
				Name:  tree.UnrestrictedName(partition.Name),
				Exprs: exprs,
			})
	}
	for _, partition := range rangePartition {
		toExpr, err := parser.ParseExprs(partition.To)
		if err != nil {
			return err
		}
		fromExpr, err := parser.ParseExprs(partition.From)
		if err != nil {
			return err
		}
		partitionBy.Range = append(partitionBy.Range,
			tree.RangePartition{
				Name: tree.UnrestrictedName(partition.Name),
				To:   toExpr,
				From: fromExpr,
			})
	}
	partitionBy.Fields = make(tree.NameList, 0, len(partitionFields))
	for _, field := range partitionFields {
		partitionBy.Fields = append(partitionBy.Fields, tree.Name(field))
	}
	// Create the paritioning
	newImplicitCols, newPartitioning, err := scbuild.CreatePartitioningCCL(ctx, m.settings, m.evalCtx, tableDesc, *indexDesc, partitionBy, allowedNewColumnNames, allowImplicitPartitioning)
	if err != nil {
		return err
	}
	tabledesc.UpdateIndexPartitioning(indexDesc, false, newImplicitCols, newPartitioning)
	return nil
}

var _ scmutationexec.Catalog = (*mutationDescGetter)(nil)
