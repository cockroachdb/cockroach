// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

type partitioner struct {
	settings    *cluster.Settings
	evalContext *tree.EvalContext
}

// AddPartitioning implements the scmutationexec.Partitioner interface.
func (s *partitioner) AddPartitioning(
	ctx context.Context,
	tbl *tabledesc.Mutable,
	index catalog.Index,
	partitionFields []string,
	listPartition []*scpb.ListPartition,
	rangePartition []*scpb.RangePartitions,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (err error) {
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
	newImplicitCols, newPartitioning, err := CreatePartitioningCCL(
		ctx,
		s.settings,
		s.evalContext,
		tbl,
		index.IndexDescDeepCopy(),
		partitionBy,
		allowedNewColumnNames,
		allowImplicitPartitioning,
	)
	if err != nil {
		return err
	}
	tabledesc.UpdateIndexPartitioning(index.IndexDesc(), false, newImplicitCols, newPartitioning)
	return nil
}

// NewPartitioner returns an implementation of scmutationexec.Partitioner.
func NewPartitioner(
	settings *cluster.Settings, evalContext *tree.EvalContext,
) scmutationexec.Partitioner {
	return &partitioner{
		settings:    settings,
		evalContext: evalContext,
	}
}

// CreatePartitioningCCL is the public hook point for the CCL-licensed
// partitioning creation code.
var CreatePartitioningCCL = func(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *tabledesc.Mutable,
	indexDesc descpb.IndexDescriptor,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning descpb.PartitioningDescriptor, err error) {
	return nil, descpb.PartitioningDescriptor{}, sqlerrors.NewCCLRequiredError(errors.New(
		"creating or manipulating partitions requires a CCL binary"))
}
