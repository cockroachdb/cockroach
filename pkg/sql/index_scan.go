// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type indexScanNode struct {
	optColumnsSlot
	table   catalog.TableDescriptor
	indexID uint32
}

// ConstructIndexScan is part of the exec.Factory interface.
func (ef *distSQLSpecExecFactory) ConstructIndexScan(
	table cat.Table, index cat.Index,
) (exec.Node, error) {
	tableDesc, err := ef.planner.Descriptors().GetImmutableTableByID(context.Background(), ef.planner.Txn(), descpb.ID(table.ID()), tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return nil, err
	}
	return &indexScanNode{
		table:   tableDesc,
		indexID: uint32(index.ID()),
	}, nil
}

// ConstructIndexScan is part of the exec.Factory interface.
func (ef *execFactory) ConstructIndexScan(table cat.Table, index cat.Index) (exec.Node, error) {
	tableDesc, err := ef.planner.Descriptors().GetImmutableTableByID(context.Background(), ef.planner.Txn(), descpb.ID(table.ID()), tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return nil, err
	}
	return &indexScanNode{
		table:   tableDesc,
		indexID: uint32(index.ID()),
	}, nil
}

func (e *indexScanNode) startExec(params runParams) error {
	panic("indexScanNode cannot be run in local mode")
}

func (e *indexScanNode) Next(params runParams) (bool, error) {
	panic("indexScanNode cannot be run in local mode")
}

func (e *indexScanNode) Values() tree.Datums {
	panic("indexScanNode cannot be run in local mode")
}

func (e *indexScanNode) Close(ctx context.Context) {
}
