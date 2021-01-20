// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// partitionByFromTableDesc constructs a PartitionBy clause from a table descriptor.
func partitionByFromTableDesc(
	codec keys.SQLCodec, tableDesc *tabledesc.Mutable,
) (*tree.PartitionBy, error) {
	// Convert the PartitioningDescriptor back into tree.PartitionBy by
	// re-parsing the SHOW CREATE partitioning statement.
	// TODO(#multiregion): clean this up by translating the descriptor back into
	// tree.PartitionBy directly.
	a := &rowenc.DatumAlloc{}
	f := tree.NewFmtCtx(tree.FmtSimple)
	if err := ShowCreatePartitioning(
		a,
		codec,
		tableDesc,
		tableDesc.GetPrimaryIndex().IndexDesc(),
		&tableDesc.GetPrimaryIndex().IndexDesc().Partitioning,
		&f.Buffer,
		0, /* indent */
		0, /* colOffset */
	); err != nil {
		return nil, errors.Wrap(err, "error recreating PARTITION BY clause for PARTITION ALL BY affected index")
	}
	stmt, err := parser.ParseOne(fmt.Sprintf("ALTER TABLE t %s", f.CloseAndGetString()))
	if err != nil {
		return nil, errors.Wrap(err, "error recreating PARTITION BY clause for PARTITION ALL BY affected index")
	}
	return stmt.AST.(*tree.AlterTable).Cmds[0].(*tree.AlterTablePartitionByTable).PartitionByTable.PartitionBy, nil
}
