// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// AlterTable implements ALTER TABLE.
func AlterTable(b BuildCtx, n *tree.AlterTable) {
	// Hoist the constraints to separate clauses because other code assumes that
	// that is how the commands will look.
	//
	// TODO(ajwerner): Clone the AST here because this mutates it in place and
	// that is bad.
	n.HoistAddColumnConstraints()

	tn := n.Table.ToTableName()
	_, tbl := b.ResolveTable(n.Table, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	if tbl == nil {
		return
	}
	if catalog.HasConcurrentSchemaChanges(tbl) {
		panic(scerrors.ConcurrentSchemaChangeError(tbl))
	}
	for _, cmd := range n.Cmds {
		alterTableCmd(b, tbl, cmd, &tn)
		b.IncrementSubWorkID()
	}
}

func alterTableCmd(
	b BuildCtx, table catalog.TableDescriptor, cmd tree.AlterTableCmd, tn *tree.TableName,
) {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		alterTableAddColumn(b, table, t, tn)
	default:
		panic(scerrors.NotImplementedError(cmd))
	}
}
