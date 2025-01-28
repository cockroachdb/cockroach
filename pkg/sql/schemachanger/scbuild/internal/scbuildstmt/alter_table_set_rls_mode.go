// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableSetRLSMode(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	n *tree.AlterTableSetRLSMode,
) {
	failIfRLSIsNotEnabled(b)

	switch n.Mode {
	case tree.TableRLSEnable:
		b.Add(&scpb.RowLevelSecurityEnabled{
			TableID: tbl.TableID,
		})
	case tree.TableRLSDisable:
		b.Drop(&scpb.RowLevelSecurityEnabled{
			TableID: tbl.TableID,
		})
	case tree.TableRLSForce:
		b.Add(&scpb.RowLevelSecurityForced{
			TableID: tbl.TableID,
		})
	case tree.TableRLSNoForce:
		b.Drop(&scpb.RowLevelSecurityForced{
			TableID: tbl.TableID,
		})
	}
}
