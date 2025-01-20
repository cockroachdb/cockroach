// Copyright 2025 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
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
		panic(scerrors.NotImplementedErrorf(n, "ALTER TABLE ... FORCE ROW LEVEL SECURITY is not yet implemented"))
	case tree.TableRLSNoForce:
		panic(scerrors.NotImplementedErrorf(n, "ALTER TABLE ... NO FORCE ROW LEVEL SECURITY is not yet implemented"))
	}
}
