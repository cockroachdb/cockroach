// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func alterTableSetRLSMode(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	n *tree.AlterTableSetRLSMode,
) {
	// The table is already known to exist, and we would have checked for
	// the CREATE privilege. However, changing the RLS mode is different,
	// as it can only be done by the table owner.
	_ = b.ResolveTable(tn.ToUnresolvedObjectName(), ResolveParams{
		RequireOwnership: true,
	})

	switch n.Mode {
	case tree.TableRLSEnable:
		telemetry.Inc(sqltelemetry.EnableRLSCounter)
		b.Add(&scpb.RowLevelSecurityEnabled{
			TableID: tbl.TableID,
		})
	case tree.TableRLSDisable:
		telemetry.Inc(sqltelemetry.DisableRLSCounter)
		b.Drop(&scpb.RowLevelSecurityEnabled{
			TableID: tbl.TableID,
		})
	case tree.TableRLSForce:
		telemetry.Inc(sqltelemetry.ForceRLSCounter)
		b.Add(&scpb.RowLevelSecurityForced{
			TableID: tbl.TableID,
		})
	case tree.TableRLSNoForce:
		telemetry.Inc(sqltelemetry.NoForceRLSCounter)
		b.Drop(&scpb.RowLevelSecurityForced{
			TableID: tbl.TableID,
		})
	}
}
