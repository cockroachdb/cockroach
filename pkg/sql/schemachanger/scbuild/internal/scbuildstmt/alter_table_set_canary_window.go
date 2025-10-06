package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func alterTableSetCanaryWindow(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	n *tree.AlterTableSetCanaryWindow,
) {
	if n.CanaryWindow == nil {
		panic(errors.AssertionFailedf("no canary window duration specified"))
	}
	canaryWindowStr, ok := n.CanaryWindow.(*tree.StrVal)
	if !ok {
		panic(errors.AssertionFailedf("canary window input is not of type string"))
	}
	b.Add(&scpb.CanaryWindow{
		TableID:     tbl.TableID,
		DurationStr: canaryWindowStr.RawString(),
	})
}
