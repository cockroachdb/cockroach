package scbuildstmt

import (
	"time"

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
	dur, err := time.ParseDuration(canaryWindowStr.RawString())
	if err != nil {
		panic(errors.AssertionFailedf("invalid canary window duration specified"))
	}
	b.Add(&scpb.CanaryWindow{
		TableID:  tbl.TableID,
		Duration: dur,
	})
}
