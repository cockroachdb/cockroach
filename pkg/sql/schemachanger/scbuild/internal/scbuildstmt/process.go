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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Process dispatches on the statement type to populate the BuilderState
// embedded in the BuildCtx. Any error will be panicked.
func Process(b BuildCtx, n tree.Statement) {
	switch n := n.(type) {
	case *tree.AlterTable:
		AlterTable(b, n)
	case *tree.CreateIndex:
		CreateIndex(b, n)
	case *tree.DropDatabase:
		DropDatabase(b, n)
	case *tree.DropSchema:
		DropSchema(b, n)
	case *tree.DropSequence:
		DropSequence(b, n)
	case *tree.DropTable:
		DropTable(b, n)
	case *tree.DropType:
		DropType(b, n)
	case *tree.DropView:
		DropView(b, n)
	default:
		panic(scerrors.NotImplementedError(n))
	}
}
