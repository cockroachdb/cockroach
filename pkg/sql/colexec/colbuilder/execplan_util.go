// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// addProjection adds a simple projection on top of op according to projection
// and returns the updated operator and type schema.
//
// Note that this method is the only place that's allowed to create a simple
// project op in colbuilder package (enforced by the linter) in order to force
// the caller to think about the type schema to prevent type schema corruption
// issues like #47889 and #107615.
func addProjection(
	op colexecop.Operator, typs []*types.T, projection []uint32,
) (colexecop.Operator, []*types.T) {
	newTypes := make([]*types.T, len(projection))
	for i, j := range projection {
		newTypes[i] = typs[j]
	}
	return colexecbase.NewSimpleProjectOp(op, len(typs), projection), newTypes
}
