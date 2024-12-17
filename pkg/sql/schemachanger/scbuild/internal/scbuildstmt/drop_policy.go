// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// DropPolicy implements DROP POLICY.
func DropPolicy(b BuildCtx, n *tree.DropPolicy) {
	panic(unimplemented.NewWithIssue(136696, "DROP POLICY is not yet implemented"))
}
