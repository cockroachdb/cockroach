// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// AlterPolicy implements ALTER POLICY.
func AlterPolicy(b BuildCtx, n *tree.AlterPolicy) {
	if n.NewPolicyName != "" {
		panic(unimplemented.NewWithIssue(136996, "ALTER POLICY is not yet implemented"))
	}
	panic(unimplemented.NewWithIssue(136997, "ALTER POLICY is not yet implemented"))
}
