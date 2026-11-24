// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtest

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/forbiddenmethod"
	"golang.org/x/tools/go/analysis"
)

// RoachtestGoAnalyzer prevents use of the `go` keyword in roachtest.
var Analyzer = forbiddenmethod.NewNakedGoAnalyzer(
	"roachtestgo",
	"Use of go keyword not allowed in roachtest, use the Task interface instead",
	"Prevents direct use of the 'go' keyword in roachtest. Goroutines should be launched through the Task interface supplied by the roachtest framework.",
	func(pass *analysis.Pass) bool {
		return strings.Contains(pass.Pkg.Path(), "cmd/roachtest")
	},
)
