// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/descriptormarshal"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/hash"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/returnerrcheck"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/timer"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/unconvert"
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() {
	multichecker.Main(
		hash.Analyzer,
		timer.Analyzer,
		unconvert.Analyzer,
		descriptormarshal.Analyzer,
		returnerrcheck.Analyzer,
	)
}
