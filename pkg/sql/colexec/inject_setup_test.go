// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
)

func init() {
	// Inject a testing helper for NewColOperator so colexec tests can
	// use NewColOperator without an import cycle.
	colexecargs.TestNewColOperator = colbuilder.NewColOperator
	// Inject a testing helper for OrderedDistinctColsToOperators so colexec tests
	// can use OrderedDistinctColsToOperators without an import cycle.
	colexectestutils.OrderedDistinctColsToOperators = colexecbase.OrderedDistinctColsToOperators
}
