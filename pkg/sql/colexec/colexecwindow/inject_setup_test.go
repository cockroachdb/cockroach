// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecwindow_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
)

func init() {
	// Inject a testing helper for NewColOperator so colexecwindow tests can
	// use NewColOperator without an import cycle.
	colexecargs.TestNewColOperator = colbuilder.NewColOperator
}
