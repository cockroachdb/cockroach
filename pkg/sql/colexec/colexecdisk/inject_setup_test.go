// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecdisk_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
)

func init() {
	// Inject a testing helper for NewColOperator so colexecdisk tests can use
	// NewColOperator without an import cycle.
	colexecargs.TestNewColOperator = colbuilder.NewColOperator
}
