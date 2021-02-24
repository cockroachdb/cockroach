// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecproj_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
)

func init() {
	// Inject a testing helper for NewColOperator so colexecproj tests can
	// use NewColOperator without an import cycle.
	colexecargs.TestNewColOperator = colbuilder.NewColOperator
}
