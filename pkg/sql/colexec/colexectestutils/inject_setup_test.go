// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexectestutils_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
)

func init() {
	colexectestutils.NewInvariantsChecker = colexec.NewInvariantsChecker
}
