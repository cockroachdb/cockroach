// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/vm"
)

type CompiledExprResult struct {
	Expr    opt.Expr
	Program vm.Program
	Failed  bool
}
