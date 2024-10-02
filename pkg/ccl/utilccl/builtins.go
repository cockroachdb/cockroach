// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utilccl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// RegisterCCLBuiltin adds a builtin defined in CCL code to the global builtins registry.
func RegisterCCLBuiltin(name string, description string, overload tree.Overload) {
	props := tree.FunctionProperties{
		Category: `CCL-only internal function`,
	}

	builtinsregistry.Register(name, &props, []tree.Overload{overload})
}
