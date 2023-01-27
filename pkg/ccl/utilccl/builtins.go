// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
