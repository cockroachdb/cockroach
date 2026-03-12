// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

// TestLocalStateFieldCompleteness is a compile-time check that ensures all
// fields of LocalState are listed here. When a new field is added to
// LocalState, this test will fail to compile, reminding the developer to
// update LocalState.copy().
func TestLocalStateFieldCompleteness(t *testing.T) {
	_ = LocalState{
		CollationEnv:               tree.CollationEnvironment{},
		ParseHelper:                pgdate.ParseHelper{},
		IVarContainer:              nil,
		iVarContainerStack:         nil,
		internalRNGFactory:         RNGFactory{},
		internalULIDEntropyFactory: ULIDEntropyFactory{},
	}
}
