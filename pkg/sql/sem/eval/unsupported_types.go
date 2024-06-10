// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type unsupportedTypeChecker struct {
	// Uncomment this when a new type is introduced.
	// version clusterversion.Handle
}

// NewUnsupportedTypeChecker returns a new tree.UnsupportedTypeChecker that can
// be used to check whether a type is allowed by the current cluster version.
func NewUnsupportedTypeChecker(clusterversion.Handle) tree.UnsupportedTypeChecker {
	// Right now we don't have any unsupported types, so there is no benefit in
	// returning a type checker that never errors, so we just return nil. (The
	// infrastructure is already set up to handle such case gracefully.)
	return nil
}

var _ tree.UnsupportedTypeChecker = &unsupportedTypeChecker{}

// CheckType implements the tree.UnsupportedTypeChecker interface.
func (tc *unsupportedTypeChecker) CheckType(ctx context.Context, typ *types.T) error {
	// NB: when adding an unsupported type here, change the constructor to not
	// return nil.
	return nil
}
