// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type unsupportedTypeChecker struct {
	// Uncomment this when a new type is introduced, or comment it out if there
	// are no types in the checker.
	version clusterversion.Handle
}

// NewUnsupportedTypeChecker returns a new tree.UnsupportedTypeChecker that can
// be used to check whether a type is allowed by the current cluster version.
func NewUnsupportedTypeChecker(handle clusterversion.Handle) tree.UnsupportedTypeChecker {
	// If there are no types in the checker, change this code to return nil.
	return &unsupportedTypeChecker{version: handle}
}

var _ tree.UnsupportedTypeChecker = &unsupportedTypeChecker{}

// CheckType implements the tree.UnsupportedTypeChecker interface.
func (tc *unsupportedTypeChecker) CheckType(ctx context.Context, typ *types.T) error {
	// NB: when adding an unsupported type here, change the constructor to not
	// return nil.
	var errorTypeString string
	switch typ.Family() {
	case types.PGVectorFamily:
		errorTypeString = "vector"
	}
	if errorTypeString != "" && !tc.version.IsActive(ctx, clusterversion.V24_2) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"%s not supported until version 24.2", errorTypeString,
		)
	}

	return nil
}
