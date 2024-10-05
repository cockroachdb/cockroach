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
	version clusterversion.Handle
}

// NewUnsupportedTypeChecker returns a new tree.UnsupportedTypeChecker that can
// be used to check whether a type is allowed by the current cluster version.
func NewUnsupportedTypeChecker(handle clusterversion.Handle) tree.UnsupportedTypeChecker {
	return &unsupportedTypeChecker{version: handle}
}

var _ tree.UnsupportedTypeChecker = &unsupportedTypeChecker{}

// CheckType implements the tree.UnsupportedTypeChecker interface.
func (tc *unsupportedTypeChecker) CheckType(ctx context.Context, typ *types.T) error {
	var errorTypeString string
	switch typ.Family() {
	case types.PGLSNFamily:
		errorTypeString = "pg_lsn"
	case types.RefCursorFamily:
		errorTypeString = "refcursor"
	}
	if errorTypeString != "" && !tc.version.IsActive(ctx, clusterversion.V23_2) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"%s not supported until version 23.2", errorTypeString,
		)
	}
	return nil
}
