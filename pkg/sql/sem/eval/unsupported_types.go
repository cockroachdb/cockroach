// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type unsupportedTypeChecker struct {
	//lint:ignore U1000 unused
	version clusterversion.Handle
}

// NewUnsupportedTypeChecker returns a new tree.UnsupportedTypeChecker that can
// be used to check whether a type is allowed by the current cluster version.
func NewUnsupportedTypeChecker(handle clusterversion.Handle) tree.UnsupportedTypeChecker {
	// There are currently no types to check. Uncomment this code if a new type is introduced.
	return &unsupportedTypeChecker{version: handle}
}

var _ tree.UnsupportedTypeChecker = (*unsupportedTypeChecker)(nil)

// CheckType implements the tree.UnsupportedTypeChecker interface.
func (tc *unsupportedTypeChecker) CheckType(ctx context.Context, typ *types.T) error {
	// NB: when adding an unsupported type here, change the constructor to not
	// return nil.
	if (typ.Oid() == oidext.T_jsonpath || typ.Oid() == oidext.T__jsonpath) &&
		!tc.version.IsActive(ctx, clusterversion.V25_2) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"%s not supported until version 25.2", typ.String(),
		)
	}
	return nil
}
