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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
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
	if typ.Family() == types.PGLSNFamily {
		errorTypeString = "pg_lsn"
	} else if typ.Oid() == oid.T_refcursor {
		errorTypeString = "refcursor"
	}
	if errorTypeString != "" && !tc.version.IsActive(ctx, clusterversion.V23_2) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"%s not supported until version 23.2", errorTypeString,
		)
	}
	return nil
}
