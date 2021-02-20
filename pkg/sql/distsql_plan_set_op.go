// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// mergeResultTypes reconciles the ResultTypes between two plans. It enforces
// that each pair of ColumnTypes must either match or be null, in which case the
// non-null type is used. This logic is necessary for cases like
// SELECT NULL UNION SELECT 1.
func mergeResultTypes(left, right []*types.T) ([]*types.T, error) {
	if len(left) != len(right) {
		return nil, errors.Errorf("ResultTypes length mismatch: %d and %d", len(left), len(right))
	}
	merged := make([]*types.T, len(left))
	for i := range left {
		leftType, rightType := left[i], right[i]
		if rightType.Family() == types.UnknownFamily {
			merged[i] = leftType
		} else if leftType.Family() == types.UnknownFamily {
			merged[i] = rightType
		} else if equivalentTypes(leftType, rightType) {
			merged[i] = leftType
		} else {
			return nil, errors.Errorf(
				"conflicting ColumnTypes: %s and %s", leftType.DebugString(), rightType.DebugString())
		}
	}
	return merged, nil
}

// equivalentType checks whether a column type is equivalent to another for the
// purpose of UNION. Precision, Width, Oid, etc. do not affect the merging of
// values.
func equivalentTypes(c, other *types.T) bool {
	return c.Equivalent(other)
}
