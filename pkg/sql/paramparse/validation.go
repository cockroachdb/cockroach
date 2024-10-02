// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package paramparse

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// UniqueConstraintParamContext is used as a validation context for
// ValidateUniqueConstraintParams function.
// IsPrimaryKey: set to true if the unique constraint for primary key.
// IsSharded: set to true if the unique constraint has a hash sharded index.
type UniqueConstraintParamContext struct {
	IsPrimaryKey bool
	IsSharded    bool
}

// ValidateUniqueConstraintParams checks if there is any storage parameters
// invalid as a param for Unique Constraint.
func ValidateUniqueConstraintParams(
	params tree.StorageParams, ctx UniqueConstraintParamContext,
) error {
	// Only `bucket_count` is allowed for primary key and unique index.
	for _, param := range params {
		switch param.Key {
		case `bucket_count`:
			if ctx.IsSharded {
				continue
			}
			return pgerror.New(
				pgcode.InvalidParameterValue,
				`"bucket_count" storage param should only be set with "USING HASH" for hash sharded index`,
			)
		default:
			if ctx.IsPrimaryKey {
				return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage param %q on primary key", param.Key)
			}
			return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage param %q on unique index", param.Key)
		}
	}
	return nil
}
