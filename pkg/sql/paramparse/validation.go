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

// IndexStorageParamContext provides context for ValidateIndexStorageParams.
type IndexStorageParamContext struct {
	IsPrimaryKey            bool
	IsUnique                bool
	IsSharded               bool
	HasImplicitPartitioning bool
}

// ValidateIndexStorageParams checks that storage parameters are valid for the
// given index type. For unique indexes and primary keys, only specific params
// are allowed. For all indexes, bucket_count and shard_columns require a
// hash-sharded index, and skip_unique_checks requires a unique, implicitly
// partitioned index.
func ValidateIndexStorageParams(
	params tree.StorageParams, ctx IndexStorageParamContext,
) error {
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
		case `shard_columns`:
			if ctx.IsSharded {
				continue
			}
			return pgerror.New(
				pgcode.InvalidParameterValue,
				`"shard_columns" storage param should only be set with "USING HASH" for hash sharded index`,
			)
		case `skip_unique_checks`:
			if !ctx.IsUnique {
				return pgerror.New(
					pgcode.InvalidParameterValue,
					"skip_unique_checks can only be set on UNIQUE indexes",
				)
			}
			if !ctx.HasImplicitPartitioning {
				return pgerror.New(
					pgcode.InvalidParameterValue,
					"skip_unique_checks can only be set on implicitly partitioned indexes",
				)
			}
			continue
		default:
			if ctx.IsPrimaryKey {
				return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage param %q on primary key", param.Key)
			}
			if ctx.IsUnique {
				return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage param %q on unique index", param.Key)
			}
			// Non-unique indexes accept additional params (geo config, etc.)
			// that are validated by storageparam.Set.
			continue
		}
	}
	return nil
}
