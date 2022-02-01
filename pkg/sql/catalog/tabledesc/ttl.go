// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// TTLDefaultExpirationColumnName is the column name representing the expiration
// column for TTL.
const TTLDefaultExpirationColumnName = "crdb_internal_expiration"

// ValidateRowLevelTTL validates that the TTL options are valid.
func ValidateRowLevelTTL(ttl *descpb.TableDescriptor_RowLevelTTL) error {
	if ttl == nil {
		return nil
	}
	if ttl.DurationExpr == "" {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`"ttl_expire_after" must be set`,
		)
	}
	return nil
}
