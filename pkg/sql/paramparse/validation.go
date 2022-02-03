// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package paramparse

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ValidateUniqueConstraintParams checks if there is any storage parameters
// invalid as a param for Unique Constraint.
func ValidateUniqueConstraintParams(params tree.StorageParams, isPK bool) error {
	// TODO (issue 75243): add `bucket_count` as a valid param.
	for _, param := range params {
		if isPK {
			return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage param %q on Primary Key", param.Key)
		} else {
			return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage param %q on Unique Index", param.Key)
		}
	}
	return nil
}
