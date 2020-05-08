// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import "github.com/cockroachdb/errors"

// NewMismatchingSRIDsError returns the error message for SRIDs of GeospatialTypes
// a and b being a mismatch.
func NewMismatchingSRIDsError(a GeospatialType, b GeospatialType) error {
	return errors.Newf(
		"operation on mixed SRIDs forbidden: (%s, %d) != (%s, %d)",
		a.Shape(),
		a.SRID(),
		b.Shape(),
		b.SRID(),
	)
}
