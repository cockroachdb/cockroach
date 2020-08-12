// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// Relate returns the DE-9IM relation between A and B.
func Relate(a *geo.Geometry, b *geo.Geometry) (string, error) {
	if a.SRID() != b.SRID() {
		return "", geo.NewMismatchingSRIDsError(a, b)
	}
	return geos.Relate(a.EWKB(), b.EWKB())
}

// RelatePattern returns whether the DE-9IM relation between A and B matches.
func RelatePattern(a *geo.Geometry, b *geo.Geometry, pattern string) (bool, error) {
	if a.SRID() != b.SRID() {
		return false, geo.NewMismatchingSRIDsError(a, b)
	}
	return geos.RelatePattern(a.EWKB(), b.EWKB(), pattern)
}

// MatchesDE9IM checks whether the given DE-9IM relation matches the DE-91M pattern.
// Assumes the relation has been computed, and such has no 'T' and '*' characters.
// See: https://en.wikipedia.org/wiki/DE-9IM.
func MatchesDE9IM(relation string, pattern string) (bool, error) {
	if len(relation) != 9 {
		return false, errors.Newf("relation %q should be of length 9", relation)
	}
	if len(pattern) != 9 {
		return false, errors.Newf("pattern %q should be of length 9", pattern)
	}
	for i := 0; i < len(relation); i++ {
		matches, err := relationByteMatchesPatternByte(relation[i], pattern[i])
		if err != nil {
			return false, err
		}
		if !matches {
			return false, nil
		}
	}
	return true, nil
}

// relationByteMatchesPatternByte matches a single byte of a DE-9IM relation
// against the DE-9IM pattern.
// Pattern matches are as follows:
// * '*': allow anything.
// * 't'/'T': allow only if the relation is true. This means the relation must be
//   '0' (point), '1' (line) or '2' (area) - which is the dimensionality of the
//   intersection.
// * 'f'/'F': allow only if relation is also false, which is of the form 'f'/'F'.
func relationByteMatchesPatternByte(r byte, p byte) (bool, error) {
	switch util.ToLowerSingleByte(p) {
	case '*':
		return true, nil
	case 't':
		if r < '0' || r > '2' {
			return false, nil
		}
	case 'f':
		if util.ToLowerSingleByte(r) != 'f' {
			return false, nil
		}
	default:
		return false, errors.Newf("unrecognized pattern character: %s", string(p))
	}
	return true, nil
}
