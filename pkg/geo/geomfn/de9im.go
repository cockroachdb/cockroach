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
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
)

// Relate returns the DE-9IM relation between A and B.
func Relate(a *geo.Geometry, b *geo.Geometry) (string, error) {
	if a.SRID() != b.SRID() {
		return "", geo.NewMismatchingSRIDsError(a, b)
	}
	return geos.Relate(a.EWKB(), b.EWKB())
}

// MatchesDE9IM checks whether the given DE-9IM relation matches the DE-91M pattern.
// See: https://en.wikipedia.org/wiki/DE-9IM.
func MatchesDE9IM(str string, pattern string) (bool, error) {
	if len(str) != 9 {
		return false, errors.Newf("str %q should be of length 9", str)
	}
	if len(pattern) != 9 {
		return false, errors.Newf("pattern %q should be of length 9", pattern)
	}
	for i := 0; i < len(str); i++ {
		switch unicode.ToLower(rune(pattern[i])) {
		case '*':
			continue
		case 't':
			if str[i] < '0' || str[i] > '2' {
				return false, nil
			}
		case 'f':
			if unicode.ToLower(rune(str[i])) != 'f' {
				return false, nil
			}
		default:
			return false, errors.Newf("unrecognized pattern character at position %d: %s", i, pattern)
		}
	}
	return true, nil
}
