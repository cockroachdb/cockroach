// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geogfn

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// spheroidBodyRE matches the body of a SPHEROID definition, i.e. the contents
// between the outer delimiters: "name",a,rf — where name is up to 19 chars
// and contains no embedded quote (matching PostGIS's SPHEROID_NAME_LEN of 20).
// The numeric captures permit surrounding whitespace, which ParseSpheroid
// strips before handing off to strconv.ParseFloat.
var spheroidBodyRE = regexp.MustCompile(`^"([^"]{1,19})",([^,]+),([^,]+)$`)

// ParseSpheroid parses a textual SPHEROID definition of the form
// SPHEROID["name",a,rf] or SPHEROID("name",a,rf), where a is the semi-major
// axis in meters and rf is the inverse flattening. The returned Spheroid uses
// flattening = 1/rf, matching the convention in PostGIS's ellipsoid_in.
// Leading and trailing whitespace in the input is ignored.
func ParseSpheroid(s string) (geoprojbase.Spheroid, error) {
	body, ok := strings.CutPrefix(strings.TrimSpace(s), "SPHEROID")
	if !ok {
		// Not strictly required for parsing — the body regex would also reject
		// this — but it produces a more specific error message for the most
		// common typo.
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"SPHEROID parser - doesn't start with SPHEROID")
	}
	var inner string
	switch {
	case strings.HasPrefix(body, "[") && strings.HasSuffix(body, "]"):
		inner = body[1 : len(body)-1]
	case strings.HasPrefix(body, "(") && strings.HasSuffix(body, ")"):
		inner = body[1 : len(body)-1]
	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"SPHEROID parser - couldn't parse the spheroid")
	}
	match := spheroidBodyRE.FindStringSubmatch(inner)
	if match == nil {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"SPHEROID parser - couldn't parse the spheroid")
	}
	// match[1] is the spheroid name, which is matched but discarded.
	a, err := strconv.ParseFloat(strings.TrimSpace(match[2]), 64)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"SPHEROID parser - couldn't parse semi-major axis")
	}
	rf, err := strconv.ParseFloat(strings.TrimSpace(match[3]), 64)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"SPHEROID parser - couldn't parse inverse flattening")
	}
	// strconv.ParseFloat accepts "NaN" and "Inf"; reject them explicitly so
	// that downstream geodesic computations don't silently produce NaN.
	if math.IsNaN(a) || math.IsInf(a, 0) || a <= 0 {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"SPHEROID semi-major axis must be a positive finite number, got %g", a)
	}
	// PostGIS does not guard the inverse flattening at all, but values outside
	// [1, +Inf) produce nonsensical spheroids: rf == 0 yields infinite
	// flattening, rf < 0 yields a "prolate" spheroid no real-world ellipsoid
	// uses, and 0 < rf < 1 yields a flattening > 1 and therefore a negative
	// minor axis. NaN/Inf produce silent NaN distances downstream.
	if math.IsNaN(rf) || math.IsInf(rf, 0) || rf < 1 {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"SPHEROID inverse flattening must be a finite number >= 1, got %g", rf)
	}
	return geoprojbase.MakeSpheroid(a, 1.0/rf)
}
