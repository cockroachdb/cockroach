// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lexbase

// AllowedExperimental contains keywords for which the EXPERIMENTAL_
// or TESTING_ prefixes are allowed to be parsed along with the
// keyword to the same token. This ambiguity exists during the
// deprecation period of an EXPERIMENTAL_ keyword as it is being
// transitioned to the normal version. Once the transition is done,
// the keyword should be removed from here as well.
var AllowedExperimental = map[string]struct{}{
	"ranges": {},
}
