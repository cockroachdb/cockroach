// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lex

// AllowedExperimental contains keywords for which the EXPERIMENTAL_
// or TESTING_ prefixes are allowed to be parsed along with the
// keyword to the same token. This ambiguity exists during the
// deprecation period of an EXPERIMENTAL_ keyword as it is being
// transitioned to the normal version. Once the transition is done,
// the keyword should be removed from here as well.
var AllowedExperimental = map[string]struct{}{
	"ranges": {},
}
