// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
)

type errorCodeSet map[pgcode.Code]struct{}

func makeExpectedErrorSet() errorCodeSet {
	return errorCodeSet(map[pgcode.Code]struct{}{})
}

func (set errorCodeSet) merge(otherSet errorCodeSet) {
	for code := range otherSet {
		set[code] = struct{}{}
	}
}

func (set errorCodeSet) add(code pgcode.Code) {
	// For ergonomics, we allow the success code to be added to errorCodeSet. As
	// it does not indicate an error, it won't actually be added.
	if code == pgcode.SuccessfulCompletion {
		return
	}
	set[code] = struct{}{}
}

func (set errorCodeSet) reset() {
	for k := range set {
		delete(set, k)
	}
}

func (set errorCodeSet) contains(code pgcode.Code) bool {
	_, ok := set[code]
	return ok
}

func (set errorCodeSet) StringSlice() []string {
	var codes []string
	for code := range set {
		codes = append(codes, code.String())
	}
	sort.Strings(codes)
	return codes
}

func (set errorCodeSet) String() string {
	var codes []string
	for code := range set {
		codes = append(codes, code.String())
	}
	sort.Strings(codes)
	return strings.Join(codes, ",")
}

func (set errorCodeSet) empty() bool {
	return len(set) == 0
}

func (s errorCodeSet) addAll(c codesWithConditions) {
	for _, cc := range c {
		if cc.condition {
			s.add(cc.code)
		}
	}
}

type codesWithConditions []struct {
	code      pgcode.Code
	condition bool
}

func (c codesWithConditions) append(code pgcode.Code) codesWithConditions {
	return append(c, codesWithConditions{
		{
			code:      code,
			condition: true,
		},
	}...)
}
