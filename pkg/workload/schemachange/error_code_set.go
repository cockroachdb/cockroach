// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
)

type errorCodeSet map[pgcode.Code]bool

func makeExpectedErrorSet() errorCodeSet {
	return errorCodeSet(map[pgcode.Code]bool{})
}

func (set errorCodeSet) merge(otherSet errorCodeSet) {
	for code := range otherSet {
		set[code] = true
	}
}

func (set errorCodeSet) add(code pgcode.Code) {
	set[code] = true
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

type codesWithConditions []struct {
	code      pgcode.Code
	condition bool
}

func (c codesWithConditions) add(s errorCodeSet) {
	for _, cc := range c {
		if cc.condition {
			s.add(cc.code)
		}
	}
}
