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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
)

type errorCodeSet map[pgcode.Code]bool

type codeWithCond struct {
	code      pgcode.Code
	condition bool
}

func makeExpectedErrorSet() errorCodeSet {
	return errorCodeSet(map[pgcode.Code]bool{})
}

func (set errorCodeSet) add(code pgcode.Code) {
	set[code] = true
}

func (set errorCodeSet) addWithConditions(codesWithConditions []codeWithCond) {
	for _, codeWithCond := range codesWithConditions {
		if codeWithCond.condition {
			set[codeWithCond.code] = true
		}
	}
}

func (set errorCodeSet) reset() {
	for k := range set {
		delete(set, k)
	}
}

func (set errorCodeSet) contains(code pgcode.Code) bool {
	if _, ok := set[code]; ok {
		return true
	}
	return false
}

func (set errorCodeSet) string() string {
	var codes []string
	for code := range set {
		codes = append(codes, code.String())
	}
	return strings.Join(codes, ",")
}

func (set errorCodeSet) empty() bool {
	for range set {
		return false
	}
	return true
}
