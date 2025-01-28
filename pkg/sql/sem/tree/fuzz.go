// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build gofuzz

package tree

import "github.com/cockroachdb/cockroach/pkg/util/timeutil"

var (
	timeCtx = NewParseContext(timeutil.Now())
)

func FuzzParseDDecimal(data []byte) int {
	_, err := ParseDDecimal(string(data))
	if err != nil {
		return 0
	}
	return 1
}

func FuzzParseDDate(data []byte) int {
	_, _, err := ParseDDate(timeCtx, string(data))
	if err != nil {
		return 0
	}
	return 1
}
