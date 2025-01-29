// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build gofuzz

package json

func FuzzParseJSON(data []byte) int {
	_, err := ParseJSON(string(data))
	if err != nil {
		return 0
	}
	return 1
}
