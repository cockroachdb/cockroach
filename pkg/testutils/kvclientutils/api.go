// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvclientutils

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// StrToCPutExistingValue takes a string that was written using, say, a Put and
// returns the bytes that can be passed to a Batch.CPut() as the expected value
// resulting from said Put.
func StrToCPutExistingValue(s string) []byte {
	var v roachpb.Value
	v.SetBytes([]byte(s))
	return v.TagAndDataBytes()
}
