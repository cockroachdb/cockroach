// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
