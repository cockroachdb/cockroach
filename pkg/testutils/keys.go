// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import "bytes"

// MakeKey makes a new key which is the concatenation of the given inputs, in
// order.
func MakeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}
