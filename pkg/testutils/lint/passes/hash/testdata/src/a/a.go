// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import "crypto/sha256"

func init() {
	var inputBytes, hashedBytes []byte
	_ = hashedBytes

	{
		h := sha256.New()
		h.Write(inputBytes)
		hashedBytes = h.Sum(nil)
	}

	{
		h := sha256.New()
		h.Write(inputBytes)
		var hashedBytes [sha256.Size]byte
		h.Sum(hashedBytes[:0])
	}

	{
		hashedBytes = sha256.New().Sum(inputBytes) // want `probable misuse of hash.Hash.Sum: provide parameter or use return value, but not both`
	}
}
