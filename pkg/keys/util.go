// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

// IsPseudoTableID returns true if id is in keys.PseudoTableIDs.
func IsPseudoTableID(id uint32) bool {
	for _, pseudoTableID := range PseudoTableIDs {
		if id == pseudoTableID {
			return true
		}
	}
	return false
}
