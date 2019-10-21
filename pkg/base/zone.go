// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

// SubzoneID represents a subzone within a zone. It's the subzone's index within
// the parent zone + 1; there's no subzone 0 so that 0 can be used as a
// sentinel.
type SubzoneID uint32

// ToSubzoneIndex turns a SubzoneID into the index corresponding to the correct
// Subzone within the parent zone's Subzones slice.
func (id SubzoneID) ToSubzoneIndex() int32 {
	return int32(id) - 1
}

// SubzoneIDFromIndex turns a subzone's index within its parent zone into its
// SubzoneID.
func SubzoneIDFromIndex(idx int) SubzoneID {
	return SubzoneID(idx + 1)
}
