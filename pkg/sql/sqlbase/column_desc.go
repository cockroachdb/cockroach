// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

// ColumnDescriptorsToReferences returns a list of references to the input
// ColumnDescriptors.
func ColumnDescriptorsToReferences(cols []ColumnDescriptor) []*ColumnDescriptor {
	ptrs := make([]*ColumnDescriptor, len(cols))
	for i := range cols {
		ptrs[i] = &cols[i]
	}
	return ptrs
}
