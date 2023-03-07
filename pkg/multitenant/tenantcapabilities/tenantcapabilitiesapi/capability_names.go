// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiesapi

func stringToCapabilityNameMap[T ~int8](index []uint8, constantString string) map[string]T {
	numCapabilities := len(index) - 1
	m := make(map[string]T, numCapabilities)
	for i := 0; i < numCapabilities; i++ {
		startIndex := index[i]
		endIndex := index[i+1]
		s := constantString[startIndex:endIndex]
		m[s] = T(i + 1)
	}
	return m
}

var stringToCapabilityIDMap = stringToCapabilityNameMap[CapabilityID](
	_CapabilityID_index[:LastCapabilityID],
	_CapabilityID_name,
)

// CapabilityIDFromString converts a string to a CapabilityID.
func CapabilityIDFromString(s string) (CapabilityID, bool) {
	capabilityID, ok := stringToCapabilityIDMap[s]
	return capabilityID, ok
}
