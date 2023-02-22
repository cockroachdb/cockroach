// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiespb

import (
	"fmt"
	"sort"
)

// valueOffset sets the iota offset to make sure the 0 value is not a valid
// enum value.
const valueOffset = 1

func stringToCapabilityNameMap[T ~int32](index []uint8, constantString string) map[string]T {
	numCapabilities := len(index) - 1
	m := make(map[string]T, numCapabilities)
	for i := 0; i < numCapabilities; i++ {
		startIndex := index[i]
		endIndex := index[i+1]
		s := constantString[startIndex:endIndex]
		m[s] = T(i + valueOffset)
	}
	return m
}

func capabilityNames[T ~int32](stringToCapabilityNameMap map[string]T) []T {
	capabilityNames := make([]T, 0, len(stringToCapabilityNameMap))
	for _, capabilityName := range stringToCapabilityNameMap {
		capabilityNames = append(capabilityNames, capabilityName)
	}
	sort.Slice(capabilityNames, func(i, j int) bool {
		return capabilityNames[i] < capabilityNames[j]
	})
	return capabilityNames
}

// BoolCapabilityName is a pseudo-enum of valid capability names.
type BoolCapabilityName int32

// IsSet returns true if the capability name has a non-zero value.
func (t BoolCapabilityName) IsSet() bool {
	return t >= valueOffset
}

var stringToBoolCapabilityNameMap = stringToCapabilityNameMap[BoolCapabilityName](
	_BoolCapabilityName_index[:],
	_BoolCapabilityName_name,
)

// BoolCapabilityNameFromString converts a string to a BoolCapabilityName.
func BoolCapabilityNameFromString(s string) (BoolCapabilityName, bool) {
	capabilityName, ok := stringToBoolCapabilityNameMap[s]
	return capabilityName, ok
}

// BoolCapabilityNames is a slice of all tenant capability names sorted lexicographically.
var BoolCapabilityNames = capabilityNames(stringToBoolCapabilityNameMap)

//go:generate stringer -type=BoolCapabilityName -linecomment
const (
	// CanAdminSplit maps to TenantCapabilities.CanAdminSplit.
	CanAdminSplit BoolCapabilityName = iota + valueOffset // can_admin_split
	// CanViewNodeInfo maps to TenantCapabilities.CanViewNodeInfo.
	CanViewNodeInfo // can_view_node_info
	// CanViewTSDBMetrics maps to TenantCapabilities.CanViewTSDBMetrics.
	CanViewTSDBMetrics // can_view_tsdb_metrics
)

func (t *TenantCapabilities) getBoolFieldRef(capabilityName BoolCapabilityName) *bool {
	switch capabilityName {
	case CanAdminSplit:
		return &t.CanAdminSplit
	case CanViewNodeInfo:
		return &t.CanViewNodeInfo
	case CanViewTSDBMetrics:
		return &t.CanViewTSDBMetrics
	default:
		panic(fmt.Sprintf("unknown capability: %q", capabilityName.String()))
	}
}

// GetBoolCapability returns the value of the corresponding flag capability.
func (t *TenantCapabilities) GetBoolCapability(capabilityName BoolCapabilityName) bool {
	return *t.getBoolFieldRef(capabilityName)
}

// SetBoolCapability sets the value of the corresponding flag capability.
func (t *TenantCapabilities) SetBoolCapability(
	capabilityName BoolCapabilityName, capabilityValue bool,
) {
	*t.getBoolFieldRef(capabilityName) = capabilityValue
}
