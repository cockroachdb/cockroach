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

	"github.com/cockroachdb/errors"
)

// TenantCapabilityName is a pseudo-enum of valid capability names.
type TenantCapabilityName int32

// valueOffset sets the iota offset to make sure the 0 value is not a valid
// enum value.
const valueOffset = 1

// IsSet returns true if the capability name has a non-zero value.
func (t TenantCapabilityName) IsSet() bool {
	return t >= valueOffset
}

var stringToTenantCapabilityName = func() map[string]TenantCapabilityName {
	numCapabilities := len(_TenantCapabilityName_index) - 1
	m := make(map[string]TenantCapabilityName, numCapabilities)
	for i := 0; i < numCapabilities; i++ {
		startIndex := _TenantCapabilityName_index[i]
		endIndex := _TenantCapabilityName_index[i+1]
		s := _TenantCapabilityName_name[startIndex:endIndex]
		m[s] = TenantCapabilityName(i + valueOffset)
	}
	return m
}()

// TenantCapabilityNameFromString converts a string to a TenantCapabilityName
// or returns an error if no conversion is possible.
func TenantCapabilityNameFromString(s string) (TenantCapabilityName, error) {
	tenantCapabilityName, ok := stringToTenantCapabilityName[s]
	if !ok {
		return 0, errors.Newf("unknown capability: %q", s)
	}
	return tenantCapabilityName, nil
}

// TenantCapabilityNames is a slice of all tenant capability names sorted lexicographically.
var TenantCapabilityNames = func() []TenantCapabilityName {
	capabilityNames := make([]TenantCapabilityName, 0, len(stringToTenantCapabilityName))
	for _, capabilityName := range stringToTenantCapabilityName {
		capabilityNames = append(capabilityNames, capabilityName)
	}
	sort.Slice(capabilityNames, func(i, j int) bool {
		return capabilityNames[i] < capabilityNames[j]
	})
	return capabilityNames
}()

//go:generate stringer -type=TenantCapabilityName -linecomment
const (
	// CanAdminSplit maps to TenantCapabilities.CanAdminSplit.
	CanAdminSplit TenantCapabilityName = iota + valueOffset // can_admin_split
	// CanAdminUnsplit maps to TenantCapabilities.CanAdminUnsplit.
	CanAdminUnsplit // can_admin_unsplit
	// CanViewNodeInfo maps to TenantCapabilities.CanViewNodeInfo.
	CanViewNodeInfo // can_view_node_info
	// CanViewTSDBMetrics maps to TenantCapabilities.CanViewTSDBMetrics.
	CanViewTSDBMetrics // can_view_tsdb_metrics
)

func (t *TenantCapabilities) getFlagFieldRef(capabilityName TenantCapabilityName) *bool {
	switch capabilityName {
	case CanAdminSplit:
		return &t.CanAdminSplit
	case CanAdminUnsplit:
		return &t.CanAdminUnsplit
	case CanViewNodeInfo:
		return &t.CanViewNodeInfo
	case CanViewTSDBMetrics:
		return &t.CanViewTSDBMetrics
	default:
		panic(fmt.Sprintf("unknown capability: %q", capabilityName))
	}
}

// GetFlagCapability returns the value of the corresponding flag capability.
func (t *TenantCapabilities) GetFlagCapability(capabilityName TenantCapabilityName) bool {
	return *t.getFlagFieldRef(capabilityName)
}

// SetFlagCapability sets the value of the corresponding flag capability.
func (t *TenantCapabilities) SetFlagCapability(
	capabilityName TenantCapabilityName, capabilityValue bool,
) {
	*t.getFlagFieldRef(capabilityName) = capabilityValue
}
