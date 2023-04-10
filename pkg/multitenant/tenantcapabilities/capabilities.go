// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilities

import (
	"github.com/cockroachdb/cockroach/pkg/util/stringerutil"
	"github.com/cockroachdb/redact"
)

// ID represents a handle to a tenant capability.
type ID uint8

// SafeValue makes ID a redact.SafeValue.
func (i ID) SafeValue() {}

// IsValid returns true if the ID is valid.
func (i ID) IsValid() bool {
	return i > 0 && i <= MaxCapabilityID
}

var _ redact.SafeValue = ID(0)

//go:generate stringer -type=ID -linecomment
const (
	_ ID = iota

	// CanAdminRelocateRange describes the ability of a tenant to perform manual
	// KV relocate range requests. These operations need a capability
	// because excessive KV range relocation can overwhelm the storage
	// cluster.
	CanAdminRelocateRange // can_admin_relocate_range

	// CanAdminScatter describes the ability of a tenant to scatter ranges using
	// an AdminScatter request. By default, secondary tenants are allowed to
	// scatter as doing so is integral to the performance of IMPORT/RESTORE.
	CanAdminScatter // can_admin_scatter

	// CanAdminSplit describes the ability of a tenant to perform KV requests to
	// split ranges. By default, secondary tenants are allowed to perform splits
	// as doing so is integral to performance of IMPORT/RESTORE.
	CanAdminSplit // can_admin_split

	// CanAdminUnsplit describes the ability of a tenant to perform manual
	// KV range unsplit requests. These operations need a capability
	// because excessive KV range unsplits can overwhelm the storage
	// cluster.
	CanAdminUnsplit // can_admin_unsplit

	// CanUseNodelocalStorage allows the tenant to access the
	// nodelocal storage service on the KV nodes.
	CanUseNodelocalStorage // can_use_nodelocal_storage

	// CanViewNodeInfo describes the ability of a tenant to read the
	// metadata for KV nodes. These operations need a capability because
	// the KV node record contains sensitive operational data which we
	// want to hide from customer tenants in CockroachCloud.
	CanViewNodeInfo // can_view_node_info

	// CanCheckConsistency allows the tenant to check range consistency.
	CanCheckConsistency // can_check_consistency

	// CanViewTSDBMetrics describes the ability of a tenant to read the
	// timeseries from the storage cluster. These operations need a
	// capability because excessive TS queries can overwhelm the storage
	// cluster.
	CanViewTSDBMetrics // can_view_tsdb_metrics

	// ExemptFromRateLimiting describes the ability of a tenant to
	// make requests without being subject to the KV-side tenant
	// rate limiter.
	ExemptFromRateLimiting // exempt_from_rate_limiting

	// TenantSpanConfigBounds contains the bounds for the tenant's
	// span configs.
	TenantSpanConfigBounds // span_config_bounds

	MaxCapabilityID ID = iota - 1
)

// IDs is a slice of all tenant capabilities.
var IDs = stringerutil.EnumValues(
	1,
	MaxCapabilityID,
)

var stringToCapabilityIDMap = stringerutil.StringToEnumValueMap(
	_ID_index[:],
	_ID_name,
	1,
	MaxCapabilityID,
)

// FromName looks up a capability by name.
func FromName(s string) (Capability, bool) {
	if id, ok := stringToCapabilityIDMap[s]; ok {
		return FromID(id)
	}
	return nil, false
}

// FromID looks up a capability by ID.
func FromID(id ID) (Capability, bool) {
	if id.IsValid() {
		return capabilities[id], true
	}
	return nil, false
}

var capabilities = [MaxCapabilityID + 1]Capability{
	CanAdminRelocateRange:  boolCapability(CanAdminRelocateRange),
	CanAdminScatter:        boolCapability(CanAdminScatter),
	CanAdminSplit:          boolCapability(CanAdminSplit),
	CanAdminUnsplit:        boolCapability(CanAdminUnsplit),
	CanCheckConsistency:    boolCapability(CanCheckConsistency),
	CanUseNodelocalStorage: boolCapability(CanUseNodelocalStorage),
	CanViewNodeInfo:        boolCapability(CanViewNodeInfo),
	CanViewTSDBMetrics:     boolCapability(CanViewTSDBMetrics),
	ExemptFromRateLimiting: boolCapability(ExemptFromRateLimiting),
	TenantSpanConfigBounds: spanConfigBoundsCapability(TenantSpanConfigBounds),
}
