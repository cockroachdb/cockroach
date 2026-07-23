// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"cmp"
	"strconv"

	"github.com/cockroachdb/redact"
)

// groupKey is the composite map key for q.mu.groups. It identifies a
// group by (tenantID, groupID). For tenant groups (serverless mode),
// tenantID is set and groupID is 0. For resource groups (RM mode),
// groupID is set and tenantID is 0. Future user-defined groups may
// have both non-zero. The string representation is "tNgN", e.g.
// "t1g0" for the system tenant, "t0g1" for the high-priority RM
// group.
type groupKey struct {
	tenantID uint64
	groupID  uint64
}

// SafeFormat implements redact.SafeFormatter. Formats as e.g.
// "t1g0" (tenant 1, no resource group) or "t0g1" (no tenant,
// resource group 1).
func (k groupKey) SafeFormat(s redact.SafePrinter, _ rune) {
	s.Printf("t%dg%d", k.tenantID, k.groupID)
}

// String implements fmt.Stringer via SafeFormat.
func (k groupKey) String() string {
	return redact.StringWithoutMarkers(k)
}

// compare orders groupKeys by tenantID, then groupID. Use as a
// slices.SortFunc comparator via the method expression
// `groupKey.compare`.
func (k groupKey) compare(other groupKey) int {
	return cmp.Or(
		cmp.Compare(k.tenantID, other.tenantID),
		cmp.Compare(k.groupID, other.groupID))
}

// tenantGroupKey returns the groupKey for a tenant container.
func tenantGroupKey(id uint64) groupKey {
	return groupKey{tenantID: id}
}

// rgGroupKey returns the groupKey for a resource group container owned
// by the given tenant. User-defined resource groups are scoped to a
// tenant, so both fields are generally non-zero. The two built-in
// groups are tenant-agnostic (tenantID 0); use highResourceGroupKey /
// lowResourceGroupKey for those rather than constructing them here.
func rgGroupKey(tenantID, groupID uint64) groupKey {
	return groupKey{tenantID: tenantID, groupID: groupID}
}

// isBuiltin reports whether k names a built-in group (one always
// installed in ResourceGroupConfigHolder via builtinGroupConfigs).
// Callers use this to give built-ins lifecycle treatment that
// caller-managed keys do not get — e.g. exemption from idle-group
// GC.
func (k groupKey) isBuiltin() bool {
	_, ok := builtinGroupConfigs[k]
	return ok
}

// isServerlessGroup reports whether k names a serverless tenant
// group (groupID==0, tenantID>0). The legacy per-tenant metric
// family is populated only for these groups; RM-mode resource
// groups (tenantID==0, groupID>0) and future user-defined groups
// (both non-zero) feed only the primary per-group family. The
// system tenant (tenantID==1, groupID==0) qualifies.
func (k groupKey) isServerlessGroup() bool {
	return k.groupID == 0 && k.tenantID > 0
}

// primaryMetricLabels returns the (tenant_id, group_id) label pair
// used by the primary per-group AggCounter family. Both labels are
// bare numeric strings; the tuple disambiguates same-numbered ids
// across namespaces (e.g. tenant 1 vs rg 1).
func (k groupKey) primaryMetricLabels() (tenantIDStr, groupIDStr string) {
	return strconv.FormatUint(k.tenantID, 10),
		strconv.FormatUint(k.groupID, 10)
}

// legacyTenantMetricLabel returns the tenant_id label used by the
// legacy per-tenant AggCounter family. Only meaningful for keys for
// which isServerlessGroup returns true.
func (k groupKey) legacyTenantMetricLabel() string {
	return strconv.FormatUint(k.tenantID, 10)
}
