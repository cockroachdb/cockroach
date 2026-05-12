// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
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

// tenantGroupKey returns the groupKey for a tenant container.
func tenantGroupKey(id uint64) groupKey {
	return groupKey{tenantID: id}
}

// rgGroupKey returns the groupKey for a resource group container.
func rgGroupKey(id uint64) groupKey {
	return groupKey{groupID: id}
}

// metricLabels returns the (kind, id) label pair for per-group
// metrics, preserving the existing metric label scheme.
//
// TODO(ssd): We will fix up metrics for the unification in a
// future commit.
func (k groupKey) metricLabels() (kindStr, idStr string) {
	if k.groupID == 0 {
		return "tenant", strconv.FormatUint(k.tenantID, 10)
	}
	return "rg", strconv.FormatUint(k.groupID, 10)
}
