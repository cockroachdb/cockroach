// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"strconv"

	"github.com/cockroachdb/redact"
)

// groupKind distinguishes the semantic origin of a groupInfo's
// container in q.mu.groups. The same numeric ID can represent
// either a tenant (in serverless mode) or a resource group (in RM
// mode), and tenant IDs and RM resource group IDs are drawn from
// the same uint64 space (e.g., system tenant ID = 1 collides with
// highResourceGroupID = 1). Pairing the ID with a kind in the map
// key prevents these two semantic meanings from ever sharing a
// container.
//
// The zero value is intentionally invalid so that an unset groupKey
// is detectably so (rather than silently identifying as "tenant 0"
// the way iota-from-0 would).
type groupKind uint8

const (
	// invalidKind is the zero value sentinel; a groupKey with this
	// kind has not been initialized via tenantGroupKey/rgGroupKey.
	invalidKind groupKind = iota
	// tenantKind identifies a container created for a tenant ID
	// (serverless mode routing via TenantID).
	tenantKind
	// rgKind identifies a container created for a resource group
	// ID (RM mode routing via priorityToResourceGroup).
	rgKind
)

// String implements fmt.Stringer.
func (k groupKind) String() string {
	switch k {
	case tenantKind:
		return "tenant"
	case rgKind:
		return "rg"
	case invalidKind:
		return "invalid"
	default:
		return "groupKind(" + strconv.FormatUint(uint64(k), 10) + ")"
	}
}

// groupKey is the composite map key for q.mu.groups. It pairs the
// uint64 group ID with its semantic kind so that, e.g., tenant 1
// and rg 1 occupy distinct map entries.
type groupKey struct {
	id   uint64
	kind groupKind
}

// isTenant reports whether k identifies a tenant container.
func (k groupKey) isTenant() bool { return k.kind == tenantKind }

// isRg reports whether k identifies a resource group container.
func (k groupKey) isRg() bool { return k.kind == rgKind }

// isValid reports whether k has been initialized via one of the
// kind-specific constructors. Useful for asserting a stored
// groupKey was actually populated.
func (k groupKey) isValid() bool { return k.kind == tenantKind || k.kind == rgKind }

// SafeFormat implements redact.SafeFormatter. Formats as e.g.
// "t1" / "rg2"; useful in SafeFormat output so callers don't have
// to switch on kind themselves.
func (k groupKey) SafeFormat(s redact.SafePrinter, _ rune) {
	switch k.kind {
	case tenantKind:
		s.Printf("t%d", k.id)
	case rgKind:
		s.Printf("rg%d", k.id)
	default:
		s.Printf("invalid(%d)", k.id)
	}
}

// String implements fmt.Stringer via SafeFormat.
func (k groupKey) String() string {
	return redact.StringWithoutMarkers(k)
}

// tenantGroupKey returns the groupKey for a tenant container.
func tenantGroupKey(id uint64) groupKey {
	return groupKey{id: id, kind: tenantKind}
}

// rgGroupKey returns the groupKey for a resource group container.
func rgGroupKey(id uint64) groupKey {
	return groupKey{id: id, kind: rgKind}
}
