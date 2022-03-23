// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"context"
	"math"
	"strconv"

	"github.com/cockroachdb/errors"
)

// SystemTenantID is the ID associated with the system's internal tenant in a
// multi-tenant cluster and the only tenant in a single-tenant cluster.
//
// The system tenant differs from all other tenants in four important ways:
// 1. the system tenant's keyspace is not prefixed with a tenant specifier.
// 2. the system tenant is created by default during cluster initialization.
// 3. the system tenant is always present and can never be destroyed.
// 4. the system tenant has the ability to create and destroy other tenants.
var SystemTenantID = MakeTenantID(1)

// MinTenantID is the minimum ID of a (non-system) tenant in a multi-tenant
// cluster.
var MinTenantID = MakeTenantID(2)

// MaxTenantID is the maximum ID of a (non-system) tenant in a multi-tenant
// cluster.
var MaxTenantID = MakeTenantID(math.MaxUint64)

// MakeTenantID constructs a new TenantID from the provided uint64.
func MakeTenantID(id uint64) TenantID {
	checkValid(id)
	return TenantID{id}
}

// ToUint64 returns the TenantID as a uint64.
func (t TenantID) ToUint64() uint64 {
	checkValid(t.InternalValue)
	return t.InternalValue
}

// String implements the fmt.Stringer interface.
func (t TenantID) String() string {
	switch t {
	case TenantID{}:
		return "invalid"
	case SystemTenantID:
		return "system"
	default:
		return strconv.FormatUint(t.InternalValue, 10)
	}
}

// SafeValue implements the redact.SafeValue interface.
func (t TenantID) SafeValue() {}

// Protects against zero value.
func checkValid(id uint64) {
	if id == 0 {
		panic("invalid tenant ID 0")
	}
}

// IsSet returns whether this tenant ID is set to a valid value (>0).
func (t TenantID) IsSet() bool {
	return t.InternalValue != 0
}

// IsSystem returns whether this ID is that of the system tenant.
func (t TenantID) IsSystem() bool {
	return IsSystemTenantID(t.InternalValue)
}

// IsSystemTenantID returns whether the provided ID corresponds to that of the
// system tenant.
func IsSystemTenantID(id uint64) bool {
	return id == SystemTenantID.ToUint64()
}

type tenantKey struct{}

// NewContextForTenant creates a new context with tenant information attached.
func NewContextForTenant(ctx context.Context, tenID TenantID) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenID)
}

// TenantFromContext returns the tenant information in ctx if it exists.
func TenantFromContext(ctx context.Context) (tenID TenantID, ok bool) {
	tenID, ok = ctx.Value(tenantKey{}).(TenantID)
	return
}

// TenantIDFromString parses a tenant ID contained within a string.
func TenantIDFromString(tenantID string) (TenantID, error) {
	tID, err := strconv.ParseUint(tenantID, 10, 64)
	if err != nil {
		return TenantID{}, errors.Wrapf(err, "invalid tenant ID %s, tenant ID should be an unsigned int greater than 0", tenantID)
	}
	return MakeTenantID(tID), nil
}

// Silence unused warning.
var _ = TenantFromContext
