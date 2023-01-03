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
	"regexp"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
var SystemTenantID = MustMakeTenantID(1)

// MinTenantID is the minimum ID of a (non-system) tenant in a multi-tenant
// cluster.
var MinTenantID = MustMakeTenantID(2)

// MaxTenantID is the maximum ID of a (non-system) tenant in a multi-tenant
// cluster.
var MaxTenantID = MustMakeTenantID(math.MaxUint64)

func init() {
	// Inject the string representation of SystemTenantID into the log package
	// to avoid an import dependency cycle.
	log.SetSystemTenantID(
		strconv.FormatUint(SystemTenantID.ToUint64(), 10))
}

// MustMakeTenantID constructs a new TenantID from the provided uint64.
// The function panics if an invalid tenant ID is given.
func MustMakeTenantID(id uint64) TenantID {
	assertValid(id)
	return TenantID{id}
}

// MakeTenantID constructs a new TenantID from the provided uint64.
// It returns an error on an invalid ID.
func MakeTenantID(id uint64) (TenantID, error) {
	if err := checkValid(id); err != nil {
		return TenantID{}, err
	}
	return TenantID{id}, nil
}

// ToUint64 returns the TenantID as a uint64.
func (t TenantID) ToUint64() uint64 {
	assertValid(t.InternalValue)
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

// ErrInvalidTenantID is returned when a function encounters tenant ID 0.
var ErrInvalidTenantID = errors.New("invalid tenant ID 0")

// Protects against zero value.
func checkValid(id uint64) error {
	if id == 0 {
		return ErrInvalidTenantID
	}
	return nil
}

func assertValid(id uint64) {
	if err := checkValid(id); err != nil {
		panic(err.Error())
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
// An empty tenID clears the respective key from the context.
func NewContextForTenant(ctx context.Context, tenID TenantID) context.Context {
	var val any
	if tenID.IsSet() {
		val = tenID
	} else {
		val = nil
	}

	ctxTenantID, _ := TenantFromContext(ctx)
	if tenID == ctxTenantID {
		// The context already has the right tenant, or no tenant at all.
		return ctx
	}

	return context.WithValue(ctx, tenantKey{}, val)
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
	return MustMakeTenantID(tID), nil
}

// TenantName is a unique name associated with a tenant in a multi-tenant
// cluster. Unlike TenantID it is not necessary for every tenant to have a name.
type TenantName string

// Equal implements the gogoproto Equal interface.
func (n TenantName) Equal(o TenantName) bool {
	return string(n) == string(o)
}

// IsSystemTenantName returns true if the provided tenantName corresponds to
// that of the system tenant.
//
// NB: The system tenant cannot be renamed.
func IsSystemTenantName(tenantName TenantName) bool {
	return tenantName == "system"
}

// We limit tenant names to what is allowed in a DNS hostname, so
// that we can use SNI to identify tenants and also as a prefix
// to a database name in connection strings. However there cannot
// be a hyphen at the end.
var tenantNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9---]{0,98}[a-z0-9])?$`)

func (n TenantName) IsValid() error {
	if !tenantNameRe.MatchString(string(n)) {
		return errors.WithHint(errors.Newf("invalid tenant name: %q", n),
			"Tenant names must start and end with a lowercase letter or digit, contain only lowercase letters, digits or hyphens, with a maximum of 100 characters.")
	}
	return nil
}

// TenantNameContainer is a shared object between the
// server controller and the tenant server that holds
// a reference to the current name of the tenant and
// updates it if needed. This facilitates some
// observability use cases where we need to tag data
// by tenant name.
type TenantNameContainer syncutil.AtomicString

func NewTenantNameContainer(name TenantName) *TenantNameContainer {
	t := &TenantNameContainer{}
	t.Set(name)
	return t
}

func (c *TenantNameContainer) Set(name TenantName) {
	(*syncutil.AtomicString)(c).Set(string(name))
}

func (c *TenantNameContainer) Get() TenantName {
	return TenantName(c.String())
}

// String implements the fmt.Strinter interface.
func (c *TenantNameContainer) String() string {
	return (*syncutil.AtomicString)(c).Get()
}
