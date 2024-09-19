// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"context"
	"math"
	"regexp"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// SystemTenantID is the ID associated with the tenant that manages a cluster.
//
// The system tenant must exist and must use shared service, to be available to
// node-internal processes. Additionally it is given special treatment in any
// authorization decisions, bypassing typical restrictions that tenants act only
// on their own key spans.
var SystemTenantID = TenantOne

// TenantOne is a special tenant ID, associated the numeric ID 1, which for
// legacy compatibility reasons stores its tables without a tenant prefix.
var TenantOne = MustMakeTenantID(1)

// MinTenantID is the minimum ID of a (non-system) tenant in a multi-tenant
// cluster.
var MinTenantID = MustMakeTenantID(2)

// MaxTenantID is the maximum ID of a (non-system) tenant in a multi-tenant
// cluster.
// We use MaxUint64-1 to ensure that we can always compute an end key
// for the keyspace by adding 1 to the tenant ID.
var MaxTenantID = MustMakeTenantID(math.MaxUint64 - 1)

func init() {
	// Inject the string representation of SystemTenantID into the log package
	// to avoid an import dependency cycle.
	serverident.SetSystemTenantID(
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

// ContextWithClientTenant creates a new context with information about the
// tenant that's the client of an RPC. The tenant ID can be retrieved later with
// ClientTenantFromContext. Use ContextWithoutClientTenant to clear the
// tenant client information from the context.
//
// An empty tenID clears the respective key from the context.
func ContextWithClientTenant(ctx context.Context, tenID TenantID) context.Context {
	if !tenID.IsSet() {
		panic("programming error: missing tenant ID")
	}

	ctxTenantID, _ := ClientTenantFromContext(ctx)
	if tenID == ctxTenantID {
		// The context already has the right tenant.
		return ctx
	}

	return context.WithValue(ctx, tenantKey{}, tenID)
}

// ContextWithoutClientTenant removes the tenant information
// from the context.
func ContextWithoutClientTenant(ctx context.Context) context.Context {
	_, ok := ClientTenantFromContext(ctx)
	if !ok {
		// The context already has no tenant.
		return ctx
	}

	return context.WithValue(ctx, tenantKey{}, nil)
}

// ClientTenantFromContext returns the ID of the tenant that's the client of the
// current RPC, if that infomation was put in the ctx by
// ContextWithClientTenant.
func ClientTenantFromContext(ctx context.Context) (tenID TenantID, ok bool) {
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

// String implements the fmt.Stringer interface.
func (c *TenantNameContainer) String() string {
	return (*syncutil.AtomicString)(c).Get()
}
