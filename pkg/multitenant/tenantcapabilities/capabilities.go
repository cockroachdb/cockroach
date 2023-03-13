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
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stringerutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Watcher presents a consistent snapshot of the global tenant capabilities
// state. It incrementally, and transparently, maintains this state by watching
// for changes to system.tenants.
type Watcher interface {
	Reader

	// Start asynchronously begins watching over the global tenant capability
	// state.
	Start(ctx context.Context) error
}

// Reader provides access to the global tenant capability state. The global
// tenant capability state may be arbitrarily stale.
type Reader interface {
	GetCapabilities(id roachpb.TenantID) (_ TenantCapabilities, found bool)
	GetCapabilitiesMap() map[roachpb.TenantID]TenantCapabilities
}

// Authorizer performs various kinds of capability checks for requests issued
// by tenants. It does so by consulting the global tenant capability state.
//
// In the future, we may want to expand the Authorizer to take into account
// signals other than just the tenant capability state. For example, request
// usage pattern over a timespan.
type Authorizer interface {
	// HasCapabilityForBatch returns an error if a tenant, referenced by its ID,
	// is not allowed to execute the supplied batch request given the capabilities
	// it possesses.
	HasCapabilityForBatch(context.Context, roachpb.TenantID, *kvpb.BatchRequest) error

	// BindReader is a mechanism by which the caller can bind a Reader[1] to the
	// Authorizer post-creation. The Authorizer uses the Reader to consult the
	// global tenant capability state to authorize incoming requests. This
	// function cannot be used to update the Reader.
	//
	//
	// [1] The canonical implementation of the Authorizer lives on GRPC
	// interceptors, and as such, must be instantiated before the GRPC Server is
	// created. However, the GRPC server is created very early on during Server
	// startup and serves as a dependency for the canonical Reader's
	// implementation. Binding the Reader late allows us to break this dependency
	// cycle.
	BindReader(reader Reader)

	// HasNodeStatusCapability returns an error if a tenant, referenced by its ID,
	// is not allowed to access cluster-level node metadata and liveness.
	HasNodeStatusCapability(ctx context.Context, tenID roachpb.TenantID) error

	// HasTSDBQueryCapability returns an error if a tenant, referenced by its ID,
	// is not allowed to query the TSDB for metrics.
	HasTSDBQueryCapability(ctx context.Context, tenID roachpb.TenantID) error
}

// Entry ties together a tenantID with its capabilities.
type Entry struct {
	TenantID           roachpb.TenantID
	TenantCapabilities TenantCapabilities
}

// Update represents an update to the global tenant capability state.
type Update struct {
	Entry
	Deleted bool // whether the entry was deleted or not
}

func (u Update) String() string {
	if u.Deleted {
		return fmt.Sprintf("delete: ten=%v", u.Entry.TenantID)
	}
	return fmt.Sprintf("update: %v", u.Entry)
}

// defaultCaps is the default state of capabilities.
// TODO(ewall): Redesign as part of
//
//	https://github.com/cockroachdb/cockroach/issues/96736.
var defaultCaps TenantCapabilities

// RegisterDefaultCapabilities is called from the tenantcapabilitiespb package.
func RegisterDefaultCapabilities(caps TenantCapabilities) { defaultCaps = caps }

// AlteredCapabilitiesString prints all altered capability values that no
// longer match DefaultCapabilities. This is different from
// TenantCapabilities.String which only prints non-zero value fields.
func AlteredCapabilitiesString(capabilities TenantCapabilities) string {
	var builder strings.Builder
	builder.WriteByte('{')
	space := ""
	for _, capID := range CapabilityIDs {
		value := capabilities.Cap(capID).Get().String()
		defaultValue := defaultCaps.Cap(capID).Get().String()
		if value != defaultValue {
			builder.WriteString(space)
			builder.WriteString(capID.String())
			builder.WriteByte(':')
			builder.WriteString(value)
			space = " "
		}
	}
	builder.WriteByte('}')
	return builder.String()
}

func (u Entry) String() string {
	return fmt.Sprintf("ten=%v altered-cap=%v", u.TenantID, AlteredCapabilitiesString(u.TenantCapabilities))
}

// CapabilityID represents a handle to a tenant capability.
type CapabilityID uint8

// Capability is the accessor to a capability's current value.
type Capability interface {
	// Get retrieves the current value of the capability.
	Get() Value
	// Set modifies the current value of the capability. Note that
	// calling Set does not persist the result. Persistence needs to be
	// arranged separately by storing the underlying protobuf; this is
	// done using a different interface. See the code for ALTER TENANT.
	Set(interface{})
}

// Value is a generic interface to the value of capabilities of
// various underlying Go types. It enables processing capabilities
// without concern for the specific underlying type, such as in SHOW
// TENANT WITH CAPABILITIES.
type Value interface {
	fmt.Stringer
	redact.SafeFormatter

	// Unwrap provides access to the underlying Go value. For example,
	// for Bool capabilities, the result of Unwrap() can be casted to
	// go's bool type.
	Unwrap() interface{}
}

// TenantCapabilities is the interface provided by the capability store,
// to provide access to capability values.
type TenantCapabilities interface {
	// Cap retrieves the accessor for a given capability.
	Cap(CapabilityID) Capability

	// GetBool is equivalent to For(cap).Get().Unwrap().(bool). It
	// is provided as an optimization. The caller is responsible for
	// ensuring that the capID argument designate a capability with type
	// Bool.
	GetBool(CapabilityID) bool
}

// MinCapabilityID is the value of the minimum CapabilityID.
const MinCapabilityID = 1

//go:generate stringer -type=CapabilityID -linecomment
const (
	CanAdminChangeReplicas CapabilityID = iota + MinCapabilityID // can_admin_change_replicas

	// CanAdminScatter describes the ability of a tenant to perform manual
	// KV scatter requests. These operations need a capability
	// because excessive KV range scatter can overwhelm the storage
	// cluster.
	CanAdminScatter // can_admin_scatter

	// CanAdminSplit describes the ability of a tenant to perform manual
	// KV range split requests. These operations need a capability
	// because excessive KV range splits can overwhelm the storage
	// cluster.
	CanAdminSplit // can_admin_split

	// CanAdminUnsplit describes the ability of a tenant to perform manual
	// KV range unsplit requests. These operations need a capability
	// because excessive KV range unsplits can overwhelm the storage
	// cluster.
	CanAdminUnsplit // can_admin_unsplit

	// CanViewNodeInfo describes the ability of a tenant to read the
	// metadata for KV nodes. These operations need a capability because
	// the KV node record contains sensitive operational data which we
	// want to hide from customer tenants in CockroachCloud.
	CanViewNodeInfo // can_view_node_info

	// CanViewTSDBMetrics describes the ability of a tenant to read the
	// timeseries from the storage cluster. These operations need a
	// capability because excessive TS queries can overwhelm the storage
	// cluster.
	CanViewTSDBMetrics // can_view_tsdb_metrics

	// TenantSpanConfigBounds contains the bounds for the tenant's
	// span configs.
	TenantSpanConfigBounds // span_config_bounds

	// MaxCapabilityID is the value of the maximum CapabilityID.
	MaxCapabilityID CapabilityID = iota + MinCapabilityID - 1
)

var stringToCapabilityIDMap = stringerutil.StringToEnumValueMap(
	_CapabilityID_index[:],
	_CapabilityID_name,
	MinCapabilityID,
	MaxCapabilityID-1, // TODO: remove -1 when spanConfigBounds are supported.
)

// CapabilityIDFromString converts a string to a CapabilityID.
func CapabilityIDFromString(s string) (CapabilityID, bool) {
	capabilityID, ok := stringToCapabilityIDMap[s]
	return capabilityID, ok
}

// CapabilityIDs is a slice of all tenant capabilities.
var CapabilityIDs = stringerutil.EnumValues(
	MinCapabilityID,
	MaxCapabilityID-1, // TODO: remove -1 when spanConfigBounds are supported.
)

// Type describes the user-facing data type of a specific capability.
type Type int8

const (
	// Bool describes the type of boolean capabilities.
	Bool Type = iota
)

// CapabilityType returns the type of a given capability.
func (c CapabilityID) CapabilityType() Type {
	switch c {
	case
		CanAdminChangeReplicas,
		CanAdminScatter,
		CanAdminSplit,
		CanAdminUnsplit,
		CanViewNodeInfo,
		CanViewTSDBMetrics:
		return Bool

	default:
		panic(errors.AssertionFailedf("missing case: %q", c))
	}
}
