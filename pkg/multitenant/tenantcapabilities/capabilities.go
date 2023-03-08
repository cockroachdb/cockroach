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
	"sort"
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

// AllCapabilitiesString prints all capability values. This is different from
// TenantCapabilities.String which only prints non-zero value fields.
func AllCapabilitiesString(capabilities TenantCapabilities) string {
	var builder strings.Builder
	builder.WriteByte('{')
	for i, capID := range CapabilityIDs {
		if i > 0 {
			builder.WriteByte(' ')
		}
		builder.WriteString(capID.String())
		builder.WriteByte(':')
		builder.WriteString(capID.Get(capabilities).String())
	}
	builder.WriteByte('}')
	return builder.String()
}

func (u Entry) String() string {
	return fmt.Sprintf("ten=%v cap=%v", u.TenantID, AllCapabilitiesString(u.TenantCapabilities))
}

// CapabilityID represents a handle to a tenant capability.
type CapabilityID interface {
	fmt.Stringer
	Get(TenantCapabilities) Value
}

type BoolCapabilityID uint8

func (c BoolCapabilityID) Get(capabilities TenantCapabilities) Value {
	return capabilities.GetBoolValue(c)
}

type SpanConfigCapabilityID uint8

func (c SpanConfigCapabilityID) Get(capabilities TenantCapabilities) Value {
	return capabilities.GetSpanConfigValue(c)
}

// Value is a generic interface to the value of capabilities of
// various underlying Go types. It enables processing capabilities
// without concern for the specific underlying type, such as in SHOW
// TENANT WITH CAPABILITIES.
type Value interface {
	fmt.Stringer
	redact.SafeFormatter
}

// TenantCapabilities is the interface provided by the capability store,
// to provide access to capability values.
type TenantCapabilities interface {
	GetBool(BoolCapabilityID) bool
	GetBoolValue(BoolCapabilityID) Value
	SetBool(BoolCapabilityID, bool)
	GetSpanConfigValue(SpanConfigCapabilityID) Value
}

//go:generate stringer -type=BoolCapabilityID -linecomment
const (
	_ BoolCapabilityID = iota

	// CanAdminSplit describes the ability of a tenant to perform manual
	// KV range split requests. These operations need a capability
	// because excessive KV range splits can overwhelm the storage
	// cluster.
	CanAdminSplit // can_admin_split

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

	MaxBoolCapabilityID BoolCapabilityID = iota - 1
)

// BoolCapabilityIDs is a slice of all tenant capabilities.
var BoolCapabilityIDs = stringerutil.EnumValues(
	1,
	MaxBoolCapabilityID,
)

//go:generate stringer -type=SpanConfigCapabilityID -linecomment
const (
	_ SpanConfigCapabilityID = iota

	// TenantSpanConfigBounds contains the bounds for the tenant's
	// span configs.
	TenantSpanConfigBounds // span_config_bounds

	MaxSpanConfigCapabilityID SpanConfigCapabilityID = iota - 1
)

// SpanConfigCapabilityIDs is a slice of all tenant capabilities.
var SpanConfigCapabilityIDs = stringerutil.EnumValues(
	1,
	MaxSpanConfigCapabilityID,
)

var stringToCapabilityIDMap = func() map[string]CapabilityID {
	m := make(map[string]CapabilityID, len(CapabilityIDs))
	for _, id := range CapabilityIDs {
		capString := id.String()
		_, ok := m[capString]
		if ok {
			panic(errors.AssertionFailedf("duplicate capability strings %q", capString))
		}
		m[capString] = id
	}
	return m
}()

// CapabilityIDFromString converts a string to a CapabilityID.
func CapabilityIDFromString(s string) (CapabilityID, bool) {
	capabilityID, ok := stringToCapabilityIDMap[s]
	return capabilityID, ok
}

var CapabilityIDs = func() (result []CapabilityID) {
	for _, id := range BoolCapabilityIDs {
		result = append(result, id)
	}
	for _, id := range SpanConfigCapabilityIDs {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].String() < result[j].String()
	})
	return
}()
