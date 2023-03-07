// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiesapi

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CapabilityID represents a handle to a tenant capability.
type CapabilityID int8

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

	// GetBool(cap) is equivalent to For(cap).Get().Unwrap().(bool). It
	// is provided as an optimization. The caller is responsible for
	// ensuring that the capID argument designate a capability with type
	// Bool.
	GetBool(CapabilityID) bool
}

//go:generate stringer -type=CapabilityID -linecomment
const (
	_ CapabilityID = iota

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

	// LastCapabilityID is a marker for the last valid capability ID.
	LastCapabilityID
)

// CapabilityIDs is a slice of all tenant capabilities.
var CapabilityIDs = func() (res []CapabilityID) {
	for i := CapabilityID(1); i < LastCapabilityID; i++ {
		res = append(res, i)
	}
	return res
}()

// Type describes the user-facing data type of a specific capability.
type Type int8

const (
	// Bool describes the type of boolean capabilities.
	Bool Type = iota
)

// CapabilityType returns the type of a given capability.
func CapabilityType(capID CapabilityID) Type {
	switch capID {
	case CanAdminSplit, CanViewNodeInfo, CanViewTSDBMetrics:
		return Bool

	default:
		panic(errors.AssertionFailedf("missing case: %d", capID))
	}
}
