// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mtinfopb

import (
	"fmt"

	// We manually import this to satisfy a dependency in info.proto.
	_ "github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/errors"
)

// TenantServiceMode describes how tenants can be served to clients.
type TenantServiceMode uint8

// Note: the constant values below are stored in system.tenants.service_mode.
const (
	// ServiceModeNode says that no service is allowed.
	ServiceModeNone TenantServiceMode = 0
	// ServiceModeExternal says that service is allowed using
	// separate processes.
	// This is the default value when the service_mode column is empty, for
	// backward-compatibility with records created for CockroachCloud
	// Serverless pre-v23.1.
	ServiceModeExternal TenantServiceMode = 1
	// ServiceModeShared says that service is allowed using shared-process
	// multitenancy on KV nodes.
	// This mode causes KV nodes to spontaneously start the SQL service
	// for the tenant.
	ServiceModeShared TenantServiceMode = 2

	// ServiceModeStopping says that the service was previusly in
	// ServiceModeShared but is in the process of stopping.
	ServiceModeStopping TenantServiceMode = 3

	// MaxServiceMode is a sentinel value.
	MaxServiceMode TenantServiceMode = ServiceModeStopping
)

// String implements fmt.Stringer.
func (s TenantServiceMode) String() string {
	switch s {
	case ServiceModeNone:
		return "none"
	case ServiceModeExternal:
		return "external"
	case ServiceModeShared:
		return "shared"
	case ServiceModeStopping:
		return "stopping"
	default:
		return fmt.Sprintf("unimplemented-%d", int(s))
	}
}

// TenantServiceModeValues facilitates the string -> TenantServiceMode conversion.
var TenantServiceModeValues = map[string]TenantServiceMode{
	"none":     ServiceModeNone,
	"external": ServiceModeExternal,
	"shared":   ServiceModeShared,
	"stopping": ServiceModeStopping,
}

// TenantDataState describes the state of a tenant's logical keyspace.
type TenantDataState uint8

// Note: the constant values below are stored in system.tenants.data_state.
const (
	// DataStateAdd indicates tenant data is being added. Not available
	// for SQL sessions.
	DataStateAdd TenantDataState = 0
	// DataStateReady indicates data is ready and SQL servers can access it.
	DataStateReady TenantDataState = 1
	// DataStateDrop indicates tenant data is being deleted. Not
	// available for SQL sessions.
	DataStateDrop TenantDataState = 2
	// MaxDataState is a sentinel value.
	MaxDataState TenantDataState = DataStateDrop
)

// String implements fmt.Stringer.
func (s TenantDataState) String() string {
	switch s {
	case DataStateAdd:
		return "add"
	case DataStateReady:
		return "ready"
	case DataStateDrop:
		return "dropping"
	default:
		return fmt.Sprintf("unimplemented-%d", int(s))
	}
}

// TenantDataStateValues facilitates the string -> TenantDataState conversion.
var TenantDataStateValues = map[string]TenantDataState{
	"add":      DataStateAdd,
	"ready":    DataStateReady,
	"dropping": DataStateDrop,
}

// TenantInfo captures both a ProtoInfo and the SQLInfo columns that
// go alongside it, sufficient to represent an entire row in
// system.tenants.
type TenantInfo struct {
	ProtoInfo
	SQLInfo
}

// ToInfo converts a TenantInfoWithUsage to an TenantInfo.
func (m *TenantInfoWithUsage) ToInfo() *TenantInfo {
	return &TenantInfo{
		ProtoInfo: m.ProtoInfo,
		SQLInfo:   m.SQLInfo,
	}
}

func (d ProtoInfo_DeprecatedDataState) ToDataState() (TenantDataState, error) {
	switch d {
	case ProtoInfo_READY:
		return DataStateReady, nil
	case ProtoInfo_ADD:
		return DataStateAdd, nil
	case ProtoInfo_DROP:
		return DataStateDrop, nil
	default:
		return 0, errors.AssertionFailedf("invalid DeprecatedDataState: %d", d)
	}
}
