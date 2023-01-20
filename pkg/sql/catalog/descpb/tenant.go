// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import "fmt"

// TenantServiceMode describes how tenants can be served to clients.
type TenantServiceMode uint8

// Note: the constant values below are stored in system.tenants.service_mode.
const (
	// ServiceModeExternal says that service is allowed using
	// separate processes.
	// This is the default value for backward-compatibility with
	// records created for CockroachCloud Serverless pre-v23.1.
	ServiceModeExternal TenantServiceMode = 0
	// ServiceModeNode says that no service is allowed.
	ServiceModeNone TenantServiceMode = 1
	// ServiceModeShared says that service is allowed using shared-process
	// multitenancy on KV nodes.
	// This mode causes KV nodes to spontaneously start the SQL service
	// for the tenant.
	ServiceModeShared TenantServiceMode = 2
)

// String implements fmt.Stringer.
func (s TenantServiceMode) String() string {
	switch s {
	case ServiceModeExternal:
		return "external"
	case ServiceModeNone:
		return "none"
	case ServiceModeShared:
		return "shared"
	default:
		return fmt.Sprintf("unimplemented-%d", int(s))
	}
}

// TenantServiceModeValues facilitates the string -> TenantServiceMode conversion.
var TenantServiceModeValues = map[string]TenantServiceMode{
	"external": ServiceModeExternal,
	"none":     ServiceModeNone,
	"shared":   ServiceModeShared,
}

// TenantDataState describes the state of a tenant's logical keyspace.
type TenantDataState uint8

// Note: the constant values below are stored in system.tenants.data_state.
const (
	// DataStateReady indicates data is ready and SQL servers can access it.
	DataStateReady TenantDataState = 0
	// DataStateAdd indicates tenant data is being added. Not available
	// for SQL sessions.
	DataStateAdd TenantDataState = 1
	// DataStateDrop indicates tenant data is being deleted. Not
	// available for SQL sessions.
	DataStateDrop TenantDataState = 2
)

// String implements fmt.Stringer.
func (s TenantDataState) String() string {
	switch s {
	case DataStateReady:
		return "ready"
	case DataStateAdd:
		return "add"
	case DataStateDrop:
		return "drop"
	default:
		return fmt.Sprintf("unimplemented-%d", int(s))
	}
}

// TenantDataStateValues facilitates the string -> TenantDataState conversion.
var TenantDataStateValues = map[string]TenantDataState{
	"ready": DataStateReady,
	"add":   DataStateAdd,
	"drop":  DataStateDrop,
}

// ExtendedTenantInfo captures both a TenantInfo and the ExtraColumns
// that go alongside it.
type ExtendedTenantInfo struct {
	TenantInfo
	TenantInfoWithUsage_ExtraColumns
}

// ToExtended converts a TenantInfoWithUsage to an ExtendedTenantInfo.
func (m *TenantInfoWithUsage) ToExtended() *ExtendedTenantInfo {
	return &ExtendedTenantInfo{
		TenantInfo:                       m.TenantInfo,
		TenantInfoWithUsage_ExtraColumns: m.TenantInfoWithUsage_ExtraColumns,
	}
}
