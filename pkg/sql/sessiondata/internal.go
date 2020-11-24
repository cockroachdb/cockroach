// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondata

import "github.com/cockroachdb/cockroach/pkg/security"

// InternalExecutorOverride is used by the InternalExecutor interface
// to allow control over some of the session data.
type InternalExecutorOverride struct {
	// User represents the user that the query will run under.
	User security.SQLUsername
	// Database represents the default database for the query.
	Database string
	// ApplicationName represents the application that the query runs under.
	ApplicationName string
	// SearchPath represents the namespaces to search in.
	SearchPath *SearchPath
	// DatabaseIDToTempSchemaID represents the mapping for temp schemas used which
	// allows temporary schema resolution by ID.
	DatabaseIDToTempSchemaID map[uint32]uint32
}

// NoSessionDataOverride is the empty InternalExecutorOverride which does not
// override any session data.
var NoSessionDataOverride = InternalExecutorOverride{}

// NodeUserSessionDataOverride is an InternalExecutorOverride which overrides
// the users to the NodeUser.
var NodeUserSessionDataOverride = InternalExecutorOverride{
	User: security.MakeSQLUsernameFromPreNormalizedString(security.NodeUser)}
