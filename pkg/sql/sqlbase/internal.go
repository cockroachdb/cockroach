// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// InternalExecutorSessionDataOverride is used by the InternalExecutor interface
// to allow control over some of the session data.
type InternalExecutorSessionDataOverride struct {
	// User represents the user that the query will run under.
	User string
	// Database represents the default database for the query.
	Database string
	// ApplicationName represents the application that the query runs under.
	ApplicationName string
	// SearchPath represents the namespaces to search in.
	SearchPath *sessiondata.SearchPath
}

// NoSessionDataOverride is the empty InternalExecutorSessionDataOverride which
// does not override any session data.
var NoSessionDataOverride = InternalExecutorSessionDataOverride{}

// NodeUserSessionDataOverride is an InternalExecutorSessionDataOverride which
// overrides the users to the NodeUser.
var NodeUserSessionDataOverride = InternalExecutorSessionDataOverride{User: security.NodeUser}
