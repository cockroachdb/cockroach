// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverpb

import "github.com/cockroachdb/cockroach/pkg/util/errorutil"

// OptionalStatusServer is a StatusServer that is only optionally present inside
// the SQL subsystem. In practice, it is present on the system tenant, and not
// present on "regular" tenants.
type OptionalStatusServer struct {
	w errorutil.TenantSQLDeprecatedWrapper // stores serverpb.StatusServer
}

// MakeOptionalStatusServer initializes and returns an OptionalStatusServer. The
// provided server will be returned via OptionalErr() if and only if it is not
// nil.
func MakeOptionalStatusServer(s StatusServer) OptionalStatusServer {
	return OptionalStatusServer{
		// Return the status server from OptionalErr() only if one was provided.
		// We don't have any calls to .Deprecated().
		w: errorutil.MakeTenantSQLDeprecatedWrapper(s, s != nil /* exposed */),
	}
}

// OptionalErr returns the wrapped StatusServer, if it is available. If it is
// not, an error referring to the optionally supplied issues is returned.
func (s *OptionalStatusServer) OptionalErr(issueNos ...int) (StatusServer, error) {
	v, err := s.w.OptionalErr(issueNos...)
	if err != nil {
		return nil, err
	}
	return v.(StatusServer), nil
}
