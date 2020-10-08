// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cliflags

// Flags specific to multi-tenancy commands.
var (
	TenantID = FlagInfo{
		Name:        "tenant-id",
		EnvVar:      "COCKROACH_TENANT_ID",
		Description: `The tenant ID under which to start the SQL server.`,
	}

	KVAddrs = FlagInfo{
		Name:        "kv-addrs",
		EnvVar:      "COCKROACH_KV_ADDRS",
		Description: `A comma-separated list of KV endpoints (load balancers allowed).`,
	}
)
