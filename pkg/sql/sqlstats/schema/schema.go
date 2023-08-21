// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schema

// InitialSQLStatsSchema defines the schema for the SQL stats.
//
// It is used to create the SQL stats tables on a new cluster,
// or to re-initialize the SQL stats tables if a user requests a reset.
//
// Note: this list of DDL should be idempotent (it may be applied
// multiple times due to retries; or applied on top of a previous
// partial application).
//
// The SQL statements are executed in scope of the observability
// database, and thus should not include a database prefix.
var InitialSQLStatsSchema = []string{
	// Unused -- will be removed before PR merges.
	"CREATE TABLE IF NOT EXISTS obs_test_table (blah INT)",
}
