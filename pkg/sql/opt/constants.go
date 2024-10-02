// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

// DefaultJoinOrderLimit denotes the default limit on the number of joins to
// reorder.
const DefaultJoinOrderLimit = 8

// MaxReorderJoinsLimit is the maximum number of joins which can be reordered.
const MaxReorderJoinsLimit = 63

// SaveTablesDatabase is the name of the database where tables created by
// the saveTableNode are stored.
const SaveTablesDatabase = "savetables"
