// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

// DefaultJoinOrderLimit denotes the default limit on the number of joins to
// reorder.
const DefaultJoinOrderLimit = 8

// MaxReorderJoinsLimit is the maximum number of joins which can be reordered.
const MaxReorderJoinsLimit = 63

// SaveTablesDatabase is the name of the database where tables created by
// the saveTableNode are stored.
const SaveTablesDatabase = "savetables"
