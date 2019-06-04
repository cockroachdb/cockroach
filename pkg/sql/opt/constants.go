// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package opt

// DefaultJoinOrderLimit denotes the default limit on the number of joins to
// reorder.
const DefaultJoinOrderLimit = 4

// SaveTablesDatabase is the name of the database where tables created by
// the saveTableNode are stored.
const SaveTablesDatabase = "savetables"
