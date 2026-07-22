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

// DefaultDisjunctionSplitCountLimit is the default maximum number of
// OR-connected filter conjuncts that the SplitDisjunction exploration rule will
// split.
const DefaultDisjunctionSplitCountLimit = 8

// MaxDisjunctionSplitCount is the maximum value that the
// optimizer_max_disjunction_split_count session variable may be set to. Because
// SplitDisjunction fan-out is exponential in the conjunct count, values much
// larger than DefaultDisjunctionSplitCountLimit risk large planning-time memory
// usage; this ceiling bounds the worst case regardless of configuration.
const MaxDisjunctionSplitCount = 32

// SaveTablesDatabase is the name of the database where tables created by
// the saveTableNode are stored.
const SaveTablesDatabase = "savetables"
