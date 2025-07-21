// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

const (

	// SQL keywords for constraints
	sqlIndex      = "INDEX"       // sqlIndex is a constant string used to represent index definitions in column definitions
	sqlNull       = "NULL"        // sqlNull is a constant string used to represent NULL values in column definitions
	sqlNotNull    = "NOT NULL"    // sqlNotNull is a constant string used to represent NOT NULL constraints in column definitions
	sqlPrimaryKey = "PRIMARY KEY" // sqlPrimaryKey is a constant string used to represent primary key constraints in column definitions
	sqlUnique     = "UNIQUE"      // sqlUnique is a constant string used to represent unique constraints in column definitions
	sqlDefault    = "DEFAULT"     // sqlDefault is a constant string used to represent default value expressions in column definitions
	sqlCheck      = "CHECK"       // sqlCheck is a constant string used to represent CHECK constraints in column definitions
	sqlForeignKey = "FOREIGN KEY" // sqlForeignKey is a constant string used to represent foreign key constraints in column definitions
	sqlFamily     = "FAMILY"      // sqlFamily is a constant string used to represent family definitions in column definitions
	sqlConstraint = "CONSTRAINT"  // sqlConstraint is a constant string used to represent constraint definitions in column definitions
)
