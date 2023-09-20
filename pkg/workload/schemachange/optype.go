// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

// opType is a enum to represent various types of "operations" that are
// supported by the schemachange workload. Each operation is mapped to a
// generator function via `opFuncs`.
//
//go:generate stringer -type=opType
type opType int

func init() {
	// Assert that every opType has a generator function in opFuncs.
	for op := opType(0); int(op) < numOpTypes; op++ {
		if opFuncs[op] == nil {
			panic(errors.AssertionFailedf(
				"no generator function registered for %q (%d). Did you add an entry to opFuncs?",
				op,
				op,
			))
		}
		if len(opWeights) < int(op) {
			panic(errors.AssertionFailedf(
				"no weight registered for %q (%d). Did you add an entry to opWeights?",
				op,
				op,
			))
		}
	}

	// Sanity check that numOpTypes represents what we expect it to.
	if len(opFuncs) != numOpTypes {
		panic(errors.AssertionFailedf(
			"len(opFuncs) and numOpTypes don't match but a missing operation wasn't found. Did the definition of numOpTypes change?",
		))
	}
}

const (
	addColumn               opType = iota // ALTER TABLE <table> ADD [COLUMN] <column> <type>
	addConstraint                         // ALTER TABLE <table> ADD CONSTRAINT <constraint> <def>
	addForeignKeyConstraint               // ALTER TABLE <table> ADD CONSTRAINT <constraint> FOREIGN KEY (<column>) REFERENCES <table> (<column>)
	addRegion                             // ALTER DATABASE <db> ADD REGION <region>
	addUniqueConstraint                   // ALTER TABLE <table> ADD CONSTRAINT <constraint> UNIQUE (<column>)

	alterTableLocality // ALTER TABLE <table> LOCALITY <locality>

	createIndex    // CREATE INDEX <index> ON <table> <def>
	createSequence // CREATE SEQUENCE <sequence> <def>
	createTable    // CREATE TABLE <table> <def>
	createTableAs  // CREATE TABLE <table> AS <def>
	createView     // CREATE VIEW <view> AS <def>
	createEnum     // CREATE TYPE <type> ENUM AS <def>
	createSchema   // CREATE SCHEMA <schema>

	dropColumn        // ALTER TABLE <table> DROP COLUMN <column>
	dropColumnDefault // ALTER TABLE <table> ALTER [COLUMN] <column> DROP DEFAULT
	dropColumnNotNull // ALTER TABLE <table> ALTER [COLUMN] <column> DROP NOT NULL
	dropColumnStored  // ALTER TABLE <table> ALTER [COLUMN] <column> DROP STORED
	dropConstraint    // ALTER TABLE <table> DROP CONSTRAINT <constraint>
	dropIndex         // DROP INDEX <index>@<table>
	dropSequence      // DROP SEQUENCE <sequence>
	dropTable         // DROP TABLE <table>
	dropEnumValue     // ALTER TYPE <type> DROP VALUE <value>
	dropView          // DROP VIEW <view>
	dropSchema        // DROP SCHEMA <schema>

	primaryRegion //  ALTER DATABASE <db> PRIMARY REGION <region>

	renameColumn   // ALTER TABLE <table> RENAME [COLUMN] <column> TO <column>
	renameIndex    // ALTER TABLE <table> RENAME CONSTRAINT <constraint> TO <constraint>
	renameSequence // ALTER SEQUENCE <sequence> RENAME TO <sequence>
	renameTable    // ALTER TABLE <table> RENAME TO <table>
	renameView     // ALTER VIEW <view> RENAME TO <view>

	setColumnDefault // ALTER TABLE <table> ALTER [COLUMN] <column> SET DEFAULT <expr>
	setColumnNotNull // ALTER TABLE <table> ALTER [COLUMN] <column> SET NOT NULL
	setColumnType    // ALTER TABLE <table> ALTER [COLUMN] <column> [SET DATA] TYPE <type>

	survive // ALTER DATABASE <db> SURVIVE <failure_mode>

	insertRow // INSERT INTO <table> (<cols>) VALUES (<values>)

	selectStmt // SELECT..

	validate // validate all table descriptors

	// numOpTypes contains the total number of opType entries and is used to
	// perform runtime assertions about various structures that aid in operation
	// generation.
	numOpTypes int = iota
)

var opFuncs = []func(*operationGenerator, context.Context, pgx.Tx) (*opStmt, error){
	addColumn:               (*operationGenerator).addColumn,
	addConstraint:           (*operationGenerator).addConstraint,
	addForeignKeyConstraint: (*operationGenerator).addForeignKeyConstraint,
	addRegion:               (*operationGenerator).addRegion,
	addUniqueConstraint:     (*operationGenerator).addUniqueConstraint,
	alterTableLocality:      (*operationGenerator).alterTableLocality,
	createIndex:             (*operationGenerator).createIndex,
	createSequence:          (*operationGenerator).createSequence,
	createTable:             (*operationGenerator).createTable,
	createTableAs:           (*operationGenerator).createTableAs,
	createView:              (*operationGenerator).createView,
	createEnum:              (*operationGenerator).createEnum,
	createSchema:            (*operationGenerator).createSchema,
	dropColumn:              (*operationGenerator).dropColumn,
	dropColumnDefault:       (*operationGenerator).dropColumnDefault,
	dropColumnNotNull:       (*operationGenerator).dropColumnNotNull,
	dropColumnStored:        (*operationGenerator).dropColumnStored,
	dropConstraint:          (*operationGenerator).dropConstraint,
	dropIndex:               (*operationGenerator).dropIndex,
	dropSequence:            (*operationGenerator).dropSequence,
	dropTable:               (*operationGenerator).dropTable,
	dropView:                (*operationGenerator).dropView,
	dropEnumValue:           (*operationGenerator).dropTypeValue,
	dropSchema:              (*operationGenerator).dropSchema,
	primaryRegion:           (*operationGenerator).primaryRegion,
	renameColumn:            (*operationGenerator).renameColumn,
	renameIndex:             (*operationGenerator).renameIndex,
	renameSequence:          (*operationGenerator).renameSequence,
	renameTable:             (*operationGenerator).renameTable,
	renameView:              (*operationGenerator).renameView,
	setColumnDefault:        (*operationGenerator).setColumnDefault,
	setColumnNotNull:        (*operationGenerator).setColumnNotNull,
	setColumnType:           (*operationGenerator).setColumnType,
	survive:                 (*operationGenerator).survive,
	insertRow:               (*operationGenerator).insertRow,
	selectStmt:              (*operationGenerator).selectStmt,
	validate:                (*operationGenerator).validate,
}

var opWeights = []int{
	addColumn:               1,
	addConstraint:           0, // TODO(spaskob): unimplemented
	addForeignKeyConstraint: 0, // Disabled and tracked with #91195
	addRegion:               1,
	addUniqueConstraint:     0,
	alterTableLocality:      1,
	createIndex:             1,
	createSequence:          1,
	createTable:             1,
	createTableAs:           1,
	createView:              1,
	createEnum:              1,
	createSchema:            1,
	dropColumn:              0,
	dropColumnDefault:       1,
	dropColumnNotNull:       1,
	dropColumnStored:        1,
	dropConstraint:          1,
	dropIndex:               1,
	dropSequence:            1,
	dropTable:               1,
	dropView:                1,
	dropEnumValue:           1,
	dropSchema:              1,
	primaryRegion:           0, // Disabled and tracked with #83831
	renameColumn:            1,
	renameIndex:             1,
	renameSequence:          1,
	renameTable:             1,
	renameView:              1,
	setColumnDefault:        1,
	setColumnNotNull:        1,
	setColumnType:           0, // Disabled and tracked with #66662.
	survive:                 0, // Disabled and tracked with #83831
	insertRow:               0, // Disabled and tracked with #91863
	selectStmt:              10,
	validate:                2, // validate twice more often
}

// This workload will maintain its own list of supported versions for declarative
// schema changer, since the cluster we are running against can be downlevel.
// The declarative schema changer builder does have a supported list, but it's not
// sufficient for that reason.
var opDeclarativeVersion = map[opType]clusterversion.Key{
	addColumn:               clusterversion.BinaryMinSupportedVersionKey,
	addForeignKeyConstraint: clusterversion.BinaryMinSupportedVersionKey,
	addUniqueConstraint:     clusterversion.BinaryMinSupportedVersionKey,
	createIndex:             clusterversion.BinaryMinSupportedVersionKey,
	createSequence:          clusterversion.BinaryMinSupportedVersionKey,
	dropColumn:              clusterversion.BinaryMinSupportedVersionKey,
	dropColumnNotNull:       clusterversion.BinaryMinSupportedVersionKey,
	dropConstraint:          clusterversion.BinaryMinSupportedVersionKey,
	dropIndex:               clusterversion.BinaryMinSupportedVersionKey,
	dropSequence:            clusterversion.BinaryMinSupportedVersionKey,
	dropTable:               clusterversion.BinaryMinSupportedVersionKey,
	dropView:                clusterversion.BinaryMinSupportedVersionKey,
	dropEnumValue:           clusterversion.BinaryMinSupportedVersionKey,
	dropSchema:              clusterversion.BinaryMinSupportedVersionKey,
}
