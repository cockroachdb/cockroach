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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
)

// seqNum may be shared across multiple instances of this, so it should only
// be change atomically.
type operationGeneratorParams struct {
	seqNum             *int64
	errorRate          int
	enumPct            int
	rng                *rand.Rand
	ops                *deck
	maxSourceTables    int
	sequenceOwnedByPct int
	fkParentInvalidPct int
	fkChildInvalidPct  int
}

// The OperationBuilder has the sole responsibility of generating ops
type operationGenerator struct {
	params               *operationGeneratorParams
	expectedExecErrors   errorCodeSet
	expectedCommitErrors errorCodeSet

	// This stores expected commit errors while an op statement
	// is still being constructed. It is possible that one of the functions in opFuncs
	// fails. In this case, the candidateExpectedCommitErrors will be discarded. If the
	// function succeeds and the op statement is constructed, then candidateExpectedCommitErrors
	// are added to expectedCommitErrors.
	candidateExpectedCommitErrors errorCodeSet

	// opsInTxn is a list of previous ops in the current transaction implemented
	// as a map for fast lookups.
	opsInTxn map[opType]bool
}

func makeOperationGenerator(params *operationGeneratorParams) *operationGenerator {
	return &operationGenerator{
		params:                        params,
		expectedExecErrors:            makeExpectedErrorSet(),
		expectedCommitErrors:          makeExpectedErrorSet(),
		candidateExpectedCommitErrors: makeExpectedErrorSet(),
		opsInTxn:                      map[opType]bool{},
	}
}

// Reset internal state used per operation within a transaction
func (og *operationGenerator) resetOpState() {
	og.expectedExecErrors.reset()
	og.candidateExpectedCommitErrors.reset()
}

// Reset internal state used per transaction
func (og *operationGenerator) resetTxnState() {
	og.expectedCommitErrors.reset()

	for k := range og.opsInTxn {
		delete(og.opsInTxn, k)
	}
}

//go:generate stringer -type=opType
type opType int

const (
	addColumn               opType = iota // ALTER TABLE <table> ADD [COLUMN] <column> <type>
	addConstraint                         // ALTER TABLE <table> ADD CONSTRAINT <constraint> <def>
	addForeignKeyConstraint               // ALTER TABLE <table> ADD CONSTRAINT <constraint> FOREIGN KEY (<column>) REFERENCES <table> (<column>)
	addUniqueConstraint                   // ALTER TABLE <table> ADD CONSTRAINT <constraint> UNIQUE (<column>)

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
	dropView          // DROP VIEW <view>
	dropSchema        // DROP SCHEMA <schema>

	renameColumn   // ALTER TABLE <table> RENAME [COLUMN] <column> TO <column>
	renameIndex    // ALTER TABLE <table> RENAME CONSTRAINT <constraint> TO <constraint>
	renameSequence // ALTER SEQUENCE <sequence> RENAME TO <sequence>
	renameTable    // ALTER TABLE <table> RENAME TO <table>
	renameView     // ALTER VIEW <view> RENAME TO <view>

	setColumnDefault // ALTER TABLE <table> ALTER [COLUMN] <column> SET DEFAULT <expr>
	setColumnNotNull // ALTER TABLE <table> ALTER [COLUMN] <column> SET NOT NULL
	setColumnType    // ALTER TABLE <table> ALTER [COLUMN] <column> [SET DATA] TYPE <type>

	insertRow // INSERT INTO <table> (<cols>) VALUES (<values>)

	validate // validate all table descriptors

	numOpTypes int = iota
)

var opFuncs = map[opType]func(*operationGenerator, *pgx.Tx) (string, error){
	addColumn:               (*operationGenerator).addColumn,
	addConstraint:           (*operationGenerator).addConstraint,
	addForeignKeyConstraint: (*operationGenerator).addForeignKeyConstraint,
	addUniqueConstraint:     (*operationGenerator).addUniqueConstraint,
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
	dropSchema:              (*operationGenerator).dropSchema,
	renameColumn:            (*operationGenerator).renameColumn,
	renameIndex:             (*operationGenerator).renameIndex,
	renameSequence:          (*operationGenerator).renameSequence,
	renameTable:             (*operationGenerator).renameTable,
	renameView:              (*operationGenerator).renameView,
	setColumnDefault:        (*operationGenerator).setColumnDefault,
	setColumnNotNull:        (*operationGenerator).setColumnNotNull,
	setColumnType:           (*operationGenerator).setColumnType,
	insertRow:               (*operationGenerator).insertRow,
	validate:                (*operationGenerator).validate,
}

func init() {
	// Validate that we have an operation function for each opType.
	if len(opFuncs) != numOpTypes {
		panic(errors.Errorf("expected %d opFuncs, got %d", numOpTypes, len(opFuncs)))
	}
}

var opWeights = []int{
	addColumn:               1,
	addConstraint:           0, // TODO(spaskob): unimplemented
	addForeignKeyConstraint: 1,
	addUniqueConstraint:     1,
	createIndex:             1,
	createSequence:          1,
	createTable:             1,
	createTableAs:           1,
	createView:              1,
	createEnum:              1,
	createSchema:            1,
	dropColumn:              1,
	dropColumnDefault:       1,
	dropColumnNotNull:       1,
	dropColumnStored:        1,
	dropConstraint:          1,
	dropIndex:               1,
	dropSequence:            1,
	dropTable:               1,
	dropView:                1,
	dropSchema:              1,
	renameColumn:            1,
	renameIndex:             1,
	renameSequence:          1,
	renameTable:             1,
	renameView:              1,
	setColumnDefault:        1,
	setColumnNotNull:        1,
	setColumnType:           1,
	insertRow:               1,
	validate:                2, // validate twice more often
}

// randOp attempts to produce a random schema change operation. It returns a
// triple `(randOp, log, error)`. On success `randOp` is the random schema
// change constructed. Constructing a random schema change may require a few
// stochastic attempts and if verbosity is >= 2 the unsuccessful attempts are
// recorded in `log` to help with debugging of the workload.
func (og *operationGenerator) randOp(tx *pgx.Tx) (stmt string, noops []string, err error) {
	savepointCount := 0
	for {
		op := opType(og.params.ops.Int())
		og.resetOpState()

		// Savepoints are used to prevent an infinite loop from occurring as a result of the transaction
		// becoming aborted by an op function. If a transaction becomes aborted, no op function will be able
		// to complete without error because all op functions rely on the passed transaction to introspect the database.
		// With sufficient verbosity, any failed op functions will be logged as NOOPs below for debugging purposes.
		//
		// For example, functions such as getTableColumns() can abort the transaction if called with
		// a non existing table. getTableColumns() is used by op functions such as createIndex().
		// Op functions normally ensure that a table exists by checking system tables before passing the
		// table to getTableColumns(). However, due to bugs such as #57494, system tables
		// may be inaccurate and can cause getTableColumns() to be called with a non-existing table. This
		// will result in an aborted transaction. Wrapping calls to op functions with savepoints will protect
		// the transaction from being aborted by spurious errors such as in the above example.
		savepointCount++
		if _, err := tx.Exec(fmt.Sprintf(`SAVEPOINT s%d`, savepointCount)); err != nil {
			return "", noops, err
		}

		stmt, err := opFuncs[op](og, tx)

		if err == nil {
			// Screen for schema change after write in the same transaction.
			if op != insertRow && op != validate {
				if _, previous := og.opsInTxn[insertRow]; previous {
					og.expectedExecErrors.add(pgcode.FeatureNotSupported)
				}
			}

			// Add candidateExpectedCommitErrors to expectedCommitErrors
			og.expectedCommitErrors.merge(og.candidateExpectedCommitErrors)

			og.opsInTxn[op] = true

			return stmt, noops, err
		}
		if _, err := tx.Exec(fmt.Sprintf(`ROLLBACK TO SAVEPOINT s%d`, savepointCount)); err != nil {
			return "", noops, err
		}

		noops = append(noops, fmt.Sprintf("NOOP: %s -> %v", op, err))
	}
}

func (og *operationGenerator) addColumn(tx *pgx.Tx) (string, error) {

	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IrrelevantColumnName string`, tableName), nil
	}

	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	typName, err := og.randType(tx, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	def := &tree.ColumnTableDef{
		Name: tree.Name(columnName),
		Type: typName,
	}
	def.Nullable.Nullability = tree.Nullability(rand.Intn(1 + int(tree.SilentNull)))

	columnExistsOnTable, err := columnExistsOnTable(tx, tableName, columnName)
	if err != nil {
		return "", err
	}
	typeExists, err := typeExists(tx, typName)
	if err != nil {
		return "", err
	}
	tableHasRows, err := tableHasRows(tx, tableName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.DuplicateColumn, condition: columnExistsOnTable},
		{code: pgcode.UndefinedObject, condition: !typeExists},
		{code: pgcode.NotNullViolation, condition: tableHasRows && def.Nullable.Nullability == tree.NotNull},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s`, tableName, tree.Serialize(def)), nil
}

func (og *operationGenerator) addConstraint(tx *pgx.Tx) (string, error) {
	// TODO(peter): unimplemented
	// - Export sqlbase.randColumnTableDef.
	return "", nil
}

func (og *operationGenerator) addUniqueConstraint(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT IrrelevantConstraintName UNIQUE (IrrelevantColumnName)`, tableName), nil
	}

	columnForConstraint, err := og.randColumnWithMeta(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	constaintName := fmt.Sprintf("%s_%s_unique", tableName.Object(), columnForConstraint.name)

	columnExistsOnTable, err := columnExistsOnTable(tx, tableName, columnForConstraint.name)
	if err != nil {
		return "", err
	}
	constraintExists, err := constraintExists(tx, constaintName)
	if err != nil {
		return "", err
	}

	canApplyConstraint := true
	if columnExistsOnTable {
		canApplyConstraint, err = canApplyUniqueConstraint(tx, tableName, []string{columnForConstraint.name})
		if err != nil {
			return "", err
		}
	}

	codesWithConditions{
		{code: pgcode.UndefinedColumn, condition: !columnExistsOnTable},
		{code: pgcode.DuplicateObject, condition: constraintExists},
		{code: pgcode.FeatureNotSupported, condition: columnExistsOnTable && !colinfo.ColumnTypeIsIndexable(columnForConstraint.typ)},
	}.add(og.expectedExecErrors)

	if !canApplyConstraint {
		og.candidateExpectedCommitErrors.add(pgcode.UniqueViolation)
	}

	return fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)`, tableName, constaintName, columnForConstraint.name), nil
}

func (og *operationGenerator) addForeignKeyConstraint(tx *pgx.Tx) (string, error) {

	parentTable, parentColumn, err := og.randParentColumnForFkRelation(tx, og.randIntn(100) >= og.params.fkParentInvalidPct)
	if err != nil {
		return "", err
	}

	fetchInvalidChild := og.randIntn(100) < og.params.fkChildInvalidPct
	// Potentially create an error by choosing the wrong type for the child column.
	childType := parentColumn.typ
	if fetchInvalidChild {
		typeName, err := og.randType(tx, og.pctExisting(true))
		if err != nil {
			return "", err
		}
		childType, err = og.typeFromTypeName(tx, typeName.String())
		if err != nil {
			return "", err
		}
	}

	childTable, childColumn, err := og.randChildColumnForFkRelation(tx, !fetchInvalidChild, childType.SQLString())
	if err != nil {
		return "", err
	}

	constraintName := tree.Name(fmt.Sprintf("%s_%s_%s_%s_fk", parentTable.Object(), parentColumn.name, childTable.Object(), childColumn.name))

	def := &tree.AlterTable{
		Table: childTable.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddConstraint{
				ConstraintDef: &tree.ForeignKeyConstraintTableDef{
					Name:     constraintName,
					Table:    *parentTable,
					FromCols: tree.NameList{tree.Name(childColumn.name)},
					ToCols:   tree.NameList{tree.Name(parentColumn.name)},
					Actions: tree.ReferenceActions{
						Update: tree.Cascade,
						Delete: tree.Cascade,
					},
				},
				ValidationBehavior: tree.ValidationDefault,
			},
		},
	}

	parentColumnHasUniqueConstraint, err := columnHasSingleUniqueConstraint(tx, parentTable, parentColumn.name)
	if err != nil {
		return "", err
	}
	childColumnIsComputed, err := columnIsComputed(tx, parentTable, parentColumn.name)
	if err != nil {
		return "", err
	}
	constraintExists, err := constraintExists(tx, string(constraintName))
	if err != nil {
		return "", err
	}
	rowsSatisfyConstraint, err := rowsSatisfyFkConstraint(tx, parentTable, parentColumn, childTable, childColumn)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.ForeignKeyViolation, condition: !parentColumnHasUniqueConstraint},
		{code: pgcode.FeatureNotSupported, condition: childColumnIsComputed},
		{code: pgcode.DuplicateObject, condition: constraintExists},
		{code: pgcode.DatatypeMismatch, condition: !childColumn.typ.Equivalent(parentColumn.typ)},
	}.add(og.expectedExecErrors)

	if !rowsSatisfyConstraint {
		og.candidateExpectedCommitErrors.add(pgcode.ForeignKeyViolation)
	}

	return tree.Serialize(def), nil
}

func (og *operationGenerator) createIndex(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		def := &tree.CreateIndex{
			Name:  tree.Name("IrrelevantName"),
			Table: *tableName,
			Columns: tree.IndexElemList{
				{Column: "IrrelevantColumn", Direction: tree.Ascending},
			},
		}
		return tree.Serialize(def), nil
	}

	columnNames, err := og.getTableColumns(tx, tableName.String(), true)
	if err != nil {
		return "", err
	}

	indexName, err := og.randIndex(tx, *tableName, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	indexExists, err := indexExists(tx, tableName, indexName)
	if err != nil {
		return "", err
	}

	def := &tree.CreateIndex{
		Name:        tree.Name(indexName),
		Table:       *tableName,
		Unique:      og.randIntn(4) == 0,  // 25% UNIQUE
		Inverted:    og.randIntn(10) == 0, // 10% INVERTED
		IfNotExists: og.randIntn(2) == 0,  // 50% IF NOT EXISTS
	}

	// Define columns on which to create an index. Check for types which cannot be indexed.
	nonIndexableType := false
	def.Columns = make(tree.IndexElemList, 1+og.randIntn(len(columnNames)))
	for i := range def.Columns {
		def.Columns[i].Column = tree.Name(columnNames[i].name)
		def.Columns[i].Direction = tree.Direction(og.randIntn(1 + int(tree.Descending)))

		if def.Inverted {
			if !colinfo.ColumnTypeIsInvertedIndexable(columnNames[i].typ) {
				nonIndexableType = true
			}
		} else {
			if !colinfo.ColumnTypeIsIndexable(columnNames[i].typ) {
				nonIndexableType = true
			}
		}
	}

	// If there are extra columns not used in the index, randomly use them
	// as stored columns.
	duplicateStore := false
	columnNames = columnNames[len(def.Columns):]
	if n := len(columnNames); n > 0 {
		def.Storing = make(tree.NameList, og.randIntn(1+n))
		for i := range def.Storing {
			def.Storing[i] = tree.Name(columnNames[i].name)
		}

		// If the column is already used in the primary key, then attempting to store
		// it using an index will produce a pgcode.DuplicateColumn error.
		colUsedInPrimaryIdx, err := columnsStoredInPrimaryIdx(tx, tableName, def.Storing)
		if err != nil {
			return "", err
		}
		if colUsedInPrimaryIdx {
			duplicateStore = true
		}
	}

	// Verify that a unique constraint can be added given the existing rows which may exist in the table.
	uniqueViolationWillNotOccur := true
	if def.Unique {
		columns := []string{}
		for _, col := range def.Columns {
			columns = append(columns, string(col.Column))
		}
		uniqueViolationWillNotOccur, err = canApplyUniqueConstraint(tx, tableName, columns)
		if err != nil {
			return "", err
		}
	}

	// When an index exists, but `IF NOT EXISTS` is used, then
	// the index will not be created and the op will complete without errors.
	if !(indexExists && def.IfNotExists) {
		codesWithConditions{
			{code: pgcode.DuplicateRelation, condition: indexExists},
			// Inverted indexes do not support stored columns.
			{code: pgcode.InvalidSQLStatementName, condition: len(def.Storing) > 0 && def.Inverted},
			// Inverted indexes do not support indexing more than one column.
			{code: pgcode.InvalidSQLStatementName, condition: len(def.Columns) > 1 && def.Inverted},
			// Inverted indexes cannot be unique.
			{code: pgcode.InvalidSQLStatementName, condition: def.Unique && def.Inverted},
			// If there is data in the table such that a unique index cannot be created,
			// a pgcode.UniqueViolation will occur and will be wrapped in a
			// pgcode.TransactionCommittedWithSchemaChangeFailure. The schemachange worker
			// is expected to parse for the underlying error.
			{code: pgcode.UniqueViolation, condition: !uniqueViolationWillNotOccur},
			{code: pgcode.DuplicateColumn, condition: duplicateStore},
			{code: pgcode.FeatureNotSupported, condition: nonIndexableType},
		}.add(og.expectedExecErrors)
	}

	return tree.Serialize(def), nil
}

func (og *operationGenerator) createSequence(tx *pgx.Tx) (string, error) {
	seqName, err := og.randSequence(tx, og.pctExisting(false), "")
	if err != nil {
		return "", err
	}

	schemaExists, err := schemaExists(tx, seqName.Schema())
	if err != nil {
		return "", err
	}
	sequenceExists, err := sequenceExists(tx, seqName)
	if err != nil {
		return "", err
	}

	// If the sequence exists and an error should be produced, then
	// exclude the IF NOT EXISTS clause from the statement. Otherwise, default
	// to including the clause prevent all pgcode.DuplicateRelation errors.
	ifNotExists := true
	if sequenceExists && og.produceError() {
		ifNotExists = false
	}

	codesWithConditions{
		{code: pgcode.UndefinedSchema, condition: !schemaExists},
		{code: pgcode.DuplicateRelation, condition: sequenceExists && !ifNotExists},
	}.add(og.expectedExecErrors)

	var seqOptions tree.SequenceOptions
	// Decide if the sequence should be owned by a column. If so, it can
	// set using the tree.SeqOptOwnedBy sequence option.
	if og.randIntn(100) < og.params.sequenceOwnedByPct {
		table, err := og.randTable(tx, og.pctExisting(true), "")
		if err != nil {
			return "", err
		}
		tableExists, err := tableExists(tx, table)
		if err != nil {
			return "", err
		}

		if !tableExists {
			seqOptions = append(
				seqOptions,
				tree.SequenceOption{
					Name:          tree.SeqOptOwnedBy,
					ColumnItemVal: &tree.ColumnItem{TableName: table.ToUnresolvedObjectName(), ColumnName: "IrrelevantColumnName"}},
			)
			og.expectedExecErrors.add(pgcode.UndefinedTable)
		} else {
			column, err := og.randColumn(tx, *table, og.pctExisting(true))
			if err != nil {
				return "", err
			}
			columnExists, err := columnExistsOnTable(tx, table, column)
			if err != nil {
				return "", err
			}
			// If a duplicate sequence exists, then a new sequence will not be created. In this case,
			// a pgcode.UndefinedColumn will not occur.
			if !columnExists && !sequenceExists {
				og.expectedExecErrors.add(pgcode.UndefinedColumn)
			}

			seqOptions = append(
				seqOptions,
				tree.SequenceOption{
					Name:          tree.SeqOptOwnedBy,
					ColumnItemVal: &tree.ColumnItem{TableName: table.ToUnresolvedObjectName(), ColumnName: tree.Name(column)}},
			)
		}
	}

	createSeq := &tree.CreateSequence{
		IfNotExists: ifNotExists,
		Name:        *seqName,
		Options:     seqOptions,
	}

	return tree.Serialize(createSeq), nil
}

func (og *operationGenerator) createTable(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(false), "")
	if err != nil {
		return "", err
	}

	tableIdx, err := strconv.Atoi(strings.TrimPrefix(tableName.Table(), "table"))
	if err != nil {
		return "", err
	}

	stmt := rowenc.RandCreateTableWithColumnIndexNumberGenerator(og.params.rng, "table", tableIdx, og.newUniqueSeqNum)
	stmt.Table = *tableName
	stmt.IfNotExists = og.randIntn(2) == 0

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	schemaExists, err := schemaExists(tx, tableName.Schema())
	if err != nil {
		return "", err
	}
	codesWithConditions{
		{code: pgcode.DuplicateRelation, condition: tableExists && !stmt.IfNotExists},
		{code: pgcode.UndefinedSchema, condition: !schemaExists},
	}.add(og.expectedExecErrors)

	return tree.Serialize(stmt), nil
}

func (og *operationGenerator) createEnum(tx *pgx.Tx) (string, error) {
	typName, err := og.randEnum(tx, og.pctExisting(false))
	if err != nil {
		return "", err
	}
	schemaExists, err := schemaExists(tx, typName.Schema())
	if err != nil {
		return "", err
	}
	typeExists, err := typeExists(tx, typName)
	if err != nil {
		return "", err
	}
	codesWithConditions{
		{code: pgcode.DuplicateObject, condition: typeExists},
		{code: pgcode.InvalidSchemaName, condition: !schemaExists},
	}.add(og.expectedExecErrors)
	stmt := rowenc.RandCreateType(og.params.rng, typName.Object(), "asdf")
	stmt.(*tree.CreateType).TypeName = typName.ToUnresolvedObjectName()
	return tree.Serialize(stmt), nil
}

func (og *operationGenerator) createTableAs(tx *pgx.Tx) (string, error) {
	numSourceTables := og.randIntn(og.params.maxSourceTables) + 1

	sourceTableNames := make([]tree.TableExpr, numSourceTables)
	sourceTableExistence := make([]bool, numSourceTables)

	// uniqueTableNames and duplicateSourceTables are used to track unique
	// tables. If there are any duplicates, then a pgcode.DuplicateAlias error
	// is expected on execution.
	uniqueTableNames := map[string]bool{}
	duplicateSourceTables := false

	// Collect a random set of size numSourceTables that contains tables and views
	// from which to use columns.
	for i := 0; i < numSourceTables; i++ {
		var tableName *tree.TableName
		var err error
		var sourceTableExists bool

		switch randInt := og.randIntn(1); randInt {
		case 0:
			tableName, err = og.randTable(tx, og.pctExisting(true), "")
			if err != nil {
				return "", err
			}
			sourceTableExists, err = tableExists(tx, tableName)
			if err != nil {
				return "", err
			}

		case 1:
			tableName, err = og.randView(tx, og.pctExisting(true), "")
			if err != nil {
				return "", err
			}
			sourceTableExists, err = viewExists(tx, tableName)
			if err != nil {
				return "", err
			}
		}

		sourceTableNames[i] = tableName
		sourceTableExistence[i] = sourceTableExists
		if _, exists := uniqueTableNames[tableName.String()]; exists {
			duplicateSourceTables = true
		} else {
			uniqueTableNames[tableName.String()] = true
		}
	}

	selectStatement := tree.SelectClause{
		From: tree.From{Tables: sourceTableNames},
	}

	// uniqueColumnNames and duplicateColumns are used to track unique
	// columns. If there are any duplicates, then a pgcode.DuplicateColumn error
	// is expected on execution.
	uniqueColumnNames := map[string]bool{}
	duplicateColumns := false
	for i := 0; i < numSourceTables; i++ {
		tableName := sourceTableNames[i]
		tableExists := sourceTableExistence[i]

		// If the table does not exist, columns cannot be fetched from it. For this reason, the placeholder
		// "IrrelevantColumnName" is used, and a pgcode.UndefinedTable error is expected on execution.
		if tableExists {
			columnNamesForTable, err := og.tableColumnsShuffled(tx, tableName.(*tree.TableName).String())
			if err != nil {
				return "", err
			}
			columnNamesForTable = columnNamesForTable[:1+og.randIntn(len(columnNamesForTable))]

			for j := range columnNamesForTable {
				colItem := tree.ColumnItem{
					TableName:  tableName.(*tree.TableName).ToUnresolvedObjectName(),
					ColumnName: tree.Name(columnNamesForTable[j]),
				}
				selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})

				if _, exists := uniqueColumnNames[columnNamesForTable[j]]; exists {
					duplicateColumns = true
				} else {
					uniqueColumnNames[columnNamesForTable[j]] = true
				}
			}
		} else {
			og.expectedExecErrors.add(pgcode.UndefinedTable)
			colItem := tree.ColumnItem{
				ColumnName: tree.Name("IrrelevantColumnName"),
			}
			selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})
		}
	}

	destTableName, err := og.randTable(tx, og.pctExisting(false), "")
	if err != nil {
		return "", err
	}
	schemaExists, err := schemaExists(tx, destTableName.Schema())
	if err != nil {
		return "", err
	}
	tableExists, err := tableExists(tx, destTableName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.InvalidSchemaName, condition: !schemaExists},
		{code: pgcode.DuplicateRelation, condition: tableExists},
		{code: pgcode.Syntax, condition: len(selectStatement.Exprs) == 0},
		{code: pgcode.DuplicateAlias, condition: duplicateSourceTables},
		{code: pgcode.DuplicateColumn, condition: duplicateColumns},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`CREATE TABLE %s AS %s`,
		destTableName, selectStatement.String()), nil
}

func (og *operationGenerator) createView(tx *pgx.Tx) (string, error) {

	numSourceTables := og.randIntn(og.params.maxSourceTables) + 1

	sourceTableNames := make([]tree.TableExpr, numSourceTables)
	sourceTableExistence := make([]bool, numSourceTables)

	// uniqueTableNames and duplicateSourceTables are used to track unique
	// tables. If there are any duplicates, then a pgcode.DuplicateColumn error
	// is expected on execution.
	uniqueTableNames := map[string]bool{}
	duplicateSourceTables := false

	// Collect a random set of size numSourceTables that contains tables and views
	// from which to use columns.
	for i := 0; i < numSourceTables; i++ {
		var tableName *tree.TableName
		var err error
		var sourceTableExists bool

		switch randInt := og.randIntn(1); randInt {
		case 0:
			tableName, err = og.randTable(tx, og.pctExisting(true), "")
			if err != nil {
				return "", err
			}
			sourceTableExists, err = tableExists(tx, tableName)
			if err != nil {
				return "", err
			}

		case 1:
			tableName, err = og.randView(tx, og.pctExisting(true), "")
			if err != nil {
				return "", err
			}
			sourceTableExists, err = viewExists(tx, tableName)
			if err != nil {
				return "", err
			}
		}

		sourceTableNames[i] = tableName
		sourceTableExistence[i] = sourceTableExists
		if _, exists := uniqueTableNames[tableName.String()]; exists {
			duplicateSourceTables = true
		} else {
			uniqueTableNames[tableName.String()] = true
		}
	}

	selectStatement := tree.SelectClause{
		From: tree.From{Tables: sourceTableNames},
	}

	// uniqueColumnNames and duplicateColumns are used to track unique
	// columns. If there are any duplicates, then a pgcode.DuplicateColumn error
	// is expected on execution.
	uniqueColumnNames := map[string]bool{}
	duplicateColumns := false
	for i := 0; i < numSourceTables; i++ {
		tableName := sourceTableNames[i]
		tableExists := sourceTableExistence[i]

		// If the table does not exist, columns cannot be fetched from it. For this reason, the placeholder
		// "IrrelevantColumnName" is used, and a pgcode.UndefinedTable error is expected on execution.
		if tableExists {
			columnNamesForTable, err := og.tableColumnsShuffled(tx, tableName.(*tree.TableName).String())
			if err != nil {
				return "", err
			}
			columnNamesForTable = columnNamesForTable[:1+og.randIntn(len(columnNamesForTable))]

			for j := range columnNamesForTable {
				colItem := tree.ColumnItem{
					TableName:  tableName.(*tree.TableName).ToUnresolvedObjectName(),
					ColumnName: tree.Name(columnNamesForTable[j]),
				}
				selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})

				if _, exists := uniqueColumnNames[columnNamesForTable[j]]; exists {
					duplicateColumns = true
				} else {
					uniqueColumnNames[columnNamesForTable[j]] = true
				}
			}
		} else {
			og.expectedExecErrors.add(pgcode.UndefinedTable)
			colItem := tree.ColumnItem{
				ColumnName: tree.Name("IrrelevantColumnName"),
			}
			selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})
		}
	}

	destViewName, err := og.randView(tx, og.pctExisting(false), "")
	if err != nil {
		return "", err
	}
	schemaExists, err := schemaExists(tx, destViewName.Schema())
	if err != nil {
		return "", err
	}
	viewExists, err := viewExists(tx, destViewName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.InvalidSchemaName, condition: !schemaExists},
		{code: pgcode.DuplicateRelation, condition: viewExists},
		{code: pgcode.Syntax, condition: len(selectStatement.Exprs) == 0},
		{code: pgcode.DuplicateAlias, condition: duplicateSourceTables},
		{code: pgcode.DuplicateColumn, condition: duplicateColumns},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`CREATE VIEW %s AS %s`,
		destViewName, selectStatement.String()), nil
}

func (og *operationGenerator) dropColumn(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "IrrelevantColumnName"`, tableName), nil
	}

	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	columnExists, err := columnExistsOnTable(tx, tableName, columnName)
	if err != nil {
		return "", err
	}
	colIsPrimaryKey, err := colIsPrimaryKey(tx, tableName, columnName)
	if err != nil {
		return "", err
	}
	columnIsDependedOn, err := columnIsDependedOn(tx, tableName, columnName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.UndefinedColumn, condition: !columnExists},
		{code: pgcode.InvalidColumnReference, condition: colIsPrimaryKey},
		{code: pgcode.DependentObjectsStillExist, condition: columnIsDependedOn},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "%s"`, tableName, columnName), nil
}

func (og *operationGenerator) dropColumnDefault(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "IrrelevantColumnName" DROP DEFAULT`, tableName), nil
	}
	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	columnExists, err := columnExistsOnTable(tx, tableName, columnName)
	if err != nil {
		return "", err
	}
	if !columnExists {
		og.expectedExecErrors.add(pgcode.UndefinedColumn)
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP DEFAULT`, tableName, columnName), nil
}

func (og *operationGenerator) dropColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "IrrelevantColumnName" DROP NOT NULL`, tableName), nil
	}
	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	columnExists, err := columnExistsOnTable(tx, tableName, columnName)
	if err != nil {
		return "", err
	}
	colIsPrimaryKey, err := colIsPrimaryKey(tx, tableName, columnName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{pgcode.UndefinedColumn, !columnExists},
		{pgcode.InvalidTableDefinition, colIsPrimaryKey},
	}.add(og.expectedExecErrors)
	if !columnExists {
		og.expectedExecErrors.add(pgcode.UndefinedColumn)
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL`, tableName, columnName), nil
}

func (og *operationGenerator) dropColumnStored(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN IrrelevantColumnName DROP STORED`, tableName), nil
	}

	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	columnExists, err := columnExistsOnTable(tx, tableName, columnName)
	if err != nil {
		return "", err
	}

	columnIsComputed, err := columnIsComputed(tx, tableName, columnName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.InvalidColumnDefinition, condition: !columnIsComputed},
		{code: pgcode.UndefinedColumn, condition: !columnExists},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP STORED`, tableName, columnName), nil
}

func (og *operationGenerator) dropConstraint(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT IrrelevantConstraintName`, tableName), nil
	}

	constraintName, err := og.randConstraint(tx, tableName.String())
	if err != nil {
		return "", err
	}

	// Dropping the primary key of a table without adding a new primary key
	// subsequently in the transaction is not supported. Since addConstraint is not implemented,
	// a replacement primary key will not be created in the same transaction. Thus,
	// dropping a primary key will always produce an error.
	constraintIsPrimary, err := constraintIsPrimary(tx, tableName, constraintName)
	if err != nil {
		return "", err
	}
	if constraintIsPrimary {
		og.candidateExpectedCommitErrors.add(pgcode.FeatureNotSupported)
	}

	// DROP INDEX CASCADE is preferred for dropping unique constraints, and
	// dropping the constraint with ALTER TABLE ... DROP CONSTRAINT is unsupported.
	constraintIsUnique, err := constraintIsUnique(tx, tableName, constraintName)
	if err != nil {
		return "", err
	}
	if constraintIsUnique {
		og.expectedExecErrors.add(pgcode.FeatureNotSupported)
	}

	return fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT "%s"`, tableName, constraintName), nil
}

func (og *operationGenerator) dropIndex(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`DROP INDEX %s@"IrrelevantIndexName"`, tableName), nil
	}

	indexName, err := og.randIndex(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	indexExists, err := indexExists(tx, tableName, indexName)
	if err != nil {
		return "", err
	}
	if !indexExists {
		og.expectedExecErrors.add(pgcode.UndefinedObject)
	}

	return fmt.Sprintf(`DROP INDEX %s@"%s" CASCADE`, tableName, indexName), nil
}

func (og *operationGenerator) dropSequence(tx *pgx.Tx) (string, error) {
	sequenceName, err := og.randSequence(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	ifExists := og.randIntn(2) == 0
	dropSeq := &tree.DropSequence{
		Names:    tree.TableNames{*sequenceName},
		IfExists: ifExists,
	}

	sequenceExists, err := sequenceExists(tx, sequenceName)
	if err != nil {
		return "", err
	}
	if !sequenceExists && !ifExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
	}
	return tree.Serialize(dropSeq), nil
}

func (og *operationGenerator) dropTable(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	tableHasDependencies, err := tableHasDependencies(tx, tableName)
	if err != nil {
		return "", err
	}

	dropBehavior := tree.DropBehavior(og.randIntn(3))

	ifExists := og.randIntn(2) == 0
	dropTable := tree.DropTable{
		Names:        []tree.TableName{*tableName},
		IfExists:     ifExists,
		DropBehavior: dropBehavior,
	}

	codesWithConditions{
		{pgcode.UndefinedTable, !ifExists && !tableExists},
		{pgcode.DependentObjectsStillExist, dropBehavior != tree.DropCascade && tableHasDependencies},
	}.add(og.expectedExecErrors)

	return dropTable.String(), nil
}

func (og *operationGenerator) dropView(tx *pgx.Tx) (string, error) {
	viewName, err := og.randView(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	viewExists, err := tableExists(tx, viewName)
	if err != nil {
		return "", err
	}
	viewHasDependencies, err := tableHasDependencies(tx, viewName)
	if err != nil {
		return "", err
	}

	dropBehavior := tree.DropBehavior(og.randIntn(3))

	ifExists := og.randIntn(2) == 0
	dropView := tree.DropView{
		Names:        []tree.TableName{*viewName},
		IfExists:     ifExists,
		DropBehavior: dropBehavior,
	}

	codesWithConditions{
		{pgcode.UndefinedTable, !ifExists && !viewExists},
		{pgcode.DependentObjectsStillExist, dropBehavior != tree.DropCascade && viewHasDependencies},
	}.add(og.expectedExecErrors)
	return dropView.String(), nil
}

func (og *operationGenerator) renameColumn(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	srcTableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !srcTableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN "IrrelevantColumnName" TO "OtherIrrelevantName"`,
			tableName), nil
	}

	srcColumnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	destColumnName, err := og.randColumn(tx, *tableName, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	srcColumnExists, err := columnExistsOnTable(tx, tableName, srcColumnName)
	if err != nil {
		return "", err
	}
	destColumnExists, err := columnExistsOnTable(tx, tableName, destColumnName)
	if err != nil {
		return "", err
	}
	columnIsDependedOn, err := columnIsDependedOn(tx, tableName, srcColumnName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{pgcode.UndefinedColumn, !srcColumnExists},
		{pgcode.DuplicateColumn, destColumnExists && srcColumnName != destColumnName},
		{pgcode.DependentObjectsStillExist, columnIsDependedOn},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN "%s" TO "%s"`,
		tableName, srcColumnName, destColumnName), nil
}

func (og *operationGenerator) renameIndex(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	srcTableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !srcTableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER INDEX %s@"IrrelevantConstraintName" RENAME TO "OtherConstraintName"`,
			tableName), nil
	}

	srcIndexName, err := og.randIndex(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	destIndexName, err := og.randIndex(tx, *tableName, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	srcIndexExists, err := indexExists(tx, tableName, srcIndexName)
	if err != nil {
		return "", err
	}
	destIndexExists, err := indexExists(tx, tableName, destIndexName)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.UndefinedObject, condition: !srcIndexExists},
		{code: pgcode.DuplicateRelation, condition: destIndexExists && srcIndexName != destIndexName},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER INDEX %s@"%s" RENAME TO "%s"`,
		tableName, srcIndexName, destIndexName), nil
}

func (og *operationGenerator) renameSequence(tx *pgx.Tx) (string, error) {
	srcSequenceName, err := og.randSequence(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	// Decide whether or not to produce a 'cannot change schema of table with RENAME' error
	desiredSchema := ""
	if !og.produceError() {
		desiredSchema = srcSequenceName.Schema()
	}

	destSequenceName, err := og.randSequence(tx, og.pctExisting(false), desiredSchema)
	if err != nil {
		return "", err
	}

	srcSequenceExists, err := sequenceExists(tx, srcSequenceName)
	if err != nil {
		return "", err
	}

	destSchemaExists, err := schemaExists(tx, destSequenceName.Schema())
	if err != nil {
		return "", err
	}

	destSequenceExists, err := sequenceExists(tx, destSequenceName)
	if err != nil {
		return "", err
	}

	srcEqualsDest := srcSequenceName.String() == destSequenceName.String()
	codesWithConditions{
		{code: pgcode.UndefinedTable, condition: !srcSequenceExists},
		{code: pgcode.UndefinedSchema, condition: !destSchemaExists},
		{code: pgcode.DuplicateRelation, condition: !srcEqualsDest && destSequenceExists},
		{code: pgcode.InvalidName, condition: srcSequenceName.Schema() != destSequenceName.Schema()},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER SEQUENCE %s RENAME TO %s`, srcSequenceName, destSequenceName), nil
}

func (og *operationGenerator) renameTable(tx *pgx.Tx) (string, error) {
	srcTableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	// Decide whether or not to produce a 'cannot change schema of table with RENAME' error
	desiredSchema := ""
	if !og.produceError() {
		desiredSchema = srcTableName.SchemaName.String()
	}
	destTableName, err := og.randTable(tx, og.pctExisting(false), desiredSchema)
	if err != nil {
		return "", err
	}

	srcTableExists, err := tableExists(tx, srcTableName)
	if err != nil {
		return "", err
	}

	destSchemaExists, err := schemaExists(tx, destTableName.Schema())
	if err != nil {
		return "", err
	}

	destTableExists, err := tableExists(tx, destTableName)
	if err != nil {
		return "", err
	}

	srcTableHasDependencies, err := tableHasDependencies(tx, srcTableName)
	if err != nil {
		return "", err
	}

	srcEqualsDest := destTableName.String() == srcTableName.String()
	codesWithConditions{
		{code: pgcode.UndefinedTable, condition: !srcTableExists},
		{code: pgcode.UndefinedSchema, condition: !destSchemaExists},
		{code: pgcode.DuplicateRelation, condition: !srcEqualsDest && destTableExists},
		{code: pgcode.DependentObjectsStillExist, condition: srcTableHasDependencies},
		{code: pgcode.InvalidName, condition: srcTableName.Schema() != destTableName.Schema()},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, srcTableName, destTableName), nil
}

func (og *operationGenerator) renameView(tx *pgx.Tx) (string, error) {
	srcViewName, err := og.randView(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	// Decide whether or not to produce a 'cannot change schema of table with RENAME' error
	desiredSchema := ""
	if !og.produceError() {
		desiredSchema = srcViewName.SchemaName.String()
	}
	destViewName, err := og.randView(tx, og.pctExisting(false), desiredSchema)
	if err != nil {
		return "", err
	}

	srcViewExists, err := viewExists(tx, srcViewName)
	if err != nil {
		return "", err
	}

	destSchemaExists, err := schemaExists(tx, destViewName.Schema())
	if err != nil {
		return "", err
	}

	destViewExists, err := viewExists(tx, destViewName)
	if err != nil {
		return "", err
	}

	srcTableHasDependencies, err := tableHasDependencies(tx, srcViewName)
	if err != nil {
		return "", err
	}

	srcEqualsDest := destViewName.String() == srcViewName.String()
	codesWithConditions{
		{code: pgcode.UndefinedTable, condition: !srcViewExists},
		{code: pgcode.UndefinedSchema, condition: !destSchemaExists},
		{code: pgcode.DuplicateRelation, condition: !srcEqualsDest && destViewExists},
		{code: pgcode.DependentObjectsStillExist, condition: srcTableHasDependencies},
		{code: pgcode.InvalidName, condition: srcViewName.Schema() != destViewName.Schema()},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER VIEW %s RENAME TO %s`, srcViewName, destViewName), nil
}

func (og *operationGenerator) setColumnDefault(tx *pgx.Tx) (string, error) {

	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN IrrelevantColumnName SET DEFAULT "IrrelevantValue"`,
			tableName), nil
	}

	columnForDefault, err := og.randColumnWithMeta(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	columnExists, err := columnExistsOnTable(tx, tableName, columnForDefault.name)
	if err != nil {
		return "", err
	}
	if !columnExists {
		og.expectedExecErrors.add(pgcode.UndefinedColumn)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET DEFAULT "IrrelevantValue"`,
			tableName, columnForDefault.name), nil
	}

	datumTyp := columnForDefault.typ
	// Optionally change the incorrect type to potentially create errors.
	if og.produceError() {
		newTypeName, err := og.randType(tx, og.pctExisting(true))
		if err != nil {
			return "", err
		}

		typeExists, err := typeExists(tx, newTypeName)
		if err != nil {
			return "", err
		}
		if !typeExists {
			og.expectedExecErrors.add(pgcode.UndefinedObject)
			return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET DEFAULT 'IrrelevantValue':::%s`, tableName, columnForDefault.name, newTypeName.SQLString()), nil
		}

		datumTyp, err = og.typeFromTypeName(tx, newTypeName.String())
		if err != nil {
			return "", err
		}
	}

	defaultDatum := rowenc.RandDatum(og.params.rng, datumTyp, columnForDefault.nullable)

	if (!datumTyp.Equivalent(columnForDefault.typ)) && defaultDatum != tree.DNull {
		og.expectedExecErrors.add(pgcode.DatatypeMismatch)
	}

	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`, tableName, columnForDefault.name, tree.AsStringWithFlags(defaultDatum, tree.FmtParsable)), nil
}

func (og *operationGenerator) setColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN IrrelevantColumnName SET NOT NULL`, tableName), nil
	}

	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	columnExists, err := columnExistsOnTable(tx, tableName, columnName)
	if err != nil {
		return "", err
	}

	if !columnExists {
		og.expectedExecErrors.add(pgcode.UndefinedColumn)
	} else {
		// If the column has null values, then a check violation will occur upon committing.
		colContainsNull, err := columnContainsNull(tx, tableName, columnName)
		if err != nil {
			return "", err
		}
		if colContainsNull {
			og.candidateExpectedCommitErrors.add(pgcode.CheckViolation)
		}
	}

	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET NOT NULL`, tableName, columnName), nil
}

func (og *operationGenerator) setColumnType(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN IrrelevantColumnName SET DATA TYPE IrrelevantDataType`, tableName), nil
	}

	columnForTypeChange, err := og.randColumnWithMeta(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	columnExists, err := columnExistsOnTable(tx, tableName, columnForTypeChange.name)
	if err != nil {
		return "", err
	}
	if !columnExists {
		og.expectedExecErrors.add(pgcode.UndefinedColumn)
		return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE IrrelevantTypeName`,
			tableName, columnForTypeChange.name), nil
	}

	newTypeName, err := og.randType(tx, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	typeExists, err := typeExists(tx, newTypeName)
	if err != nil {
		return "", err
	}
	newType, err := og.typeFromTypeName(tx, newTypeName.String())
	if err != nil {
		return "", err
	}

	columnHasDependencies, err := columnIsDependedOn(tx, tableName, columnForTypeChange.name)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.UndefinedObject, condition: !typeExists},
		{code: pgcode.DependentObjectsStillExist, condition: columnHasDependencies},
		// If the type of the column is not equivalent to the new type, then
		// it is possible to see either a pgcode.CannotCoerce error or a pgcode.FeatureNotSupported
		// error.
		// pgcode.CannotCoerce represents an invalid type conversion (eg. array to enum),
		// and pgcode.FeatureNotSupported represents a conversion which is only supported
		// experimentally (eg.string to enum).
		{code: pgcode.CannotCoerce, condition: !columnForTypeChange.typ.Equivalent(newType)},
		{code: pgcode.FeatureNotSupported, condition: !columnForTypeChange.typ.Equivalent(newType)},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE %s`,
		tableName, columnForTypeChange.name, newTypeName.SQLString()), nil
}

func (og *operationGenerator) insertRow(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", errors.Wrapf(err, "error getting random table name")
	}
	tableExists, err := tableExists(tx, tableName)
	if err != nil {
		return "", err
	}
	if !tableExists {
		og.expectedExecErrors.add(pgcode.UndefinedTable)
		return fmt.Sprintf(
			`INSERT INTO %s (IrrelevantColumnName) VALUES ("IrrelevantValue")`,
			tableName,
		), nil
	}
	cols, err := og.getTableColumns(tx, tableName.String(), false)
	if err != nil {
		return "", errors.Wrapf(err, "error getting table columns for insert row")
	}
	colNames := []string{}
	rows := [][]string{}
	for _, col := range cols {
		colNames = append(colNames, col.name)
	}
	numRows := og.randIntn(3) + 1
	for i := 0; i < numRows; i++ {
		var row []string
		for _, col := range cols {
			d := rowenc.RandDatum(og.params.rng, col.typ, col.nullable)
			row = append(row, tree.AsStringWithFlags(d, tree.FmtParsable))
		}

		rows = append(rows, row)
	}

	// Verify if the new row will violate unique constraints by checking the constraints and
	// existing rows in the database.
	uniqueConstraintViolation, err := violatesUniqueConstraints(tx, tableName, colNames, rows)
	if err != nil {
		return "", err
	}

	// Verify if the new row will violate fk constraints by checking the constraints and rows
	// in the database.
	foreignKeyViolation, err := violatesFkConstraints(tx, tableName, colNames, rows)
	if err != nil {
		return "", err
	}

	codesWithConditions{
		{code: pgcode.UniqueViolation, condition: uniqueConstraintViolation},
		{code: pgcode.ForeignKeyViolation, condition: foreignKeyViolation},
	}.add(og.expectedExecErrors)

	formattedRows := []string{}
	for _, row := range rows {
		formattedRows = append(formattedRows, fmt.Sprintf("(%s)", strings.Join(row, ",")))
	}

	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES %s`,
		tableName,
		strings.Join(colNames, ","),
		strings.Join(formattedRows, ","),
	), nil
}

func (og *operationGenerator) validate(tx *pgx.Tx) (string, error) {
	validateStmt := "SELECT 'validating all objects'"
	rows, err := tx.Query(`SELECT * FROM "".crdb_internal.invalid_objects ORDER BY id`)
	if err != nil {
		return validateStmt, err
	}
	defer rows.Close()

	var errs []string
	for rows.Next() {
		var id int64
		var dbName, schemaName, objName, errStr string
		if err := rows.Scan(&id, &dbName, &schemaName, &objName, &errStr); err != nil {
			return validateStmt, err
		}
		errs = append(
			errs,
			fmt.Sprintf("id %d, db %s, schema %s, name %s: %s", id, dbName, schemaName, objName, errStr),
		)
	}

	if rows.Err() != nil {
		return "", errors.Wrap(rows.Err(), "querying for validation erors failed")
	}

	if len(errs) == 0 {
		return validateStmt, nil
	}
	return validateStmt, errors.Errorf("Validation FAIL:\n%s", strings.Join(errs, "\n"))
}

type column struct {
	name     string
	typ      *types.T
	nullable bool
}

func (og *operationGenerator) getTableColumns(
	tx *pgx.Tx, tableName string, shuffle bool,
) ([]column, error) {
	q := fmt.Sprintf(`
  SELECT column_name, data_type, is_nullable
    FROM [SHOW COLUMNS FROM %s]
`, tableName)
	rows, err := tx.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var typNames []string
	var ret []column
	for rows.Next() {
		var c column
		var typName string
		err := rows.Scan(&c.name, &typName, &c.nullable)
		if err != nil {
			return nil, err
		}
		if c.name != "rowid" {
			typNames = append(typNames, typName)
			ret = append(ret, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ret) == 0 {
		return nil, pgx.ErrNoRows
	}
	for i := range ret {
		c := &ret[i]
		c.typ, err = og.typeFromTypeName(tx, typNames[i])
		if err != nil {
			return nil, err
		}
	}

	if shuffle {
		og.params.rng.Shuffle(len(ret), func(i, j int) {
			ret[i], ret[j] = ret[j], ret[i]
		})
	}

	return ret, nil
}

func (og *operationGenerator) randColumn(
	tx *pgx.Tx, tableName tree.TableName, pctExisting int,
) (string, error) {
	if og.randIntn(100) >= pctExisting {
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		return fmt.Sprintf("col%s_%d",
			strings.TrimPrefix(tableName.Table(), "table"), og.newUniqueSeqNum()), nil
	}
	q := fmt.Sprintf(`
  SELECT column_name
    FROM [SHOW COLUMNS FROM %s]
   WHERE column_name != 'rowid'
ORDER BY random()
   LIMIT 1;
`, tableName.String())
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

// randColumnWithMeta is implemented in the same way as randColumn with the exception that
// it will return a column struct, which includes type and nullability information, instead of
// a column name string.
func (og *operationGenerator) randColumnWithMeta(
	tx *pgx.Tx, tableName tree.TableName, pctExisting int,
) (column, error) {
	if og.randIntn(100) >= pctExisting {
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		return column{
			name: fmt.Sprintf("col%s_%d",
				strings.TrimPrefix(tableName.Table(), "table"), og.newUniqueSeqNum()),
		}, nil
	}
	q := fmt.Sprintf(`
 SELECT column_name, data_type, is_nullable
   FROM [SHOW COLUMNS FROM %s]
  WHERE column_name != 'rowid'
ORDER BY random()
  LIMIT 1;
`, tableName.String())
	var col column
	var typ string
	if err := tx.QueryRow(q).Scan(&col.name, &typ, &col.nullable); err != nil {
		return column{}, err
	}

	var err error
	col.typ, err = og.typeFromTypeName(tx, typ)
	if err != nil {
		return column{}, err
	}

	return col, nil
}

// randChildColumnForFkRelation gets a column to use as the child column in a foreign key relation.
// To successfully use a column as the child, the column must have the same type as the parent and must not be computed.
func (og *operationGenerator) randChildColumnForFkRelation(
	tx *pgx.Tx, isNotComputed bool, typ string,
) (*tree.TableName, *column, error) {

	query := strings.Builder{}
	query.WriteString(`
    SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable
      FROM information_schema.columns
		 WHERE table_name ~ 'table[0-9]+'
  `)
	query.WriteString(fmt.Sprintf(`
			AND crdb_sql_type = '%s'
	`, typ))

	if isNotComputed {
		query.WriteString(`AND is_generated = 'NO'`)
	} else {
		query.WriteString(`AND is_generated = 'YES'`)
	}

	var tableSchema string
	var tableName string
	var columnName string
	var typName string
	var nullable string

	err := tx.QueryRow(query.String()).Scan(&tableSchema, &tableName, &columnName, &typName, &nullable)
	if err != nil {
		return nil, nil, err
	}

	columnToReturn := column{
		name:     columnName,
		nullable: nullable == "YES",
	}
	table := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
		SchemaName:     tree.Name(tableSchema),
		ExplicitSchema: true,
	}, tree.Name(tableName))

	columnToReturn.typ, err = og.typeFromTypeName(tx, typName)
	if err != nil {
		return nil, nil, err
	}

	return &table, &columnToReturn, nil
}

// randParentColumnForFkRelation fetches a column and table to use as the parent in a single-column foreign key relation.
// To successfully use a column as the parent, the column must be unique and must not be generated.
func (og *operationGenerator) randParentColumnForFkRelation(
	tx *pgx.Tx, unique bool,
) (*tree.TableName, *column, error) {

	subQuery := strings.Builder{}
	subQuery.WriteString(`
		SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable, contype, conkey
      FROM (
        SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable, ordinal_position,
               concat(table_schema, '.', table_name)::REGCLASS::INT8 AS tableid
          FROM information_schema.columns
           ) AS cols
		  JOIN (
		        SELECT contype, conkey, conrelid
		          FROM pg_catalog.pg_constraint
		       ) AS cons ON cons.conrelid = cols.tableid
		 WHERE table_name ~ 'table[0-9]+'
  `)
	if unique {
		subQuery.WriteString(`
		 AND (contype = 'u' OR contype = 'p')
		 AND array_length(conkey, 1) = 1
		 AND conkey[1] = ordinal_position
		`)
	}

	subQuery.WriteString(`
		ORDER BY random()
    LIMIT 1
  `)

	var tableSchema string
	var tableName string
	var columnName string
	var typName string
	var nullable string

	err := tx.QueryRow(fmt.Sprintf(`
	SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable FROM (
		%s
	)`, subQuery.String())).Scan(&tableSchema, &tableName, &columnName, &typName, &nullable)
	if err != nil {
		return nil, nil, err
	}

	columnToReturn := column{
		name:     columnName,
		nullable: nullable == "YES",
	}
	table := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
		SchemaName:     tree.Name(tableSchema),
		ExplicitSchema: true,
	}, tree.Name(tableName))

	columnToReturn.typ, err = og.typeFromTypeName(tx, typName)
	if err != nil {
		return nil, nil, err
	}

	return &table, &columnToReturn, nil
}

func (og *operationGenerator) randConstraint(tx *pgx.Tx, tableName string) (string, error) {
	q := fmt.Sprintf(`
  SELECT constraint_name
    FROM [SHOW CONSTRAINTS FROM %s]
ORDER BY random()
   LIMIT 1;
`, tableName)
	var name string
	err := tx.QueryRow(q).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (og *operationGenerator) randIndex(
	tx *pgx.Tx, tableName tree.TableName, pctExisting int,
) (string, error) {
	if og.randIntn(100) >= pctExisting {
		// We make a unique name for all indices by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		return fmt.Sprintf("index%s_%d",
			strings.TrimPrefix(tableName.Table(), "table"), og.newUniqueSeqNum()), nil
	}
	q := fmt.Sprintf(`
  SELECT index_name
    FROM [SHOW INDEXES FROM %s]
	WHERE index_name LIKE 'index%%'
ORDER BY random()
   LIMIT 1;
`, tableName.String())
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

// randSequence returns a sequence qualified by a schema
func (og *operationGenerator) randSequence(
	tx *pgx.Tx, pctExisting int, desiredSchema string,
) (*tree.TableName, error) {

	if desiredSchema != "" {
		if og.randIntn(100) >= pctExisting {
			treeSeqName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
				SchemaName:     tree.Name(desiredSchema),
				ExplicitSchema: true,
			}, tree.Name(fmt.Sprintf("seq%d", og.newUniqueSeqNum())))
			return &treeSeqName, nil
		}
		q := fmt.Sprintf(`
   SELECT sequence_name
     FROM [SHOW SEQUENCES]
    WHERE sequence_name LIKE 'seq%%'
			AND sequence_schema = '%s'
 ORDER BY random()
		LIMIT 1;
		`, desiredSchema)

		var seqName string
		if err := tx.QueryRow(q).Scan(&seqName); err != nil {
			treeSeqName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeSeqName, err
		}

		treeSeqName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(desiredSchema),
			ExplicitSchema: true,
		}, tree.Name(seqName))
		return &treeSeqName, nil
	}

	if og.randIntn(100) >= pctExisting {
		// Most of the time, this case is for creating sequences, so it
		// is preferable that the schema exists.
		randSchema, err := og.randSchema(tx, og.pctExisting(true))
		if err != nil {
			treeSeqName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeSeqName, err
		}
		treeSeqName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(randSchema),
			ExplicitSchema: true,
		}, tree.Name(fmt.Sprintf("seq%d", og.newUniqueSeqNum())))
		return &treeSeqName, nil
	}

	q := `
   SELECT sequence_schema, sequence_name
     FROM [SHOW SEQUENCES]
    WHERE sequence_name LIKE 'seq%%'
 ORDER BY random()
		LIMIT 1;
		`

	var schemaName string
	var seqName string
	if err := tx.QueryRow(q).Scan(&schemaName, &seqName); err != nil {
		treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
		return &treeTableName, err
	}

	treeSeqName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
		SchemaName:     tree.Name(schemaName),
		ExplicitSchema: true,
	}, tree.Name(seqName))
	return &treeSeqName, nil

}

func (og *operationGenerator) randEnum(tx *pgx.Tx, pctExisting int) (*tree.TypeName, error) {
	if og.randIntn(100) >= pctExisting {
		// Most of the time, this case is for creating enums, so it
		// is preferable that the schema exists
		randSchema, err := og.randSchema(tx, og.pctExisting(true))
		if err != nil {
			return nil, err
		}
		typeName := tree.MakeSchemaQualifiedTypeName(randSchema, fmt.Sprintf("enum%d", og.newUniqueSeqNum()))
		return &typeName, nil
	}
	const q = `
  SELECT schema, name
    FROM [SHOW ENUMS]
   WHERE name LIKE 'enum%'
ORDER BY random()
   LIMIT 1;
`
	var schemaName string
	var typName string
	if err := tx.QueryRow(q).Scan(&schemaName, &typName); err != nil {
		return nil, err
	}
	typeName := tree.MakeSchemaQualifiedTypeName(schemaName, typName)
	return &typeName, nil
}

// randTable returns a schema name along with a table name
func (og *operationGenerator) randTable(
	tx *pgx.Tx, pctExisting int, desiredSchema string,
) (*tree.TableName, error) {

	if desiredSchema != "" {
		if og.randIntn(100) >= pctExisting {
			treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
				SchemaName:     tree.Name(desiredSchema),
				ExplicitSchema: true,
			}, tree.Name(fmt.Sprintf("table%d", og.newUniqueSeqNum())))
			return &treeTableName, nil
		}
		q := fmt.Sprintf(`
		  SELECT table_name
		    FROM [SHOW TABLES]
		   WHERE table_name ~ 'table[0-9]+'
				 AND schema_name = '%s'
		ORDER BY random()
		   LIMIT 1;
		`, desiredSchema)

		var tableName string
		if err := tx.QueryRow(q).Scan(&tableName); err != nil {
			treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeTableName, err
		}

		treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(desiredSchema),
			ExplicitSchema: true,
		}, tree.Name(tableName))
		return &treeTableName, nil
	}

	if og.randIntn(100) >= pctExisting {
		// Most of the time, this case is for creating tables, so it
		// is preferable that the schema exists
		randSchema, err := og.randSchema(tx, og.pctExisting(true))
		if err != nil {
			treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeTableName, err
		}

		treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(randSchema),
			ExplicitSchema: true,
		}, tree.Name(fmt.Sprintf("table%d", og.newUniqueSeqNum())))
		return &treeTableName, nil
	}

	const q = `
  SELECT schema_name, table_name
    FROM [SHOW TABLES]
   WHERE table_name ~ 'table[0-9]+'
ORDER BY random()
   LIMIT 1;
`
	var schemaName string
	var tableName string
	if err := tx.QueryRow(q).Scan(&schemaName, &tableName); err != nil {
		treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
		return &treeTableName, err
	}

	treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
		SchemaName:     tree.Name(schemaName),
		ExplicitSchema: true,
	}, tree.Name(tableName))
	return &treeTableName, nil
}

func (og *operationGenerator) randView(
	tx *pgx.Tx, pctExisting int, desiredSchema string,
) (*tree.TableName, error) {
	if desiredSchema != "" {
		if og.randIntn(100) >= pctExisting {
			treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
				SchemaName:     tree.Name(desiredSchema),
				ExplicitSchema: true,
			}, tree.Name(fmt.Sprintf("view%d", og.newUniqueSeqNum())))
			return &treeViewName, nil
		}

		q := fmt.Sprintf(`
		  SELECT schema_name, table_name
		    FROM [SHOW TABLES]
		   WHERE table_name LIKE 'view%%'
				 AND schema_name = '%s'
		ORDER BY random()
		   LIMIT 1;
		`, desiredSchema)

		var viewName string
		if err := tx.QueryRow(q).Scan(&viewName); err != nil {
			treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeViewName, err
		}
		treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(desiredSchema),
			ExplicitSchema: true,
		}, tree.Name(viewName))
		return &treeViewName, nil
	}

	if og.randIntn(100) >= pctExisting {
		// Most of the time, this case is for creating views, so it
		// is preferable that the schema exists
		randSchema, err := og.randSchema(tx, og.pctExisting(true))
		if err != nil {
			treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeViewName, err
		}
		treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(randSchema),
			ExplicitSchema: true,
		}, tree.Name(fmt.Sprintf("view%d", og.newUniqueSeqNum())))
		return &treeViewName, nil
	}
	const q = `
  SELECT schema_name, table_name
    FROM [SHOW TABLES]
   WHERE table_name LIKE 'view%'
ORDER BY random()
   LIMIT 1;
`
	var schemaName string
	var viewName string
	if err := tx.QueryRow(q).Scan(&schemaName, &viewName); err != nil {
		treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
		return &treeViewName, err
	}
	treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
		SchemaName:     tree.Name(schemaName),
		ExplicitSchema: true,
	}, tree.Name(viewName))
	return &treeViewName, nil
}

func (og *operationGenerator) tableColumnsShuffled(tx *pgx.Tx, tableName string) ([]string, error) {
	q := fmt.Sprintf(`
SELECT column_name
FROM [SHOW COLUMNS FROM %s];
`, tableName)

	rows, err := tx.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "rowid" {
			columnNames = append(columnNames, name)
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	og.params.rng.Shuffle(len(columnNames), func(i, j int) {
		columnNames[i], columnNames[j] = columnNames[j], columnNames[i]
	})

	if len(columnNames) <= 0 {
		return nil, errors.Errorf("table %s has no columns", tableName)
	}
	return columnNames, nil
}

func (og *operationGenerator) randType(tx *pgx.Tx, enumPctExisting int) (*tree.TypeName, error) {
	if og.randIntn(100) <= og.params.enumPct {
		// TODO(ajwerner): Support arrays of enums.
		typName, err := og.randEnum(tx, enumPctExisting)
		if err != nil {
			return nil, err
		}
		return typName, err
	}
	typ := rowenc.RandSortingType(og.params.rng)
	typeName := tree.MakeUnqualifiedTypeName(tree.Name(typ.SQLString()))
	return &typeName, nil
}

func (og *operationGenerator) createSchema(tx *pgx.Tx) (string, error) {
	schemaName, err := og.randSchema(tx, og.pctExisting(false))
	if err != nil {
		return "", err
	}
	ifNotExists := og.randIntn(2) == 0

	schemaExists, err := schemaExists(tx, schemaName)
	if err != nil {
		return "", err
	}
	if schemaExists && !ifNotExists {
		og.expectedExecErrors.add(pgcode.DuplicateSchema)
	}

	// TODO(jayshrivastava): Support authorization
	stmt := rowenc.MakeSchemaName(ifNotExists, schemaName, security.RootUserName())
	return tree.Serialize(stmt), nil
}

func (og *operationGenerator) randSchema(tx *pgx.Tx, pctExisting int) (string, error) {
	if og.randIntn(100) >= pctExisting {
		return fmt.Sprintf("schema%d", og.newUniqueSeqNum()), nil
	}
	const q = `
  SELECT schema_name
    FROM information_schema.schemata
   WHERE schema_name
    LIKE 'schema%'
      OR schema_name = 'public'
ORDER BY random()
   LIMIT 1;
`
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

func (og *operationGenerator) dropSchema(tx *pgx.Tx) (string, error) {
	schemaName, err := og.randSchema(tx, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	schemaExists, err := schemaExists(tx, schemaName)
	if err != nil {
		return "", err
	}
	codesWithConditions{
		{pgcode.UndefinedSchema, !schemaExists},
		{pgcode.InvalidSchemaName, schemaName == tree.PublicSchema},
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schemaName), nil
}

// pctExisting is used to specify the probability that a name exists when getting a random name. It
// is a function of the configured error rate and the parameter `shouldAlreadyExist`, which specifies
// if the name should exist in the non error case.
//
// Ex. When adding a column to a table, a table name needs to be fetched first. In cases where
// the errorRate low, pctExisting should be high because the table should exist for the op to succeed.
//
// Ex. When adding a new column to a table, a column name needs to be generated. In cases where
// the errorRate low, pctExisting should be low because the column name should not already exist for the op to succeed.
func (og *operationGenerator) pctExisting(shouldAlreadyExist bool) int {
	if shouldAlreadyExist {
		return 100 - og.params.errorRate
	}
	return og.params.errorRate
}

func (og *operationGenerator) produceError() bool {
	return og.randIntn(100) < og.params.errorRate
}

// Returns an int in the range [0,topBound). It panics if topBound <= 0.
func (og *operationGenerator) randIntn(topBound int) int {
	return og.params.rng.Intn(topBound)
}

func (og *operationGenerator) newUniqueSeqNum() int64 {
	return atomic.AddInt64(og.params.seqNum, 1)
}

// typeFromTypeName resolves a type string to a types.T struct so that it can be
// compared with other types.
func (og *operationGenerator) typeFromTypeName(tx *pgx.Tx, typeName string) (*types.T, error) {
	stmt, err := parser.ParseOne(fmt.Sprintf("SELECT 'placeholder'::%s", typeName))
	if err != nil {
		return nil, err
	}
	typ, err := tree.ResolveType(
		context.Background(),
		stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.CastExpr).Type,
		&txTypeResolver{tx: tx},
	)
	if err != nil {
		return nil, err
	}
	return typ, nil
}
