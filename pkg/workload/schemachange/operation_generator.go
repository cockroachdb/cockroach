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
	seqNum          *int64
	errorRate       int
	enumPct         int
	rng             *rand.Rand
	ops             *deck
	maxSourceTables int
}

// The OperationBuilder has the sole responsibility of generating ops
type operationGenerator struct {
	params               *operationGeneratorParams
	screenForExecErrors  bool
	expectedExecErrors   errorCodeSet
	expectedCommitErrors errorCodeSet
}

func makeOperationGenerator(params *operationGeneratorParams) *operationGenerator {
	return &operationGenerator{
		params:               params,
		expectedExecErrors:   makeExpectedErrorSet(),
		expectedCommitErrors: makeExpectedErrorSet(),
	}
}

// Reset internal state used per operation within a transaction
func (og *operationGenerator) resetOpState() {
	og.screenForExecErrors = false
	og.expectedExecErrors.reset()
}

// Reset internal state used per transaction
func (og *operationGenerator) resetTxnState() {
	og.expectedCommitErrors.reset()
}

//go:generate stringer -type=opType
type opType int

// opsWithErrorScreening stores ops which currently check for exec
// errors and update expectedExecErrors in the op generator state
var opsWithExecErrorScreening = map[opType]bool{
	addColumn:     true,
	createTable:   true,
	createTableAs: true,
	createView:    true,
}

func opScreensForExecErrors(op opType) bool {
	if _, exists := opsWithExecErrorScreening[op]; exists {
		return true
	}
	return false
}

const (
	addColumn     opType = iota // ALTER TABLE <table> ADD [COLUMN] <column> <type>
	addConstraint               // ALTER TABLE <table> ADD CONSTRAINT <constraint> <def>

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
	addColumn:         (*operationGenerator).addColumn,
	addConstraint:     (*operationGenerator).addConstraint,
	createIndex:       (*operationGenerator).createIndex,
	createSequence:    (*operationGenerator).createSequence,
	createTable:       (*operationGenerator).createTable,
	createTableAs:     (*operationGenerator).createTableAs,
	createView:        (*operationGenerator).createView,
	createEnum:        (*operationGenerator).createEnum,
	createSchema:      (*operationGenerator).createSchema,
	dropColumn:        (*operationGenerator).dropColumn,
	dropColumnDefault: (*operationGenerator).dropColumnDefault,
	dropColumnNotNull: (*operationGenerator).dropColumnNotNull,
	dropColumnStored:  (*operationGenerator).dropColumnStored,
	dropConstraint:    (*operationGenerator).dropConstraint,
	dropIndex:         (*operationGenerator).dropIndex,
	dropSequence:      (*operationGenerator).dropSequence,
	dropTable:         (*operationGenerator).dropTable,
	dropView:          (*operationGenerator).dropView,
	dropSchema:        (*operationGenerator).dropSchema,
	renameColumn:      (*operationGenerator).renameColumn,
	renameIndex:       (*operationGenerator).renameIndex,
	renameSequence:    (*operationGenerator).renameSequence,
	renameTable:       (*operationGenerator).renameTable,
	renameView:        (*operationGenerator).renameView,
	setColumnDefault:  (*operationGenerator).setColumnDefault,
	setColumnNotNull:  (*operationGenerator).setColumnNotNull,
	setColumnType:     (*operationGenerator).setColumnType,
	insertRow:         (*operationGenerator).insertRow,
	validate:          (*operationGenerator).validate,
}

func init() {
	// Validate that we have an operation function for each opType.
	if len(opFuncs) != numOpTypes {
		panic(errors.Errorf("expected %d opFuncs, got %d", numOpTypes, len(opFuncs)))
	}
}

var opWeights = []int{
	addColumn:         1,
	addConstraint:     0, // TODO(spaskob): unimplemented
	createIndex:       1,
	createSequence:    1,
	createTable:       1,
	createTableAs:     1,
	createView:        1,
	createEnum:        1,
	createSchema:      1,
	dropColumn:        1,
	dropColumnDefault: 1,
	dropColumnNotNull: 1,
	dropColumnStored:  1,
	dropConstraint:    1,
	dropIndex:         1,
	dropSequence:      1,
	dropTable:         1,
	dropView:          1,
	dropSchema:        1,
	renameColumn:      1,
	renameIndex:       1,
	renameSequence:    1,
	renameTable:       1,
	renameView:        1,
	setColumnDefault:  0, // TODO(spaskob): unimplemented
	setColumnNotNull:  1,
	setColumnType:     1,
	insertRow:         1,
	validate:          2, // validate twice more often
}

// randOp attempts to produce a random schema change operation. It returns a
// triple `(randOp, log, error)`. On success `randOp` is the random schema
// change constructed. Constructing a random schema change may require a few
// stochastic attempts and if verbosity is >= 2 the unsuccessful attempts are
// recorded in `log` to help with debugging of the workload.
func (og *operationGenerator) randOp(tx *pgx.Tx) (string, string, error) {
	var log strings.Builder
	for {
		op := opType(og.params.ops.Int())
		stmt, err := opFuncs[op](og, tx)
		// TODO(spaskob): use more fine-grained error reporting.
		if stmt == "" || errors.Is(err, pgx.ErrNoRows) {
			log.WriteString(fmt.Sprintf("NOOP: %s -> %v\n", op, err))
			continue
		}

		if opScreensForExecErrors(op) {
			og.screenForExecErrors = true
		}
		return stmt, log.String(), err
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

	typ, err := og.randType(tx, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	def := &tree.ColumnTableDef{
		Name: tree.Name(columnName),
		Type: typ,
	}
	def.Nullable.Nullability = tree.Nullability(rand.Intn(1 + int(tree.SilentNull)))

	columnExistsOnTable, err := columnExistsOnTable(tx, tableName, columnName)
	if err != nil {
		return "", err
	}
	typeExists, err := typeExists(tx, typ)
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

func (og *operationGenerator) createIndex(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	columnNames, err := og.tableColumnsShuffled(tx, tableName.String())
	if err != nil {
		return "", err
	}

	indexName, err := og.randIndex(tx, *tableName, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	def := &tree.CreateIndex{
		Name:        tree.Name(indexName),
		Table:       *tableName,
		Unique:      og.randIntn(4) == 0,  // 25% UNIQUE
		Inverted:    og.randIntn(10) == 0, // 10% INVERTED
		IfNotExists: og.randIntn(2) == 0,  // 50% IF NOT EXISTS
		Columns:     make(tree.IndexElemList, 1+og.randIntn(len(columnNames))),
	}

	for i := range def.Columns {
		def.Columns[i].Column = tree.Name(columnNames[i])
		def.Columns[i].Direction = tree.Direction(og.randIntn(1 + int(tree.Descending)))
	}
	columnNames = columnNames[len(def.Columns):]

	if n := len(columnNames); n > 0 {
		def.Storing = make(tree.NameList, og.randIntn(1+n))
		for i := range def.Storing {
			def.Storing[i] = tree.Name(columnNames[i])
		}
	}

	return tree.Serialize(def), nil
}

func (og *operationGenerator) createSequence(tx *pgx.Tx) (string, error) {
	return fmt.Sprintf(`CREATE SEQUENCE "seq%d"`, og.newUniqueSeqNum()), nil
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

	stmt := rowenc.RandCreateTable(og.params.rng, "table", tableIdx)
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
	stmt := rowenc.RandCreateType(og.params.rng, typName.String(), "asdf")
	return tree.Serialize(stmt), nil
}

func (og *operationGenerator) createTableAs(tx *pgx.Tx) (string, error) {
	numSourceTables := og.randIntn(og.params.maxSourceTables) + 1

	sourceTableNames := make([]tree.TableExpr, numSourceTables)
	sourceTableExistence := make([]bool, numSourceTables)

	uniqueTableNames := map[string]bool{}
	duplicateSourceTables := false

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

	for i := 0; i < numSourceTables; i++ {
		tableName := sourceTableNames[i]
		tableExists := sourceTableExistence[i]

		if tableExists {
			columnNamesForTable, err := og.tableColumnsShuffled(tx, tableName.(*tree.TableName).String())
			if err != nil {
				return "", err
			}
			columnNamesForTable = columnNamesForTable[:1+og.randIntn(len(columnNamesForTable))]

			for i := range columnNamesForTable {
				colItem := tree.ColumnItem{
					ColumnName: tree.Name(columnNamesForTable[i]),
				}
				selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})
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
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`CREATE TABLE %s AS %s`,
		destTableName, selectStatement.String()), nil
}

func (og *operationGenerator) createView(tx *pgx.Tx) (string, error) {

	numSourceTables := og.randIntn(og.params.maxSourceTables) + 1

	sourceTableNames := make([]tree.TableExpr, numSourceTables)
	sourceTableExistence := make([]bool, numSourceTables)

	uniqueTableNames := map[string]bool{}
	duplicateSourceTables := false

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

	for i := 0; i < numSourceTables; i++ {
		tableName := sourceTableNames[i]
		tableExists := sourceTableExistence[i]

		if tableExists {
			columnNamesForTable, err := og.tableColumnsShuffled(tx, tableName.(*tree.TableName).String())
			if err != nil {
				return "", err
			}
			columnNamesForTable = columnNamesForTable[:1+og.randIntn(len(columnNamesForTable))]

			for i := range columnNamesForTable {
				colItem := tree.ColumnItem{
					ColumnName: tree.Name(columnNamesForTable[i]),
				}
				selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})
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
	}.add(og.expectedExecErrors)

	return fmt.Sprintf(`CREATE VIEW %s AS %s`,
		destViewName, selectStatement.String()), nil
}

func (og *operationGenerator) dropColumn(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "%s"`, tableName, columnName), nil
}

func (og *operationGenerator) dropColumnDefault(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP DEFAULT`, tableName, columnName), nil
}

func (og *operationGenerator) dropColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL`, tableName, columnName), nil
}

func (og *operationGenerator) dropColumnStored(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP STORED`, tableName, columnName), nil
}

func (og *operationGenerator) dropConstraint(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	constraintName, err := og.randConstraint(tx, tableName.String())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT "%s"`, tableName, constraintName), nil
}

func (og *operationGenerator) dropIndex(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	indexName, err := og.randIndex(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP INDEX %s@"%s"`, tableName, indexName), nil
}

func (og *operationGenerator) dropSequence(tx *pgx.Tx) (string, error) {
	sequenceName, err := og.randSequence(tx, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP SEQUENCE "%s"`, sequenceName), nil
}

func (og *operationGenerator) dropTable(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP TABLE %s`, tableName), nil
}

func (og *operationGenerator) dropView(tx *pgx.Tx) (string, error) {
	viewName, err := og.randView(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP VIEW %s`, viewName), nil
}

func (og *operationGenerator) renameColumn(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	srcColumnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	destColumnName, err := og.randColumn(tx, *tableName, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN "%s" TO "%s"`,
		tableName, srcColumnName, destColumnName), nil
}

func (og *operationGenerator) renameIndex(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	srcIndexName, err := og.randIndex(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	destIndexName, err := og.randIndex(tx, *tableName, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER TABLE %s RENAME CONSTRAINT "%s" TO "%s"`,
		tableName, srcIndexName, destIndexName), nil
}

func (og *operationGenerator) renameSequence(tx *pgx.Tx) (string, error) {
	srcSequenceName, err := og.randSequence(tx, og.pctExisting(true))
	if err != nil {
		return "", err
	}

	destSequenceName, err := og.randSequence(tx, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER SEQUENCE "%s" RENAME TO "%s"`, srcSequenceName, destSequenceName), nil
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

	return fmt.Sprintf(`ALTER VIEW %s RENAME TO %s`, srcViewName, destViewName), nil
}

func (og *operationGenerator) setColumnDefault(tx *pgx.Tx) (string, error) {
	// TODO(peter): unimplemented
	return "", nil
}

func (og *operationGenerator) setColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}

	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET NOT NULL`, tableName, columnName), nil
}

func (og *operationGenerator) setColumnType(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", err
	}
	columnName, err := og.randColumn(tx, *tableName, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	typ, err := og.randType(tx, og.pctExisting(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE %s`,
		tableName, columnName, typ), nil
}

func (og *operationGenerator) insertRow(tx *pgx.Tx) (string, error) {
	tableName, err := og.randTable(tx, og.pctExisting(true), "")
	if err != nil {
		return "", errors.Wrapf(err, "error getting random table name")
	}
	cols, err := og.getTableColumns(tx, tableName.String())
	if err != nil {
		return "", errors.Wrapf(err, "error getting table columns for insert row")
	}
	colNames := []string{}
	rows := []string{}
	for _, col := range cols {
		colNames = append(colNames, fmt.Sprintf(`"%s"`, col.name))
	}
	numRows := og.randIntn(10) + 1
	for i := 0; i < numRows; i++ {
		var row []string
		for _, col := range cols {
			d := rowenc.RandDatum(og.params.rng, col.typ, col.nullable)
			row = append(row, tree.AsStringWithFlags(d, tree.FmtParsable))
		}
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(row, ",")))
	}
	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES %s`,
		tableName,
		strings.Join(colNames, ","),
		strings.Join(rows, ","),
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

func (og *operationGenerator) getTableColumns(tx *pgx.Tx, tableName string) ([]column, error) {
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
		typNames = append(typNames, typName)
		ret = append(ret, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range ret {
		c := &ret[i]
		stmt, err := parser.ParseOne(fmt.Sprintf("SELECT 'otan wuz here'::%s", typNames[i]))
		if err != nil {
			return nil, err
		}
		c.typ, err = tree.ResolveType(
			context.Background(),
			stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.CastExpr).Type,
			&txTypeResolver{tx: tx},
		)
		if err != nil {
			return nil, err
		}
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
ORDER BY random()
   LIMIT 1;
`, tableName.String())
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
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
ORDER BY random()
   LIMIT 1;
`, tableName.String())
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

func (og *operationGenerator) randSequence(tx *pgx.Tx, pctExisting int) (string, error) {
	if og.randIntn(100) >= pctExisting {
		return fmt.Sprintf(`seq%d`, og.newUniqueSeqNum()), nil
	}
	const q = `
  SELECT sequence_name
    FROM [SHOW SEQUENCES]
   WHERE sequence_name LIKE 'seq%'
ORDER BY random()
   LIMIT 1;
`
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

func (og *operationGenerator) randEnum(tx *pgx.Tx, pctExisting int) (tree.UnresolvedName, error) {
	if og.randIntn(100) >= pctExisting {
		// Most of the time, this case is for creating enums, so it
		// is preferable that the schema exists
		randSchema, err := og.randSchema(tx, og.pctExisting(true))
		if err != nil {
			return tree.MakeUnresolvedName(), err
		}
		return tree.MakeUnresolvedName(randSchema, fmt.Sprintf("enum%d", og.newUniqueSeqNum())), nil
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
		return tree.MakeUnresolvedName(), err
	}
	return tree.MakeUnresolvedName(schemaName, typName), nil
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
		   WHERE table_name LIKE 'table%%'
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
		}, tree.Name(fmt.Sprintf("table%d", og.newUniqueSeqNum())))
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
   WHERE table_name LIKE 'table%'
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

func (og *operationGenerator) randType(
	tx *pgx.Tx, enumPctExisting int,
) (tree.ResolvableTypeReference, error) {
	if og.randIntn(100) <= og.params.enumPct {
		// TODO(ajwerner): Support arrays of enums.
		typName, err := og.randEnum(tx, enumPctExisting)
		if err != nil {
			return nil, err
		}
		return typName.ToUnresolvedObjectName(tree.NoAnnotation)
	}
	return rowenc.RandSortingType(og.params.rng), nil
}

func (og *operationGenerator) createSchema(tx *pgx.Tx) (string, error) {
	schemaName, err := og.randSchema(tx, og.pctExisting(false))
	if err != nil {
		return "", err
	}

	// TODO(jayshrivastava): Support authorization
	stmt := rowenc.MakeSchemaName(og.randIntn(2) == 0, schemaName, security.RootUserName())
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
	return fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schemaName), nil
}

// pctExisting is used to specify the probability that a name exists when getting a random name. It
// is a function of the configured error rate and the the parameter `shouldAlreadyExist`, which specifies
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
