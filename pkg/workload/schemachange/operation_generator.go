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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
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
	params                *operationGeneratorParams
	expectedCommitErrors  errorCodeSet
	potentialCommitErrors errorCodeSet

	// This stores expected commit errors while an op statement
	// is still being constructed. It is possible that one of the functions in opFuncs
	// fails. In this case, the candidateExpectedCommitErrors will be discarded. If the
	// function succeeds and the op statement is constructed, then candidateExpectedCommitErrors
	// are added to expectedCommitErrors.
	candidateExpectedCommitErrors errorCodeSet

	// opsInTxn is a list of previous ops in the current transaction to check
	// for DDLs after writes.
	opsInTxn []opType

	// stmtsInTxn is a list of statements in the current transaction.
	stmtsInTxt []*opStmt

	// opGenLog log of statement used to generate the current statement.
	opGenLog strings.Builder
}

// LogQueryResults logs a string query result.
func (og *operationGenerator) LogQueryResults(queryName string, result string) {
	og.opGenLog.WriteString(fmt.Sprintf("QUERY [%s] :", queryName))
	og.opGenLog.WriteString(result)
	og.opGenLog.WriteString("\n")
}

// LogQueryResultArray logs a query result that is a strng array.
func (og *operationGenerator) LogQueryResultArray(queryName string, results []string) {
	og.opGenLog.WriteString(fmt.Sprintf("QUERY [%s] : ", queryName))
	for _, result := range results {
		og.opGenLog.WriteString(result)
		og.opGenLog.WriteString(",")
	}

	og.opGenLog.WriteString("\n")
}

// GetOpGenLog fetches the generated log entries.
func (og *operationGenerator) GetOpGenLog() string {
	return og.opGenLog.String()
}

func makeOperationGenerator(params *operationGeneratorParams) *operationGenerator {
	return &operationGenerator{
		params:                        params,
		expectedCommitErrors:          makeExpectedErrorSet(),
		potentialCommitErrors:         makeExpectedErrorSet(),
		candidateExpectedCommitErrors: makeExpectedErrorSet(),
	}
}

// Reset internal state used per operation within a transaction
func (og *operationGenerator) resetOpState() {
	og.candidateExpectedCommitErrors.reset()
	og.opGenLog = strings.Builder{}
}

// Reset internal state used per transaction
func (og *operationGenerator) resetTxnState() {
	og.expectedCommitErrors.reset()
	og.potentialCommitErrors.reset()
	og.opsInTxn = nil
	og.stmtsInTxt = nil
}

//go:generate stringer -type=opType
type opType int

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

	numOpTypes int = iota
)

var opFuncs = map[opType]func(*operationGenerator, context.Context, pgx.Tx) (*opStmt, error){
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
	dropSchema:              1,
	primaryRegion:           0, // Disabled and tracked with #83831
	renameColumn:            1,
	renameIndex:             1,
	renameSequence:          1,
	renameTable:             1,
	renameView:              1,
	setColumnDefault:        1,
	setColumnNotNull:        1,
	setColumnType:           0,  // Disabled and tracked with #66662.
	survive:                 0,  // Disabled and tracked with #83831
	insertRow:               10, // Temporarily reduced because of #80820
	selectStmt:              10,
	validate:                2, // validate twice more often
}

// adjustOpWeightsForActiveVersion adjusts the weights for the active cockroach
// version, allowing us to disable certain operations in mixed version scenarios.
func adjustOpWeightsForCockroachVersion(
	ctx context.Context, pool *workload.MultiConnPool, opWeights []int,
) error {
	tx, err := pool.Get().Begin(ctx)
	if err != nil {
		return err
	}
	// First validate if we even support foreign key constraints on the active
	// version, since we need builtins.
	fkConstraintsEnabled, err := isFkConstraintsEnabled(ctx, tx)
	if err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			err = errors.WithSecondaryError(err, rbErr)
		}
		return err
	}
	if !fkConstraintsEnabled {
		opWeights[addForeignKeyConstraint] = 0
	}
	return tx.Rollback(ctx)
}

// randOp attempts to produce a random schema change operation. It returns a
// triple `(randOp, log, error)`. On success `randOp` is the random schema
// change constructed. Constructing a random schema change may require a few
// stochastic attempts and if verbosity is >= 2 the unsuccessful attempts are
// recorded in `log` to help with debugging of the workload.
func (og *operationGenerator) randOp(ctx context.Context, tx pgx.Tx) (stmt *opStmt, err error) {
	for {
		op := opType(og.params.ops.Int())
		og.resetOpState()
		stmt, err = opFuncs[op](og, ctx, tx)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			// Table select had a primary key swap, so no statement
			// generated.
			if errors.Is(err, ErrSchemaChangesDisallowedDueToPkSwap) {
				continue
			}
			return nil, err
		}
		// Screen for schema change after write in the same transaction.
		og.stmtsInTxt = append(og.stmtsInTxt, stmt)
		// Add candidateExpectedCommitErrors to expectedCommitErrors
		og.expectedCommitErrors.merge(og.candidateExpectedCommitErrors)
		og.opsInTxn = append(og.opsInTxn, op)
		break
	}

	return stmt, err
}

func (og *operationGenerator) addColumn(ctx context.Context, tx pgx.Tx) (*opStmt, error) {

	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IrrelevantColumnName string`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(false))
	if err != nil {
		return nil, err
	}

	typName, typ, err := og.randType(ctx, tx, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	def := &tree.ColumnTableDef{
		Name: tree.Name(columnName),
		Type: typName,
	}
	def.Nullable.Nullability = tree.Nullability(og.randIntn(1 + int(tree.SilentNull)))

	databaseHasRegionChange, err := og.databaseHasRegionChange(ctx, tx)
	if err != nil {
		return nil, err
	}
	tableIsRegionalByRow, err := og.tableIsRegionalByRow(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	if !(tableIsRegionalByRow && databaseHasRegionChange) && og.randIntn(10) == 0 {
		def.Unique.IsUnique = true
	}

	columnExistsOnTable, err := og.columnExistsOnTable(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	var hasRows bool
	if tableExists {
		hasRows, err = og.tableHasRows(ctx, tx, tableName)
		if err != nil {
			return nil, err
		}
	}

	hasAlterPKSchemaChange, err := og.tableHasOngoingAlterPKSchemaChanges(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	op := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.DuplicateColumn, condition: columnExistsOnTable},
		{code: pgcode.UndefinedObject, condition: typ == nil},
		{code: pgcode.NotNullViolation, condition: hasRows && def.Nullable.Nullability == tree.NotNull},
		{code: pgcode.FeatureNotSupported, condition: hasAlterPKSchemaChange},
		// UNIQUE is only supported for indexable types.
		{
			code:      pgcode.FeatureNotSupported,
			condition: def.Unique.IsUnique && typ != nil && !colinfo.ColumnTypeIsIndexable(typ),
		},
	}.add(op.expectedExecErrors)
	op.sql = fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s`, tableName, tree.Serialize(def))
	return op, nil
}

func (og *operationGenerator) addConstraint(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	// TODO(peter): unimplemented
	// - Export sqlbase.randColumnTableDef.
	return nil, nil
}

func (og *operationGenerator) addUniqueConstraint(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT IrrelevantConstraintName UNIQUE (IrrelevantColumnName)`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnForConstraint, err := og.randColumnWithMeta(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	constaintName := fmt.Sprintf("%s_%s_unique", tableName.Object(), columnForConstraint.name)

	columnExistsOnTable, err := og.columnExistsOnTable(ctx, tx, tableName, columnForConstraint.name)
	if err != nil {
		return nil, err
	}
	constraintExists, err := og.constraintExists(ctx, tx, constaintName)
	if err != nil {
		return nil, err
	}

	canApplyConstraint := true
	if columnExistsOnTable {
		canApplyConstraint, err = og.canApplyUniqueConstraint(ctx, tx, tableName, []string{columnForConstraint.name})
		if err != nil {
			return nil, err
		}
	}

	hasAlterPKSchemaChange, err := og.tableHasOngoingAlterPKSchemaChanges(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	databaseHasRegionChange, err := og.databaseHasRegionChange(ctx, tx)
	if err != nil {
		return nil, err
	}
	tableIsRegionalByRow, err := og.tableIsRegionalByRow(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.UndefinedColumn, condition: !columnExistsOnTable},
		{code: pgcode.DuplicateObject, condition: constraintExists},
		{code: pgcode.FeatureNotSupported, condition: columnExistsOnTable && !colinfo.ColumnTypeIsIndexable(columnForConstraint.typ)},
		{pgcode.FeatureNotSupported, hasAlterPKSchemaChange},
		{code: pgcode.ObjectNotInPrerequisiteState, condition: databaseHasRegionChange && tableIsRegionalByRow},
	}.add(stmt.expectedExecErrors)

	if !canApplyConstraint {
		og.candidateExpectedCommitErrors.add(pgcode.UniqueViolation)
	}

	stmt.sql = fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)`, tableName, constaintName, columnForConstraint.name)
	return stmt, nil
}

func (og *operationGenerator) alterTableLocality(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s SET LOCALITY REGIONAL BY ROW`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	databaseRegionNames, err := og.getDatabaseRegionNames(ctx, tx)
	if err != nil {
		return nil, err
	}
	if len(databaseRegionNames) == 0 {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s SET LOCALITY REGIONAL BY ROW`, tableName),
			pgcode.InvalidTableDefinition), nil
	}

	hasSchemaChange, err := og.tableHasOngoingSchemaChanges(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	databaseHasRegionChange, err := og.databaseHasRegionChange(ctx, tx)
	if err != nil {
		return nil, err
	}
	databaseHasMultiRegion, err := og.databaseIsMultiRegion(ctx, tx)
	if err != nil {
		return nil, err
	}
	if hasSchemaChange || databaseHasRegionChange || !databaseHasMultiRegion {
		return makeOpStmtForSingleError(OpStmtDDL,
			`ALTER TABLE invalid_table SET LOCALITY REGIONAL BY ROW`,
			pgcode.UndefinedTable), nil
	}
	stmt := makeOpStmt(OpStmtDDL)
	localityOptions := []func() (string, error){
		func() (string, error) {
			return "REGIONAL BY TABLE", nil
		},
		func() (string, error) {
			idx := og.params.rng.Intn(len(databaseRegionNames))
			regionName := tree.Name(databaseRegionNames[idx])
			return fmt.Sprintf(`REGIONAL BY TABLE IN %s`, regionName.String()), nil
		},
		func() (string, error) {
			return "GLOBAL", nil
		},
		func() (string, error) {
			columnForAs, err := og.randColumnWithMeta(ctx, tx, *tableName, og.alwaysExisting())
			columnForAsUsed := false
			if err != nil {
				return "", err
			}
			ret := "REGIONAL BY ROW"
			if columnForAs.typ.TypeMeta.Name != nil {
				if columnForAs.typ.TypeMeta.Name.Basename() == tree.RegionEnum &&
					!columnForAs.nullable {
					ret += " AS " + columnForAs.name
					columnForAsUsed = true
				}
			}
			// If the table has a crdb_region column, make sure that it's not
			// nullable. This is required to handle the case where there's an
			// existing crdb_region column, but it is nullable, and therefore
			// cannot be used as the implicit partitioning column.
			if !columnForAsUsed {
				columnNames, err := og.getTableColumns(ctx, tx, tableName.String(), true)
				if err != nil {
					return "", err
				}
				for _, col := range columnNames {
					if col.name == tree.RegionalByRowRegionDefaultCol &&
						col.nullable {
						stmt.expectedExecErrors.add(pgcode.InvalidTableDefinition)
					}
				}
			}

			return ret, nil
		},
	}
	idx := og.params.rng.Intn(len(localityOptions))
	toLocality, err := localityOptions[idx]()
	if err != nil {
		return nil, err
	}
	stmt.sql = fmt.Sprintf(`ALTER TABLE %s SET LOCALITY %s`, tableName, toLocality)
	return stmt, nil
}

func (og *operationGenerator) getClusterRegionNames(
	ctx context.Context, tx pgx.Tx,
) (catpb.RegionNames, error) {
	return og.scanRegionNames(ctx, tx, "SELECT region FROM [SHOW REGIONS FROM CLUSTER]")
}

func (og *operationGenerator) getDatabaseRegionNames(
	ctx context.Context, tx pgx.Tx,
) (catpb.RegionNames, error) {
	return og.scanRegionNames(ctx, tx, "SELECT region FROM [SHOW REGIONS FROM DATABASE]")
}

func (og *operationGenerator) getDatabase(ctx context.Context, tx pgx.Tx) (string, error) {
	var database string
	err := tx.QueryRow(ctx, "SHOW DATABASE").Scan(&database)
	og.LogQueryResults("SHOW DATABASE", database)
	return database, err
}

type getRegionsResult struct {
	regionNamesInDatabase catpb.RegionNames
	regionNamesInCluster  catpb.RegionNames

	regionNamesNotInDatabase catpb.RegionNames
}

func (og *operationGenerator) getRegions(ctx context.Context, tx pgx.Tx) (getRegionsResult, error) {
	regionNamesInCluster, err := og.getClusterRegionNames(ctx, tx)
	if err != nil {
		return getRegionsResult{}, err
	}
	regionNamesNotInDatabaseSet := make(map[catpb.RegionName]struct{}, len(regionNamesInCluster))
	for _, clusterRegionName := range regionNamesInCluster {
		regionNamesNotInDatabaseSet[clusterRegionName] = struct{}{}
	}
	regionNamesInDatabase, err := og.getDatabaseRegionNames(ctx, tx)
	if err != nil {
		return getRegionsResult{}, err
	}
	for _, databaseRegionName := range regionNamesInDatabase {
		delete(regionNamesNotInDatabaseSet, databaseRegionName)
	}

	regionNamesNotInDatabase := make(catpb.RegionNames, 0, len(regionNamesNotInDatabaseSet))
	for regionName := range regionNamesNotInDatabaseSet {
		regionNamesNotInDatabase = append(regionNamesNotInDatabase, regionName)
	}
	return getRegionsResult{
		regionNamesInDatabase:    regionNamesInDatabase,
		regionNamesInCluster:     regionNamesInCluster,
		regionNamesNotInDatabase: regionNamesNotInDatabase,
	}, nil
}

func (og *operationGenerator) scanRegionNames(
	ctx context.Context, tx pgx.Tx, query string,
) (catpb.RegionNames, error) {
	var regionNames catpb.RegionNames
	var regionNamesForLog []string
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var regionName catpb.RegionName
		if err := rows.Scan(&regionName); err != nil {
			return nil, err
		}
		regionNames = append(regionNames, regionName)
		regionNamesForLog = append(regionNamesForLog, regionName.String())
	}
	if rows.Err() != nil {
		return nil, errors.Wrapf(rows.Err(), "failed to get regions: %s", query)
	}
	og.LogQueryResultArray(query, regionNamesForLog)
	return regionNames, nil
}

func (og *operationGenerator) addRegion(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	regionResult, err := og.getRegions(ctx, tx)
	if err != nil {
		return nil, err
	}
	database, err := og.getDatabase(ctx, tx)
	if err != nil {
		return nil, err
	}
	// No regions in cluster, try add an invalid region and expect an error.
	if len(regionResult.regionNamesInCluster) == 0 {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER DATABASE %s ADD REGION "invalid-region"`, database),
			pgcode.InvalidDatabaseDefinition), nil
	}
	// No regions in database, add a random region from the cluster and expect an error.
	if len(regionResult.regionNamesInDatabase) == 0 {
		idx := og.params.rng.Intn(len(regionResult.regionNamesInCluster))
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(
				`ALTER DATABASE %s ADD REGION "%s"`,
				database,
				regionResult.regionNamesInCluster[idx],
			),
			pgcode.InvalidDatabaseDefinition), nil
	}
	// If the database is undergoing a regional by row related change on the
	// database, error out.
	if len(regionResult.regionNamesInDatabase) > 0 {
		databaseHasRegionalByRowChange, err := og.databaseHasRegionalByRowChange(ctx, tx)
		if err != nil {
			return nil, err
		}
		if databaseHasRegionalByRowChange {
			// There's a timing hole here, as by the time we issue the ADD
			// REGION statement, the above REGIONAL BY ROW change may have
			// already completed. Either way, we'll get one of the following
			// two errors (the first, if the schema change has completed, and
			// the second, if it has not).
			return makeOpStmtForSingleError(OpStmtDDL,
				fmt.Sprintf(`ALTER DATABASE %s ADD REGION "invalid-region"`, database),
				pgcode.InvalidName,
				pgcode.ObjectNotInPrerequisiteState), nil
		}
	}
	// All regions are already in the database, expect an error with adding an existing one.
	if len(regionResult.regionNamesNotInDatabase) == 0 {
		idx := og.params.rng.Intn(len(regionResult.regionNamesInDatabase))
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(
				`ALTER DATABASE %s ADD REGION "%s"`,
				database,
				regionResult.regionNamesInDatabase[idx],
			),
			pgcode.DuplicateObject), nil
	}
	// Here we have a region that is not yet marked as public on the enum.
	// Double check this first.
	stmt := makeOpStmt(OpStmtDDL)
	idx := og.params.rng.Intn(len(regionResult.regionNamesNotInDatabase))
	region := regionResult.regionNamesNotInDatabase[idx]
	valuePresent, err := og.enumMemberPresent(ctx, tx, tree.RegionEnum, string(region))
	if err != nil {
		return nil, err
	}
	if valuePresent {
		stmt.expectedExecErrors.add(pgcode.DuplicateObject)
	}
	stmt.sql = fmt.Sprintf(
		`ALTER DATABASE %s ADD REGION "%s"`,
		database,
		region,
	)
	return stmt, nil
}

func (og *operationGenerator) primaryRegion(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	regionResult, err := og.getRegions(ctx, tx)
	if err != nil {
		return nil, err
	}
	database, err := og.getDatabase(ctx, tx)
	if err != nil {
		return nil, err
	}

	// No regions in cluster, try PRIMARY REGION an invalid region and expect an error.
	if len(regionResult.regionNamesInCluster) == 0 {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER DATABASE %s PRIMARY REGION "invalid-region"`, database),
			pgcode.InvalidDatabaseDefinition), nil
	}

	// Conversion to multi-region is only allowed if the data is not already
	// partitioned.
	stmt := makeOpStmt(OpStmtDDL)
	hasPartitioning, err := og.databaseHasTablesWithPartitioning(ctx, tx, database)
	if err != nil {
		return nil, err
	}
	if hasPartitioning {
		stmt.expectedExecErrors.add(pgcode.ObjectNotInPrerequisiteState)
	}

	// No regions in database, set a random region to be the PRIMARY REGION.
	if len(regionResult.regionNamesInDatabase) == 0 {
		idx := og.params.rng.Intn(len(regionResult.regionNamesInCluster))
		stmt.sql = fmt.Sprintf(
			`ALTER DATABASE %s PRIMARY REGION "%s"`,
			database,
			regionResult.regionNamesInCluster[idx],
		)
		return stmt, nil
	}

	// Regions exist in database, so set a random region to be the primary region.
	idx := og.params.rng.Intn(len(regionResult.regionNamesInDatabase))
	stmt.sql = fmt.Sprintf(
		`ALTER DATABASE %s PRIMARY REGION "%s"`,
		database,
		regionResult.regionNamesInDatabase[idx],
	)
	return stmt, nil
}

func (og *operationGenerator) addForeignKeyConstraint(
	ctx context.Context, tx pgx.Tx,
) (*opStmt, error) {
	parentTable, parentColumn, err := og.randParentColumnForFkRelation(ctx, tx, og.randIntn(100) >= og.params.fkParentInvalidPct)
	if err != nil {
		return nil, err
	}

	fetchInvalidChild := og.randIntn(100) < og.params.fkChildInvalidPct
	// Potentially create an error by choosing the wrong type for the child column.
	childType := parentColumn.typ
	if fetchInvalidChild {
		_, typ, err := og.randType(ctx, tx, og.pctExisting(true))
		if err != nil {
			return nil, err
		}
		if typ != nil {
			childType = typ
		}
	}

	childTable, childColumn, err := og.randChildColumnForFkRelation(ctx, tx, !fetchInvalidChild, childType.SQLString())
	if err != nil {
		return nil, err
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

	parentColumnHasUniqueConstraint, err := og.columnHasSingleUniqueConstraint(ctx, tx, parentTable, parentColumn.name)
	if err != nil {
		return nil, err
	}
	parentColumnIsVirtualComputed, err := og.columnIsVirtualComputed(ctx, tx, parentTable, parentColumn.name)
	if err != nil {
		return nil, err
	}
	childColumnIsVirtualComputed, err := og.columnIsVirtualComputed(ctx, tx, childTable, childColumn.name)
	if err != nil {
		return nil, err
	}
	childColumnIsStoredVirtual, err := og.columnIsStoredComputed(ctx, tx, childTable, childColumn.name)
	if err != nil {
		return nil, err
	}
	constraintExists, err := og.constraintExists(ctx, tx, string(constraintName))
	if err != nil {
		return nil, err
	}
	// If we are intentionally using an invalid child type, then it doesn't make
	// sense to validate if the rows validate the constraint.
	rowsSatisfyConstraint := true
	if !fetchInvalidChild {
		rowsSatisfyConstraint, err = og.rowsSatisfyFkConstraint(ctx, tx, parentTable, parentColumn, childTable, childColumn)
		if err != nil {
			return nil, err
		}
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.ForeignKeyViolation, condition: !parentColumnHasUniqueConstraint},
		{code: pgcode.FeatureNotSupported, condition: childColumnIsVirtualComputed},
		{code: pgcode.FeatureNotSupported, condition: childColumnIsStoredVirtual},
		{code: pgcode.FeatureNotSupported, condition: parentColumnIsVirtualComputed},
		{code: pgcode.DuplicateObject, condition: constraintExists},
		{code: pgcode.DatatypeMismatch, condition: !childColumn.typ.Equivalent(parentColumn.typ)},
	}.add(stmt.expectedExecErrors)
	codesWithConditions{}.add(og.expectedCommitErrors)

	// TODO(fqazi): We need to do after the fact validation for foreign key violations
	// errors. Due to how adding foreign key constraints are implemented with a
	// separate job validating the constraint, we can't at transaction time predict,
	// perfectly if an error is expected. We can confirm post transaction with a time
	// travel query.
	_ = rowsSatisfyConstraint
	stmt.potentialExecErrors.add(pgcode.ForeignKeyViolation)
	og.potentialCommitErrors.add(pgcode.ForeignKeyViolation)

	// It's possible for the table to be dropped concurrently, while we are running
	// validation. In which case a potential commit error is an undefined table
	// error.
	og.potentialCommitErrors.add(pgcode.UndefinedTable)
	stmt.sql = tree.Serialize(def)
	return stmt, nil
}

func (og *operationGenerator) createIndex(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		def := &tree.CreateIndex{
			Name:  tree.Name("IrrelevantName"),
			Table: *tableName,
			Columns: tree.IndexElemList{
				{Column: "IrrelevantColumn", Direction: tree.Ascending},
			},
		}
		return makeOpStmtForSingleError(OpStmtDDL,
			tree.Serialize(def),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnNames, err := og.getTableColumns(ctx, tx, tableName.String(), true)
	if err != nil {
		return nil, err
	}

	indexName, err := og.randIndex(ctx, tx, *tableName, og.pctExisting(false))
	if err != nil {
		return nil, err
	}

	indexExists, err := og.indexExists(ctx, tx, tableName, indexName)
	if err != nil {
		return nil, err
	}

	// Only generate invisible indexes when they are supported.
	invisibleIndexesIsNotSupported, err := isClusterVersionLessThan(
		ctx,
		tx,
		clusterversion.ByKey(clusterversion.Start22_2))
	if err != nil {
		return nil, err
	}

	def := &tree.CreateIndex{
		Name:        tree.Name(indexName),
		Table:       *tableName,
		Unique:      og.randIntn(4) == 0,                                     // 25% UNIQUE
		Inverted:    og.randIntn(10) == 0,                                    // 10% INVERTED
		IfNotExists: og.randIntn(2) == 0,                                     // 50% IF NOT EXISTS
		NotVisible:  og.randIntn(20) == 0 && !invisibleIndexesIsNotSupported, // 5% NOT VISIBLE
	}

	regionColumn := ""
	tableIsRegionalByRow, err := og.tableIsRegionalByRow(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if tableIsRegionalByRow {
		regionColumn, err = og.getRegionColumn(ctx, tx, tableName)
		if err != nil {
			return nil, err
		}
	}

	// Define columns on which to create an index. Check for types which cannot be indexed.
	duplicateRegionColumn := false
	nonIndexableType := false
	def.Columns = make(tree.IndexElemList, 1+og.randIntn(len(columnNames)))
	for i := range def.Columns {
		def.Columns[i].Column = tree.Name(columnNames[i].name)
		def.Columns[i].Direction = tree.Direction(og.randIntn(1 + int(tree.Descending)))

		// When creating an index, the column being used as the region column
		// for a REGIONAL BY ROW table can only be included in indexes as the
		// first column. If it's not the first column, we need to add an error
		// below.
		if columnNames[i].name == regionColumn && i != 0 {
			duplicateRegionColumn = true
		}
		if def.Inverted {
			// We can have an inverted index on a set of columns if the last column
			// is an inverted indexable type and the preceding columns are not.
			invertedIndexableType := colinfo.ColumnTypeIsInvertedIndexable(columnNames[i].typ)
			if (invertedIndexableType && i < len(def.Columns)-1) ||
				(!invertedIndexableType && i == len(def.Columns)-1) {
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
	stmt := makeOpStmt(OpStmtDDL)
	duplicateStore := false
	virtualComputedStored := false
	regionColStored := false
	columnNames = columnNames[len(def.Columns):]
	if n := len(columnNames); n > 0 {
		def.Storing = make(tree.NameList, og.randIntn(1+n))
		for i := range def.Storing {
			def.Storing[i] = tree.Name(columnNames[i].name)

			// The region column can not be stored.
			if tableIsRegionalByRow && columnNames[i].name == regionColumn {
				regionColStored = true
			}

			// Virtual computed columns are not allowed to be indexed
			if columnNames[i].generated && !virtualComputedStored {
				isStored, err := og.columnIsStoredComputed(ctx, tx, tableName, columnNames[i].name)
				if err != nil {
					return nil, err
				}
				if !isStored {
					virtualComputedStored = true
				}
			}

			// If the column is already used in the primary key, then attempting to store
			// it using an index will produce a pgcode.DuplicateColumn error.
			if !duplicateStore {
				colUsedInPrimaryIdx, err := og.colIsPrimaryKey(ctx, tx, tableName, columnNames[i].name)
				if err != nil {
					return nil, err
				}
				if colUsedInPrimaryIdx {
					duplicateStore = true
				}
			}
		}
	}

	// Verify that a unique constraint can be added given the existing rows which may exist in the table.
	uniqueViolationWillNotOccur := true
	if def.Unique {
		columns := []string{}
		for _, col := range def.Columns {
			columns = append(columns, string(col.Column))
		}
		uniqueViolationWillNotOccur, err = og.canApplyUniqueConstraint(ctx, tx, tableName, columns)
		if err != nil {
			return nil, err
		}
	}

	hasAlterPKSchemaChange, err := og.tableHasOngoingAlterPKSchemaChanges(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	databaseHasRegionChange, err := og.databaseHasRegionChange(ctx, tx)
	if err != nil {
		return nil, err
	}
	if databaseHasRegionChange && tableIsRegionalByRow {
		stmt.expectedExecErrors.add(pgcode.ObjectNotInPrerequisiteState)
	}

	// When an index exists, but `IF NOT EXISTS` is used, then
	// the index will not be created and the op will complete without errors.
	if !(indexExists && def.IfNotExists) {
		codesWithConditions{
			{code: pgcode.DuplicateRelation, condition: indexExists},
			// Inverted indexes do not support stored columns.
			{code: pgcode.InvalidSQLStatementName, condition: len(def.Storing) > 0 && def.Inverted},
			// Inverted indexes cannot be unique.
			{code: pgcode.InvalidSQLStatementName, condition: def.Unique && def.Inverted},
			// If there is data in the table such that a unique index cannot be created,
			// a pgcode.UniqueViolation will occur and will be wrapped in a
			// pgcode.TransactionCommittedWithSchemaChangeFailure. The schemachange worker
			// is expected to parse for the underlying error.
			{code: pgcode.UniqueViolation, condition: !uniqueViolationWillNotOccur},
			{code: pgcode.DuplicateColumn, condition: duplicateStore},
			{code: pgcode.FeatureNotSupported, condition: nonIndexableType},
			{code: pgcode.FeatureNotSupported, condition: regionColStored},
			{code: pgcode.FeatureNotSupported, condition: duplicateRegionColumn},
			{code: pgcode.Uncategorized, condition: virtualComputedStored},
			{code: pgcode.FeatureNotSupported, condition: hasAlterPKSchemaChange},
		}.add(stmt.expectedExecErrors)
	}

	stmt.sql = tree.Serialize(def)
	return stmt, nil
}

func (og *operationGenerator) createSequence(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	seqName, err := og.randSequence(ctx, tx, og.pctExisting(false), "")
	if err != nil {
		return nil, err
	}

	schemaExists, err := og.schemaExists(ctx, tx, seqName.Schema())
	if err != nil {
		return nil, err
	}
	sequenceExists, err := og.sequenceExists(ctx, tx, seqName)
	if err != nil {
		return nil, err
	}

	// If the sequence exists and an error should be produced, then
	// exclude the IF NOT EXISTS clause from the statement. Otherwise, default
	// to including the clause prevent all pgcode.DuplicateRelation errors.
	ifNotExists := true
	if sequenceExists && og.produceError() {
		ifNotExists = false
	}
	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.UndefinedSchema, condition: !schemaExists},
		{code: pgcode.DuplicateRelation, condition: sequenceExists && !ifNotExists},
	}.add(stmt.expectedExecErrors)

	var seqOptions tree.SequenceOptions
	// Decide if the sequence should be owned by a column. If so, it can
	// set using the tree.SeqOptOwnedBy sequence option.
	if og.randIntn(100) < og.params.sequenceOwnedByPct {
		table, err := og.randTable(ctx, tx, og.pctExisting(true), "")
		if err != nil {
			return nil, err
		}
		tableExists, err := og.tableExists(ctx, tx, table)
		if err != nil {
			return nil, err
		}

		if !tableExists {
			seqOptions = append(
				seqOptions,
				tree.SequenceOption{
					Name:          tree.SeqOptOwnedBy,
					ColumnItemVal: &tree.ColumnItem{TableName: table.ToUnresolvedObjectName(), ColumnName: "IrrelevantColumnName"}},
			)
			if !(sequenceExists && ifNotExists) { // IF NOT EXISTS prevents the error
				stmt.expectedExecErrors.add(pgcode.UndefinedTable)
			}
		} else {
			column, err := og.randColumn(ctx, tx, *table, og.pctExisting(true))
			if err != nil {
				return nil, err
			}
			columnExists, err := og.columnExistsOnTable(ctx, tx, table, column)
			if err != nil {
				return nil, err
			}
			// If a duplicate sequence exists, then a new sequence will not be created. In this case,
			// a pgcode.UndefinedColumn will not occur.
			if !columnExists && !sequenceExists {
				stmt.expectedExecErrors.add(pgcode.UndefinedColumn)
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

	stmt.sql = tree.Serialize(createSeq)
	return stmt, nil
}

func (og *operationGenerator) createTable(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(false), "")
	if err != nil {
		return nil, err
	}

	tableIdx, err := strconv.Atoi(strings.TrimPrefix(tableName.Table(), "table"))
	if err != nil {
		return nil, err
	}
	databaseHasMultiRegion, err := og.databaseIsMultiRegion(ctx, tx)
	if err != nil {
		return nil, err
	}

	stmt := randgen.RandCreateTableWithColumnIndexNumberGenerator(og.params.rng, "table", tableIdx, databaseHasMultiRegion, og.newUniqueSeqNum)
	stmt.Table = *tableName
	stmt.IfNotExists = og.randIntn(2) == 0
	trigramIsNotSupported, err := isClusterVersionLessThan(
		ctx,
		tx,
		clusterversion.ByKey(clusterversion.TrigramInvertedIndexes))
	if err != nil {
		return nil, err
	}
	hasTrigramIdxUnsupported := func() bool {
		if !trigramIsNotSupported {
			return false
		}
		// Check if any of the indexes have trigrams involved.
		for _, def := range stmt.Defs {
			if idx, ok := def.(*tree.IndexTableDef); ok && idx.Inverted {
				lastColumn := idx.Columns[len(idx.Columns)-1]
				switch lastColumn.OpClass {
				case "gin_trgm_ops", "gist_trgm_ops":
					return true
				}
			}
		}
		return false
	}()

	invisibleIndexesIsNotSupported, err := isClusterVersionLessThan(
		ctx,
		tx,
		clusterversion.ByKey(clusterversion.Start22_2))
	if err != nil {
		return nil, err
	}
	hasInvisibleIndexesUnsupported := func() bool {
		if !invisibleIndexesIsNotSupported {
			return false
		}
		// Check if any of the indexes have trigrams involved.
		for _, def := range stmt.Defs {
			if idx, ok := def.(*tree.IndexTableDef); ok && idx.NotVisible {
				return true
			}
		}
		return false
	}()

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	schemaExists, err := og.schemaExists(ctx, tx, tableName.Schema())
	if err != nil {
		return nil, err
	}
	opStmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.DuplicateRelation, condition: tableExists && !stmt.IfNotExists},
		{code: pgcode.UndefinedSchema, condition: !schemaExists},
	}.add(opStmt.expectedExecErrors)
	// Compatibility errors aren't guaranteed since the cluster version update is not
	// fully transaction aware.
	codesWithConditions{
		{code: pgcode.FeatureNotSupported, condition: hasTrigramIdxUnsupported},
		{code: pgcode.Syntax, condition: hasInvisibleIndexesUnsupported},
	}.add(opStmt.potentialExecErrors)
	opStmt.sql = tree.Serialize(stmt)
	return opStmt, nil
}

func (og *operationGenerator) createEnum(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	typName, typeExists, err := og.randEnum(ctx, tx, og.pctExisting(false))
	if err != nil {
		return nil, err
	}
	schemaExists, err := og.schemaExists(ctx, tx, typName.Schema())
	if err != nil {
		return nil, err
	}
	opStmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.DuplicateObject, condition: typeExists},
		{code: pgcode.InvalidSchemaName, condition: !schemaExists},
	}.add(opStmt.expectedExecErrors)
	stmt := randgen.RandCreateType(og.params.rng, typName.Object(), "asdf")
	stmt.(*tree.CreateType).TypeName = typName.ToUnresolvedObjectName()
	opStmt.sql = tree.Serialize(stmt)
	return opStmt, nil
}

func (og *operationGenerator) createTableAs(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	const MaxRowsToConsume = 300000
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
			tableName, err = og.randTable(ctx, tx, og.pctExisting(true), "")
			if err != nil {
				return nil, err
			}
			sourceTableExists, err = og.tableExists(ctx, tx, tableName)
			if err != nil {
				return nil, err
			}

		case 1:
			tableName, err = og.randView(ctx, tx, og.pctExisting(true), "")
			if err != nil {
				return nil, err
			}
			sourceTableExists, err = og.viewExists(ctx, tx, tableName)
			if err != nil {
				return nil, err
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
	opStmt := makeOpStmt(OpStmtDDL)
	uniqueColumnNames := map[string]bool{}
	duplicateColumns := false
	for i := 0; i < numSourceTables; i++ {
		tableName := sourceTableNames[i]
		tableExists := sourceTableExistence[i]

		// If the table does not exist, columns cannot be fetched from it. For this reason, the placeholder
		// "IrrelevantColumnName" is used, and a pgcode.UndefinedTable error is expected on execution.
		if tableExists {
			columnNamesForTable, err := og.tableColumnsShuffled(ctx, tx, tableName.(*tree.TableName).String())
			if err != nil {
				return nil, err
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
			opStmt.expectedExecErrors.add(pgcode.UndefinedTable)
			colItem := tree.ColumnItem{
				ColumnName: tree.Name("IrrelevantColumnName"),
			}
			selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})
		}
	}

	destTableName, err := og.randTable(ctx, tx, og.pctExisting(false), "")
	if err != nil {
		return nil, err
	}
	schemaExists, err := og.schemaExists(ctx, tx, destTableName.Schema())
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, destTableName)
	if err != nil {
		return nil, err
	}

	codesWithConditions{
		{code: pgcode.InvalidSchemaName, condition: !schemaExists},
		{code: pgcode.DuplicateRelation, condition: tableExists},
		{code: pgcode.Syntax, condition: len(selectStatement.Exprs) == 0},
		{code: pgcode.DuplicateAlias, condition: duplicateSourceTables},
		{code: pgcode.DuplicateColumn, condition: duplicateColumns},
	}.add(opStmt.expectedExecErrors)

	// Confirm the select itself doesn't run into any column generation errors,
	// by executing it independently first until we add validation when adding
	// generated columns. See issue: #81698?, which will allow us to remove this
	// logic in the future.
	if opStmt.expectedExecErrors.empty() {
		opStmt.potentialExecErrors.merge(getValidGenerationErrors())
	}

	opStmt.sql = fmt.Sprintf(`CREATE TABLE %s AS %s FETCH FIRST %d ROWS ONLY`,
		destTableName, selectStatement.String(), MaxRowsToConsume)
	return opStmt, nil
}

func (og *operationGenerator) createView(ctx context.Context, tx pgx.Tx) (*opStmt, error) {

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
			tableName, err = og.randTable(ctx, tx, og.pctExisting(true), "")
			if err != nil {
				return nil, err
			}
			sourceTableExists, err = og.tableExists(ctx, tx, tableName)
			if err != nil {
				return nil, err
			}

		case 1:
			tableName, err = og.randView(ctx, tx, og.pctExisting(true), "")
			if err != nil {
				return nil, err
			}
			sourceTableExists, err = og.viewExists(ctx, tx, tableName)
			if err != nil {
				return nil, err
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
	opStmt := makeOpStmt(OpStmtDDL)
	uniqueColumnNames := map[string]bool{}
	duplicateColumns := false
	for i := 0; i < numSourceTables; i++ {
		tableName := sourceTableNames[i]
		tableExists := sourceTableExistence[i]

		// If the table does not exist, columns cannot be fetched from it. For this reason, the placeholder
		// "IrrelevantColumnName" is used, and a pgcode.UndefinedTable error is expected on execution.
		if tableExists {
			columnNamesForTable, err := og.tableColumnsShuffled(ctx, tx, tableName.(*tree.TableName).String())
			if err != nil {
				return nil, err
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
			opStmt.expectedExecErrors.add(pgcode.UndefinedTable)
			colItem := tree.ColumnItem{
				ColumnName: tree.Name("IrrelevantColumnName"),
			}
			selectStatement.Exprs = append(selectStatement.Exprs, tree.SelectExpr{Expr: &colItem})
		}
	}

	destViewName, err := og.randView(ctx, tx, og.pctExisting(false), "")
	if err != nil {
		return nil, err
	}
	schemaExists, err := og.schemaExists(ctx, tx, destViewName.Schema())
	if err != nil {
		return nil, err
	}
	viewExists, err := og.viewExists(ctx, tx, destViewName)
	if err != nil {
		return nil, err
	}

	codesWithConditions{
		{code: pgcode.InvalidSchemaName, condition: !schemaExists},
		{code: pgcode.DuplicateRelation, condition: viewExists},
		{code: pgcode.Syntax, condition: len(selectStatement.Exprs) == 0},
		{code: pgcode.DuplicateAlias, condition: duplicateSourceTables},
		{code: pgcode.DuplicateColumn, condition: duplicateColumns},
	}.add(opStmt.expectedExecErrors)

	opStmt.sql = fmt.Sprintf(`CREATE VIEW %s AS %s`,
		destViewName, selectStatement.String())
	return opStmt, nil
}

func (og *operationGenerator) dropColumn(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "IrrelevantColumnName"`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}
	columnExists, err := og.columnExistsOnTable(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	colIsPrimaryKey, err := og.colIsPrimaryKey(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	columnIsDependedOn, err := og.columnIsDependedOn(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	columnIsInDroppingIndex, err := og.columnIsInDroppingIndex(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	hasAlterPKSchemaChange, err := og.tableHasOngoingAlterPKSchemaChanges(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.ObjectNotInPrerequisiteState, condition: columnIsInDroppingIndex},
		{code: pgcode.UndefinedColumn, condition: !columnExists},
		{code: pgcode.InvalidColumnReference, condition: colIsPrimaryKey},
		{code: pgcode.DependentObjectsStillExist, condition: columnIsDependedOn},
		{code: pgcode.FeatureNotSupported, condition: hasAlterPKSchemaChange},
	}.add(stmt.expectedExecErrors)
	stmt.sql = fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "%s"`, tableName, columnName)
	return stmt, nil
}

func (og *operationGenerator) dropColumnDefault(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "IrrelevantColumnName" DROP DEFAULT`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	columnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}
	columnExists, err := og.columnExistsOnTable(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	stmt := makeOpStmt(OpStmtDDL)
	if !columnExists {
		stmt.expectedExecErrors.add(pgcode.UndefinedColumn)
	}
	stmt.sql = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP DEFAULT`, tableName, columnName)
	return stmt, nil
}

func (og *operationGenerator) dropColumnNotNull(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "IrrelevantColumnName" DROP NOT NULL`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	columnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}
	columnExists, err := og.columnExistsOnTable(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	colIsPrimaryKey, err := og.colIsPrimaryKey(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}

	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{pgcode.UndefinedColumn, !columnExists},
		{pgcode.InvalidTableDefinition, colIsPrimaryKey},
	}.add(stmt.expectedExecErrors)
	stmt.sql = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL`, tableName, columnName)
	return stmt, nil
}

func (og *operationGenerator) dropColumnStored(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN IrrelevantColumnName DROP STORED`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}
	columnExists, err := og.columnExistsOnTable(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}

	columnIsStored, err := og.columnIsStoredComputed(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.InvalidColumnDefinition, condition: !columnIsStored},
		{code: pgcode.UndefinedColumn, condition: !columnExists},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP STORED`, tableName, columnName)
	return stmt, nil
}

func (og *operationGenerator) dropConstraint(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT IrrelevantConstraintName`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	constraintName, err := og.randConstraint(ctx, tx, tableName.String())
	if err != nil {
		return nil, err
	}

	// Dropping the primary key of a table without adding a new primary key
	// subsequently in the transaction is not supported. Since addConstraint is not implemented,
	// a replacement primary key will not be created in the same transaction. Thus,
	// dropping a primary key will always produce an error.
	stmt := makeOpStmt(OpStmtDDL)
	constraintIsPrimary, err := og.constraintIsPrimary(ctx, tx, tableName, constraintName)
	if err != nil {
		return nil, err
	}
	if constraintIsPrimary {
		og.candidateExpectedCommitErrors.add(pgcode.FeatureNotSupported)
	}

	// DROP INDEX CASCADE is preferred for dropping unique constraints, and
	// dropping the constraint with ALTER TABLE ... DROP CONSTRAINT is unsupported.
	constraintIsUnique, err := og.constraintIsUnique(ctx, tx, tableName, constraintName)
	if err != nil {
		return nil, err
	}
	if constraintIsUnique {
		stmt.expectedExecErrors.add(pgcode.FeatureNotSupported)
	}

	constraintBeingDropped, err := og.constraintInDroppingState(ctx, tx, tableName, constraintName)
	if err != nil {
		return nil, err
	}
	if constraintBeingDropped {
		stmt.expectedExecErrors.add(pgcode.FeatureNotSupported)
	}

	stmt.sql = fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT "%s"`, tableName, constraintName)
	return stmt, nil
}

func (og *operationGenerator) dropIndex(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`DROP INDEX %s@"IrrelevantIndexName"`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	indexName, err := og.randIndex(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	stmt := makeOpStmt(OpStmtDDL)
	indexExists, err := og.indexExists(ctx, tx, tableName, indexName)
	if err != nil {
		return nil, err
	}
	if !indexExists {
		stmt.expectedExecErrors.add(pgcode.UndefinedObject)
	}

	hasAlterPKSchemaChange, err := og.tableHasOngoingAlterPKSchemaChanges(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if hasAlterPKSchemaChange {
		stmt.expectedExecErrors.add(pgcode.FeatureNotSupported)
	}

	databaseHasRegionChange, err := og.databaseHasRegionChange(ctx, tx)
	if err != nil {
		return nil, err
	}
	tableIsRegionalByRow, err := og.tableIsRegionalByRow(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if databaseHasRegionChange && tableIsRegionalByRow {
		stmt.expectedExecErrors.add(pgcode.ObjectNotInPrerequisiteState)
	}

	stmt.sql = fmt.Sprintf(`DROP INDEX %s@"%s" CASCADE`, tableName, indexName)
	return stmt, nil
}

func (og *operationGenerator) dropSequence(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	sequenceName, err := og.randSequence(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	ifExists := og.randIntn(2) == 0
	dropSeq := &tree.DropSequence{
		Names:    tree.TableNames{*sequenceName},
		IfExists: ifExists,
	}

	stmt := makeOpStmt(OpStmtDDL)
	sequenceExists, err := og.sequenceExists(ctx, tx, sequenceName)
	if err != nil {
		return nil, err
	}
	if !sequenceExists && !ifExists {
		stmt.expectedExecErrors.add(pgcode.UndefinedTable)
	}
	stmt.sql = tree.Serialize(dropSeq)
	return stmt, nil
}

func (og *operationGenerator) dropTable(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	tableHasDependencies, err := og.tableHasDependencies(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	dropBehavior := tree.DropBehavior(og.randIntn(3))

	ifExists := og.randIntn(2) == 0
	dropTable := tree.DropTable{
		Names:        []tree.TableName{*tableName},
		IfExists:     ifExists,
		DropBehavior: dropBehavior,
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{pgcode.UndefinedTable, !ifExists && !tableExists},
		{pgcode.DependentObjectsStillExist, dropBehavior != tree.DropCascade && tableHasDependencies},
	}.add(stmt.expectedExecErrors)
	stmt.sql = dropTable.String()
	return stmt, nil
}

func (og *operationGenerator) dropView(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	viewName, err := og.randView(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}
	viewExists, err := og.tableExists(ctx, tx, viewName)
	if err != nil {
		return nil, err
	}
	viewHasDependencies, err := og.tableHasDependencies(ctx, tx, viewName)
	if err != nil {
		return nil, err
	}

	dropBehavior := tree.DropBehavior(og.randIntn(3))

	ifExists := og.randIntn(2) == 0
	dropView := tree.DropView{
		Names:        []tree.TableName{*viewName},
		IfExists:     ifExists,
		DropBehavior: dropBehavior,
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{pgcode.UndefinedTable, !ifExists && !viewExists},
		{pgcode.DependentObjectsStillExist, dropBehavior != tree.DropCascade && viewHasDependencies},
	}.add(stmt.expectedExecErrors)
	stmt.sql = dropView.String()
	return stmt, nil
}

func (og *operationGenerator) renameColumn(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	srcTableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !srcTableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN "IrrelevantColumnName" TO "OtherIrrelevantName"`,
				tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	srcColumnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	destColumnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(false))
	if err != nil {
		return nil, err
	}

	srcColumnExists, err := og.columnExistsOnTable(ctx, tx, tableName, srcColumnName)
	if err != nil {
		return nil, err
	}
	destColumnExists, err := og.columnExistsOnTable(ctx, tx, tableName, destColumnName)
	if err != nil {
		return nil, err
	}
	columnIsDependedOn, err := og.columnIsDependedOn(ctx, tx, tableName, srcColumnName)
	if err != nil {
		return nil, err
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{pgcode.UndefinedColumn, !srcColumnExists},
		{pgcode.DuplicateColumn, destColumnExists && srcColumnName != destColumnName},
		{pgcode.DependentObjectsStillExist, columnIsDependedOn},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN "%s" TO "%s"`,
		tableName, srcColumnName, destColumnName)
	return stmt, nil
}

func (og *operationGenerator) renameIndex(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	srcTableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !srcTableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER INDEX %s@"IrrelevantConstraintName" RENAME TO "OtherConstraintName"`,
				tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	srcIndexName, err := og.randIndex(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	destIndexName, err := og.randIndex(ctx, tx, *tableName, og.pctExisting(false))
	if err != nil {
		return nil, err
	}

	srcIndexExists, err := og.indexExists(ctx, tx, tableName, srcIndexName)
	if err != nil {
		return nil, err
	}
	destIndexExists, err := og.indexExists(ctx, tx, tableName, destIndexName)
	if err != nil {
		return nil, err
	}

	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.UndefinedObject, condition: !srcIndexExists},
		{code: pgcode.DuplicateRelation, condition: destIndexExists && srcIndexName != destIndexName},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`ALTER INDEX %s@"%s" RENAME TO "%s"`,
		tableName, srcIndexName, destIndexName)
	return stmt, nil
}

func (og *operationGenerator) renameSequence(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	srcSequenceName, err := og.randSequence(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	// Decide whether or not to produce a 'cannot change schema of table with RENAME' error
	desiredSchema := ""
	if !og.produceError() {
		desiredSchema = srcSequenceName.Schema()
	}

	destSequenceName, err := og.randSequence(ctx, tx, og.pctExisting(false), desiredSchema)
	if err != nil {
		return nil, err
	}

	srcSequenceExists, err := og.sequenceExists(ctx, tx, srcSequenceName)
	if err != nil {
		return nil, err
	}

	destSchemaExists, err := og.schemaExists(ctx, tx, destSequenceName.Schema())
	if err != nil {
		return nil, err
	}

	destSequenceExists, err := og.sequenceExists(ctx, tx, destSequenceName)
	if err != nil {
		return nil, err
	}

	srcEqualsDest := srcSequenceName.String() == destSequenceName.String()
	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.UndefinedTable, condition: !srcSequenceExists},
		{code: pgcode.UndefinedSchema, condition: !destSchemaExists},
		{code: pgcode.DuplicateRelation, condition: !srcEqualsDest && destSequenceExists},
		{code: pgcode.InvalidName, condition: srcSequenceName.Schema() != destSequenceName.Schema()},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`ALTER SEQUENCE %s RENAME TO %s`, srcSequenceName, destSequenceName)
	return stmt, nil
}

func (og *operationGenerator) renameTable(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	srcTableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	// Decide whether or not to produce a 'cannot change schema of table with RENAME' error
	desiredSchema := ""
	if !og.produceError() {
		desiredSchema = srcTableName.SchemaName.String()
	}
	destTableName, err := og.randTable(ctx, tx, og.pctExisting(false), desiredSchema)
	if err != nil {
		return nil, err
	}

	srcTableExists, err := og.tableExists(ctx, tx, srcTableName)
	if err != nil {
		return nil, err
	}
	if srcTableExists {
		err = og.tableHasPrimaryKeySwapActive(ctx, tx, srcTableName)
		if err != nil {
			return nil, err
		}
	}

	destSchemaExists, err := og.schemaExists(ctx, tx, destTableName.Schema())
	if err != nil {
		return nil, err
	}

	destTableExists, err := og.tableExists(ctx, tx, destTableName)
	if err != nil {
		return nil, err
	}

	srcTableHasDependencies, err := og.tableHasDependencies(ctx, tx, srcTableName)
	if err != nil {
		return nil, err
	}

	srcEqualsDest := destTableName.String() == srcTableName.String()
	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.UndefinedTable, condition: !srcTableExists},
		{code: pgcode.UndefinedSchema, condition: !destSchemaExists},
		{code: pgcode.DuplicateRelation, condition: !srcEqualsDest && destTableExists},
		{code: pgcode.DependentObjectsStillExist, condition: srcTableHasDependencies},
		{code: pgcode.InvalidName, condition: srcTableName.Schema() != destTableName.Schema()},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, srcTableName, destTableName)
	return stmt, nil
}

func (og *operationGenerator) renameView(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	srcViewName, err := og.randView(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	// Decide whether or not to produce a 'cannot change schema of table with RENAME' error
	desiredSchema := ""
	if !og.produceError() {
		desiredSchema = srcViewName.SchemaName.String()
	}
	destViewName, err := og.randView(ctx, tx, og.pctExisting(false), desiredSchema)
	if err != nil {
		return nil, err
	}

	srcViewExists, err := og.viewExists(ctx, tx, srcViewName)
	if err != nil {
		return nil, err
	}

	destSchemaExists, err := og.schemaExists(ctx, tx, destViewName.Schema())
	if err != nil {
		return nil, err
	}

	destViewExists, err := og.viewExists(ctx, tx, destViewName)
	if err != nil {
		return nil, err
	}

	srcTableHasDependencies, err := og.tableHasDependencies(ctx, tx, srcViewName)
	if err != nil {
		return nil, err
	}

	srcEqualsDest := destViewName.String() == srcViewName.String()
	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{code: pgcode.UndefinedTable, condition: !srcViewExists},
		{code: pgcode.UndefinedSchema, condition: !destSchemaExists},
		{code: pgcode.DuplicateRelation, condition: !srcEqualsDest && destViewExists},
		{code: pgcode.DependentObjectsStillExist, condition: srcTableHasDependencies},
		{code: pgcode.InvalidName, condition: srcViewName.Schema() != destViewName.Schema()},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`ALTER VIEW %s RENAME TO %s`, srcViewName, destViewName)
	return stmt, nil
}

func (og *operationGenerator) setColumnDefault(ctx context.Context, tx pgx.Tx) (*opStmt, error) {

	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN IrrelevantColumnName SET DEFAULT "IrrelevantValue"`,
				tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnForDefault, err := og.randColumnWithMeta(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}
	columnExists, err := og.columnExistsOnTable(ctx, tx, tableName, columnForDefault.name)
	if err != nil {
		return nil, err
	}
	if !columnExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET DEFAULT "IrrelevantValue"`,
				tableName, columnForDefault.name),
			pgcode.UndefinedColumn), nil
	}

	datumTyp := columnForDefault.typ
	// Optionally change the incorrect type to potentially create errors.
	if og.produceError() {
		newTypeName, newTyp, err := og.randType(ctx, tx, og.pctExisting(true))
		if err != nil {
			return nil, err
		}
		if newTyp == nil {
			return makeOpStmtForSingleError(OpStmtDDL,
				fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET DEFAULT 'IrrelevantValue':::%s`, tableName, columnForDefault.name, newTypeName.SQLString()),
				pgcode.UndefinedColumn), nil
		}
		datumTyp = newTyp
	}

	defaultDatum := randgen.RandDatum(og.params.rng, datumTyp, columnForDefault.nullable)
	stmt := makeOpStmt(OpStmtDDL)
	if (!datumTyp.Equivalent(columnForDefault.typ)) && defaultDatum != tree.DNull {
		stmt.expectedExecErrors.add(pgcode.DatatypeMismatch)
	}

	// Generated columns cannot have default values.
	if columnForDefault.generated {
		stmt.expectedExecErrors.add(pgcode.InvalidTableDefinition)
	}

	stmt.sql = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`, tableName, columnForDefault.name, tree.AsStringWithFlags(defaultDatum, tree.FmtParsable))
	return stmt, nil
}

func (og *operationGenerator) setColumnNotNull(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN IrrelevantColumnName SET NOT NULL`, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnName, err := og.randColumn(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}
	columnExists, err := og.columnExistsOnTable(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	constraintBeingAdded, err := og.columnNotNullConstraintInMutation(ctx, tx, tableName, columnName)
	if err != nil {
		return nil, err
	}
	stmt := makeOpStmt(OpStmtDDL)
	if constraintBeingAdded {
		stmt.expectedExecErrors.add(pgcode.ObjectNotInPrerequisiteState)
	}

	if !columnExists {
		stmt.expectedExecErrors.add(pgcode.UndefinedColumn)
	} else {
		// If the column has null values, then a check violation will occur upon committing.
		colContainsNull, err := og.columnContainsNull(ctx, tx, tableName, columnName)
		if err != nil {
			return nil, err
		}
		if colContainsNull {
			og.candidateExpectedCommitErrors.add(pgcode.CheckViolation)
		}
	}

	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	stmt.sql = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET NOT NULL`, tableName, columnName)
	return stmt, nil
}

func (og *operationGenerator) setColumnType(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, err
	}

	const setSessionVariableString = `SET enable_experimental_alter_column_type_general = true;`

	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`%s ALTER TABLE %s ALTER COLUMN IrrelevantColumnName SET DATA TYPE IrrelevantDataType`, setSessionVariableString, tableName),
			pgcode.UndefinedTable), nil
	}
	err = og.tableHasPrimaryKeySwapActive(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}

	columnForTypeChange, err := og.randColumnWithMeta(ctx, tx, *tableName, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	columnExists, err := og.columnExistsOnTable(ctx, tx, tableName, columnForTypeChange.name)
	if err != nil {
		return nil, err
	}
	if !columnExists {
		return makeOpStmtForSingleError(OpStmtDDL,
			fmt.Sprintf(`%s ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE IrrelevantTypeName`,
				setSessionVariableString, tableName, columnForTypeChange.name),
			pgcode.UndefinedColumn), nil
	}

	newTypeName, newType, err := og.randType(ctx, tx, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	columnHasDependencies, err := og.columnIsDependedOn(ctx, tx, tableName, columnForTypeChange.name)
	if err != nil {
		return nil, err
	}

	stmt := makeOpStmt(OpStmtDDL)
	if newType != nil {
		// Ignoring the error here intentionally, as we want to carry on with
		// the operation and not fail it prematurely.
		kind, _ := schemachange.ClassifyConversion(context.Background(), columnForTypeChange.typ, newType)
		codesWithConditions{
			{code: pgcode.CannotCoerce, condition: kind == schemachange.ColumnConversionImpossible},
			{code: pgcode.FeatureNotSupported, condition: kind != schemachange.ColumnConversionTrivial},
		}.add(stmt.expectedExecErrors)
	}

	codesWithConditions{
		{code: pgcode.UndefinedObject, condition: newType == nil},
		{code: pgcode.DependentObjectsStillExist, condition: columnHasDependencies},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`%s ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE %s`,
		setSessionVariableString, tableName, columnForTypeChange.name, newTypeName.SQLString())
	return stmt, nil
}

func (og *operationGenerator) survive(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	dbRegions, err := og.getDatabaseRegionNames(ctx, tx)
	if err != nil {
		return nil, err
	}

	// Choose a survival mode based on a coin toss.
	needsAtLeastThreeRegions := false
	survive := "ZONE FAILURE"
	if coinToss := og.randIntn(2); coinToss == 1 {
		survive = "REGION FAILURE"
		needsAtLeastThreeRegions = true
	}

	// Expect 0 regions to fail, and less than three regions to fail
	// if there are < 3 regions.
	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{
			code:      pgcode.InvalidName,
			condition: len(dbRegions) == 0,
		},
		{
			code:      pgcode.InvalidParameterValue,
			condition: needsAtLeastThreeRegions && len(dbRegions) < 3,
		},
	}.add(stmt.expectedExecErrors)

	dbName, err := og.getDatabase(ctx, tx)
	if err != nil {
		return nil, err
	}
	stmt.sql = fmt.Sprintf(`ALTER DATABASE %s SURVIVE %s`, dbName, survive)
	return stmt, nil
}

func (og *operationGenerator) insertRow(ctx context.Context, tx pgx.Tx) (stmt *opStmt, err error) {
	tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
	if err != nil {
		return nil, errors.Wrapf(err, "error getting random table name")
	}
	tableExists, err := og.tableExists(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	// If we aren't on 22.2 then disable the insert plugin, since 21.X
	// can have schema instrospection queries fail due to an optimizer bug.
	skipInserts, err := isClusterVersionLessThan(ctx, tx, clusterversion.ByKey(clusterversion.Start22_2))
	if err != nil {
		return nil, err
	}
	// If inserts are to be skipped, we will intentionally, target the insert towards
	// a non-existent table, so that they become no-ops.
	if skipInserts {
		tableExists = false
		tableName.SchemaName = "InvalidObjectName"
	}
	if !tableExists {
		return makeOpStmtForSingleError(OpStmtDML,
			fmt.Sprintf(
				`INSERT INTO %s (IrrelevantColumnName) VALUES ("IrrelevantValue")`,
				tableName,
			),
			pgcode.UndefinedTable), nil
	}
	allColumns, err := og.getTableColumns(ctx, tx, tableName.String(), false)
	nonGeneratedCols := allColumns
	if err != nil {
		return nil, errors.Wrapf(err, "error getting table columns for insert row")
	}

	// Filter out computed columns.
	{
		truncated := nonGeneratedCols[:0]
		for _, c := range nonGeneratedCols {
			if !c.generated {
				truncated = append(truncated, c)
			}
		}
		nonGeneratedCols = truncated
	}
	colNames := []string{}
	rows := [][]string{}
	for _, col := range nonGeneratedCols {
		colNames = append(colNames, col.name)
	}
	numRows := og.randIntn(3) + 1
	for i := 0; i < numRows; i++ {
		var row []string
		for _, col := range nonGeneratedCols {
			d := randgen.RandDatum(og.params.rng, col.typ, col.nullable)
			// Unfortunately, RandDatum for OIDs only selects random values, which will
			// always fail validation. So, for OIDs we will select a random known type
			// instead.
			if col.typ.Family() == types.Oid.Family() {
				d = tree.NewDOid(randgen.RandColumnType(og.params.rng).Oid())
			}
			str := tree.AsStringWithFlags(d, tree.FmtParsable)
			// For strings use the actual type, so that comparisons for NULL values are sane.
			if col.typ.Family() == types.StringFamily {
				str = strings.Replace(str, ":::STRING", fmt.Sprintf("::%s", col.typ.SQLString()), -1)
			}
			row = append(row, str)
		}

		rows = append(rows, row)
	}
	// Verify that none of the generated expressions will blow up on this insert.
	anyInvalidInserts := false
	stmt = makeOpStmt(OpStmtDML)
	for _, row := range rows {
		invalidInsert, generatedErrors, potentialErrors, err := og.validateGeneratedExpressionsForInsert(ctx, tx, tableName, colNames, allColumns, row)
		if err != nil {
			return nil, err
		}
		// We may have errors that are possible, but not guaranteed.
		potentialErrors.add(stmt.potentialExecErrors)
		if invalidInsert {
			generatedErrors.add(stmt.expectedExecErrors)
			// We will be pessimistic and assume that other column related errors can
			// be hit, since the function above fails only on generated columns. But,
			// there maybe index expressions with the exact same problem.
			stmt.expectedExecErrors.add(pgcode.NumericValueOutOfRange)
			stmt.expectedExecErrors.add(pgcode.FloatingPointException)
			stmt.expectedExecErrors.add(pgcode.NotNullViolation)
			anyInvalidInserts = true
		}
	}

	// Only evaluate these if we know that the inserted values are sane, since
	// we will need to evaluate generated expressions below.
	uniqueConstraintViolation := false
	fkViolation := false
	if !anyInvalidInserts {
		// Verify if the new row will violate unique constraints by checking the constraints and
		// existing rows in the database.
		var generatedErrors codesWithConditions
		uniqueConstraintViolation, generatedErrors, err = og.valuesViolateUniqueConstraints(ctx, tx, tableName, colNames, allColumns, rows)
		if err != nil {
			return nil, err
		}
		if !uniqueConstraintViolation {
			generatedErrors.add(stmt.expectedExecErrors)
		}
		// Verify if the new row will violate fk constraints by checking the constraints and rows
		// in the database.
		fkConstraintsEnabled, err := isFkConstraintsEnabled(ctx, tx)
		if err != nil {
			return nil, err
		}
		if fkConstraintsEnabled {
			fkViolation, err = og.violatesFkConstraints(ctx, tx, tableName, colNames, rows)
		}
		if err != nil {
			return nil, err
		}
	}

	codesWithConditions{
		{code: pgcode.UniqueViolation, condition: uniqueConstraintViolation},
	}.add(stmt.expectedExecErrors)
	codesWithConditions{
		{code: pgcode.ForeignKeyViolation, condition: fkViolation},
	}.add(stmt.potentialExecErrors)
	codesWithConditions{
		{code: pgcode.ForeignKeyViolation, condition: fkViolation},
	}.add(og.expectedCommitErrors)

	var formattedRows []string
	for _, row := range rows {
		formattedRows = append(formattedRows, fmt.Sprintf("(%s)", strings.Join(row, ",")))
	}

	stmt.sql = fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES %s`,
		tableName,
		strings.Join(colNames, ","),
		strings.Join(formattedRows, ","),
	)
	return stmt, nil
}

type opStmtType int

const (
	// OpStmtDDL statement is a data definition language statement.
	OpStmtDDL opStmtType = 1
	// OpStmtDML statement is a data manipulation language statement.
	OpStmtDML opStmtType = 2
)

type opStmtQueryResultCallback func(ctx context.Context, rows pgx.Rows) error

// opStmt a generated statement that is either DDL or DML, including the potential
// set of execution errors this statement can generate.
type opStmt struct {
	// sql the query being executed.
	sql string
	// queryType family of the query type being executed (DML or DDL).
	queryType opStmtType
	// expectedExecErrors expected set of execution errors.
	expectedExecErrors errorCodeSet
	// potentialExecErrors errors that could be potentially seen on execution.
	potentialExecErrors errorCodeSet
	queryResultCallback opStmtQueryResultCallback
}

// String implements Stringer
func (s *opStmt) String() string {
	return fmt.Sprintf("QUERY: %s, Expected Errors: %s, Potential Errors: %s",
		s.sql,
		s.expectedExecErrors,
		s.potentialExecErrors)
}

// makeOpStmtForSingleError constructs a statement that will only produce
// an error.
func makeOpStmtForSingleError(queryType opStmtType, sql string, codes ...pgcode.Code) *opStmt {
	s := makeOpStmt(queryType)
	s.sql = sql
	for _, code := range codes {
		s.expectedExecErrors.add(code)
	}
	return s
}

// makeOpStmt constructs an empty operation of a given type.
func makeOpStmt(queryType opStmtType) *opStmt {
	return &opStmt{
		queryType:           queryType,
		expectedExecErrors:  makeExpectedErrorSet(),
		potentialExecErrors: makeExpectedErrorSet(),
	}
}

// getErrorState dumps the object state when an error is hit
func (og *operationGenerator) getErrorState(op *opStmt) string {
	return fmt.Sprintf("Dumping state before death:\n"+
		"Expected errors: %s\n"+
		"Potential errors: %s\n"+
		"Expected commit errors: %s\n"+
		"Potential commit errors: %s\n"+
		"===========================\n"+
		"Executed queries for generating errors: %s\n"+
		"===========================\n"+
		"Previous statements %s\n",
		op.expectedExecErrors,
		op.potentialExecErrors,
		og.expectedCommitErrors.String(),
		og.potentialCommitErrors.String(),
		og.GetOpGenLog(),
		og.stmtsInTxt)
}

// executeStmt executes the given operation statement, and validates the result
// of the execution. Note: Commit time failures will be handled separately from
// statement specific logic.
func (s *opStmt) executeStmt(ctx context.Context, tx pgx.Tx, og *operationGenerator) error {
	var err error
	var rows pgx.Rows
	// Statement doesn't produce any result set that needs to be validated.
	if s.queryResultCallback == nil {
		_, err = tx.Exec(ctx, s.sql)
	} else {
		rows, err = tx.Query(ctx, s.sql)
	}
	if err != nil {
		// If the error not an instance of pgconn.PgError, then it is unexpected.
		pgErr := new(pgconn.PgError)
		if !errors.As(err, &pgErr) {
			return errors.Mark(
				errors.Wrapf(err, "***UNEXPECTED ERROR; Received a non pg error.\n %s",
					og.getErrorState(s)),
				errRunInTxnFatalSentinel,
			)
		}
		if pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
			return err
		}
		if !s.expectedExecErrors.contains(pgcode.MakeCode(pgErr.Code)) &&
			!s.potentialExecErrors.contains(pgcode.MakeCode(pgErr.Code)) {
			return errors.Mark(
				errors.Wrapf(err, "***UNEXPECTED ERROR; Received an unexpected execution error.\n %s",
					og.getErrorState(s)),
				errRunInTxnFatalSentinel,
			)
		}
		return errors.Mark(
			errors.Wrapf(err, "ROLLBACK; Successfully got expected execution error.\n %s",
				og.getErrorState(s)),
			errRunInTxnRbkSentinel,
		)
	}
	if !s.expectedExecErrors.empty() {
		return errors.Mark(
			errors.Newf("***FAIL; Failed to receive an execution error when errors were expected. %s",
				og.getErrorState(s)),
			errRunInTxnFatalSentinel,
		)
	}
	// Next validate the result set.
	if s.queryResultCallback != nil {
		if err := s.queryResultCallback(ctx, rows); err != nil {
			return err
		}
	}
	return nil
}

func (og *operationGenerator) validate(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	// Finish validation off by validating multi region zone configs are as expected.
	// Configs can be invalid if a user decides to override a multi-region field, but
	// this is not performed by the schemachange workload.
	validateStmt := makeOpStmt(OpStmtDML)
	validateStmt.sql = "SELECT 'validating all objects', crdb_internal.validate_multi_region_zone_configs()"
	rows, err := tx.Query(ctx, `SELECT * FROM "".crdb_internal.invalid_objects ORDER BY id`)
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
		return nil, errors.Wrap(rows.Err(), "querying for validation errors failed")
	}

	if len(errs) == 0 {
		return validateStmt, nil
	}
	return validateStmt, errors.Errorf("Validation FAIL:\n%s", strings.Join(errs, "\n"))
}

type column struct {
	name                string
	typ                 *types.T
	nullable            bool
	generated           bool
	generatedExpression string
}

func (og *operationGenerator) getTableColumns(
	ctx context.Context, tx pgx.Tx, tableName string, shuffle bool,
) ([]column, error) {
	q := fmt.Sprintf(`
    WITH tab_json AS (
                    SELECT crdb_internal.pb_to_json(
                            'desc',
                            descriptor
                           )->'table' AS t
                      FROM system.descriptor
                     WHERE id = $1::REGCLASS
                  ),
         columns_json AS (
                        SELECT json_array_elements(t->'columns') AS c FROM tab_json
                      ),
         columns AS (
                  SELECT c->>'computeExpr' AS generation_expression,
                         c->>'name' AS column_name
                    FROM columns_json
                 )
  SELECT show_columns.column_name,
         show_columns.data_type,
         show_columns.is_nullable,
         columns.generation_expression IS NOT NULL AS is_generated,
         COALESCE(columns.generation_expression, '') as generated_expression
    FROM [SHOW COLUMNS FROM %s] AS show_columns, columns
   WHERE show_columns.column_name != 'rowid'
         AND show_columns.column_name = columns.column_name
`, tableName)
	rows, err := tx.Query(ctx, q, tableName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting table columns from %s", tableName)
	}
	defer rows.Close()
	var typNames []string
	var ret []column
	for rows.Next() {
		var c column
		var typName string
		err := rows.Scan(&c.name, &typName, &c.nullable, &c.generated, &c.generatedExpression)
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
		c.typ, err = og.typeFromTypeName(ctx, tx, typNames[i])
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
	ctx context.Context, tx pgx.Tx, tableName tree.TableName, pctExisting int,
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
	if err := tx.QueryRow(ctx, q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

// randColumnWithMeta is implemented in the same way as randColumn with the exception that
// it will return a column struct, which includes type and nullability information, instead of
// a column name string.
func (og *operationGenerator) randColumnWithMeta(
	ctx context.Context, tx pgx.Tx, tableName tree.TableName, pctExisting int,
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
 SELECT 
column_name, 
data_type, 
is_nullable, 
generation_expression != '' AS is_generated
   FROM [SHOW COLUMNS FROM %s]
  WHERE column_name != 'rowid'
ORDER BY random()
  LIMIT 1;
`, tableName.String())
	var col column
	var typ string
	if err := tx.QueryRow(ctx, q).Scan(&col.name, &typ, &col.nullable, &col.generated); err != nil {
		return column{}, errors.Wrapf(err, "randColumnWithMeta: %q", q)
	}

	var err error
	col.typ, err = og.typeFromTypeName(ctx, tx, typ)
	if err != nil {
		return column{}, err
	}

	return col, nil
}

// randChildColumnForFkRelation gets a column to use as the child column in a foreign key relation.
// To successfully use a column as the child, the column must have the same type as the parent and must not be computed.
func (og *operationGenerator) randChildColumnForFkRelation(
	ctx context.Context, tx pgx.Tx, isNotComputed bool, typ string,
) (*tree.TableName, *column, error) {

	query := strings.Builder{}
	query.WriteString(`
    SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable
      FROM information_schema.columns
		 WHERE table_name ~ 'table[0-9]+' AND column_name <> 'rowid'
  `)
	query.WriteString(fmt.Sprintf(`
			AND crdb_sql_type = '%s'
	`, typ))

	if isNotComputed {
		query.WriteString(`AND is_generated = 'NEVER'`)
	} else {
		query.WriteString(`AND is_generated = 'ALWAYS'`)
	}

	var tableSchema string
	var tableName string
	var columnName string
	var typName string
	var nullable string

	err := tx.QueryRow(ctx, query.String()).Scan(&tableSchema, &tableName, &columnName, &typName, &nullable)
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

	columnToReturn.typ, err = og.typeFromTypeName(ctx, tx, typName)
	if err != nil {
		return nil, nil, err
	}

	return &table, &columnToReturn, nil
}

// randParentColumnForFkRelation fetches a column and table to use as the parent in a single-column foreign key relation.
// To successfully use a column as the parent, the column must be unique and must not be generated.
func (og *operationGenerator) randParentColumnForFkRelation(
	ctx context.Context, tx pgx.Tx, unique bool,
) (*tree.TableName, *column, error) {

	subQuery := strings.Builder{}
	subQuery.WriteString(`
		SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable, contype, conkey
      FROM (
        SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable, ordinal_position,
               concat(table_schema, '.', table_name)::REGCLASS::INT8 AS tableid
          FROM information_schema.columns
					WHERE column_name <> 'rowid'
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

	for {
		nestedTxn, err := tx.Begin(ctx)
		if err != nil {
			return nil, nil, err
		}
		err = nestedTxn.QueryRow(ctx, fmt.Sprintf(`
	SELECT table_schema, table_name, column_name, crdb_sql_type, is_nullable FROM (
		%s
	)`, subQuery.String())).Scan(&tableSchema, &tableName, &columnName, &typName, &nullable)
		if err == nil {
			err := nestedTxn.Commit(ctx)
			if err != nil {
				return nil, nil, err
			}
			break
		}
		pgErr := (*pgconn.PgError)(nil)
		if !errors.As(err, &pgErr) {
			return nil, nil, err
		}
		// Intermittent undefined table errors are valid for the query above, since
		// we are not leasing out the tables, when picking our random table.
		if code := pgcode.MakeCode(pgErr.Code); code == pgcode.UndefinedTable ||
			code == pgcode.UndefinedSchema {
			err := nestedTxn.Rollback(ctx)
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		if rbErr := nestedTxn.Rollback(ctx); rbErr != nil {
			err = errors.CombineErrors(err, rbErr)
		}
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

	var err error
	columnToReturn.typ, err = og.typeFromTypeName(ctx, tx, typName)
	if err != nil {
		return nil, nil, err
	}

	return &table, &columnToReturn, nil
}

func (og *operationGenerator) randConstraint(
	ctx context.Context, tx pgx.Tx, tableName string,
) (string, error) {
	q := fmt.Sprintf(`
  SELECT constraint_name
    FROM [SHOW CONSTRAINTS FROM %s]
ORDER BY random()
   LIMIT 1;
`, tableName)
	var name string
	err := tx.QueryRow(ctx, q).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (og *operationGenerator) randIndex(
	ctx context.Context, tx pgx.Tx, tableName tree.TableName, pctExisting int,
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
	if err := tx.QueryRow(ctx, q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

// randSequence returns a sequence qualified by a schema
func (og *operationGenerator) randSequence(
	ctx context.Context, tx pgx.Tx, pctExisting int, desiredSchema string,
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
		if err := tx.QueryRow(ctx, q).Scan(&seqName); err != nil {
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
		randSchema, err := og.randSchema(ctx, tx, og.pctExisting(true))
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
	if err := tx.QueryRow(ctx, q).Scan(&schemaName, &seqName); err != nil {
		treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
		return &treeTableName, err
	}

	treeSeqName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
		SchemaName:     tree.Name(schemaName),
		ExplicitSchema: true,
	}, tree.Name(seqName))
	return &treeSeqName, nil

}

func (og *operationGenerator) randEnum(
	ctx context.Context, tx pgx.Tx, pctExisting int,
) (name *tree.TypeName, exists bool, _ error) {
	if og.randIntn(100) >= pctExisting {
		// Most of the time, this case is for creating enums, so it
		// is preferable that the schema exists
		randSchema, err := og.randSchema(ctx, tx, og.pctExisting(true))
		if err != nil {
			return nil, false, err
		}
		typeName := tree.MakeSchemaQualifiedTypeName(randSchema, fmt.Sprintf("enum%d", og.newUniqueSeqNum()))
		return &typeName, false, nil
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
	if err := tx.QueryRow(ctx, q).Scan(&schemaName, &typName); err != nil {
		return nil, false, err
	}
	typeName := tree.MakeSchemaQualifiedTypeName(schemaName, typName)
	return &typeName, true, nil
}

// randTable returns a schema name along with a table name
func (og *operationGenerator) randTable(
	ctx context.Context, tx pgx.Tx, pctExisting int, desiredSchema string,
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
		if err := tx.QueryRow(ctx, q).Scan(&tableName); err != nil {
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
		randSchema, err := og.randSchema(ctx, tx, og.pctExisting(true))
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
	if err := tx.QueryRow(ctx, q).Scan(&schemaName, &tableName); err != nil {
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
	ctx context.Context, tx pgx.Tx, pctExisting int, desiredSchema string,
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
		  SELECT table_name
		    FROM [SHOW TABLES]
		   WHERE table_name LIKE 'view%%'
				 AND schema_name = '%s'
		ORDER BY random()
		   LIMIT 1;
		`, desiredSchema)

		var viewName string
		if err := tx.QueryRow(ctx, q).Scan(&viewName); err != nil {
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
		randSchema, err := og.randSchema(ctx, tx, og.pctExisting(true))
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
	if err := tx.QueryRow(ctx, q).Scan(&schemaName, &viewName); err != nil {
		treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
		return &treeViewName, err
	}
	treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
		SchemaName:     tree.Name(schemaName),
		ExplicitSchema: true,
	}, tree.Name(viewName))
	return &treeViewName, nil
}

func (og *operationGenerator) tableColumnsShuffled(
	ctx context.Context, tx pgx.Tx, tableName string,
) ([]string, error) {
	q := fmt.Sprintf(`
SELECT column_name
FROM [SHOW COLUMNS FROM %s];
`, tableName)

	rows, err := tx.Query(ctx, q)
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
	ctx context.Context, tx pgx.Tx, enumPctExisting int,
) (*tree.TypeName, *types.T, error) {
	if og.randIntn(100) <= og.params.enumPct {
		// TODO(ajwerner): Support arrays of enums.
		typName, exists, err := og.randEnum(ctx, tx, enumPctExisting)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			return typName, nil, nil
		}
		typ, err := og.typeFromTypeName(ctx, tx, typName.String())
		if err != nil {
			return nil, nil, err
		}
		return typName, typ, nil
	}
	typ := randgen.RandSortingType(og.params.rng)
	typeName := tree.MakeUnqualifiedTypeName(typ.SQLString())
	return &typeName, typ, nil
}

func (og *operationGenerator) createSchema(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	schemaName, err := og.randSchema(ctx, tx, og.pctExisting(false))
	if err != nil {
		return nil, err
	}
	ifNotExists := og.randIntn(2) == 0

	schemaExists, err := og.schemaExists(ctx, tx, schemaName)
	if err != nil {
		return nil, err
	}
	opStmt := makeOpStmt(OpStmtDDL)
	if schemaExists && !ifNotExists {
		opStmt.expectedExecErrors.add(pgcode.DuplicateSchema)
	}

	// TODO(jayshrivastava): Support authorization
	stmt := randgen.MakeSchemaName(ifNotExists, schemaName, tree.MakeRoleSpecWithRoleName(username.RootUserName().Normalized()))
	opStmt.sql = tree.Serialize(stmt)
	return opStmt, nil
}

func (og *operationGenerator) randSchema(
	ctx context.Context, tx pgx.Tx, pctExisting int,
) (string, error) {
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
	if err := tx.QueryRow(ctx, q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

func (og *operationGenerator) dropSchema(ctx context.Context, tx pgx.Tx) (*opStmt, error) {
	schemaName, err := og.randSchema(ctx, tx, og.pctExisting(true))
	if err != nil {
		return nil, err
	}

	schemaExists, err := og.schemaExists(ctx, tx, schemaName)
	if err != nil {
		return nil, err
	}
	crossReferences, err := og.schemaContainsTypesWithCrossSchemaReferences(ctx, tx, schemaName)
	if err != nil {
		return nil, err
	}
	stmt := makeOpStmt(OpStmtDDL)
	codesWithConditions{
		{pgcode.UndefinedSchema, !schemaExists},
		{pgcode.InvalidSchemaName, schemaName == tree.PublicSchema},
		{pgcode.FeatureNotSupported, crossReferences},
	}.add(stmt.expectedExecErrors)

	stmt.sql = fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schemaName)
	return stmt, nil
}

func (og *operationGenerator) selectStmt(ctx context.Context, tx pgx.Tx) (stmt *opStmt, err error) {
	const maxTablesForSelect = 3
	const maxColumnsForSelect = 16
	const maxRowsToConsume = 1
	// Select the number of target tables.
	numTables := og.randIntn(maxTablesForSelect) + 1
	tableNames := make([]*tree.TableName, numTables)
	colInfos := make([][]column, numTables)
	allTableExists := true
	totalColumns := 0
	for idx := range tableNames {
		tableName, err := og.randTable(ctx, tx, og.pctExisting(true), "")
		if err != nil {
			return nil, errors.Wrapf(err, "error getting random table name")
		}
		tableExists, err := og.tableExists(ctx, tx, tableName)
		if err != nil {
			return nil, err
		}
		tableNames[idx] = tableName
		if !tableExists {
			allTableExists = false
			continue
		}
		colInfo, err := og.getTableColumns(ctx, tx, tableName.String(), false)
		if err != nil {
			return nil, err
		}
		colInfos[idx] = colInfo
		totalColumns += len(colInfo)
	}
	// Determine which columns to select.
	selectColumns := strings.Builder{}
	numColumnsToSelect := og.randIntn(maxColumnsForSelect) + 1
	if numColumnsToSelect > totalColumns {
		numColumnsToSelect = totalColumns
	}
	// Randomly select our columns from the set of tables.
	for colIdx := 0; colIdx < numColumnsToSelect; colIdx++ {
		tableIdx := og.randIntn(len(colInfos))
		// Skip over empty tables.
		if len(colInfos[tableIdx]) == 0 {
			colIdx--
			continue
		}
		col := colInfos[tableIdx][og.randIntn(len(colInfos[tableIdx]))]
		if colIdx != 0 {
			selectColumns.WriteString(",")
		}
		selectColumns.WriteString(fmt.Sprintf("t%d.", tableIdx))
		selectColumns.WriteString(col.name)
		selectColumns.WriteString(" AS ")
		selectColumns.WriteString(fmt.Sprintf("col%d", colIdx))
	}
	// No columns, so anything goes
	if totalColumns == 0 {
		selectColumns.WriteString("*")
	}

	// TODO(fqazi): Start injecting WHERE clauses, joins, and aggregations too
	selectQuery := strings.Builder{}
	selectQuery.WriteString("SELECT ")
	selectQuery.WriteString(selectColumns.String())
	selectQuery.WriteString(" FROM ")
	for idx, tableName := range tableNames {
		if idx != 0 {
			selectQuery.WriteString(",")
		}
		selectQuery.WriteString(tableName.String())
		selectQuery.WriteString(" AS ")
		selectQuery.WriteString(fmt.Sprintf("t%d ", idx))
	}
	if maxRowsToConsume > 0 {
		selectQuery.WriteString(fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", maxRowsToConsume))
	}
	// Setup a statement with the query and a call back to validate the result
	// set.
	stmt = makeOpStmt(OpStmtDML)
	stmt.sql = selectQuery.String()
	stmt.queryResultCallback = func(ctx context.Context, rows pgx.Rows) error {
		// Only read rows from the select for up to a minute.
		const MaxTimeForRead = time.Minute
		startTime := timeutil.Now()
		defer rows.Close()
		for rows.Next() && timeutil.Since(startTime) < MaxTimeForRead {
			// Detect if the context is cancelled while processing
			// the result set.
			if err = ctx.Err(); err != nil {
				return err
			}
			rawValues := rows.RawValues()
			if len(rawValues) != numColumnsToSelect {
				return errors.AssertionFailedf("query returned incorrect number of columns. "+
					"Got: %d Expected:%d",
					len(rawValues),
					totalColumns)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	}
	codesWithConditions{
		{code: pgcode.UndefinedTable, condition: !allTableExists},
	}.add(stmt.expectedExecErrors)
	// TODO(fqazi): Temporarily allow out of memory errors on select queries. Not
	// sure where we are hitting these, need to investigate further.
	stmt.potentialExecErrors.add(pgcode.OutOfMemory)
	return stmt, nil
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

func (og operationGenerator) alwaysExisting() int {
	return 100
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
func (og *operationGenerator) typeFromTypeName(
	ctx context.Context, tx pgx.Tx, typeName string,
) (*types.T, error) {
	stmt, err := parser.ParseOne(fmt.Sprintf("SELECT 'placeholder'::%s", typeName))
	if err != nil {
		return nil, errors.Wrapf(err, "typeFromTypeName: %s", typeName)
	}
	typ, err := tree.ResolveType(
		context.Background(),
		stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.CastExpr).Type,
		&txTypeResolver{tx: tx},
	)
	if err != nil {
		return nil, errors.Wrapf(err, "ResolveType: %v", typeName)
	}
	return typ, nil
}

// Check if the test is running with a mixed version cluster, with a version
// less than or equal to the target version number. This can be used to detect
// in mixed version environments if certain errors should be encountered.
func isClusterVersionLessThan(
	ctx context.Context, tx pgx.Tx, targetVersion roachpb.Version,
) (bool, error) {
	var clusterVersionStr string
	row := tx.QueryRow(ctx, `SHOW CLUSTER SETTING version`)
	if err := row.Scan(&clusterVersionStr); err != nil {
		return false, err
	}
	clusterVersion, err := roachpb.ParseVersion(clusterVersionStr)
	if err != nil {
		return false, err
	}
	return clusterVersion.LessEq(targetVersion), nil
}

// isFkConstraintsEnabled detects if server side builtins for validating
// foreign key constraints are available.
func isFkConstraintsEnabled(ctx context.Context, tx pgx.Tx) (bool, error) {
	fkConstraintDisabledVersion, err := isClusterVersionLessThan(ctx,
		tx,
		clusterversion.ByKey(clusterversion.Start22_2))
	return !fkConstraintDisabledVersion, err
}
