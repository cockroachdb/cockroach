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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/lib/pq/oid"
	"github.com/spf13/pflag"
)

// This workload executes batches of schema changes asynchronously. Each
// batch is executed in a separate transaction and transactions run in
// parallel. Batches are drawn from a pre-defined distribution.
// Currently all schema change ops are equally likely to be chosen. This
// includes table creation but note that the tables contain no data.
//
// Example usage:
// `bin/workload run schemachange --init --concurrency=2 --verbose=0 --max-ops-per-worker=1000`
// will execute up to 1000 schema change operations per txn in two concurrent txns.
//
// TODO(peter): This is still work in progress, we need to
// - support more than 1 database
// - reference sequences in column defaults
// - create foreign keys
// - support `ADD CONSTRAINT`
// - support `SET COLUMN DEFAULT`
//
// TODO(spaskob): introspect errors returned from the workload and determine
// whether they're expected or unexpected. Flag `tolerate-errors` should be
// added to tolerate unexpected errors and then unexpected errors should fail
// the workload.
//
//For example, an attempt to do something we don't support should be swallowed (though if we can detect that maybe we should just not do it, e.g). It will be hard to use this test for anything more than liveness detection until we go through the tedious process of classifying errors.:

const (
	defaultMaxOpsPerWorker = 5
	defaultExistingPct     = 10
	defaultEnumPct         = 10
)

type schemaChange struct {
	flags           workload.Flags
	dbOverride      string
	concurrency     int
	maxOpsPerWorker int
	existingPct     int
	enumPct         int
	verbose         int
	dryRun          bool
}

var schemaChangeMeta = workload.Meta{
	Name:        `schemachange`,
	Description: `schemachange randomly generates concurrent schema changes`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		s := &schemaChange{}
		s.flags.FlagSet = pflag.NewFlagSet(`schemachange`, pflag.ContinueOnError)
		s.flags.StringVar(&s.dbOverride, `db`, ``,
			`Override for the SQL database to use. If empty, defaults to the generator name`)
		s.flags.IntVar(&s.concurrency, `concurrency`, 2*runtime.NumCPU(), /* TODO(spaskob): sensible default? */
			`Number of concurrent workers`)
		s.flags.IntVar(&s.maxOpsPerWorker, `max-ops-per-worker`, defaultMaxOpsPerWorker,
			`Number of operations to execute in a single transaction`)
		s.flags.IntVar(&s.existingPct, `existing-pct`, defaultExistingPct,
			`Percentage of times to use existing name`)
		s.flags.IntVar(&s.enumPct, `enum-pct`, defaultEnumPct,
			`Percentage of times when picking a type that an enum type is picked`)
		s.flags.IntVarP(&s.verbose, `verbose`, `v`, 0, ``)
		s.flags.BoolVarP(&s.dryRun, `dry-run`, `n`, false, ``)
		return s
	},
}

func init() {
	workload.Register(schemaChangeMeta)
}

//go:generate stringer -type=opType
type opType int

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
)

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

// Meta implements the workload.Generator interface.
func (s *schemaChange) Meta() workload.Meta {
	return schemaChangeMeta
}

// Flags implements the workload.Flagser interface.
func (s *schemaChange) Flags() workload.Flags {
	return s.flags
}

// Tables implements the workload.Generator interface.
func (s *schemaChange) Tables() []workload.Table {
	return nil
}

// Tables implements the workload.Opser interface.
func (s *schemaChange) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(s, s.dbOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	cfg := workload.MultiConnPoolCfg{
		MaxTotalConnections: s.concurrency * 2, //TODO(spaskob): pick a sensible default.
	}
	pool, err := workload.NewMultiConnPool(cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	seqNum, err := s.initSeqNum(pool)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ops := newDeck(rand.New(rand.NewSource(timeutil.Now().UnixNano())), opWeights...)
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < s.concurrency; i++ {
		w := &schemaChangeWorker{
			verbose:         s.verbose,
			dryRun:          s.dryRun,
			maxOpsPerWorker: s.maxOpsPerWorker,
			existingPct:     s.existingPct,
			enumPct:         s.enumPct,
			rng:             rand.New(rand.NewSource(timeutil.Now().UnixNano())),
			ops:             ops,
			pool:            pool,
			hists:           reg.GetHandle(),
			seqNum:          seqNum,
		}
		ql.WorkerFns = append(ql.WorkerFns, w.run)
	}
	return ql, nil
}

// initSeqName returns the smallest available sequence number to be
// used to generate new unique names. Note that this assumes that no
// other workload is being run at the same time.
// TODO(spaskob): Do we need to protect from workloads running concurrently.
// It's not obvious how the workloads will behave when accessing the same
// cluster.
func (s *schemaChange) initSeqNum(pool *workload.MultiConnPool) (*int64, error) {
	seqNum := new(int64)

	const q = `
SELECT max(regexp_extract(name, '[0-9]+$')::int)
  FROM ((SELECT table_name FROM [SHOW TABLES]) UNION (SELECT sequence_name FROM [SHOW SEQUENCES])) AS obj(name)
 WHERE name ~ '^(table|view|seq)[0-9]+$';
`
	var max gosql.NullInt64
	if err := pool.Get().QueryRow(q).Scan(&max); err != nil {
		return nil, err
	}
	if max.Valid {
		*seqNum = max.Int64 + 1
	}

	return seqNum, nil
}

type schemaChangeWorker struct {
	verbose         int
	dryRun          bool
	maxOpsPerWorker int
	existingPct     int
	enumPct         int
	rng             *rand.Rand
	ops             *deck
	pool            *workload.MultiConnPool
	hists           *histogram.Histograms
	seqNum          *int64
}

// handleOpError returns an error if the op error is considered serious and
// we should terminate the workload.
func handleOpError(err error) error {
	if err == nil {
		return nil
	}
	if pgErr := (pgx.PgError{}); errors.As(err, &pgErr) {
		sqlstate := pgErr.SQLState()
		class := sqlstate[0:2]
		switch class {
		case "09":
			return errors.Wrap(err, "Class 09 - Triggered Action Exception")
		case "XX":
			return errors.Wrap(err, "Class XX - Internal Error")
		}
	} else {
		return errors.Wrapf(err, "unexpected error %v", err)
	}
	return nil
}

var (
	errRunInTxnFatalSentinel = errors.New("fatal error when running txn")
	errRunInTxnRbkSentinel   = errors.New("txn needs to rollback")
)

func (w *schemaChangeWorker) runInTxn(tx *pgx.Tx, opsNum int) (string, error) {
	var log strings.Builder
	for i := 0; i < opsNum; i++ {
		op, noops, err := w.randOp(tx)
		if err != nil {
			return noops, errors.Mark(
				errors.Wrap(err, "could not generate a random operation"),
				errRunInTxnFatalSentinel,
			)
		}
		if w.verbose >= 2 {
			// Print the failed attempts to produce a random operation.
			log.WriteString(noops)
		}
		log.WriteString(fmt.Sprintf("  %s;\n", op))
		if !w.dryRun {
			histBin := "opOk"
			start := timeutil.Now()
			if _, err = tx.Exec(op); err != nil {
				histBin = "txnRbk"
				log.WriteString(fmt.Sprintf("***FAIL: %v\n", err))
				log.WriteString("ROLLBACK;\n")
				return log.String(), errors.Mark(err, errRunInTxnRbkSentinel)
			}
			elapsed := timeutil.Since(start)
			w.hists.Get(histBin).Record(elapsed)
		}
	}
	return log.String(), nil
}

func (w *schemaChangeWorker) run(_ context.Context) error {
	tx, err := w.pool.Get().Begin()
	if err != nil {
		return errors.Wrap(err, "cannot get a connection and begin a txn")
	}
	opsNum := 1 + w.rng.Intn(w.maxOpsPerWorker)

	// Run between 1 and maxOpsPerWorker schema change operations.
	start := timeutil.Now()
	logs, err := w.runInTxn(tx, opsNum)
	logs = "BEGIN\n" + logs
	defer func() {
		if w.verbose >= 1 {
			fmt.Print(logs)
		}
	}()

	if err != nil {
		// Rollback in all cases to release the txn object and its conn pool.
		if rbkErr := tx.Rollback(); rbkErr != nil {
			return errors.Wrapf(err, "Could not rollback %v", rbkErr)
		}
		switch {
		case errors.Is(err, errRunInTxnFatalSentinel):
			return err
		case errors.Is(err, errRunInTxnRbkSentinel):
			if seriousErr := handleOpError(err); seriousErr != nil {
				return seriousErr
			}
			return nil
		default:
			return errors.Wrapf(err, "Unexpected error")
		}
	}

	// If there were no errors commit the txn.
	histBin := "txnOk"
	cmtErrMsg := ""
	if err = tx.Commit(); err != nil {
		histBin = "txnCmtErr"
		cmtErrMsg = fmt.Sprintf("***FAIL: %v", err)
	}
	w.hists.Get(histBin).Record(timeutil.Since(start))
	logs = logs + fmt.Sprintf("COMMIT;  %s\n", cmtErrMsg)
	return nil
}

// randOp attempts to produce a random schema change operation. It returns a
// triple `(randOp, log, error)`. On success `randOp` is the random schema
// change constructed. Constructing a random schema change may require a few
// stochastic attempts and if verbosity is >= 2 the unsuccessful attempts are
// recorded in `log` to help with debugging of the workload.
func (w *schemaChangeWorker) randOp(tx *pgx.Tx) (string, string, error) {
	var log strings.Builder
	for {
		var stmt string
		var err error
		op := opType(w.ops.Int())
		switch op {
		case addColumn:
			stmt, err = w.addColumn(tx)

		case addConstraint:
			stmt, err = w.addConstraint(tx)

		case createIndex:
			stmt, err = w.createIndex(tx)

		case createSequence:
			stmt, err = w.createSequence(tx)

		case createTable:
			stmt, err = w.createTable(tx)

		case createTableAs:
			stmt, err = w.createTableAs(tx)

		case createView:
			stmt, err = w.createView(tx)

		case createEnum:
			stmt, err = w.createEnum(tx)

		case createSchema:
			stmt, err = w.createSchema(tx)

		case dropColumn:
			stmt, err = w.dropColumn(tx)

		case dropColumnDefault:
			stmt, err = w.dropColumnDefault(tx)

		case dropColumnNotNull:
			stmt, err = w.dropColumnNotNull(tx)

		case dropColumnStored:
			stmt, err = w.dropColumnStored(tx)

		case dropConstraint:
			stmt, err = w.dropConstraint(tx)

		case dropIndex:
			stmt, err = w.dropIndex(tx)

		case dropSequence:
			stmt, err = w.dropSequence(tx)

		case dropTable:
			stmt, err = w.dropTable(tx)

		case dropView:
			stmt, err = w.dropView(tx)

		case dropSchema:
			stmt, err = w.dropSchema(tx)

		case renameColumn:
			stmt, err = w.renameColumn(tx)

		case renameIndex:
			stmt, err = w.renameIndex(tx)

		case renameSequence:
			stmt, err = w.renameSequence(tx)

		case renameTable:
			stmt, err = w.renameTable(tx)

		case renameView:
			stmt, err = w.renameView(tx)

		case setColumnDefault:
			stmt, err = w.setColumnDefault(tx)

		case setColumnNotNull:
			stmt, err = w.setColumnNotNull(tx)

		case setColumnType:
			stmt, err = w.setColumnType(tx)

		case insertRow:
			stmt, err = w.insertRow(tx)

		case validate:
			stmt, err = w.validate(tx)
		}

		// TODO(spaskob): use more fine-grained error reporting.
		if stmt == "" || errors.Is(err, pgx.ErrNoRows) {
			log.WriteString(fmt.Sprintf("NOOP: %s -> %v\n", op, err))
			continue
		}
		return stmt, log.String(), err
	}
}

func (w *schemaChangeWorker) addColumn(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	columnName, err := w.randColumn(tx, tableName.String(), w.existingPct)
	if err != nil {
		return "", err
	}
	typ, err := w.randType(tx)
	if err != nil {
		return "", err
	}

	def := &tree.ColumnTableDef{
		Name: tree.Name(columnName),
		Type: typ,
	}
	def.Nullable.Nullability = tree.Nullability(rand.Intn(1 + int(tree.SilentNull)))
	return fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s`, tableName, tree.Serialize(def)), nil
}

func (w *schemaChangeWorker) addConstraint(tx *pgx.Tx) (string, error) {
	// TODO(peter): unimplemented
	// - Export sqlbase.randColumnTableDef.
	return "", nil
}

func (w *schemaChangeWorker) createIndex(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	columnNames, err := w.tableColumnsShuffled(tx, tableName.String())
	if err != nil {
		return "", err
	}

	indexName, err := w.randIndex(tx, tableName.String(), w.existingPct)
	if err != nil {
		return "", err
	}

	def := &tree.CreateIndex{
		Name:        tree.Name(indexName),
		Table:       *tableName,
		Unique:      w.rng.Intn(4) == 0,  // 25% UNIQUE
		Inverted:    w.rng.Intn(10) == 0, // 10% INVERTED
		IfNotExists: w.rng.Intn(2) == 0,  // 50% IF NOT EXISTS
		Columns:     make(tree.IndexElemList, 1+w.rng.Intn(len(columnNames))),
	}

	for i := range def.Columns {
		def.Columns[i].Column = tree.Name(columnNames[i])
		def.Columns[i].Direction = tree.Direction(w.rng.Intn(1 + int(tree.Descending)))
	}
	columnNames = columnNames[len(def.Columns):]

	if n := len(columnNames); n > 0 {
		def.Storing = make(tree.NameList, w.rng.Intn(1+n))
		for i := range def.Storing {
			def.Storing[i] = tree.Name(columnNames[i])
		}
	}

	return tree.Serialize(def), nil
}

func (w *schemaChangeWorker) createSequence(tx *pgx.Tx) (string, error) {
	return fmt.Sprintf(`CREATE SEQUENCE "seq%d"`, atomic.AddInt64(w.seqNum, 1)), nil
}

func (w *schemaChangeWorker) createTable(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 10)
	if err != nil {
		return "", err
	}

	stmt := rowenc.RandCreateTable(w.rng, "table", int(atomic.AddInt64(w.seqNum, 1)))
	stmt.Table = *tableName
	stmt.IfNotExists = w.rng.Intn(2) == 0
	return tree.Serialize(stmt), nil
}

func (w *schemaChangeWorker) createEnum(tx *pgx.Tx) (string, error) {
	typName, err := w.randEnum(tx, w.existingPct)
	if err != nil {
		return "", err
	}
	stmt := rowenc.RandCreateType(w.rng, typName.String(), "asdf")
	return tree.Serialize(stmt), nil
}

func (w *schemaChangeWorker) createTableAs(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	columnNames, err := w.tableColumnsShuffled(tx, tableName.String())
	if err != nil {
		return "", err
	}
	columnNames = columnNames[:1+w.rng.Intn(len(columnNames))]

	names := make(tree.NameList, len(columnNames))
	for i := range names {
		names[i] = tree.Name(columnNames[i])
	}

	destTableName, err := w.randTable(tx, 10)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`CREATE TABLE %s AS SELECT %s FROM %s`,
		destTableName, tree.Serialize(&names), tableName), nil
}

func (w *schemaChangeWorker) createView(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	columnNames, err := w.tableColumnsShuffled(tx, tableName.String())
	if err != nil {
		return "", err
	}
	columnNames = columnNames[:1+w.rng.Intn(len(columnNames))]

	names := make(tree.NameList, len(columnNames))
	for i := range names {
		names[i] = tree.Name(columnNames[i])
	}

	destViewName, err := w.randView(tx, w.existingPct)
	if err != nil {
		return "", err
	}

	// TODO(peter): Create views that are dependent on multiple tables.
	return fmt.Sprintf(`CREATE VIEW %s AS SELECT %s FROM %s`,
		destViewName, tree.Serialize(&names), tableName), nil
}

func (w *schemaChangeWorker) dropColumn(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	columnName, err := w.randColumn(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "%s"`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropColumnDefault(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP DEFAULT`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropColumnStored(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	columnName, err := w.randColumn(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP STORED`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropConstraint(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	constraintName, err := w.randConstraint(tx, tableName.String())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT "%s"`, tableName, constraintName), nil
}

func (w *schemaChangeWorker) dropIndex(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	indexName, err := w.randIndex(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP INDEX %s@"%s"`, tableName, indexName), nil
}

func (w *schemaChangeWorker) dropSequence(tx *pgx.Tx) (string, error) {
	sequenceName, err := w.randSequence(tx, 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP SEQUENCE "%s"`, sequenceName), nil
}

func (w *schemaChangeWorker) dropTable(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP TABLE %s`, tableName), nil
}

func (w *schemaChangeWorker) dropView(tx *pgx.Tx) (string, error) {
	viewName, err := w.randView(tx, 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP VIEW %s`, viewName), nil
}

func (w *schemaChangeWorker) renameColumn(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	srcColumnName, err := w.randColumn(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}

	destColumnName, err := w.randColumn(tx, tableName.String(), 50)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN "%s" TO "%s"`,
		tableName, srcColumnName, destColumnName), nil
}

func (w *schemaChangeWorker) renameIndex(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	srcIndexName, err := w.randIndex(tx, tableName.String(), w.existingPct)
	if err != nil {
		return "", err
	}

	destIndexName, err := w.randIndex(tx, tableName.String(), 50)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER TABLE %s RENAME CONSTRAINT "%s" TO "%s"`,
		tableName, srcIndexName, destIndexName), nil
}

func (w *schemaChangeWorker) renameSequence(tx *pgx.Tx) (string, error) {
	srcSequenceName, err := w.randSequence(tx, 100)
	if err != nil {
		return "", err
	}

	destSequenceName, err := w.randSequence(tx, 50)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER SEQUENCE "%s" RENAME TO "%s"`, srcSequenceName, destSequenceName), nil
}

func (w *schemaChangeWorker) renameTable(tx *pgx.Tx) (string, error) {
	srcTableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	destTableName, err := w.randTable(tx, 50)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, srcTableName, destTableName), nil
}

func (w *schemaChangeWorker) renameView(tx *pgx.Tx) (string, error) {
	srcViewName, err := w.randView(tx, 100)
	if err != nil {
		return "", err
	}

	destViewName, err := w.randView(tx, 50)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`ALTER VIEW %s RENAME TO %s`, srcViewName, destViewName), nil
}

func (w *schemaChangeWorker) setColumnDefault(tx *pgx.Tx) (string, error) {
	// TODO(peter): unimplemented
	return "", nil
}

func (w *schemaChangeWorker) setColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}

	columnName, err := w.randColumn(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET NOT NULL`, tableName, columnName), nil
}

func (w *schemaChangeWorker) setColumnType(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName.String(), 100)
	if err != nil {
		return "", err
	}
	typ, err := w.randType(tx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE %s`,
		tableName, columnName, typ), nil
}

func (w *schemaChangeWorker) insertRow(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx, 100)
	if err != nil {
		return "", errors.Wrapf(err, "error getting random table name")
	}
	cols, err := w.getTableColumns(tx, tableName.String())
	if err != nil {
		return "", errors.Wrapf(err, "error getting table columns for insert row")
	}
	colNames := []string{}
	rows := []string{}
	for _, col := range cols {
		colNames = append(colNames, fmt.Sprintf(`"%s"`, col.name))
	}
	numRows := w.rng.Intn(10) + 1
	for i := 0; i < numRows; i++ {
		var row []string
		for _, col := range cols {
			d := rowenc.RandDatum(w.rng, col.typ, col.nullable)
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

func (w *schemaChangeWorker) validate(tx *pgx.Tx) (string, error) {
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

func (w *schemaChangeWorker) getTableColumns(tx *pgx.Tx, tableName string) ([]column, error) {
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

func (w *schemaChangeWorker) randColumn(
	tx *pgx.Tx, tableName string, pctExisting int,
) (string, error) {
	if w.rng.Intn(100) >= pctExisting {
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		return fmt.Sprintf("col%s_%d",
			strings.TrimPrefix(tableName, "table"), atomic.AddInt64(w.seqNum, 1)), nil
	}
	q := fmt.Sprintf(`
  SELECT column_name
    FROM [SHOW COLUMNS FROM %s]
ORDER BY random()
   LIMIT 1;
`, tableName)
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

func (w *schemaChangeWorker) randConstraint(tx *pgx.Tx, tableName string) (string, error) {
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

func (w *schemaChangeWorker) randIndex(
	tx *pgx.Tx, tableName string, pctExisting int,
) (string, error) {
	if w.rng.Intn(100) >= pctExisting {
		// We make a unique name for all indices by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		return fmt.Sprintf("index%s_%d",
			strings.TrimPrefix(tableName, "table"), atomic.AddInt64(w.seqNum, 1)), nil
	}
	q := fmt.Sprintf(`
  SELECT index_name
    FROM [SHOW INDEXES FROM %s]
ORDER BY random()
   LIMIT 1;
`, tableName)
	var name string
	if err := tx.QueryRow(q).Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

func (w *schemaChangeWorker) randSequence(tx *pgx.Tx, pctExisting int) (string, error) {
	if w.rng.Intn(100) >= pctExisting {
		return fmt.Sprintf(`seq%d`, atomic.AddInt64(w.seqNum, 1)), nil
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

func (w *schemaChangeWorker) randEnum(tx *pgx.Tx, pctExisting int) (tree.UnresolvedName, error) {
	if w.rng.Intn(100) >= pctExisting {
		randSchema, err := w.randSchema(tx, 100-pctExisting)
		if err != nil {
			return tree.MakeUnresolvedName(), err
		}
		return tree.MakeUnresolvedName(randSchema, fmt.Sprintf("enum%d", atomic.AddInt64(w.seqNum, 1))), nil
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
func (w *schemaChangeWorker) randTable(tx *pgx.Tx, pctExisting int) (*tree.TableName, error) {
	if w.rng.Intn(100) >= pctExisting {
		randSchema, err := w.randSchema(tx, 100-pctExisting)

		if err != nil {
			treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeTableName, err
		}

		treeTableName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(randSchema),
			ExplicitSchema: true,
		}, tree.Name(fmt.Sprintf("table%d", atomic.AddInt64(w.seqNum, 1))))
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

func (w *schemaChangeWorker) randView(tx *pgx.Tx, pctExisting int) (*tree.TableName, error) {
	if w.rng.Intn(100) >= pctExisting {
		randSchema, err := w.randSchema(tx, 100-pctExisting)
		if err != nil {
			treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{}, "")
			return &treeViewName, err
		}
		treeViewName := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{
			SchemaName:     tree.Name(randSchema),
			ExplicitSchema: true,
		}, tree.Name(fmt.Sprintf("view%d", atomic.AddInt64(w.seqNum, 1))))
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

func (w *schemaChangeWorker) tableColumnsShuffled(tx *pgx.Tx, tableName string) ([]string, error) {
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
		columnNames = append(columnNames, name)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	w.rng.Shuffle(len(columnNames), func(i, j int) {
		columnNames[i], columnNames[j] = columnNames[j], columnNames[i]
	})

	if len(columnNames) <= 0 {
		return nil, errors.Errorf("table %s has no columns", tableName)
	}
	return columnNames, nil
}

func (w *schemaChangeWorker) randType(tx *pgx.Tx) (tree.ResolvableTypeReference, error) {
	if w.rng.Intn(100) <= w.enumPct {
		// TODO(ajwerner): Support arrays of enums.
		typName, err := w.randEnum(tx, 100)
		if err != nil {
			return nil, err
		}
		return typName.ToUnresolvedObjectName(tree.NoAnnotation)
	}
	return rowenc.RandSortingType(w.rng), nil
}

func (w *schemaChangeWorker) createSchema(tx *pgx.Tx) (string, error) {
	schemaName, err := w.randSchema(tx, 10)
	if err != nil {
		return "", err
	}

	// TODO(jayshrivastava): Support authorization
	stmt := rowenc.MakeSchemaName(w.rng.Intn(2) == 0, schemaName, "")
	return tree.Serialize(stmt), nil
}

func (w *schemaChangeWorker) randSchema(tx *pgx.Tx, pctExisting int) (string, error) {
	if w.rng.Intn(100) >= pctExisting {
		return fmt.Sprintf("schema%d", atomic.AddInt64(w.seqNum, 1)), nil
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

func (w *schemaChangeWorker) dropSchema(tx *pgx.Tx) (string, error) {
	schemaName, err := w.randSchema(tx, 100)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schemaName), nil
}

// txTypeResolver is a minimal type resolver to support writing enum values to
// columns.
type txTypeResolver struct {
	tx *pgx.Tx
}

func (t txTypeResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	rows, err := t.tx.Query(`
  SELECT enumlabel, enumsortorder
    FROM pg_enum AS pge, pg_type AS pgt, pg_namespace AS pgn
   WHERE (pgt.typnamespace = pgn.oid AND pgt.oid = pge.enumtypid)
         AND typcategory = 'E'
         AND typname = $1
ORDER BY enumsortorder`, name.Object())
	if err != nil {
		return nil, err
	}
	var logicalReps []string
	var physicalReps [][]byte
	var readOnly []bool
	for rows.Next() {
		var logicalRep string
		var order int64
		if err := rows.Scan(&logicalRep, &order); err != nil {
			return nil, err
		}
		logicalReps = append(logicalReps, logicalRep)
		physicalReps = append(physicalReps, encoding.EncodeUntaggedIntValue(nil, order))
		readOnly = append(readOnly, false)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// TODO(ajwerner): Fill in some more fields here to generate better errors
	// down the line.
	n := types.UserDefinedTypeName{Name: name.Object()}
	return &types.T{
		InternalType: types.InternalType{
			Family: types.EnumFamily,
		},
		TypeMeta: types.UserDefinedTypeMetadata{
			Name: &n,
			EnumData: &types.EnumMetadata{
				LogicalRepresentations:  logicalReps,
				PhysicalRepresentations: physicalReps,
				IsMemberReadOnly:        readOnly,
			},
		},
	}, nil
}

func (t txTypeResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return nil, pgerror.Newf(pgcode.UndefinedObject, "type %d does not exist", oid)
}

var _ tree.TypeReferenceResolver = (*txTypeResolver)(nil)
