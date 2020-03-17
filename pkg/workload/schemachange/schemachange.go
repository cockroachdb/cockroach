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
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/jackc/pgx"
	"github.com/spf13/pflag"
)

// TODO(peter):
// - support more than 1 database
// - reference sequences in column defaults
// - create foreign keys

var schemaChangeMeta = workload.Meta{
	Name:        `schemachange`,
	Description: `schemachange randomly generates concurrent schema changes`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		s := &schemaChange{}
		s.flags.FlagSet = pflag.NewFlagSet(`schemachange`, pflag.ContinueOnError)
		s.flags.StringVar(&s.dbOverride, `db`, ``,
			`Override for the SQL database to use. If empty, defaults to the generator name`)
		s.flags.IntVar(&s.concurrency, `concurrency`, 1, /* TODO(peter): 2*runtime.NumCPU() */
			`Number of concurrent workers`)
		s.flags.BoolVarP(&s.verbose, `verbose`, `v`, true, ``)
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

	dropColumn        // ALTER TABLE <table> DROP COLUMN <column>
	dropColumnDefault // ALTER TABLE <table> ALTER [COLUMN] <column> DROP DEFAULT
	dropColumnNotNull // ALTER TABLE <table> ALTER [COLUMN] <column> DROP NOT NULL
	dropColumnStored  // ALTER TABLE <table> ALTER [COLUMN] <column> DROP STORED
	dropConstraint    // ALTER TABLE <table> DROP CONSTRAINT <constraint>
	dropIndex         // DROP INDEX <index>@<table>
	dropSequence      // DROP SEQUENCE <sequence>
	dropTable         // DROP TABLE <table>
	dropView          // DROP VIEW <view>

	renameColumn   // ALTER TABLE <table> RENAME [COLUMN] <column> TO <column>
	renameIndex    // ALTER TABLE <table> RENAME CONSTRAINT <constraint> TO <constraint>
	renameSequence // ALTER SEQUENCE <sequence> RENAME TO <sequence>
	renameTable    // ALTER TABLE <table> RENAME TO <table>
	renameView     // ALTER VIEW <view> RENAME TO <view>

	setColumnDefault // ALTER TABLE <table> ALTER [COLUMN] <column> SET DEFAULT <expr>
	setColumnNotNull // ALTER TABLE <table> ALTER [COLUMN] <column> SET NOT NULL
	setColumnType    // ALTER TABLE <table> ALTER [COLUMN] <column> [SET DATA] TYPE <type>
)

var opWeights = []int{
	addColumn:         1,
	addConstraint:     0, // TODO(peter): unimplemented
	createIndex:       1,
	createSequence:    1,
	createTable:       1,
	createTableAs:     1,
	createView:        1,
	dropColumn:        1,
	dropColumnDefault: 1,
	dropColumnNotNull: 1,
	dropColumnStored:  1,
	dropConstraint:    1,
	dropIndex:         1,
	dropSequence:      1,
	dropTable:         1,
	dropView:          1,
	renameColumn:      1,
	renameIndex:       1,
	renameSequence:    1,
	renameTable:       1,
	renameView:        1,
	setColumnDefault:  0, // TODO(peter): unimplemented
	setColumnNotNull:  1,
	setColumnType:     1,
}

type schemaChange struct {
	flags       workload.Flags
	dbOverride  string
	concurrency int
	verbose     bool
	dryRun      bool
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
func (s *schemaChange) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(s, s.dbOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	cfg := workload.MultiConnPoolCfg{
		MaxTotalConnections: s.concurrency * 2,
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
			verbose: s.verbose,
			dryRun:  s.dryRun,
			rng:     rand.New(rand.NewSource(timeutil.Now().UnixNano())),
			ops:     ops,
			pool:    pool,
			hists:   reg.GetHandle(),
			seqNum:  seqNum,
		}
		ql.WorkerFns = append(ql.WorkerFns, w.run)
	}
	return ql, nil
}

func (s *schemaChange) initSeqNum(pool *workload.MultiConnPool) (*int64, error) {
	seqNum := new(int64)

	const q = `
SELECT max(regexp_extract(name, '[0-9]+$')::int)
  FROM ((SELECT * FROM [SHOW TABLES]) UNION (SELECT * FROM [SHOW SEQUENCES])) AS obj(name)
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
	verbose bool
	dryRun  bool
	rng     *rand.Rand
	ops     *deck
	pool    *workload.MultiConnPool
	hists   *histogram.Histograms
	seqNum  *int64
}

func (w *schemaChangeWorker) run(ctx context.Context) (err error) {
	defer func(start time.Time) {
		elapsed := timeutil.Since(start)
		if err != nil {
			if w.verbose {
				fmt.Printf("%v\n", err)
			}
			w.hists.Get("err").Record(elapsed)
		} else {
			w.hists.Get("ok").Record(elapsed)
		}
		err = nil
	}(timeutil.Now())

	tx, err := w.pool.Get().Begin()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if w.verbose {
		fmt.Fprintf(&buf, "BEGIN;\n")
	}
	defer func() {
		if err == nil {
			if w.verbose {
				fmt.Printf("%sCOMMIT;\n", buf.String())
			}
			err = tx.Commit()
		} else {
			if w.verbose {
				fmt.Printf("%sROLLBACK;\n", buf.String())
			}
			_ = tx.Rollback()
		}
	}()

	// Run between 1 and 5 schema change operations.
	n := 1 + w.rng.Intn(5)
	for i := 0; i < n; i++ {
		op, err := w.randOp(tx)
		if err != nil {
			return err
		}
		if w.verbose {
			fmt.Fprintf(&buf, "  %s;\n", op)
		}
		if !w.dryRun {
			if _, err := tx.Exec(op); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *schemaChangeWorker) randOp(tx *pgx.Tx) (string, error) {
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
		}

		if stmt == "" || err == pgx.ErrNoRows {
			if w.verbose {
				fmt.Printf("NOOP: %s -> %v\n", op, err)
			}
			continue
		}
		return stmt, err
	}
}

func (w *schemaChangeWorker) addColumn(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}

	var columnName string
	switch w.rng.Intn(10) {
	case 0:
		// Existing name 10% of the time.
		var err error
		columnName, err = w.randColumn(tx, tableName)
		if err != nil {
			return "", err
		}

	default:
		// New unique name 90% of the time.
		columnName = fmt.Sprintf("col%s_%d",
			strings.TrimPrefix(tableName, "table"), atomic.AddInt64(w.seqNum, 1))
	}

	def := &tree.ColumnTableDef{
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		Name: tree.Name(columnName),
		Type: sqlbase.RandSortingType(w.rng),
	}
	def.Nullable.Nullability = tree.Nullability(rand.Intn(1 + int(tree.SilentNull)))
	return fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN %s`, tableName, tree.Serialize(def)), nil
}

func (w *schemaChangeWorker) addConstraint(tx *pgx.Tx) (string, error) {
	// TODO(peter): unimplemented
	// - Export sqlbase.randColumnTableDef.
	return "", nil
}

func (w *schemaChangeWorker) createIndex(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}

	columnNames, err := w.tableColumns(tx, tableName)
	if err != nil {
		return "", err
	}

	w.rng.Shuffle(len(columnNames), func(i, j int) {
		columnNames[i], columnNames[j] = columnNames[j], columnNames[i]
	})

	var indexName string
	switch w.rng.Intn(10) {
	case 0:
		// Existing name 10% of the time.
		var err error
		indexName, err = w.randIndex(tx, tableName)
		if err != nil {
			return "", err
		}

	default:
		// New unique name 90% of the time.
		indexName = fmt.Sprintf("index%d", atomic.AddInt64(w.seqNum, 1))
	}
	def := &tree.CreateIndex{
		Name:        tree.Name(indexName),
		Table:       tree.MakeUnqualifiedTableName(tree.Name(tableName)),
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
	var tableName string
	switch w.rng.Intn(10) {
	case 0:
		// Existing name 10% of the time.
		var err error
		tableName, err = w.randTable(tx)
		if err != nil {
			return "", err
		}

	default:
		// New unique name 90% of the time.
		tableName = fmt.Sprintf("table%d", atomic.AddInt64(w.seqNum, 1))
	}

	stmt := sqlbase.RandCreateTable(w.rng, "table", int(atomic.AddInt64(w.seqNum, 1)))
	stmt.Table = tree.MakeUnqualifiedTableName(tree.Name(tableName))
	stmt.IfNotExists = w.rng.Intn(2) == 0
	return tree.Serialize(stmt), nil
}

func (w *schemaChangeWorker) createTableAs(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}

	columnNames, err := w.tableColumns(tx, tableName)
	if err != nil {
		return "", err
	}

	w.rng.Shuffle(len(columnNames), func(i, j int) {
		columnNames[i], columnNames[j] = columnNames[j], columnNames[i]
	})
	columnNames = columnNames[:1+w.rng.Intn(len(columnNames))]

	names := make(tree.NameList, len(columnNames))
	for i := range names {
		names[i] = tree.Name(columnNames[i])
	}

	var destTableName string
	switch w.rng.Intn(10) {
	case 0:
		// Existing name 10% of the time.
		var err error
		destTableName, err = w.randTable(tx)
		if err != nil {
			return "", err
		}

	default:
		// New unique name 90% of the time.
		destTableName = fmt.Sprintf("table%d", atomic.AddInt64(w.seqNum, 1))
	}

	return fmt.Sprintf(`CREATE TABLE "%s" AS SELECT %s FROM "%s"`,
		destTableName, tree.Serialize(&names), tableName), nil
}

func (w *schemaChangeWorker) createView(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}

	columnNames, err := w.tableColumns(tx, tableName)
	if err != nil {
		return "", err
	}

	w.rng.Shuffle(len(columnNames), func(i, j int) {
		columnNames[i], columnNames[j] = columnNames[j], columnNames[i]
	})
	columnNames = columnNames[:1+w.rng.Intn(len(columnNames))]

	names := make(tree.NameList, len(columnNames))
	for i := range names {
		names[i] = tree.Name(columnNames[i])
	}

	var destViewName string
	switch w.rng.Intn(10) {
	case 0:
		// Existing name 10% of the time.
		var err error
		destViewName, err = w.randView(tx)
		if err != nil {
			return "", err
		}

	default:
		// New unique name 90% of the time.
		destViewName = fmt.Sprintf("view%d", atomic.AddInt64(w.seqNum, 1))
	}

	// TODO(peter): Create views that are dependent on multiple tables.
	return fmt.Sprintf(`CREATE VIEW "%s" AS SELECT %s FROM "%s"`,
		destViewName, tree.Serialize(&names), tableName), nil
}

func (w *schemaChangeWorker) dropColumn(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE "%s" DROP COLUMN "%s"`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropColumnDefault(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropColumnStored(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP STORED`, tableName, columnName), nil
}

func (w *schemaChangeWorker) dropConstraint(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	constraintName, err := w.randConstraint(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE "%s" DROP CONSTRAINT "%s"`, tableName, constraintName), nil
}

func (w *schemaChangeWorker) dropIndex(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	indexName, err := w.randIndex(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP INDEX "%s"@"%s"`, tableName, indexName), nil
}

func (w *schemaChangeWorker) dropSequence(tx *pgx.Tx) (string, error) {
	sequenceName, err := w.randSequence(tx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP SEQUENCE "%s"`, sequenceName), nil
}

func (w *schemaChangeWorker) dropTable(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP TABLE "%s"`, tableName), nil
}

func (w *schemaChangeWorker) dropView(tx *pgx.Tx) (string, error) {
	viewName, err := w.randView(tx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`DROP VIEW "%s"`, viewName), nil
}

func (w *schemaChangeWorker) renameColumn(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}

	srcColumnName, err := w.randColumn(tx, tableName)
	if err != nil {
		return "", err
	}

	var destColumnName string
	switch w.rng.Intn(2) {
	case 0:
		// Rename to new unique name 50% of time.
		destColumnName = fmt.Sprintf("col%s_%d",
			strings.TrimPrefix(tableName, "table"), atomic.AddInt64(w.seqNum, 1))

	case 1:
		// Rename to existing name 50% of time.
		var err error
		destColumnName, err = w.randColumn(tx, tableName)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(`ALTER TABLE "%s" RENAME COLUMN "%s" TO "%s"`,
		tableName, srcColumnName, destColumnName), nil
}

func (w *schemaChangeWorker) renameIndex(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}

	srcIndexName, err := w.randIndex(tx, tableName)
	if err != nil {
		return "", err
	}

	var destIndexName string
	switch w.rng.Intn(2) {
	case 0:
		// Rename to new unique name 50% of time.
		destIndexName = fmt.Sprintf("index%d", atomic.AddInt64(w.seqNum, 1))

	case 1:
		// Rename to existing name 50% of time.
		var err error
		destIndexName, err = w.randIndex(tx, tableName)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(`ALTER TABLE "%s" RENAME CONSTRAINT "%s" TO "%s"`,
		tableName, srcIndexName, destIndexName), nil
}

func (w *schemaChangeWorker) renameSequence(tx *pgx.Tx) (string, error) {
	srcSequenceName, err := w.randSequence(tx)
	if err != nil {
		return "", err
	}

	var destSequenceName string
	switch w.rng.Intn(2) {
	case 0:
		// Rename to new unique name 50% of time.
		destSequenceName = fmt.Sprintf("seq%d", atomic.AddInt64(w.seqNum, 1))

	case 1:
		// Rename to existing name 50% of time.
		var err error
		destSequenceName, err = w.randSequence(tx)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(`ALTER SEQUENCE "%s" RENAME TO "%s"`, srcSequenceName, destSequenceName), nil
}

func (w *schemaChangeWorker) renameTable(tx *pgx.Tx) (string, error) {
	srcTableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}

	var destTableName string
	switch w.rng.Intn(2) {
	case 0:
		// Rename to new unique name 50% of time.
		destTableName = fmt.Sprintf("table%d", atomic.AddInt64(w.seqNum, 1))

	case 1:
		// Rename to existing name 50% of time.
		var err error
		destTableName, err = w.randTable(tx)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, srcTableName, destTableName), nil
}

func (w *schemaChangeWorker) renameView(tx *pgx.Tx) (string, error) {
	srcViewName, err := w.randView(tx)
	if err != nil {
		return "", err
	}

	var destViewName string
	switch w.rng.Intn(2) {
	case 0:
		// Rename to new unique name 50% of time.
		destViewName = fmt.Sprintf("view%d", atomic.AddInt64(w.seqNum, 1))

	case 1:
		// Rename to existing name 50% of time.
		var err error
		destViewName, err = w.randView(tx)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(`ALTER VIEW "%s" RENAME TO "%s"`, srcViewName, destViewName), nil
}

func (w *schemaChangeWorker) setColumnDefault(tx *pgx.Tx) (string, error) {
	// TODO(peter): unimplemented
	return "", nil
}

func (w *schemaChangeWorker) setColumnNotNull(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL`, tableName, columnName), nil
}

func (w *schemaChangeWorker) setColumnType(tx *pgx.Tx) (string, error) {
	tableName, err := w.randTable(tx)
	if err != nil {
		return "", err
	}
	columnName, err := w.randColumn(tx, tableName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET DATA TYPE %s`,
		tableName, columnName, sqlbase.RandSortingType(w.rng)), nil
}

func (w *schemaChangeWorker) randColumn(tx *pgx.Tx, tableName string) (string, error) {
	q := fmt.Sprintf(`
  SELECT column_name
    FROM [SHOW COLUMNS FROM "%s"]
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

func (w *schemaChangeWorker) randConstraint(tx *pgx.Tx, tableName string) (string, error) {
	q := fmt.Sprintf(`
  SELECT constraint_name
    FROM [SHOW CONSTRAINTS FROM "%s"]
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

func (w *schemaChangeWorker) randIndex(tx *pgx.Tx, tableName string) (string, error) {
	q := fmt.Sprintf(`
  SELECT index_name
    FROM [SHOW INDEXES FROM "%s"]
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

func (w *schemaChangeWorker) randSequence(tx *pgx.Tx) (string, error) {
	const q = `
  SELECT sequence_name
    FROM [SHOW SEQUENCES]
   WHERE sequence_name LIKE 'seq%'
ORDER BY random()
   LIMIT 1;
`
	var name string
	err := tx.QueryRow(q).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (w *schemaChangeWorker) randTable(tx *pgx.Tx) (string, error) {
	const q = `
  SELECT table_name
    FROM [SHOW TABLES]
   WHERE table_name LIKE 'table%'
ORDER BY random()
   LIMIT 1;
`
	var name string
	err := tx.QueryRow(q).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (w *schemaChangeWorker) randView(tx *pgx.Tx) (string, error) {
	const q = `
  SELECT table_name
    FROM [SHOW TABLES]
   WHERE table_name LIKE 'view%'
ORDER BY random()
   LIMIT 1;
`
	var name string
	err := tx.QueryRow(q).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (w *schemaChangeWorker) tableColumns(tx *pgx.Tx, tableName string) ([]string, error) {
	q := fmt.Sprintf(`
SELECT column_name
  FROM [SHOW COLUMNS FROM "%s"];
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
		return nil, err
	}

	return columnNames, nil
}
