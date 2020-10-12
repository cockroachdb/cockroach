// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interleavebench

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// interleaveBench is a workload that can be used to benchmark the performance
// of DELETE queries on interleaved and non-interleaved tables. With `init`
// command, it creates a hierarchy of related tables that can be interleaved
// or not and can have foreign key relationship or not, etc, and then inserts
// many rows that satisfy the desired ratios and count arguments. With `run`
// command, it executes DELETE queries one at a time and measures the time it
// takes to remove all "related" rows from the whole hierarchy ("related" is in
// quotes because in a non-interleaved case without adding FKs the relationship
// between parent and child tables is implicit and is not enforced in the
// schema itself).
type interleaveBench struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	verbose         bool
	interleave      bool
	addFKs          bool
	levels          int
	ratio           float64
	intraLevelRatio float64
	parentCount     int
	rangesToSplit   int
	hierarchy       string
	tablesPerLevel  []int

	deleteFromLevel       int
	numRowsInSingleDelete int
	inClause              bool
}

func init() {
	workload.Register(interleaveBenchMeta)
}

var interleaveBenchMeta = workload.Meta{
	Name:        `interleavebench`,
	Description: `interleavebench is a tool for benchmarking DELETE queries on interleaved and non-interleaved tables. Note that it ignores --concurrency flag and always runs with no concurrency`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		b := &interleaveBench{}
		b.flags.FlagSet = pflag.NewFlagSet(`interleavebench`, pflag.ContinueOnError)
		b.flags.Meta = map[string]workload.FlagMeta{
			`delete-from`:           {RuntimeOnly: true},
			`rows-in-single-delete`: {RuntimeOnly: true},
			`in-clause`:             {RuntimeOnly: true},
		}
		b.flags.BoolVar(&b.verbose, `verbose`, false, `Specifies whether the workload should print some of the queries`)
		b.flags.BoolVar(&b.interleave, `interleave`, false, `Specifies whether the hierarchy of tables is interleaved`)
		b.flags.BoolVar(&b.addFKs, `fks`, true, `Specifies whether foreign keys are added`)
		b.flags.IntVar(&b.levels, `levels`, 3, `Specifies the number of levels (grandparent, parent, child - these count as 3)`)
		b.flags.StringVar(&b.hierarchy, `hierarchy`, ``, `Comma-separated numbers of tables at each level (e.g. "1,1,1")`)
		b.flags.Float64Var(&b.ratio, `ratio`, 10, `Specifies the ratio between the first tables in consequent levels (e.g. # rows in child = 10 x # rows in parent)`)
		b.flags.Float64Var(&b.intraLevelRatio, `intra-level-ratio`, 0.1, `Specifies the intra-level ratio (i.e. between consequent child tables)`)
		b.flags.IntVar(&b.parentCount, `parent-count`, 100, `Specifies the number of rows on the parent level`)
		b.flags.IntVar(&b.deleteFromLevel, `delete-from`, 0, `Specifies the level to issue deletes from (0 is the parent, 1 is the child, etc)`)
		b.flags.IntVar(&b.numRowsInSingleDelete, `rows-in-single-delete`, 1, `Specifies the maximum number of rows to be deleted in a single DELETE query (rows from child levels don't count towards this)'`)
		b.flags.IntVar(&b.rangesToSplit, `to-split`, 0, `Specifies the number of ranges to have (we will disable range merges, split the parent table into this number manually, and scatter those ranges)'`)
		b.flags.BoolVar(&b.inClause, `in-clause`, true, `Specifies whether rows to delete are specified with an IN clause or a range filter`)
		b.connFlags = workload.NewConnFlags(&b.flags)
		return b
	},
}

const (
	// We only support concurrency of 1 (this makes it easy to track which rows
	// have already been deleted).
	maxWorkers              = 1
	maxSingleTableSize      = 1 << 33
	maxSingleInsertRowCount = 10000
)

// Meta implements the Generator interface.
func (*interleaveBench) Meta() workload.Meta { return interleaveBenchMeta }

// Flags implements the Flagser interface.
func (b *interleaveBench) Flags() workload.Flags { return b.flags }

// Hooks implements the Hookser interface.
func (b *interleaveBench) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			// Override the concurrency flag regardless of the user-specified
			// option.
			b.connFlags.Concurrency = maxWorkers
			if b.levels < 2 {
				return errors.Errorf("invalid number of levels %d (needs to be at least 2)", b.levels)
			}
			b.tablesPerLevel = make([]int, b.levels)
			for i := range b.tablesPerLevel {
				b.tablesPerLevel[i] = 1
			}
			if b.hierarchy != "" {
				tokens := strings.Split(b.hierarchy, ",")
				if len(tokens) != b.levels {
					return errors.Errorf("mismatched --levels and --hierarchy arguments")
				}
				for l, token := range tokens {
					numTables, err := strconv.Atoi(token)
					if err != nil {
						return err
					}
					if numTables < 1 {
						return errors.Errorf("invalid number of tables %d at level %d", numTables, l)
					}
					if l == 0 && numTables != 1 {
						return errors.Errorf("zeroth level must contain exactly one table")
					}
					b.tablesPerLevel[l] = numTables
				}
			}
			if b.ratio <= 0 {
				return errors.Errorf("invalid ratio %.2f (needs to be greater than 0)", b.ratio)
			}
			if b.intraLevelRatio <= 0 {
				return errors.Errorf("invalid intra-level ratio %.2f (needs to be greater than 0)", b.intraLevelRatio)
			}
			if b.parentCount < 1 {
				return errors.Errorf("invalid parent-count %d (needs to be at least 1)", b.parentCount)
			}
			if float64(b.parentCount)*math.Pow(b.ratio, float64(b.levels-1)) > maxSingleTableSize {
				return errors.Errorf("a table on %d level will have more than %d rows", b.levels-1, maxSingleTableSize)
			}
			if b.deleteFromLevel < 0 || b.deleteFromLevel >= b.levels {
				return errors.Errorf("invalid delete-from level %d (needs to be in [0, %d) range)", b.deleteFromLevel, b.levels)
			}
			if b.numRowsInSingleDelete < 1 {
				return errors.Errorf("invalid rows-in-single-delete %d number (needs to be at least 1)", b.numRowsInSingleDelete)
			}
			if b.rangesToSplit < 0 || b.rangesToSplit > b.parentCount {
				return errors.Errorf("invalid to-split %d (should be in [0, %d] range)", b.rangesToSplit, b.parentCount)
			}
			return nil
		},
		PostLoad: func(db *gosql.DB) error {
			if b.addFKs {
				for level := 1; level < b.levels; level++ {
					for ordinal := 0; ordinal < b.tablesPerLevel[level]; ordinal++ {
						addFK := "ALTER TABLE " + b.getTableName(level, ordinal) + " ADD CONSTRAINT fk FOREIGN KEY " +
							b.getColumnsRange(level-1) + " REFERENCES " + b.getTableName(level-1, 0) +
							b.getColumnsRange(level-1) + " ON DELETE CASCADE;"
						if b.verbose {
							fmt.Printf("%s\n", addFK)
						}
						if _, err := db.Exec(addFK); err != nil {
							return err
						}
					}
				}
			}
			getInsert := func(level, ordinal int, start, end int) string {
				s := "INSERT INTO " + b.getTableName(level, ordinal) + " (SELECT "
				for l := 0; l < level; l++ {
					ancestorRowCount := b.getTableRowCount(l, 0)
					rowCount := b.getTableRowCount(level, ordinal)
					s += fmt.Sprintf("floor((i-1)*%.10f)::INT+1, ", ancestorRowCount/rowCount)
				}
				s += fmt.Sprintf("i FROM generate_series(%d, %d) AS i);", start, end)
				return s
			}
			for level := 0; level < b.levels; level++ {
				for ordinal := 0; ordinal < b.tablesPerLevel[level]; ordinal++ {
					start, end := 1, int(b.getTableRowCount(level, ordinal))
					numInserts := 0
					for start <= end {
						currentEnd := end
						if currentEnd-start > maxSingleInsertRowCount {
							currentEnd = start + maxSingleInsertRowCount - 1
						}
						insertStmt := getInsert(level, ordinal, start, currentEnd)
						if b.verbose {
							fmt.Printf("%s\n", insertStmt)
						}
						if _, err := db.Exec(insertStmt); err != nil {
							return err
						}
						start = currentEnd + 1
						numInserts++
					}
				}
			}
			if b.rangesToSplit > 0 {
				// We always split and scatter the parent table.
				if err := b.splitAndScatter(db, 0 /* level */); err != nil {
					return err
				}
				if !b.interleave {
					// In non-interleaved case, we split and scatter all tables.
					for level := 1; level < b.levels; level++ {
						if err := b.splitAndScatter(db, level); err != nil {
							return err
						}
					}
				}
			}
			return nil
		},
	}
}

func (b *interleaveBench) getTableRowCount(level, ordinal int) float64 {
	if level == 0 {
		return float64(b.parentCount)
	}
	rowCount := b.getTableRowCount(level-1, 0) * b.ratio
	for i := 0; i < ordinal; i++ {
		rowCount *= b.intraLevelRatio
	}
	return rowCount
}

func (b *interleaveBench) getTableName(level, ordinal int) string {
	return fmt.Sprintf("table%d_%d", level, ordinal)
}

func (b *interleaveBench) getColumnName(level int) string {
	return fmt.Sprintf("c%d", level)
}

// Returns '(c0, c1, ..., c<n-1>)'.
func (b *interleaveBench) getColumnsRange(lastLevel int) string {
	s := "("
	for i := 0; i <= lastLevel; i++ {
		s += b.getColumnName(i)
		if i < lastLevel {
			s += ", "
		}
	}
	s += ")"
	return s
}

// splitAndScatter splits all tables at the specified level into b.rangesToSplit
// ranges and scatters them.
func (b *interleaveBench) splitAndScatter(db *gosql.DB, level int) error {
	settingQuery := "SET CLUSTER SETTING kv.range_merge.queue_enabled=false;"
	if b.verbose {
		fmt.Printf("%s\n", settingQuery)
	}
	if _, err := db.Exec(settingQuery); err != nil {
		return err
	}
	for ordinal := 0; ordinal < b.tablesPerLevel[level]; ordinal++ {
		splitQuery := fmt.Sprintf(
			"ALTER TABLE %s SPLIT AT SELECT i FROM generate_series(1, %d, %d) AS i;",
			b.getTableName(level, ordinal), int(b.getTableRowCount(0, 0)), int(b.getTableRowCount(0, 0)/float64(b.rangesToSplit)),
		)
		scatterQuery := fmt.Sprintf("ALTER TABLE %s SCATTER;", b.getTableName(level, ordinal))
		if b.verbose {
			fmt.Printf("%s\n%s\n%s\n", settingQuery, splitQuery, scatterQuery)
		}
		if _, err := db.Exec(splitQuery); err != nil {
			return err
		}
		if _, err := db.Exec(scatterQuery); err != nil {
			return err
		}
	}
	return nil
}

// Tables implements the Generator interface.
func (b *interleaveBench) Tables() []workload.Table {
	if b.tablesPerLevel == nil {
		b.tablesPerLevel = make([]int, b.levels)
		for i := range b.tablesPerLevel {
			b.tablesPerLevel[i] = 1
		}
	}
	getTableSchema := func(level, ordinal int) string {
		schema := "("
		for i := 0; i <= level; i++ {
			schema += fmt.Sprintf("%s INT, ", b.getColumnName(i))
		}
		schema += "PRIMARY KEY " + b.getColumnsRange(level) + ")"
		if b.interleave && level > 0 {
			schema += " INTERLEAVE IN PARENT "
			schema += b.getTableName(level-1, 0)
			schema += b.getColumnsRange(level - 1)
		}
		return schema
	}
	var tables []workload.Table
	for level := 0; level < b.levels; level++ {
		for ordinal := 0; ordinal < b.tablesPerLevel[level]; ordinal++ {
			tables = append(tables, workload.Table{
				Name:   b.getTableName(level, ordinal),
				Schema: getTableSchema(level, ordinal),
			})
		}
	}
	return tables
}

// Ops implements the Opser interface.
func (b *interleaveBench) Ops(
	_ context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(b, b.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of 1 connection to the database.
	db.SetMaxOpenConns(maxWorkers)
	db.SetMaxIdleConns(maxWorkers)

	worker := &worker{
		config:         b,
		hists:          reg.GetHandle(),
		db:             db,
		rng:            rand.New(rand.NewSource(int64(timeutil.Now().Nanosecond()))),
		alreadyDeleted: make([]bool, int(b.getTableRowCount(b.deleteFromLevel, 0))+1),
	}
	// Note that we always has a single worker because we override the
	// concurrency flag to 1.
	ql := workload.QueryLoad{
		SQLDatabase: sqlDatabase,
		WorkerFns:   []func(context.Context) error{worker.run},
	}
	return ql, nil
}

type worker struct {
	config         *interleaveBench
	hists          *histogram.Histograms
	db             *gosql.DB
	rng            *rand.Rand
	alreadyDeleted []bool
	deletedCount   int
}

func (w *worker) run(context.Context) error {
	level := w.config.deleteFromLevel
	levelRowCount := int(w.config.getTableRowCount(level, 0))
	if w.deletedCount >= levelRowCount {
		return nil
	}
	var filter string
	if w.config.inClause {
		ids := make([]int, 0, w.config.numRowsInSingleDelete)
		for len(ids) < w.config.numRowsInSingleDelete && w.deletedCount < levelRowCount {
			id := 1 + w.rng.Intn(levelRowCount)
			for w.alreadyDeleted[id] {
				id++
				if id > levelRowCount {
					id = 1
				}
			}
			ids = append(ids, id)
			w.alreadyDeleted[id] = true
			w.deletedCount++
		}
		filter = fmt.Sprintf("WHERE %s IN (", w.config.getColumnName(level))
		for i, id := range ids {
			if i > 0 {
				filter += ", "
			}
			filter += fmt.Sprintf("%d", id)
		}
		filter += ")"
	} else {
		var startRow int
		for {
			startRow = 1 + w.rng.Intn(levelRowCount)
			if !w.alreadyDeleted[startRow] {
				w.alreadyDeleted[startRow] = true
				break
			}
		}
		count := 1
		for startRow+count < levelRowCount && count < w.config.numRowsInSingleDelete {
			if w.alreadyDeleted[startRow+count] {
				break
			}
			w.alreadyDeleted[startRow+count] = true
			count++
		}
		filter = fmt.Sprintf(
			"WHERE %s >= %d AND %s < %d",
			w.config.getColumnName(level), startRow, w.config.getColumnName(level), startRow+count,
		)
	}
	var query string
	if !w.config.addFKs {
		// Foreign key relationships were not established which means that we
		// are simulating a "fake cascade" operator, so we manually issue the
		// DELETE queries for all descendant tables.
		for l := w.config.levels - 1; l > level; l-- {
			for ord := 0; ord < w.config.tablesPerLevel[l]; ord++ {
				query += fmt.Sprintf("DELETE FROM %s %s; ", w.config.getTableName(l, ord), filter)
			}
		}
	}
	query += fmt.Sprintf("DELETE FROM %s %s;", w.config.getTableName(level, 0), filter)
	if w.config.verbose && w.rng.Float64() < 0.1 {
		// We arbitrarily choose to print about every tenth query when
		// verbosity is requested.
		fmt.Printf("%s\n", query)
	}
	start := timeutil.Now()
	if _, err := w.db.Exec(query); err != nil {
		return err
	}
	w.hists.Get("").Record(timeutil.Since(start))
	return nil
}
