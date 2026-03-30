// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadsql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
)

// Setup creates the given tables and fills them with initial data.
//
// The size of the loaded data is returned in bytes, suitable for use with
// SetBytes of benchmarks. The exact definition of this is deferred to the
// InitialDataLoader implementation.
func Setup(
	ctx context.Context, db *gosql.DB, gen workload.Generator, l workload.InitialDataLoader,
) (int64, error) {
	var hooks workload.Hooks
	if h, ok := gen.(workload.Hookser); ok {
		hooks = h.Hooks()
	}

	if hooks.PreCreate != nil {
		if err := hooks.PreCreate(db); err != nil {
			return 0, errors.Wrapf(err, "Could not pre-create")
		}
	}

	bytes, err := l.InitialDataLoad(ctx, db, gen)
	if err != nil {
		return 0, err
	}

	tables := gen.Tables()
	log.Dev.Infof(ctx, "starting split operations for %d tables", len(tables))
	const tableConcurrency = 10
	if err := Split(ctx, db, tables, tableConcurrency); err != nil {
		return 0, err
	}
	log.Dev.Infof(ctx, "finished split operations for %d tables", len(tables))

	if hooks.PostLoad != nil {
		log.Dev.Infof(ctx, "starting PostLoad hook")
		if err := hooks.PostLoad(ctx, db); err != nil {
			return 0, errors.Wrapf(err, "Could not postload")
		}
		log.Dev.Infof(ctx, "finished PostLoad hook")
	}

	return bytes, nil
}

// collectSplitPoints gathers and sorts all split points for a table.
func collectSplitPoints(table workload.Table) [][]interface{} {
	splitPoints := make(
		[][]interface{}, 0, table.Splits.NumBatches,
	)
	for splitIdx := 0; splitIdx < table.Splits.NumBatches; splitIdx++ {
		splitPoints = append(
			splitPoints, table.Splits.BatchRows(splitIdx)...,
		)
	}
	sort.Sort(sliceSliceInterface(splitPoints))
	return splitPoints
}

// executeSplits performs ALTER TABLE ... SPLIT AT for all tables
// concurrently, issuing one statement per table with all split points.
func executeSplits(
	ctx context.Context, db *gosql.DB, tables []workload.Table, concurrency int,
) error {
	g := ctxgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, table := range tables {
		pts := collectSplitPoints(table)
		if len(pts) == 0 {
			continue
		}
		name := table.Name
		g.GoCtx(func(ctx context.Context) error {
			log.Dev.Infof(ctx, "splitting %s into %d ranges",
				name, len(pts))
			var buf bytes.Buffer
			fmt.Fprintf(&buf, `ALTER TABLE %s SPLIT AT VALUES `,
				tree.NameString(name))
			for i, pt := range pts {
				if i > 0 {
					buf.WriteString(`, `)
				}
				fmt.Fprintf(&buf, `(%s)`,
					strings.Join(StringTuple(pt), `, `))
			}
			// If you're investigating an error from this Exec, see
			// the HACK comment in ColBatchToRows.
			stmt := buf.String()
			if _, err := db.Exec(stmt); err != nil {
				if strings.Contains(
					err.Error(),
					errorutil.UnsupportedUnderClusterVirtualizationMessage,
				) {
					// Splits are unsupported under cluster
					// virtualization; skip without error.
					// TODO(knz): This should be possible with the
					// right capability. See: #109422.
					return nil
				}
				return errors.Wrapf(err, "executing split for table %s", name)
			}
			log.Dev.Infof(ctx, "finished splitting %s", name)
			return nil
		})
	}
	return g.Wait()
}

// scatterTables runs ALTER TABLE ... SCATTER for each table
// concurrently, bounded by the given concurrency limit.
func scatterTables(
	ctx context.Context, db *gosql.DB, tables []workload.Table, concurrency int,
) error {
	g := ctxgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, table := range tables {
		name := table.Name
		g.GoCtx(func(ctx context.Context) error {
			log.Dev.Infof(ctx, "scattering %s", name)
			stmt := fmt.Sprintf(
				`ALTER TABLE %s SCATTER`, tree.NameString(name),
			)
			if _, err := db.Exec(stmt); err != nil {
				// SCATTER can collide with replicate queue operations
				// and fail spuriously, so only log a warning.
				log.Dev.Warningf(ctx, `%s: %v`, stmt, err)
			}
			log.Dev.Infof(ctx, "finished scattering %s", name)
			return nil
		})
	}
	return g.Wait()
}

// Split creates the range splits defined by the given tables and
// scatters them. Tables with no split points are skipped.
func Split(ctx context.Context, db *gosql.DB, tables []workload.Table, concurrency int) error {
	var tablesWithSplits []workload.Table
	for _, table := range tables {
		if table.Splits.NumBatches > 0 {
			tablesWithSplits = append(tablesWithSplits, table)
		}
	}
	if len(tablesWithSplits) == 0 {
		return nil
	}

	if err := executeSplits(ctx, db, tablesWithSplits, concurrency); err != nil {
		return err
	}
	return scatterTables(ctx, db, tablesWithSplits, concurrency)
}

// StringTuple returns the given datums as strings suitable for direct
// use in SQL.
func StringTuple(datums []interface{}) []string {
	s := make([]string, len(datums))
	for i, datum := range datums {
		if datum == nil {
			s[i] = `NULL`
			continue
		}
		switch x := datum.(type) {
		case int:
			s[i] = strconv.Itoa(x)
		case int64:
			s[i] = strconv.FormatInt(x, 10)
		case uint64:
			s[i] = strconv.FormatUint(x, 10)
		case string:
			s[i] = lexbase.EscapeSQLString(x)
		case float64:
			s[i] = fmt.Sprintf(`%f`, x)
		case []byte:
			s[i] = lexbase.EscapeSQLString(string(x))
		default:
			panic(errors.AssertionFailedf("unsupported type %T: %v", x, x))
		}
	}
	return s
}

type sliceSliceInterface [][]interface{}

func (s sliceSliceInterface) Len() int      { return len(s) }
func (s sliceSliceInterface) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sliceSliceInterface) Less(i, j int) bool {
	for offset := 0; ; offset++ {
		iLen, jLen := len(s[i]), len(s[j])
		if iLen <= offset || jLen <= offset {
			return iLen < jLen
		}
		var cmp int
		switch x := s[i][offset].(type) {
		case int:
			if y := s[j][offset].(int); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case int64:
			if y := s[j][offset].(int64); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case float64:
			if y := s[j][offset].(float64); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case uint64:
			if y := s[j][offset].(uint64); x < y {
				return true
			} else if x > y {
				return false
			}
			continue
		case string:
			cmp = strings.Compare(x, s[j][offset].(string))
		case []byte:
			cmp = bytes.Compare(x, s[j][offset].([]byte))
		default:
			panic(errors.AssertionFailedf("unsupported type %T: %v", x, x))
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
}
