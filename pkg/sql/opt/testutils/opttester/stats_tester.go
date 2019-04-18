// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package opttester

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const rewriteActualFlag = "rewrite-actual-stats"

var (
	rewriteActualStats = flag.Bool(
		rewriteActualFlag, false,
		"used to update the actual statistics for statistics quality tests. If true, the opttester "+
			"will actually run the test queries to calculate actual statistics for comparison with the "+
			"estimated stats.",
	)
	pgurl = flag.String(
		"url", "postgresql://localhost:26257/?sslmode=disable&user=root",
		"the database url to connect to",
	)
)

// statsTester is used for testing the quality of our estimated statistics
// by comparing the estimated stats for a given expression to the actual stats
// for that expression. See the comments above testStats for more details.
type statsTester struct {
	evalCtx  *tree.EvalContext
	catalog  *testcat.Catalog
	table    string
	database string
	d        *datadriven.TestData
}

// testStats compares actual and estimated stats for the given relational
// expression, and returns formatted tabular output of the stats themselves as
// well as the estimation error.
//
// The output looks like this:
//
// column_names  row_count  distinct_count  null_count
// ...           ...        ...             ...
// ~~~~
// column_names  row_count_est  row_count_err  distinct_count_est  distinct_count_err  null_count_est  null_count_err
// ...           ...            ...            ...                 ...                 ...             ...
//
// The data above the "~~~~" are the actual stats for the expression, and the
// data below the "~~~~" are the estimated stats as well as the estimation
// error in comparison to the actual stats.
//
// If rewriteActualStats=true, testStats will recalculate the actual statistics
// for the given expression. Otherwise, it will reuse the actual stats in the
// test output (calculated previously) to compare against the estimated stats.
//
func (st *statsTester) testStats(rel memo.RelExpr) (string, error) {
	// Get the actual stats.
	const sep = "~~~~"
	actualStats, actualStatsMap, err := st.getActualStats(st.d, st.table, sep)
	if err != nil {
		return "", err
	}

	// Make sure we have estimated stats for all the output columns.
	outputCols := rel.Relational().OutputCols
	for i, ok := outputCols.Next(0); ok; i, ok = outputCols.Next(i + 1) {
		memo.RequestColStat(st.evalCtx, rel, util.MakeFastIntSet(i))
	}
	estimatedStats := rel.Relational().Stats
	jsonStats := make([]stats.JSONStatistic, 0, estimatedStats.ColStats.Count())

	// Set up the test result columns and add column names to the output.
	columns := []string{
		"column_names",
		"row_count_est",
		"row_count_err",
		"distinct_count_est",
		"distinct_count_err",
		"null_count_est",
		"null_count_err",
	}
	res := make([]string, len(columns), len(columns)*(estimatedStats.ColStats.Count()+1))
	for i := range columns {
		res[i] = columns[i]
	}

	format := func(val float64) string {
		return strconv.FormatFloat(val, 'f', 2, 64)
	}
	for i, n := 0, estimatedStats.ColStats.Count(); i < n; i++ {
		stat := estimatedStats.ColStats.Get(i)
		if stat.Cols.Len() != 1 {
			// We don't collect multi-column stats yet, so we can't compare this to
			// anything.
			continue
		}
		col, _ := stat.Cols.Next(0)

		pres := rel.RequiredPhysical().Presentation
		for j := range pres {
			if pres[j].ID != opt.ColumnID(col) {
				continue
			}
			colNames := fmt.Sprintf("{%s}", pres[j].Alias)
			actualStat, ok := actualStatsMap[colNames]
			if !ok {
				return "", fmt.Errorf("could not find actual stat for columns %s", colNames)
			}

			res = append(res, colNames)
			res = append(res, format(estimatedStats.RowCount))
			res = append(res, format(st.fractionErr(estimatedStats.RowCount, actualStat.rowCount)))
			res = append(res, format(stat.DistinctCount))
			res = append(res, format(st.fractionErr(stat.DistinctCount, actualStat.distinctCount)))
			res = append(res, format(stat.NullCount))
			res = append(res, format(st.fractionErr(stat.NullCount, actualStat.nullCount)))

			jsonStats = append(jsonStats, st.makeStat(
				[]string{pres[j].Alias},
				uint64(int64(math.Round(estimatedStats.RowCount))),
				uint64(int64(math.Round(stat.DistinctCount))),
				uint64(int64(math.Round(stat.NullCount))),
			))
		}
	}

	st.injectStats(st.table, jsonStats)

	formattedResults := st.formatValues(res, len(columns))
	return strings.Join(append(append(actualStats, sep), formattedResults...), "\n"), nil
}

// getActualStats gets the actual statistics from the test output or
// recalculates them if rewriteActualStats is true.
// Returns:
// 1. The actual statistics as a slice of strings (one for each row)
// 2. A map from column names to statistic for comparison with the estimated
//    stats.
func (st *statsTester) getActualStats(
	d *datadriven.TestData, tableName string, sep string,
) ([]string, map[string]statistic, error) {
	expected := strings.Split(d.Expected, sep)
	if len(expected) < 2 && !*rewriteActualStats {
		return nil, nil, fmt.Errorf(
			"must run with -%s=true to calculate actual stats first", rewriteActualFlag,
		)
	}
	var actualStats []string
	if *rewriteActualStats {
		var err error
		if actualStats, err = st.calculateActualStats(tableName); err != nil {
			return nil, nil, err
		}
	} else {
		actualStats = strings.Split(expected[0], "\n")
	}
	// Remove the last line, which is empty.
	actualStats = actualStats[:len(actualStats)-1]
	actualStatsMap, err := st.getActualStatsMap(actualStats)
	if err != nil {
		return nil, nil, err
	}

	return actualStats, actualStatsMap, nil
}

// calculateActualStats calculates actual statistics for the given table
// and returns them as a list of formatted rows.
//
// It works by connecting to a running database, and executing a CREATE TABLE
// <name> AS <query> statement to create the table in the database. Then it
// calls CREATE STATISTICS on that table, and finally uses SHOW STATISTICS to
// get the result rows.
func (st *statsTester) calculateActualStats(tableName string) ([]string, error) {
	db, err := sql.Open("postgres", *pgurl)
	if err != nil {
		return nil, errors.Wrap(err,
			"can only recompute actual stats when pointed at a running Cockroach cluster",
		)
	}

	ctx := context.Background()

	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	if _, err := c.ExecContext(ctx,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`,
	); err != nil {
		return nil, err
	}

	if _, err := c.ExecContext(ctx, fmt.Sprintf("USE %s", st.database)); err != nil {
		return nil, err
	}

	if _, err := c.ExecContext(ctx,
		fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName),
	); err != nil {
		return nil, err
	}

	// Input is of the form CREATE TABLE <name> AS <query>.
	if _, err := c.ExecContext(ctx, st.d.Input); err != nil {
		return nil, err
	}

	const statName = "s"
	if _, err := c.ExecContext(ctx,
		fmt.Sprintf("CREATE STATISTICS %s FROM %s", statName, tableName),
	); err != nil {
		return nil, err
	}

	rows, err := c.QueryContext(ctx,
		fmt.Sprintf("SELECT column_names, row_count, distinct_count, null_count FROM "+
			"[SHOW STATISTICS FOR TABLE %s] WHERE statistics_name = '%s'", tableName, statName),
	)
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	matrix, err := sqlutils.RowsToStrMatrix(rows)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(cols)*(len(matrix)+1))
	res = append(res, cols...)
	for i := range matrix {
		res = append(res, matrix[i]...)
	}
	return st.formatValues(res, len(cols)), nil
}

type statistic struct {
	rowCount      float64
	distinctCount float64
	nullCount     float64
}

// getActualStatsMap gets a map of statistics keyed by column names.
// It is used to quickly find the actual stats for comparison with estimated
// statistics on a given set of columns.
func (st *statsTester) getActualStatsMap(actualStats []string) (map[string]statistic, error) {
	statsMap := make(map[string]statistic, len(actualStats)-1)

	const (
		colNames = iota
		rowCount
		distinctCount
		nullCount
		numColumns
	)

	// Skip the first line, which contains the column names.
	for i := 1; i < len(actualStats); i++ {
		tokens := strings.Fields(actualStats[i])
		if len(tokens) != numColumns {
			return nil, fmt.Errorf("expected %d values per line but found %d", numColumns, len(tokens))
		}
		rowCount, err := strconv.ParseFloat(strings.TrimSpace(tokens[rowCount]), 64)
		if err != nil {
			return nil, err
		}
		distinctCount, err := strconv.ParseFloat(strings.TrimSpace(tokens[distinctCount]), 64)
		if err != nil {
			return nil, err
		}
		nullCount, err := strconv.ParseFloat(strings.TrimSpace(tokens[nullCount]), 64)
		if err != nil {
			return nil, err
		}

		statsMap[tokens[colNames]] = statistic{
			rowCount:      rowCount,
			distinctCount: distinctCount,
			nullCount:     nullCount,
		}
	}

	return statsMap, nil
}

// fractionErr calculates the estimation error for the given estimated
// and actual values. It returns a value between 0 (no error) and
// 1 (max error).
func (st *statsTester) fractionErr(estimated, actual float64) float64 {
	max := math.Max(estimated, actual)
	if max == 0 {
		return 0
	}
	return math.Abs(estimated-actual) / max
}

// injectStats injects statistics into the given table in the test catalog.
func (st *statsTester) injectStats(tabName string, jsonStats []stats.JSONStatistic) error {
	encoded, err := json.Marshal(jsonStats)
	if err != nil {
		return err
	}
	alterStmt := fmt.Sprintf("ALTER TABLE %s INJECT STATISTICS '%s'", tabName, encoded)
	stmt, err := parser.ParseOne(alterStmt)
	if err != nil {
		return err
	}
	st.catalog.AlterTable(stmt.AST.(*tree.AlterTable))
	return nil
}

// makeStat creates a JSONStatistic for the given columns, rowCount,
// distinctCount, and nullCount.
func (st *statsTester) makeStat(
	columns []string, rowCount, distinctCount, nullCount uint64,
) stats.JSONStatistic {
	return stats.JSONStatistic{
		Name: stats.AutoStatsName,
		CreatedAt: tree.AsStringWithFlags(
			&tree.DTimestamp{Time: timeutil.Now()}, tree.FmtBareStrings,
		),
		Columns:       columns,
		RowCount:      rowCount,
		DistinctCount: distinctCount,
		NullCount:     nullCount,
	}
}

// formatValues formats data in a tabular output format. It is copied from
// sql/logictest/logic.go.
func (st *statsTester) formatValues(vals []string, valsPerLine int) []string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	for line := 0; line < len(vals)/valsPerLine; line++ {
		for i := 0; i < valsPerLine; i++ {
			fmt.Fprintf(tw, "%s\t", vals[line*valsPerLine+i])
		}
		fmt.Fprint(tw, "\n")
	}
	_ = tw.Flush()

	// Split into lines and trim any trailing whitespace.
	// Note that the last line will be empty (which is what we want).
	results := make([]string, 0, len(vals)/valsPerLine)
	for _, s := range strings.Split(buf.String(), "\n") {
		results = append(results, strings.TrimRight(s, " "))
	}
	return results
}
