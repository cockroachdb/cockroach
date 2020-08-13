// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opttester

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
)

// statsTester is used for testing the quality of our estimated statistics
// by comparing the estimated stats for a given expression to the actual stats
// for that expression. See the comments above testStats for more details.
type statsTester struct {
}

// testStats compares actual and estimated stats for a relational expression,
// and returns formatted tabular output of the stats themselves as well as
// the estimation error.
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
// testStats expects that a table with the given tableName representing the
// relational expression has already been created (e.g., by a previous
// invocation of stats-quality in the test file). The table should exist in the
// test catalog with estimated stats already injected.
//
// If rewriteActualStats=true, the table should also exist in the savetables
// database in the running CockroachDB cluster and contain the output of the
// relational expression. If rewriteActualStats=true, testStats will use this
// table to recalculate the actual statistics. Otherwise, it will reuse the
// actual stats in the test output (calculated previously) to compare against
// the estimated stats.
//
func (st statsTester) testStats(
	catalog *testcat.Catalog, prevOutputs []string, tableName, headingSep string,
) (_ string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.AssertionFailedf("%v", r)
		}
	}()

	// Attempt to find a previous stats output corresponding to this table name.
	var prevOutput string
	for i := range prevOutputs {
		split := strings.Split(prevOutputs[i], headingSep)
		if len(split) == 2 && split[0] == tableName {
			prevOutput = split[1]
			break
		}
	}

	const warning = "WARNING: No previous statistics output was found. " +
		"To collect actual statistics,\nrun with the rewrite-actual-stats flag set to true."
	if !*rewriteActualStats &&
		(prevOutput == "" || strings.TrimSuffix(prevOutput, "\n") == warning) {
		// No previous stats output was found.
		return warning + "\n", nil
	}

	// Get the actual stats.
	const sep = "~~~~"
	actualStats, actualStatsMap, err := st.getActualStats(prevOutput, tableName, sep)
	if err != nil {
		return "", err
	}

	// Get the estimated stats, which have been stored in the test catalog.
	// Sort them by column names for consistent test output.
	tab := catalog.Table(tree.NewUnqualifiedTableName(tree.Name(tableName)))
	estimatedStats := tab.Stats
	sort.Slice(estimatedStats, func(i, j int) bool {
		// TODO(rytaft): update this function when we support multi-column stats.
		coli := tab.Column(estimatedStats[i].ColumnOrdinal(0)).ColName()
		colj := tab.Column(estimatedStats[j].ColumnOrdinal(0)).ColName()
		return coli < colj
	})

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
	res := make([]string, len(columns), len(columns)*(len(estimatedStats)+1))
	copy(res, columns)

	format := func(val float64) string {
		return strconv.FormatFloat(val, 'f', 2, 64)
	}
	formatQErr := func(val float64) string {
		// It was shown in "Preventing Bad Plans by Bounding the Impact of
		// Cardinality Estimation Errors" by Moerkotte et al. that q-error less
		// than or equal to 1.9 does not affect plan quality. Mark errors that
		// are above this threshold for easier identification.
		const maxQ = 1.9
		var marker string
		if val > maxQ {
			marker = " <=="
		}
		return format(val) + marker
	}
	for i := 0; i < len(estimatedStats); i++ {
		stat := estimatedStats[i]
		if stat.ColumnCount() != 1 {
			// We don't collect multi-column stats yet, so we can't compare this to
			// anything.
			continue
		}
		col := stat.ColumnOrdinal(0)
		colNames := fmt.Sprintf("{%s}", tab.Column(col).ColName())
		actualStat, ok := actualStatsMap[colNames]
		if !ok {
			return "", fmt.Errorf("could not find actual stat for columns %s", colNames)
		}

		res = append(res,
			colNames,
			format(float64(stat.RowCount())),
			formatQErr(st.qErr(float64(stat.RowCount()), actualStat.rowCount)),
			format(float64(stat.DistinctCount())),
			formatQErr(st.qErr(float64(stat.DistinctCount()), actualStat.distinctCount)),
			format(float64(stat.NullCount())),
			formatQErr(st.qErr(float64(stat.NullCount()), actualStat.nullCount)),
		)
	}

	formattedResults := st.formatValues(res, len(columns))
	return strings.Join(append(append(actualStats, sep), formattedResults...), "\n"), nil
}

// getActualStats gets the actual statistics from the test output or
// recalculates them if rewriteActualStats is true.
// Returns:
// 1. The actual statistics as a slice of strings (one for each row)
// 2. A map from column names to statistic for comparison with the estimated
//    stats.
func (st statsTester) getActualStats(
	prevOutput string, tableName string, sep string,
) ([]string, map[string]statistic, error) {
	expected := strings.Split(prevOutput, sep)
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
// It works by connecting to a running database, calling CREATE STATISTICS on
// the given table, and finally using SHOW STATISTICS to get the result rows.
func (st statsTester) calculateActualStats(tableName string) ([]string, error) {
	db, err := gosql.Open("postgres", *pgurl)
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

	if _, err := c.ExecContext(ctx, fmt.Sprintf("USE %s", opt.SaveTablesDatabase)); err != nil {
		return nil, err
	}

	const statName = "s"
	if _, err := c.ExecContext(ctx,
		fmt.Sprintf("CREATE STATISTICS %s FROM %s", statName, tableName),
	); err != nil {
		return nil, err
	}

	// Exclude stats for rowid since that column was added when the table was
	// created by the saveTableNode. It would not have been part of the original
	// relational expression represented by the table.
	rows, err := c.QueryContext(ctx,
		fmt.Sprintf("SELECT column_names, row_count, distinct_count, null_count FROM "+
			"[SHOW STATISTICS FOR TABLE %s] WHERE statistics_name = '%s' "+
			"AND column_names != '{rowid}'::string[] ORDER BY column_names::string", tableName, statName,
		),
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
func (st statsTester) getActualStatsMap(actualStats []string) (map[string]statistic, error) {
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

// qErr calculates the q-error for the given estimated and actual values.
// q-error is symmetric and multiplicative, and satisfies the formula:
//
//   (1/q) * actual <= estimated <= q * actual
//
// A q-error of 1 is a perfect estimate, and a q-error <= 1.9 is considered
// acceptable.
//
// We use q-error because it is a better predictor of plan quality than
// other error metrics. See "Preventing Bad Plans by Bounding the Impact of
// Cardinality Estimation Errors" by Moerkotte et al. for details.
func (st statsTester) qErr(estimated, actual float64) float64 {
	var min, max float64
	if estimated < actual {
		min, max = estimated, actual
	} else {
		min, max = actual, estimated
	}
	if max == 0 && min == 0 {
		return 1
	}
	return max / min
}

// formatValues formats data in a tabular output format. It is copied from
// sql/logictest/logic.go.
func (st statsTester) formatValues(vals []string, valsPerLine int) []string {
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
