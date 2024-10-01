// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// RunQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
func (sqlExecCtx *Context) RunQuery(
	ctx context.Context, conn clisqlclient.Conn, fn clisqlclient.QueryFn, showMoreChars bool,
) (retCols []string, retRows [][]string, retErr error) {
	rows, _, err := fn(ctx, conn)
	if err != nil {
		return nil, nil, err
	}

	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
	return sqlRowsToStrings(rows, showMoreChars)
}

// RunQueryAndFormatResults takes a 'query' with optional 'parameters'.
// It runs the sql query and writes output to 'w'.
// Timings is enabled are written to 'tw'.
// Errors and warnings, if any, are printed to 'ew'.
func (sqlExecCtx *Context) RunQueryAndFormatResults(
	ctx context.Context, conn clisqlclient.Conn, w, tw, ew io.Writer, fn clisqlclient.QueryFn,
) (err error) {
	startTime := timeutil.Now()
	rows, isMultiStatementQuery, err := fn(ctx, conn)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := rows.Close()
		err = errors.CombineErrors(err, closeErr)
	}()
	for {
		// pgx is not able to tell us before the first call to Next()
		// whether a statement returns either
		// - a rows result set with zero rows (e.g. SELECT on an empty table), or
		// - no rows result set, but a valid value for RowsAffected (e.g. INSERT), or
		// - doesn't return any rows whatsoever (e.g. SET).
		//
		// To distinguish them we must go through Next() somehow, which is what the
		// render() function does. So we ask render() to call this noRowsHook
		// when Next() has completed its work and no rows where observed, to decide
		// what to do.
		noRowsHook := func() (bool, error) {
			tag, err := rows.Tag()
			if err != nil {
				return false, err
			}
			nRows := tag.RowsAffected()
			tagString := tag.String()

			// This may be either something like INSERT with a valid
			// RowsAffected value, or a statement like SET.
			if strings.HasPrefix(tagString, "SELECT") && nRows == 0 {
				// As explained above, the pgx driver unhelpfully does not
				// distinguish between a statement returning zero rows and a
				// statement returning an affected row count of zero.
				// noRowsHook is called non-discriminatingly for both
				// situations.
				return false, nil
			}
			// SET, etc.: just print the tag, or OK if there's no tag.
			// INSERT, DELETE, etc. all contain the row count in the tag itself.
			if tagString == "" {
				tagString = "OK"
			}
			fmt.Fprintln(w, tagString)
			return true, nil
		}

		cols := getColumnStrings(rows, true /* showMoreChars */)
		reporter, cleanup, err := sqlExecCtx.makeReporter(w)
		if err != nil {
			return err
		}

		var queryCompleteTime time.Time
		completedHook := func() { queryCompleteTime = timeutil.Now() }

		if err := func() error {
			if cleanup != nil {
				defer cleanup()
			}
			return render(reporter, w, ew, cols, newRowIter(rows, true /* showMoreChars */), completedHook, noRowsHook)
		}(); err != nil {
			return err
		}

		if more, err := rows.NextResultSet(); err != nil {
			return err
		} else if !more {
			// We must call maybeShowTimes after rows has been closed, which is after
			// NextResultSet returns false.
			sqlExecCtx.maybeShowTimes(ctx, conn, tw, ew, isMultiStatementQuery, startTime, queryCompleteTime)
			return nil
		}
	}
}

// maybeShowTimes displays the execution time if show_times has been set.
func (sqlExecCtx *Context) maybeShowTimes(
	ctx context.Context,
	conn clisqlclient.Conn,
	w, ew io.Writer,
	isMultiStatementQuery bool,
	startTime, queryCompleteTime time.Time,
) {
	if !sqlExecCtx.ShowTimes {
		return
	}

	defer func() {
		// If there was noticeable overhead, let the user know.
		renderDelay := timeutil.Since(queryCompleteTime)
		if renderDelay >= 1*time.Second && sqlExecCtx.IsInteractive() {
			fmt.Fprintf(ew,
				"\nNote: an additional delay of %s was spent formatting the results.\n"+
					"You can use \\set display_format to change the formatting.\n",
				renderDelay)
		}
		// An additional empty line as separator.
		fmt.Fprintln(w)
	}()

	clientSideQueryLatency := queryCompleteTime.Sub(startTime)
	// We don't print timings for multi-statement queries as we don't have an
	// accurate way to measure them currently. See #48180.
	if isMultiStatementQuery {
		// No need to print if no one's watching.
		if sqlExecCtx.IsInteractive() {
			fmt.Fprintf(ew, "\nNote: timings for multiple statements on a single line are not supported. See %s.\n",
				build.MakeIssueURL(48180))
		}
		return
	}

	// Print a newline early. This provides a discreet visual
	// feedback that execution finished, and that the next line of
	// output will be a warning or execution time(s).
	fmt.Fprintln(w)

	// We accumulate the timing details into a buffer prior to emitting
	// them to the output stream, so as to avoid interleaving warnings
	// or SQL notices with the full timing string.
	var stats strings.Builder

	// Suggested by Radu: for sub-second results, show simplified
	// timings in milliseconds.
	unit := "s"
	multiplier := 1.
	precision := 3
	if clientSideQueryLatency.Seconds() < 1 {
		unit = "ms"
		multiplier = 1000.
		precision = 0
	}

	if sqlExecCtx.VerboseTimings {
		fmt.Fprintf(&stats, "Time: %s", clientSideQueryLatency)
	} else {
		// Simplified displays: human users typically can't
		// distinguish sub-millisecond latencies.
		fmt.Fprintf(&stats, "Time: %.*f%s", precision, clientSideQueryLatency.Seconds()*multiplier, unit)
	}

	// If discrete server/network timings are available, also print them.
	detailedStats, err := conn.GetLastQueryStatistics(ctx)
	if err != nil {
		fmt.Fprintln(w, stats.String())
		fmt.Fprintf(ew, "\nwarning: %v", err)
		return
	}
	if !detailedStats.Enabled {
		fmt.Fprintln(w, stats.String())
		return
	}

	fmt.Fprint(&stats, " total")

	containsJobLat := detailedStats.PostCommitJobs.Valid
	parseLat := detailedStats.Parse.Value
	serviceLat := detailedStats.Service.Value
	planLat := detailedStats.Plan.Value
	execLat := detailedStats.Exec.Value
	jobsLat := detailedStats.PostCommitJobs.Value

	networkLat := clientSideQueryLatency - (serviceLat + jobsLat)
	// serviceLat can be greater than clientSideQueryLatency for some extremely quick
	// statements (eg. BEGIN). So as to not confuse the user, we attribute all of
	// the clientSideQueryLatency to the network in such cases.
	if networkLat.Seconds() < 0 {
		networkLat = clientSideQueryLatency
	}
	otherLat := serviceLat - parseLat - planLat - execLat
	if sqlExecCtx.VerboseTimings {
		// Only display schema change latency if the server provided that
		// information to not confuse users.
		// TODO(arul): this can be removed in 22.1.
		if containsJobLat {
			fmt.Fprintf(&stats, " (parse %s / plan %s / exec %s / schema change %s / other %s / network %s)",
				parseLat, planLat, execLat, jobsLat, otherLat, networkLat)
		} else {
			fmt.Fprintf(&stats, " (parse %s / plan %s / exec %s / other %s / network %s)",
				parseLat, planLat, execLat, otherLat, networkLat)
		}
	} else {
		// Simplified display: just show the execution/network breakdown.
		//
		// Note: we omit the report details for queries that
		// last for a millisecond or less. This is because for such
		// small queries, the detail is just noise to the human observer.
		sep := " ("
		reportTiming := func(label string, lat time.Duration) {
			fmt.Fprintf(&stats, "%s%s %.*f%s", sep, label, precision, lat.Seconds()*multiplier, unit)
			sep = " / "
		}
		reportTiming("execution", serviceLat+jobsLat)
		reportTiming("network", networkLat)
		fmt.Fprint(&stats, ")")
	}
	fmt.Fprintln(w, stats.String())
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a list of column values.
// 'rows' should be closed by the caller.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
func sqlRowsToStrings(rows clisqlclient.Rows, showMoreChars bool) ([]string, [][]string, error) {
	cols := getColumnStrings(rows, showMoreChars)
	allRows, err := getAllRowStrings(rows, showMoreChars)
	if err != nil {
		return nil, nil, err
	}
	return cols, allRows, nil
}

func getColumnStrings(rows clisqlclient.Rows, showMoreChars bool) []string {
	srcCols := rows.Columns()
	cols := make([]string, len(srcCols))
	for i, c := range srcCols {
		cols[i] = FormatVal(c, showMoreChars, showMoreChars)
	}
	return cols
}
