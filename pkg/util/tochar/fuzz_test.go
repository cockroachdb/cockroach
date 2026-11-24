// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar_test

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var (
	pgURL        = flag.String("pg-url", "", "PostgreSQL connection URL for fuzz comparison")
	fuzzDuration = flag.Duration("fuzz-duration", 1*time.Minute, "duration to run each fuzz subtest")
)

// TestFuzzFormatting generates random inputs and compares the output of
// data type formatting functions (to_char, to_number, to_date, to_timestamp)
// between CockroachDB and PostgreSQL.
func TestFuzzFormatting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if *pgURL == "" {
		skip.IgnoreLint(t, "-pg-url not provided")
	}

	ctx := context.Background()
	srv, crdbDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	pgDB, err := gosql.Open("postgres", *pgURL)
	require.NoError(t, err)
	defer pgDB.Close()

	// Force UTC on the postgres connection to match CRDB defaults.
	_, err = pgDB.Exec("SET TIME ZONE 'UTC'")
	require.NoError(t, err)

	rng, seed := randutil.NewTestRand()
	t.Logf("random seed: %d", seed)

	t.Run("to_char_numeric", func(t *testing.T) {
		runFuzzSubtest(t, rng, *fuzzDuration, func(t *testing.T, rng *rand.Rand) {
			val := randNumericValue(rng)
			format := randNumericFormat(rng)
			query := fmt.Sprintf("SELECT to_char(%s::numeric, '%s')", val, escapeSQLString(format))
			compareQuery(t, pgDB, crdbDB, query)
		})
	})

	t.Run("to_char_timestamp", func(t *testing.T) {
		runFuzzSubtest(t, rng, *fuzzDuration, func(t *testing.T, rng *rand.Rand) {
			val := randTimestampLiteral(rng)
			format := randDatetimeFormat(rng)
			query := fmt.Sprintf("SELECT to_char(%s::timestamp, '%s')", val, escapeSQLString(format))
			compareQuery(t, pgDB, crdbDB, query)
		})
	})

	t.Run("to_char_interval", func(t *testing.T) {
		runFuzzSubtest(t, rng, *fuzzDuration, func(t *testing.T, rng *rand.Rand) {
			val := randIntervalLiteral(rng)
			format := randDatetimeFormat(rng)
			query := fmt.Sprintf("SELECT to_char(%s::interval, '%s')", val, escapeSQLString(format))
			compareQuery(t, pgDB, crdbDB, query)
		})
	})

	t.Run("to_number", func(t *testing.T) {
		runFuzzSubtest(t, rng, *fuzzDuration, func(t *testing.T, rng *rand.Rand) {
			// Generate a value by formatting a random number with a random
			// format via postgres, then use that (value, format) pair as
			// input to to_number.
			numVal := randNumericValue(rng)
			format := randNumericFormat(rng)
			formatted := queryOneString(pgDB, fmt.Sprintf(
				"SELECT to_char(%s::numeric, '%s')", numVal, escapeSQLString(format),
			))
			if formatted == "" {
				return
			}
			query := fmt.Sprintf(
				"SELECT to_number('%s', '%s')::text",
				escapeSQLString(formatted), escapeSQLString(format),
			)
			compareQuery(t, pgDB, crdbDB, query)
		})
	})

	t.Run("to_timestamp", func(t *testing.T) {
		runFuzzSubtest(t, rng, *fuzzDuration, func(t *testing.T, rng *rand.Rand) {
			// Generate a value by formatting a random timestamp with a random
			// format via postgres, then use that (value, format) pair as
			// input to to_timestamp.
			ts := randTimestampLiteral(rng)
			format := randDatetimeFormat(rng)
			formatted := queryOneString(pgDB, fmt.Sprintf(
				"SELECT to_char(%s::timestamp, '%s')", ts, escapeSQLString(format),
			))
			if formatted == "" {
				return
			}
			query := fmt.Sprintf(
				"SELECT to_timestamp('%s', '%s')::text",
				escapeSQLString(formatted), escapeSQLString(format),
			)
			compareQuery(t, pgDB, crdbDB, query)
		})
	})

	t.Run("to_date", func(t *testing.T) {
		runFuzzSubtest(t, rng, *fuzzDuration, func(t *testing.T, rng *rand.Rand) {
			// Generate a value by formatting a random timestamp with a random
			// format via postgres, then use that (value, format) pair as
			// input to to_date.
			ts := randTimestampLiteral(rng)
			format := randDatetimeFormat(rng)
			formatted := queryOneString(pgDB, fmt.Sprintf(
				"SELECT to_char(%s::timestamp, '%s')", ts, escapeSQLString(format),
			))
			if formatted == "" {
				return
			}
			query := fmt.Sprintf(
				"SELECT to_date('%s', '%s')::text",
				escapeSQLString(formatted), escapeSQLString(format),
			)
			compareQuery(t, pgDB, crdbDB, query)
		})
	})
}

// runFuzzSubtest runs fn repeatedly for the given duration.
func runFuzzSubtest(
	t *testing.T, rng *rand.Rand, duration time.Duration, fn func(t *testing.T, rng *rand.Rand),
) {
	t.Helper()
	deadline := time.Now().Add(duration)
	iterations := 0
	for time.Now().Before(deadline) {
		fn(t, rng)
		iterations++
	}
	t.Logf("completed %d iterations", iterations)
}

// compareQuery runs the same query against both databases and reports
// mismatches. It does not fail the test on mismatch so that fuzzing can
// continue to find more issues.
func compareQuery(t *testing.T, pgDB, crdbDB *gosql.DB, query string) {
	t.Helper()

	var pgResult string
	pgErr := pgDB.QueryRow(query).Scan(&pgResult)

	var crdbResult string
	crdbErr := crdbDB.QueryRow(query).Scan(&crdbResult)

	if pgErr != nil && crdbErr != nil {
		// Both errored; that's fine.
		return
	}
	if pgErr != nil && crdbErr == nil {
		t.Errorf("postgres errored but CRDB succeeded\n  query:  %s\n  pg err: %v\n  crdb:   %q",
			query, pgErr, crdbResult)
		return
	}
	if pgErr == nil && crdbErr != nil {
		t.Errorf("postgres succeeded but CRDB errored\n  query:  %s\n  pg:     %q\n  crdb err: %v",
			query, pgResult, crdbErr)
		return
	}
	if pgResult != crdbResult {
		t.Errorf("result mismatch\n  query: %s\n  pg:    %q\n  crdb:  %q",
			query, pgResult, crdbResult)
	}
}

// queryOneString runs a query and returns the single string result, or empty
// string on any error.
func queryOneString(db *gosql.DB, query string) string {
	var result string
	if err := db.QueryRow(query).Scan(&result); err != nil {
		return ""
	}
	return result
}

// escapeSQLString escapes single quotes for use in SQL string literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// Numeric format elements used by to_char(numeric, text) and to_number.
var numericFormatElements = []string{
	"9", "0", ".", ",", "D", "G",
	"FM", "MI", "PL", "SG", "PR",
	"S", "RN", "TH", "V", "EEEE", "B", "L",
}

// randNumericFormat builds a random numeric format string by concatenating
// 1-5 random format elements.
func randNumericFormat(rng *rand.Rand) string {
	n := rng.Intn(5) + 1
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(numericFormatElements[rng.Intn(len(numericFormatElements))])
	}
	return sb.String()
}

// randNumericValue returns a random numeric value as a string suitable for
// use in a SQL expression.
func randNumericValue(rng *rand.Rand) string {
	values := []func() string{
		// Zero.
		func() string { return "0" },
		// Small ints.
		func() string { return fmt.Sprintf("%d", rng.Intn(1999)-999) },
		// Large ints.
		func() string { return fmt.Sprintf("%d", rng.Intn(20000000)-10000000) },
		// Fractions.
		func() string { return fmt.Sprintf("%.6f", rng.Float64()*2000-1000) },
		// Very small fractions.
		func() string { return fmt.Sprintf("%.10f", (rng.Float64()-0.5)*0.001) },
		// Large values.
		func() string { return fmt.Sprintf("%.2f", (rng.Float64()-0.5)*1e12) },
	}
	return values[rng.Intn(len(values))]()
}

// Datetime format elements.
var datetimeFormatElements = []string{
	"YYYY", "YYY", "YY", "Y", "MM", "MON", "MONTH",
	"DD", "DDD", "D", "DAY", "DY",
	"HH24", "HH12", "HH", "MI", "SS", "MS", "US",
	"FF1", "FF2", "FF3", "FF4", "FF5", "FF6",
	"AM", "PM", "BC", "AD",
	"CC", "Q", "J", "W", "WW", "IW",
	"IYYY", "IY",
	"TH", "FM", "RM",
	"Y,YYY",
}

// Separators used between datetime format elements.
var datetimeSeparators = []string{"-", "/", " ", ":", "."}

// randDatetimeFormat builds a random datetime format string by concatenating
// 1-5 random format elements with optional separators.
func randDatetimeFormat(rng *rand.Rand) string {
	n := rng.Intn(5) + 1
	var sb strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 && rng.Intn(2) == 0 {
			sb.WriteString(datetimeSeparators[rng.Intn(len(datetimeSeparators))])
		}
		elem := datetimeFormatElements[rng.Intn(len(datetimeFormatElements))]
		// Occasionally add quoted literal text.
		if rng.Intn(10) == 0 {
			sb.WriteString(`"`)
			sb.WriteString(elem)
			sb.WriteString(`"`)
		} else {
			sb.WriteString(elem)
		}
	}
	return sb.String()
}

// randTimestampLiteral returns a random timestamp as a SQL timestamp literal
// string. The range is constrained to years 1-9999 for PostgreSQL compat.
func randTimestampLiteral(rng *rand.Rand) string {
	timestamps := []func() string{
		// Epoch.
		func() string { return "'1970-01-01 00:00:00'" },
		// Recent dates.
		func() string {
			year := 2000 + rng.Intn(26)
			month := rng.Intn(12) + 1
			day := rng.Intn(28) + 1
			hour := rng.Intn(24)
			min := rng.Intn(60)
			sec := rng.Intn(60)
			us := rng.Intn(1000000)
			return fmt.Sprintf("'%04d-%02d-%02d %02d:%02d:%02d.%06d'",
				year, month, day, hour, min, sec, us)
		},
		// Far past (but > year 1).
		func() string {
			year := rng.Intn(1969) + 1
			month := rng.Intn(12) + 1
			day := rng.Intn(28) + 1
			return fmt.Sprintf("'%04d-%02d-%02d 12:00:00'", year, month, day)
		},
		// Far future (< year 9999).
		func() string {
			year := 2026 + rng.Intn(7974)
			month := rng.Intn(12) + 1
			day := rng.Intn(28) + 1
			return fmt.Sprintf("'%04d-%02d-%02d 12:00:00'", year, month, day)
		},
		// Various times of day with sub-second precision.
		func() string {
			hour := rng.Intn(24)
			min := rng.Intn(60)
			sec := rng.Intn(60)
			ms := rng.Intn(1000)
			return fmt.Sprintf("'2024-06-15 %02d:%02d:%02d.%03d'", hour, min, sec, ms)
		},
	}
	return timestamps[rng.Intn(len(timestamps))]()
}

// randIntervalLiteral returns a random interval as a SQL interval literal.
func randIntervalLiteral(rng *rand.Rand) string {
	intervals := []func() string{
		// Zero.
		func() string { return "'0'" },
		// Positive durations.
		func() string {
			return fmt.Sprintf("'%d days %d hours %d minutes %d seconds'",
				rng.Intn(365), rng.Intn(24), rng.Intn(60), rng.Intn(60))
		},
		// Negative durations.
		func() string {
			return fmt.Sprintf("'-%d days -%d hours'", rng.Intn(365), rng.Intn(24))
		},
		// Large months.
		func() string {
			return fmt.Sprintf("'%d months %d days'", rng.Intn(1200)-600, rng.Intn(60)-30)
		},
		// Mixed signs.
		func() string {
			return fmt.Sprintf("'%d years %d months -%d days %d hours'",
				rng.Intn(100)-50, rng.Intn(12), rng.Intn(30), rng.Intn(24))
		},
	}
	return intervals[rng.Intn(len(intervals))]()
}
