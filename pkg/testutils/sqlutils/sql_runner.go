// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// SQLRunner wraps a Fataler and *gosql.DB connection and provides
// convenience functions to run SQL statements and fail the test on any errors.
type SQLRunner struct {
	DB                   DBHandle
	SucceedsSoonDuration time.Duration // defaults to testutils.DefaultSucceedsSoonDuration
}

// DBHandle is an interface that applies to *gosql.DB, *gosql.Conn, and
// *gosql.Tx, as well as *RoundRobinDBHandle.
type DBHandle interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (gosql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*gosql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *gosql.Row
}

var _ DBHandle = &gosql.DB{}
var _ DBHandle = &gosql.Conn{}
var _ DBHandle = &gosql.Tx{}

// MakeSQLRunner returns a SQLRunner for the given database connection.
// The argument can be a *gosql.DB, *gosql.Conn, or *gosql.Tx object.
func MakeSQLRunner(db DBHandle) *SQLRunner {
	return &SQLRunner{DB: db}
}

// MakeRoundRobinSQLRunner returns a SQLRunner that uses a set of database
// connections, in a round-robin fashion.
func MakeRoundRobinSQLRunner(dbs ...DBHandle) *SQLRunner {
	return MakeSQLRunner(MakeRoundRobinDBHandle(dbs...))
}

type helperI interface {
	Helper()
}

func helperOrNoop(t interface{}) func() {
	if ht, ok := t.(helperI); ok {
		return ht.Helper
	}
	return func() {}
}

// Fataler is the subset of testing.TB relevant to this package.
type Fataler interface {
	Fatalf(string, ...interface{})
}

// Exec is a wrapper around gosql.Exec that kills the test on error.
func (sr *SQLRunner) Exec(t Fataler, query string, args ...interface{}) gosql.Result {
	helperOrNoop(t)()
	r, err := sr.DB.ExecContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("error executing '%s': %s", query, err)
	}
	return r
}

// ExecMultiple is a wrapper around gosql.Exec that executes multiple statements
// and kills the test on error.
func (sr *SQLRunner) ExecMultiple(t Fataler, queries ...string) {
	helperOrNoop(t)()
	for _, query := range queries {
		_, err := sr.DB.ExecContext(context.Background(), query)
		if err != nil {
			t.Fatalf("error executing '%s': %s", query, err)
		}
	}
}

type requireT struct {
	Fataler
}

func (t requireT) Errorf(format string, args ...interface{}) {
	t.Fatalf(format, args...)
}

func (t requireT) FailNow() {
	t.Fatalf("failing")
}

func (sr *SQLRunner) succeedsWithin(t Fataler, f func() error) {
	helperOrNoop(t)()
	d := sr.SucceedsSoonDuration
	if d == 0 {
		d = testutils.DefaultSucceedsSoonDuration
	}
	require.NoError(requireT{t}, testutils.SucceedsWithinError(f, d))
}

// ExecSucceedsSoon is a wrapper around gosql.Exec that wraps
// the exec in a succeeds soon.
func (sr *SQLRunner) ExecSucceedsSoon(t Fataler, query string, args ...interface{}) {
	helperOrNoop(t)()
	sr.succeedsWithin(t, func() error {
		_, err := sr.DB.ExecContext(context.Background(), query, args...)
		return err
	})
}

// ExecRowsAffected executes the statement and verifies that RowsAffected()
// matches the expected value. It kills the test on errors.
func (sr *SQLRunner) ExecRowsAffected(
	t Fataler, expRowsAffected int, query string, args ...interface{},
) {
	helperOrNoop(t)()
	r := sr.Exec(t, query, args...)
	numRows, err := r.RowsAffected()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if numRows != int64(expRowsAffected) {
		t.Fatalf("expected %d affected rows, got %d on '%s'", expRowsAffected, numRows, query)
	}
}

// ExpectErr runs the given statement and verifies that it returns an error
// matching the given regex.
func (sr *SQLRunner) ExpectErr(t Fataler, errRE string, query string, args ...interface{}) {
	helperOrNoop(t)()
	_, err := sr.DB.ExecContext(context.Background(), query, args...)
	sr.expectErr(t, err, errRE)
}

func (sr *SQLRunner) expectErr(t Fataler, err error, errRE string) {
	helperOrNoop(t)()
	if !testutils.IsError(err, errRE) {
		s := "nil"
		if err != nil {
			s = pgerror.FullError(err)
		}
		t.Fatalf("expected error '%s', got: %s", errRE, s)
	}
}

// ExpectErrWithHint runs the given statement and verifies that it returns an error
// with a hint matching the expected hint.
func (sr *SQLRunner) ExpectErrWithHint(
	t Fataler, errRE string, hintRE string, query string, args ...interface{},
) {
	helperOrNoop(t)()
	_, err := sr.DB.ExecContext(context.Background(), query, args...)

	sr.expectErr(t, err, errRE)

	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		matched, merr := regexp.MatchString(hintRE, pqErr.Hint)
		if !matched || merr != nil {
			t.Fatalf("expected error with hint '%s', but got error with hint '%s'", hintRE, pqErr.Hint)
		}
	} else {
		t.Fatalf("could not parse pq error")
	}
}

// ExpectErrSucceedsSoon wraps ExpectErr with a SucceedsSoon.
func (sr *SQLRunner) ExpectErrSucceedsSoon(
	t Fataler, errRE string, query string, args ...interface{},
) {
	helperOrNoop(t)()
	sr.succeedsWithin(t, func() error {
		_, err := sr.DB.ExecContext(context.Background(), query, args...)
		if !testutils.IsError(err, errRE) {
			return errors.Newf("expected error '%s', got: %s", errRE, pgerror.FullError(err))
		}
		return nil
	})
}

// Query is a wrapper around gosql.Query that kills the test on error.
func (sr *SQLRunner) Query(t Fataler, query string, args ...interface{}) *gosql.Rows {
	helperOrNoop(t)()
	r, err := sr.DB.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("error executing '%s': %s", query, err)
	}
	return r
}

// Row is a wrapper around gosql.Row that kills the test on error.
type Row struct {
	Fataler
	row *gosql.Row
}

// Scan is a wrapper around (*gosql.Row).Scan that kills the test on error.
func (r *Row) Scan(dest ...interface{}) {
	helperOrNoop(r.Fataler)()
	if err := r.row.Scan(dest...); err != nil {
		r.Fatalf("error scanning '%v': %+v", r.row, err)
	}
}

// QueryRow is a wrapper around gosql.QueryRow that kills the test on error.
func (sr *SQLRunner) QueryRow(t Fataler, query string, args ...interface{}) *Row {
	helperOrNoop(t)()
	return &Row{t, sr.DB.QueryRowContext(context.Background(), query, args...)}
}

// QueryStr runs a Query and converts the result using RowsToStrMatrix. Kills
// the test on errors.
func (sr *SQLRunner) QueryStr(t Fataler, query string, args ...interface{}) [][]string {
	helperOrNoop(t)()
	rows := sr.Query(t, query, args...)
	r, err := RowsToStrMatrix(rows)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return r
}

// RowsToStrMatrix converts the given result rows to a string matrix; nulls are
// represented as "NULL". Empty results are represented by an empty (but
// non-nil) slice.
func RowsToStrMatrix(rows *gosql.Rows) ([][]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	res := [][]string{}
	const maxRowsForMatrix = 200_000
	for rows.Next() {
		if len(res) > maxRowsForMatrix {
			rows.Close()
			return nil, errors.Errorf("More than %d rows in result set!\n", maxRowsForMatrix)
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, err
		}
		row := make([]string, len(vals))
		for j, v := range vals {
			if val := *v.(*interface{}); val != nil {
				switch t := val.(type) {
				case []byte:
					row[j] = string(t)
				default:
					row[j] = fmt.Sprint(val)
				}
			} else {
				row[j] = "NULL"
			}
		}
		res = append(res, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

// MatrixToStr converts a set of rows into a single string where each row is on
// a separate line and the columns with a row are comma separated.
func MatrixToStr(rows [][]string) string {
	res := strings.Builder{}
	for _, row := range rows {
		res.WriteString(strings.Join(row, ", "))
		res.WriteRune('\n')
	}
	return res.String()
}

// CheckQueryResults checks that the rows returned by a query match the expected
// response.
func (sr *SQLRunner) CheckQueryResults(t Fataler, query string, expected [][]string) {
	helperOrNoop(t)()
	res := sr.QueryStr(t, query)
	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("query '%s': expected:\n%v\ngot:\n%v\n",
			query, MatrixToStr(expected), MatrixToStr(res),
		)
	}
}

// CheckQueryResultsRetry checks that the rows returned by a query match the
// expected response. If the results don't match right away, it will retry
// using testutils.SucceedsSoon.
func (sr *SQLRunner) CheckQueryResultsRetry(t Fataler, query string, expected [][]string) {
	helperOrNoop(t)()
	sr.succeedsWithin(t, func() error {
		res := sr.QueryStr(t, query)
		if !reflect.DeepEqual(res, expected) {
			return errors.Errorf("query '%s': expected:\n%v\ngot:\n%v\n",
				query, MatrixToStr(expected), MatrixToStr(res),
			)
		}
		return nil
	})
}

// RoundRobinDBHandle aggregates multiple DBHandles into a single one; each time
// a query is issued, a handle is selected in round-robin fashion.
type RoundRobinDBHandle struct {
	handles []DBHandle
	current int
}

var _ DBHandle = &RoundRobinDBHandle{}

// MakeRoundRobinDBHandle creates a RoundRobinDBHandle.
func MakeRoundRobinDBHandle(handles ...DBHandle) *RoundRobinDBHandle {
	return &RoundRobinDBHandle{handles: handles}
}

func (rr *RoundRobinDBHandle) next() DBHandle {
	h := rr.handles[rr.current]
	rr.current = (rr.current + 1) % len(rr.handles)
	return h
}

// ExecContext is part of the DBHandle interface.
func (rr *RoundRobinDBHandle) ExecContext(
	ctx context.Context, query string, args ...interface{},
) (gosql.Result, error) {
	return rr.next().ExecContext(ctx, query, args...)
}

// QueryContext is part of the DBHandle interface.
func (rr *RoundRobinDBHandle) QueryContext(
	ctx context.Context, query string, args ...interface{},
) (*gosql.Rows, error) {
	return rr.next().QueryContext(ctx, query, args...)
}

// QueryRowContext is part of the DBHandle interface.
func (rr *RoundRobinDBHandle) QueryRowContext(
	ctx context.Context, query string, args ...interface{},
) *gosql.Row {
	return rr.next().QueryRowContext(ctx, query, args...)
}
