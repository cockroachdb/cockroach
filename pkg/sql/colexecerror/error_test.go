// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecerror_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCatchVectorizedRuntimeError verifies that the panic-catcher doesn't catch
// panics that originate outside of the vectorized engine and correctly
// annotates errors that are propagated via
// colexecerror.(Internal|Expected)Error methods.
func TestCatchVectorizedRuntimeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Setup multiple levels of catchers to ensure that the panic-catcher
	// doesn't fool itself into catching panics that the inner catcher emitted.
	require.Panics(t, func() {
		require.NoError(t, colexecerror.CatchVectorizedRuntimeError(func() {
			require.NoError(t, colexecerror.CatchVectorizedRuntimeError(func() {
				colexecerror.NonCatchablePanic(errors.New("should not be caught"))
			}))
		}))
	})

	const shouldBeCaughtText = "should be caught"
	shouldBeCaughtErr := errors.New(shouldBeCaughtText)
	const annotationText = "unexpected error from the vectorized engine"

	// Propagate an error as an internal one (this should add annotations to the
	// returned error).
	annotatedErr := colexecerror.CatchVectorizedRuntimeError(func() {
		colexecerror.InternalError(shouldBeCaughtErr)
	})
	require.NotNil(t, annotatedErr)
	require.True(t, strings.Contains(annotatedErr.Error(), shouldBeCaughtText))
	require.True(t, strings.Contains(annotatedErr.Error(), annotationText))

	// Propagate an error as an expected one (this should *not* add annotations
	// to the returned error).
	notAnnotatedErr := colexecerror.CatchVectorizedRuntimeError(func() {
		colexecerror.ExpectedError(shouldBeCaughtErr)
	})
	require.NotNil(t, notAnnotatedErr)
	require.True(t, strings.Contains(notAnnotatedErr.Error(), shouldBeCaughtText))
	require.False(t, strings.Contains(notAnnotatedErr.Error(), annotationText))
}

// TestNonCatchablePanicIsNotCaught verifies that panics emitted via
// NonCatchablePanic() method are not caught by the catcher.
func TestNonCatchablePanicIsNotCaught(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.Panics(t, func() {
		require.NoError(t, colexecerror.CatchVectorizedRuntimeError(func() {
			colexecerror.NonCatchablePanic("should panic")
		}))
	})
}

// BenchmarkCatchVectorizedRuntimeError measures the time for
// CatchVectorizedRuntimeError to catch and process an error.
func BenchmarkCatchVectorizedRuntimeError(b *testing.B) {
	err := errors.New("oops")
	storageErr := colexecerror.NewStorageError(err)
	pgErr := pgerror.WithCandidateCode(err, pgcode.Warning)

	cases := []struct {
		name    string
		thrower func()
	}{
		{
			"noError",
			func() {},
		},
		{
			"expected",
			func() {
				colexecerror.ExpectedError(err)
			},
		},
		{
			"storage",
			func() {
				colexecerror.InternalError(storageErr)
			},
		},
		{
			"contextCanceled",
			func() {
				colexecerror.InternalError(context.Canceled)
			},
		},
		{
			"internalWithCode",
			func() {
				colexecerror.InternalError(pgErr)
			},
		},
		{
			"internal",
			func() {
				colexecerror.InternalError(err)
			},
		},
		{
			"runtime",
			func() {
				arr := []int{0, 1, 2}
				_ = arr[3]
			},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = colexecerror.CatchVectorizedRuntimeError(tc.thrower)
				}
			})
		})
	}
}

// BenchmarkSQLCatchVectorizedRuntimeError measures the time for
// CatchVectorizedRuntimeError to catch and process an error with a deeper stack
// than in BenchmarkCatchVectorizedRuntimeError.
func BenchmarkSQLCatchVectorizedRuntimeError(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	cases := []struct {
		name    string
		builtin string
	}{
		{
			"noError",
			"crdb_internal.void_func()",
		},
		{
			"expectedWithCode",
			"crdb_internal.force_error('01000', 'oops')",
		},
		{
			"expectedAssertion",
			"crdb_internal.force_assertion_error('oops')",
		},
		{
			"internalAssertion",
			"crdb_internal.force_panic('oops', 'internalAssertion')",
		},
		{
			"internalIndexOutOfRange",
			"crdb_internal.force_panic('oops', 'indexOutOfRange')",
		},
		{
			"internalDivideByZero",
			"crdb_internal.force_panic('oops', 'divideByZero')",
		},
		{
			"contextCanceled",
			"crdb_internal.force_panic('oops', 'contextCanceled')",
		},
	}

	// We execute this SELECT statement with various error-producing
	// builtins. Ordering the projection this way creates a moderately deep stack
	// with several nested calls to CatchVectorizedRuntimeError.
	sqlFmt := `SELECT count(%s) OVER (),
  0,
  '',
  0.0,
  NULL,
  '2000-01-01 00:00:00'::timestamptz,
  b'00000000',
  i + 0,
  i * 1.5,
  i / 100
  FROM generate_series(0, 0) AS s(i)
`

	ctx := context.Background()
	s := serverutils.StartServerOnly(b, base.TestServerArgs{SQLMemoryPoolSize: 10 << 30})
	defer s.Stopper().Stop(ctx)

	for _, parallelism := range []int{1, 20, 50} {
		numConns := runtime.GOMAXPROCS(0) * parallelism
		b.Run(fmt.Sprintf("conns=%d", numConns), func(b *testing.B) {
			for _, tc := range cases {
				stmt := fmt.Sprintf(sqlFmt, tc.builtin)
				b.Run(tc.name, func(b *testing.B) {
					// Create as many warm connections as we will need for the benchmark.
					conns := make(chan *gosql.DB, numConns)
					for i := 0; i < numConns; i++ {
						conn := s.ApplicationLayer().SQLConn(b, serverutils.DBName(""))
						// Make sure we're using local, vectorized execution.
						sqlDB := sqlutils.MakeSQLRunner(conn)
						sqlDB.Exec(b, "SET distsql = off")
						sqlDB.Exec(b, "SET vectorize = on")
						// Warm up the connection by executing the statement once. We should
						// always go through the query plan cache after this.
						_, _ = conn.Exec(stmt)
						conns <- conn
					}
					b.SetParallelism(parallelism)
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						var conn *gosql.DB
						select {
						case conn = <-conns:
						default:
							b.Fatal("not enough warm connections")
						}
						for pb.Next() {
							_, _ = conn.Exec(stmt)
						}
					})
				})
			}
		})
	}
}
