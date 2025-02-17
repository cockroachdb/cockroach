// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func TestRetryOfIndexEntryBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	db := srv.SystemLayer().InternalDB().(isql.DB)

	const initialChunkSize int64 = 50000
	oomErr := mon.NewMemoryBudgetExceededError(1, 1, 1)
	nonOomErr := sqlerrors.NewUndefinedUserError(username.NodeUserName())

	for _, tc := range []struct {
		desc              string
		errs              []error
		retryErr          error
		expectedErr       error
		expectedChunkSize int64
	}{
		{"happy-path", nil, nil, nil, initialChunkSize},
		{"retry-once", []error{oomErr}, nil, nil, initialChunkSize >> 1},
		{"retry-then-fail", []error{oomErr, oomErr, nonOomErr}, nil, nonOomErr, initialChunkSize >> 2},
		{"retry-exhaustive", []error{oomErr, oomErr, oomErr, oomErr}, nil, oomErr, initialChunkSize >> 3},
		{"retry-error", []error{oomErr}, nonOomErr, oomErr, initialChunkSize},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			i := 0
			br := indexBatchRetry{
				nextChunkSize: initialChunkSize,
				retryOpts: retry.Options{
					InitialBackoff: 2 * time.Millisecond,
					Multiplier:     2,
					MaxRetries:     2,
					MaxBackoff:     10 * time.Millisecond,
				},
				buildIndexChunk: func(ctx context.Context, txn isql.Txn) error {
					if i < len(tc.errs) {
						return tc.errs[i]
					}
					return nil
				},
				resetForNextAttempt: func(ctx context.Context) error {
					i++
					return tc.retryErr
				},
			}
			err := br.buildBatchWithRetry(ctx, db)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedChunkSize, br.nextChunkSize)
		})
	}
}

func BenchmarkIndexBackfill(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	stopTimer := func() {}
	startTimer := func() {}

	dir, dirCleanupFn := testutils.TempDir(b)
	defer dirCleanupFn()

	srv, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				// Disable the randomization of some batch sizes to get more
				// consistent results.
				ForceProductionValues: true,
			},
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				BeforeStage: func(p scplan.Plan, stageIdx int) error {
					s := p.Stages[stageIdx]
					if s.Phase == scop.PostCommitPhase && s.Type() == scop.BackfillType {
						if _, ok := s.Ops()[0].(*scop.BackfillIndex); ok {
							startTimer()
						}
					}
					return nil
				},
				AfterStage: func(p scplan.Plan, stageIdx int) error {
					s := p.Stages[stageIdx]
					if s.Phase == scop.PostCommitPhase && s.Type() == scop.BackfillType {
						if _, ok := s.Ops()[0].(*scop.BackfillIndex); ok {
							stopTimer()
						}
					}
					return nil
				},
			},
		},
		StoreSpecs: []base.StoreSpec{{InMemory: false, Path: filepath.Join(dir, "testserver")}},
	})
	defer srv.Stopper().Stop(ctx)

	for _, tc := range []struct {
		desc    string
		numRows int
	}{
		{"100 rows", 100},
		{"10,000 rows", 10000},
		{"1,000,000 rows", 1000000},
	} {
		b.Run(tc.desc, func(b *testing.B) {
			_, err := sqlDB.Exec("CREATE TABLE t (k INT PRIMARY KEY, v INT)")
			if err != nil {
				b.Fatal(err)
			}
			_, err = sqlDB.Exec("INSERT INTO t SELECT generate_series(1, $1), 1", tc.numRows)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			runningCreateIndex := false
			timerStopped := 0
			startTimer = func() {
				if runningCreateIndex {
					b.StartTimer()
				}
			}
			stopTimer = func() {
				if runningCreateIndex {
					b.StopTimer()
					timerStopped++
				}
			}

			for i := 0; i < b.N; i++ {
				runningCreateIndex = true
				_, err = sqlDB.Exec("CREATE INDEX idx ON t (v)")
				if err != nil {
					b.Fatal(err)
				}
				runningCreateIndex = false
				_, err = sqlDB.Exec("DROP INDEX idx")
				if err != nil {
					b.Fatal(err)
				}
			}

			// Sanity check that we called startTimer and stopTimer the correct
			// number of times.
			if timerStopped != b.N {
				b.Fatalf("expected %d calls to stopTimer, got %d", b.N, timerStopped)
			}

			_, err = sqlDB.Exec("DROP TABLE t")
			if err != nil {
				b.Fatal(err)
			}
		})
	}
}
