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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
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

	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			DB:       srv.InternalDB().(descs.DB),
			Settings: srv.ClusterSettings(),
			Codec:    srv.Codec(),
		},
		EvalCtx: &eval.Context{
			Codec:    srv.Codec(),
			Settings: srv.ClusterSettings(),
		},
	}

	const initialChunkSize int64 = 50000
	oomErr := mon.NewMemoryBudgetExceededError(1, 1, 1)
	nonOomErr := sqlerrors.NewUndefinedUserError(username.NodeUserName())

	for _, tc := range []struct {
		desc              string
		errs              []error
		expectedErr       error
		expectedChunkSize int64
	}{
		{"happy-path", nil, nil, initialChunkSize},
		{"retry-once", []error{oomErr}, nil, initialChunkSize >> 1},
		{"retry-then-fail", []error{oomErr, oomErr, nonOomErr}, nonOomErr, initialChunkSize >> 2},
		{"retry-exhaustive", []error{oomErr, oomErr, oomErr, oomErr}, oomErr, initialChunkSize >> 3},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			i := -1
			retryOpts := retry.Options{
				InitialBackoff: 2 * time.Millisecond,
				Multiplier:     2,
				MaxRetries:     2,
				MaxBackoff:     10 * time.Millisecond,
			}
			// Use a custom builder function that returns errors from the test case.
			builder := func(ctx context.Context,
				txn *kv.Txn,
				tableDesc catalog.TableDescriptor,
				sp roachpb.Span,
				chunkSize int64,
				traceKV bool,
			) ([]rowenc.IndexEntry, roachpb.Key, int64, error) {
				i++
				if i < len(tc.errs) {
					return nil, nil, 0, tc.errs[i]
				}
				return nil, nil, 0, nil
			}

			ib := &indexBackfiller{
				flowCtx: &flowCtx,
				spec:    execinfrapb.BackfillerSpec{ChunkSize: initialChunkSize},
			}
			_, _, _, err := ib.buildIndexEntryBatchWithRetry(ctx, roachpb.Span{}, srv.Clock().Now(), retryOpts, builder)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedChunkSize, ib.spec.ChunkSize)
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
