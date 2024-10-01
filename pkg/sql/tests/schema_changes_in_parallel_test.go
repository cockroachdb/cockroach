// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestSchemaChangesInParallel exists to try to shake out races in the
// declarative schema changer infrastructure. At its time of writing, it
// effectively reproduced a race in the rules engine's object pooling.
func TestSchemaChangesInParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			GCJob: &sql.GCJobTestingKnobs{
				SkipWaitingForMVCCGC: true,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
		SQLMemoryPoolSize: 1 << 30, /* 1GiB */
	})
	defer s.Stopper().Stop(ctx)

	const N = 4
	run := func(i int) func() (retErr error) {
		return func() (retErr error) {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			defer func() {
				retErr = errors.CombineErrors(retErr, conn.Close())
			}()
			for _, stmt := range []string{
				fmt.Sprintf("CREATE DATABASE db%d", i),
				fmt.Sprintf("USE db%d", i),
				"CREATE TABLE t (i INT PRIMARY KEY, k INT)",
				"ALTER TABLE t ADD COLUMN j INT DEFAULT 42",
				"ALTER TABLE t DROP COLUMN k",
				"CREATE SEQUENCE s",
				"ALTER TABLE t ADD COLUMN l INT DEFAULT nextval('s')",
				fmt.Sprintf("DROP DATABASE db%d", i),
			} {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					return err
				}
			}
			return nil
		}
	}
	var g errgroup.Group
	for i := 0; i < N; i++ {
		g.Go(run(i))
	}
	require.NoError(t, g.Wait())
}
