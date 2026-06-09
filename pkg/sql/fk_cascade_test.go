// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestFKCascadeWithConcurrentSchemaChange attempts to reproduce the
// assertion failure from issue #143402: "execution requires all update
// columns have a fetch column" during FK cascade execution (ON DELETE
// SET NULL) with a concurrent schema change.
//
// Goroutine 1 loops doing INSERT parent -> INSERT child -> DELETE parent,
// triggering ON DELETE SET NULL cascades. Goroutine 2 concurrently adds and
// drops columns on the child table. With enough iterations the schema change
// can slip through descriptor leasing, causing a version mismatch during
// cascade planning.
//
// Adapted from yuzefovich's reproduction in the GitHub issue.
func TestFKCascadeWithConcurrentSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	setupDB := sqlutils.MakeSQLRunner(db)
	setupDB.Exec(t, `CREATE TABLE parent (id INT8 PRIMARY KEY)`)
	setupDB.Exec(t, `
		CREATE TABLE child (
			id INT8 PRIMARY KEY,
			parent_id INT8 NULL,
			FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL
		)
	`)

	var g errgroup.Group

	g.Go(func() error {
		rng, _ := randutil.NewTestRand()
		for i := 0; i < 1000; i++ {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO parent (id) VALUES (%d)", i)); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO child (id, parent_id) VALUES (%[1]d, %[1]d)", i)); err != nil {
				return err
			}
			if rng.Float64() < 0.5 {
				time.Sleep(time.Millisecond * time.Duration(rng.Intn(10)))
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DELETE FROM parent WHERE id = %d", i)); err != nil {
				return err
			}
		}
		return nil
	})

	g.Go(func() error {
		rng, _ := randutil.NewTestRand()
		db2 := s.SQLConn(t)
		for i := 0; i < 5; i++ {
			if _, err := db2.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE child ADD COLUMN col%d INT NOT NULL DEFAULT 0`, i)); err != nil {
				return err
			}
			if rng.Float64() < 0.5 {
				time.Sleep(time.Millisecond * time.Duration(rng.Intn(10)))
			}
			if rng.Float64() < 0.5 {
				if _, err := db2.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE child DROP COLUMN col%d`, i)); err != nil {
					return err
				}
			}
		}
		return nil
	})

	require.NoError(t, g.Wait())
}
