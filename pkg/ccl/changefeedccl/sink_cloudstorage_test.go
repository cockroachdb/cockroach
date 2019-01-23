// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TODO(dan): More extensive cloudStorageSink testing.
// - multi node cluster
// - job restarts
// - ValidationsTest (+ maybe chaos?)
func TestCloudStorageSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	flushCh := make(chan struct{}, 1)
	defer close(flushCh)
	knobs := base.TestingKnobs{DistSQL: &distsqlrun.TestingKnobs{Changefeed: &TestingKnobs{
		AfterSinkFlush: func() error {
			select {
			case flushCh <- struct{}{}:
			default:
			}
			return nil
		},
	}}}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase:   "d",
		ExternalIODir: dir,
		Knobs:         knobs,
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)

	f := makeCloud(s, db, dir, flushCh)

	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

	foo := f.Feed(t, `CREATE CHANGEFEED FOR foo WITH resolved, envelope=value_only`)

	sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'b')`)

	assertPayloads(t, foo, []string{
		`foo: ->{"a": 1}`,
		`foo: ->{"a": 2, "b": "b"}`,
	})
}
