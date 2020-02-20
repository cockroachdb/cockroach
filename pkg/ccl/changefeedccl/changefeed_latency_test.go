// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestChangefeedLatency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = 't'")
	sqlDB.Exec(t, "CREATE TABLE events (id UUID NOT NULL PRIMARY KEY, ts TIMESTAMPTZ);")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := ctxgroup.WithContext(ctx)
	type readEvent struct {
		id     string
		ts     time.Time
		readAt time.Time
	}
	const N = 100
	readCh := make(chan readEvent, N)
	firstResolved := make(chan struct{})
	type ChangefeedEvent struct {
		Resolved *string
		After    *struct {
			ID string
			TS time.Time
		}
		Updated string
	}

	g.GoCtx(func(ctx context.Context) error {
		rows, err := db.QueryContext(ctx, "CREATE CHANGEFEED FOR events WITH updated, resolved='1s'")
		if err != nil {
			return err
		}
		defer close(readCh)
		defer func() { _ = rows.Close() }()
		defer sqlDB.Exec(t, "CANCEL SESSIONS (SELECT session_id FROM [SHOW SESSIONS] "+
			"WHERE active_queries LIKE '%CHANGEFEED%' "+
			"AND active_queries NOT LIKE '%SESSIONS%' );")
		var read int
		var resolved int
		for rows.Next() {
			var cfEv ChangefeedEvent
			var table, key sql.NullString
			var value string
			if err := rows.Scan(&table, &key, &value); err != nil {
				return err
			}
			fmt.Println(value)
			json.NewDecoder(strings.NewReader(value)).Decode(&cfEv)
			fmt.Println(cfEv)

			if cfEv.Resolved != nil {
				if resolved++; resolved >= 2 {
					close(firstResolved)
				}
				fmt.Println("woo!", *cfEv.Resolved)

			} else if cfEv.After != nil {
				fmt.Println("hi!", cfEv)
				read++
				readCh <- readEvent{
					id:     cfEv.After.ID,
					ts:     cfEv.After.TS,
					readAt: timeutil.Now(),
				}
				if read == N {
					break
				}
			} else {
				fmt.Println("2i!", cfEv)
			}
		}
		fmt.Println("done")
		return rows.Err()
	})

	g.GoCtx(func(ctx context.Context) error {
		for i := 0; i < N; i++ {
			select {
			case r, ok := <-readCh:
				if !ok {
					return nil
				}
				const limit = 200 * time.Millisecond
				if took := r.readAt.Sub(r.ts); took > limit {
					t.Errorf("expected event in %v, got %v", took, limit)
				}
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})
	<-firstResolved
	for i := 0; i < N; i++ {
		id := uuid.MakeV4()
		sqlDB.Exec(t, "INSERT INTO events VALUES ($1, $2)", id.String(), timeutil.Now())
	}
	require.NoError(t, g.Wait())
}
