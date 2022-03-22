// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type mtRow struct {
	val      int64
	sts      string
	node, tb int64
}

type mtRows []mtRow

func (r mtRows) Len() int {
	return len(r)
}

func (r mtRows) Less(i, j int) bool {
	if r[i].val == r[j].val {
		return r[i].sts < r[j].sts
	}
	return r[i].val < r[j].val
}

func (r mtRows) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r mtRows) String() string {
	var buf bytes.Buffer
	for i, row := range r {
		prefix := "ok"
		if i > 0 && r.Less(i, i-1) {
			prefix = "!!"
		}
		fmt.Fprintf(&buf, "%s %+v\n", prefix, row)
	}
	return buf.String()
}

type mtClient struct {
	*gosql.DB
	ID int
}

// TestMonotonicInserts replicates the 'monotonic' test from the Jepsen
// CockroachDB test suite:
//   https://github.com/jepsen-io/jepsen/blob/master/cockroachdb/src/jepsen/cockroach/monotonic.clj
func TestMonotonicInserts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 68784, "flaky test")

	for _, distSQLMode := range []sessiondatapb.DistSQLExecMode{
		sessiondatapb.DistSQLOff, sessiondatapb.DistSQLOn,
	} {
		t.Run(fmt.Sprintf("distsql=%s", distSQLMode), func(t *testing.T) {
			testMonotonicInserts(t, distSQLMode)
		})
	}
}

func testMonotonicInserts(t *testing.T, distSQLMode sessiondatapb.DistSQLExecMode) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs:      base.TestServerArgs{},
		},
	)
	defer tc.Stopper().Stop(ctx)

	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		sql.DistSQLClusterExecMode.Override(ctx, &st.SV, int64(distSQLMode))
		// Let transactions push immediately to detect deadlocks. The test creates a
		// large amount of contention and dependency cycles, and could take a long
		// time to complete without this.
		concurrency.LockTableDeadlockDetectionPushDelay.Override(ctx, &st.SV, 0)
	}

	var clients []mtClient
	for i := range tc.Conns {
		clients = append(clients, mtClient{ID: i, DB: tc.Conns[i]})
	}
	// We will insert into this table by selecting MAX(val) and increasing by
	// one and expect that val and sts (the commit timestamp) are both
	// simultaneously increasing.
	if _, err := clients[0].Exec(`
CREATE DATABASE mono;
CREATE TABLE IF NOT EXISTS mono.mono (val INT, sts STRING, node INT, tb INT);
INSERT INTO mono.mono VALUES(-1, '0', -1, -1)`); err != nil {
		t.Fatal(err)
	}

	var idGen uint64

	invoke := func(client mtClient) {
		logPrefix := fmt.Sprintf("%03d.%03d: ", atomic.AddUint64(&idGen, 1), client.ID)
		l := func(msg string, args ...interface{}) {
			log.Infof(ctx, logPrefix+msg /* nolint:fmtsafe */, args...)
		}
		l("begin")
		defer l("done")

		var exRow, insRow mtRow
		var attempt int
		if err := crdb.ExecuteTx(ctx, client.DB, nil, func(tx *gosql.Tx) error {
			attempt++
			l("attempt %d", attempt)
			if err := tx.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(
				&insRow.sts,
			); err != nil {
				l(err.Error())
				return err
			}

			l("read max val")
			if err := tx.QueryRow(`SELECT max(val) AS m FROM mono.mono`).Scan(
				&exRow.val,
			); err != nil {
				l(err.Error())
				return err
			}

			l("read max row for val=%d", exRow.val)
			if err := tx.QueryRow(`SELECT sts, node, tb FROM mono.mono WHERE val = $1`,
				exRow.val,
			).Scan(
				&exRow.sts, &exRow.node, &exRow.tb,
			); err != nil {
				l(err.Error())
				return err
			}

			l("insert")
			if err := tx.QueryRow(`
INSERT INTO mono.mono (val, sts, node, tb) VALUES($1, $2, $3, $4)
RETURNING val, sts, node, tb`,
				exRow.val+1, insRow.sts, client.ID, 0,
			).Scan(
				&insRow.val, &insRow.sts, &insRow.node, &insRow.tb,
			); err != nil {
				l(err.Error())
				return err
			}
			l("commit")
			return nil
		}); err != nil {
			t.Errorf("%T: %v", err, err)
		}
	}

	verify := func() {
		client := clients[0]
		var numDistinct int
		if err := client.QueryRow("SELECT count(DISTINCT(val)) FROM mono.mono").Scan(
			&numDistinct,
		); err != nil {
			t.Fatal(err)
		}
		rows, err := client.Query("SELECT val, sts, node, tb FROM mono.mono ORDER BY val ASC, sts ASC")
		if err != nil {
			t.Fatal(err)
		}
		var results mtRows
		for rows.Next() {
			var row mtRow
			if err := rows.Scan(&row.val, &row.sts, &row.node, &row.tb); err != nil {
				t.Fatal(err)
			}
			results = append(results, row)
		}

		if !sort.IsSorted(results) {
			t.Errorf("results are not sorted:\n%s", results)
		}

		if numDistinct != len(results) {
			t.Errorf("'val' column is not unique: %d results, but %d distinct:\n%s",
				len(results), numDistinct, results)
		}
	}

	sem := make(chan struct{}, 2*len(tc.Conns))
	timer := time.After(5 * time.Second)

	defer verify()
	defer func() {
		// Now that consuming has stopped, fill up the semaphore (i.e. wait for
		// still-running goroutines to stop)
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
	}()

	for {
		select {
		case sem <- struct{}{}:
		case <-tc.Stopper().ShouldQuiesce():
			return
		case <-timer:
			return
		}
		go func(client mtClient) {
			invoke(client)
			<-sem
		}(clients[rand.Intn(len(clients))])
	}
}
