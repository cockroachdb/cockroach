// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package acceptance

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/util/log"
)

func TestMonotonicInserts(t *testing.T) {
	runTestOnConfigs(t, testMonotonicInsertsInner)
}

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
		if ok := i == 0 || r.Less(i-1, i); !ok {
			prefix = "!!"
		}
		_, _ = buf.WriteString(fmt.Sprintf("%s %+v\n", prefix, row))
	}
	return buf.String()
}

type mtClient struct {
	*sql.DB
	ID int
}

func testMonotonicInsertsInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	var clients []mtClient
	for i := 0; i < c.NumNodes(); i++ {
		clients = append(clients, mtClient{ID: i, DB: makePGClient(t, c.PGUrl(i))})
	}
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
			log.Infof(logPrefix+msg, args...)
		}
		l("begin")
		defer l("done")

		var exRow, insRow mtRow
		var attempt int
		if err := crdb.ExecuteTx(client.DB, func(tx *sql.Tx) error {
			attempt++
			l("attempt %d", attempt)
			if err := tx.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(
				&insRow.sts,
			); err != nil {
				l(err.Error())
				return err
			}

			l("read max val")
			if err := tx.QueryRow(`SELECT MAX(val) AS m FROM mono.mono`).Scan(
				&exRow.val,
			); err != nil {
				l(err.Error())
				return err
			}

			sleepDuration := time.Duration(rand.Int63n(int64(10 * time.Millisecond)))
			time.Sleep(sleepDuration)

			l("read max row for val=%d after sleeping %s", exRow.val, sleepDuration)
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
		}); err != nil && !strings.Contains(err.Error(), "refused") {
			t.Errorf("%T: %v", err, err)
		}
	}

	verify := func() {
		client := clients[0]
		var numDistinct int
		if err := client.QueryRow("SELECT COUNT(DISTINCT(val)) FROM mono.mono").Scan(
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
			t.Error("results are not sorted")
		}

		if numDistinct != len(results) {
			t.Error("'val' column is not unique: %d results, but %d distinct",
				len(results), numDistinct)
		}

		fmt.Println(results)
	}

	const concurrency = 10

	sem := make(chan struct{}, concurrency)
	timer := time.After(cfg.Duration)

	defer verify()
	defer func() {
		// Now that consuming has stopped, fill up the semaphore (i.e. wait for
		// still-running goroutines to stop)
		for i := 0; i < concurrency; i++ {
			sem <- struct{}{}
		}
	}()

	for {
		select {
		case sem <- struct{}{}:
		case <-stopper:
			return
		case <-timer:
			return
		}
		go func(client mtClient) {
			invoke(client)
			<-sem
		}(clients[rand.Intn(c.NumNodes())])
	}
}
