// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

// This example uses a log-style table in an approximation of the
// "fake real time" system used at Friendfeed. Two tables are used: a
// `messages` table stores the complete data for all messages
// organized by channel, and a global `updates` table stores metadata
// about recently-updated channels.
//
// The example currently runs a number of writers, which update both
// tables transactionally. Future work includes implementing a reader
// which will effectively tail the `updates` table to learn when it
// needs to query a channel's messages from the `messages` table.
//
// The implementation currently guarantees that `update_ids` are
// strictly monotonic, which is extremely expensive under contention
// (two writer threads will fight with each other for 20 seconds or
// more before making progress).
//
// One alternate solution is to relax the monotonicity constraint (by
// using a timestmp instead of a sequential ID), at the expense of
// making the reader more complicated (since it would need to re-scan
// records it had already read to see if anything "in the past"
// committed since it last read). We would still need to put some sort
// of bound on the reader's scans; any updates that went longer than
// this without being committed would go unnoticed.
//
// Another alternative is to reduce contention by adding a small
// random number (in range (0, N)) to the beginning of the `updates`
// table primary key. This reduces contention but makes the reader do
// N queries to see all updates. This is effectively what Friendfeed
// did, since there was one `updates` table per MySQL shard.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/montanaflynn/stats"
)

func createTables(db *sql.DB) error {
	statements := []string{
		`create database if not exists fakerealtime`,

		`drop table if exists fakerealtime.updates`,
		`create table fakerealtime.updates (
        update_id int,
        channel string,
        msg_id int,
        primary key (update_id, channel)
    )`,

		`drop table if exists fakerealtime.messages`,
		`create table fakerealtime.messages (
        channel string,
        msg_id int,
        message string,
        primary key (channel, msg_id)
    )`,
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

type statistics struct {
	sync.Mutex
	writeTimes stats.Float64Data
}

func (s *statistics) recordWrite(start time.Time) {
	duration := time.Now().Sub(start)
	s.Lock()
	defer s.Unlock()
	s.writeTimes = append(s.writeTimes, float64(duration.Nanoseconds()))
}

func (s *statistics) report() {
	for range time.Tick(time.Second) {
		s.Lock()
		writeTimes := s.writeTimes
		s.writeTimes = nil
		s.Unlock()

		// The stats functions return an error only when the input is empty.
		mean, _ := stats.Mean(writeTimes)
		stddev, _ := stats.StandardDeviation(writeTimes)
		log.Infof("wrote %d messages, latency mean=%s, stddev=%s",
			len(writeTimes), time.Duration(mean), time.Duration(stddev))
	}
}

type writer struct {
	db          *sql.DB
	numChannels int
	wg          *sync.WaitGroup
	stats       *statistics
}

func (w writer) run() {
	defer w.wg.Done()
	for {
		if err := w.writeMessage(); err != nil {
			log.Errorf("error writing message: %s", err)
		}
	}
}

func (w writer) writeMessage() error {
	start := time.Now()
	defer w.stats.recordWrite(start)
	channel := fmt.Sprintf("room-%d", rand.Int31n(int32(w.numChannels)))
	message := start.String()

	// TODO(bdarnell): retry only on certain errors.
	for {
		txn, err := w.db.Begin()
		if err != nil {
			continue
		}

		// TODO(bdarnell): make this a subquery when subqueries are supported on insert.
		row := txn.QueryRow(`select max(msg_id) from fakerealtime.messages where channel=$1`, channel)
		var maxMsgID sql.NullInt64
		if err := row.Scan(&maxMsgID); err != nil {
			_ = txn.Rollback()
			continue
		}
		if !maxMsgID.Valid {
			maxMsgID.Int64 = 0
		}
		newMsgID := maxMsgID.Int64 + 1

		row = txn.QueryRow(`select max(update_id) from fakerealtime.updates`, channel)
		var maxUpdateID sql.NullInt64
		if err := row.Scan(&maxUpdateID); err != nil {
			_ = txn.Rollback()
			continue
		}
		if !maxUpdateID.Valid {
			maxUpdateID.Int64 = 0
		}
		newUpdateID := maxUpdateID.Int64 + 1

		if _, err := txn.Exec(`insert into fakerealtime.messages (channel, msg_id, message) values ($1, $2, $3)`,
			channel, newMsgID, message); err != nil {
			_ = txn.Rollback()
			continue
		}

		if _, err := txn.Exec(`insert into fakerealtime.updates (update_id, channel, msg_id) values ($1, $2, $3)`,
			newUpdateID, channel, newMsgID); err != nil {
			_ = txn.Rollback()
			continue
		}

		if err := txn.Commit(); err == nil {
			return nil
		}
	}
}

func main() {
	var dbDriver string
	flag.StringVar(&dbDriver, "db-driver", "cockroach", "database driver to use")
	var dbAddr string
	flag.StringVar(&dbAddr, "db-addr", "", "database address. defaults to ephemeral cockroach instance")

	numChannels := flag.Int("num-channels", 100, "number of channels")
	numWriters := flag.Int("num-writers", 2, "number of writers")
	flag.Parse()

	if dbAddr == "" {
		security.SetReadFileFn(securitytest.Asset)
		srv := server.StartTestServer(nil)
		defer srv.Stop()

		dbDriver = "cockroach"
		dbAddr = fmt.Sprintf("https://root@%s?certs=test_certs", srv.ServingAddr())
	}
	db, err := sql.Open(dbDriver, dbAddr)
	if err != nil {
		log.Fatal(err)
	}

	if err := createTables(db); err != nil {
		log.Fatal(err)
	}

	var stats statistics
	var wg sync.WaitGroup
	for i := 0; i < *numWriters; i++ {
		wg.Add(1)
		w := writer{db, *numChannels, &wg, &stats}
		go w.run()
	}
	go stats.report()
	wg.Wait()
}
