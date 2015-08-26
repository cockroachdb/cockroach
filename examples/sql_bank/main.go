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
// Author: Tamir Duberstein

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/log"
)

var maxTransfer = flag.Int("max-transfer", 999, "Maximum amount to transfer in one transaction.")
var numAccounts = flag.Int("num-accounts", 999, "Number of accounts.")
var concurrency = flag.Int("concurrency", 5, "Number of concurrent actors moving money.")
var aggregate = flag.Bool("aggregate", true, "Use aggregate function to verify conservation of money.")
var usePostgres = flag.Bool("use-postgres", false, "Use postgres instead of cockroach.")

var numTransfers uint64

func moveMoney(db *sql.DB) {
	for {
		from, to := rand.Intn(*numAccounts), rand.Intn(*numAccounts)
		if from == to {
			continue
		}
		amount := rand.Intn(*maxTransfer)
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		startTime := time.Now()
		// Query is very slow and will be fixed by https://github.com/cockroachdb/cockroach/issues/2140
		query := fmt.Sprintf("SELECT id, balance FROM accounts WHERE id IN (%d, %d)", from, to)
		rows, err := tx.Query(query)
		elapsed := time.Now().Sub(startTime)
		if elapsed > 10*time.Millisecond {
			log.Infof("%s took %v", query, elapsed)
		}
		if err != nil {
			if log.V(1) {
				log.Warning(err)
			}
			if err = tx.Rollback(); err != nil {
				log.Fatal(err)
			}
			continue
		}
		var fromBalance, toBalance int
		for rows.Next() {
			var id, balance int
			if err = rows.Scan(&id, &balance); err != nil {
				log.Fatal(err)
			}
			switch id {
			case from:
				fromBalance = balance
			case to:
				toBalance = balance
			default:
				panic(fmt.Sprintf("got unexpected account %d", id))
			}
		}
		startTime = time.Now()
		update := fmt.Sprintf("UPDATE accounts SET balance=%d WHERE id=%d", fromBalance-amount, from)
		if _, err = tx.Exec(update); err != nil {
			if log.V(1) {
				log.Warning(err)
			}
			if err = tx.Rollback(); err != nil {
				log.Fatal(err)
			}
			continue
		}
		elapsed = time.Now().Sub(startTime)
		if elapsed > 50*time.Millisecond {
			log.Infof("%s took %v", update, elapsed)
		}
		update = fmt.Sprintf("UPDATE accounts SET balance=%d WHERE id=%d", toBalance+amount, to)
		if _, err = tx.Exec(update); err != nil {
			if log.V(1) {
				log.Warning(err)
			}
			if err = tx.Rollback(); err != nil {
				log.Fatal(err)
			}
			continue
		}
		elapsed = time.Now().Sub(startTime)
		if elapsed > 50*time.Millisecond {
			log.Infof("%s took %v", update, elapsed)
		}

		if err = tx.Commit(); err != nil {
			if log.V(1) {
				log.Warning(err)
			}
		} else {
			atomic.AddUint64(&numTransfers, 1)
		}
	}
}

func verifyBank(db *sql.DB) {
	var sum int64
	if *aggregate {
		if err := db.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&sum); err != nil {
			log.Fatal(err)
		}
	} else {
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		rows, err := tx.Query("SELECT balance FROM accounts")
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			var balance int64
			if err = rows.Scan(&balance); err != nil {
				log.Fatal(err)
			}
			sum += balance
		}
		if err = tx.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	if sum == 0 {
		log.Info("The bank is in good order.")
	} else {
		log.Fatalf("The bank is not in good order. Total value: %d", sum)
	}
}

func main() {
	flag.Parse()
	var db *sql.DB
	var err error
	var url string
	if *usePostgres {
		if db, err = sql.Open("postgres", "dbname=postgres sslmode=disable"); err != nil {
			log.Fatal(err)
		}
	} else {
		security.SetReadFileFn(securitytest.Asset)
		serv := server.StartTestServer(nil)
		defer serv.Stop()
		url = "https://root@" + serv.ServingAddr() + "?certs=test_certs"
		if db, err = sql.Open("cockroach", url); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE DATABASE bank"); err != nil {
		if pqErr, ok := err.(*pq.Error); *usePostgres && (!ok || pqErr.Code.Name() != "duplicate_database") {
			log.Fatal(err)
		}
	}
	db.Close()

	// Open db client with database settings.
	if *usePostgres {
		if db, err = sql.Open("postgres", "dbname=bank sslmode=disable"); err != nil {
			log.Fatal(err)
		}
	} else {
		if db, err = sql.Open("cockroach", url+"&database=bank"); err != nil {
			log.Fatal(err)
		}
	}

	// concurrency + 1, for this thread and the "concurrency" number of
	// goroutines that move money
	db.SetMaxOpenConns(*concurrency + 1)

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)"); err != nil {
		log.Fatal(err)
	}

	if _, err = db.Exec("TRUNCATE TABLE accounts"); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < *numAccounts; i++ {
		if _, err = db.Exec("INSERT INTO accounts (id, balance) VALUES ($1, $2)", i, 0); err != nil {
			log.Fatal(err)
		}
	}

	verifyBank(db)

	var lastNumTransfers uint64
	lastNow := time.Now()

	for i := 0; i < *concurrency; i++ {
		go moveMoney(db)
	}

	for range time.NewTicker(time.Second).C {
		now := time.Now()
		elapsed := time.Since(lastNow)
		numTransfers := atomic.LoadUint64(&numTransfers)
		log.Infof("%d transfers were executed at %.1f/second.", (numTransfers - lastNumTransfers), float64(numTransfers-lastNumTransfers)/elapsed.Seconds())
		verifyBank(db)
		lastNumTransfers = numTransfers
		lastNow = now
	}
}
