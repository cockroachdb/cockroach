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
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/util/log"
)

const (
	maxTransfer = 999
	numAccounts = 999
	concurrency = 25
	aggregate   = true
)

var numTransfers uint64

func moveMoney(db *sql.DB) {
	for {
		from, to := rand.Intn(numAccounts), rand.Intn(numAccounts)
		if from == to {
			continue
		}
		amount := rand.Intn(maxTransfer)
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		if _, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
			log.Fatal(err)
		}
		rows, err := tx.Query("SELECT id, balance FROM accounts WHERE id IN ($1, $2)", from, to)
		if err != nil {
			log.Fatal(err)
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

		if _, err = tx.Exec("UPDATE accounts SET balance=$2 WHERE id=$1", from, fromBalance-amount); err != nil {
			if log.V(1) {
				log.Warning(err)
			}
			if err = tx.Rollback(); err != nil {
				log.Fatal(err)
			}
			continue
		}
		if _, err = tx.Exec("UPDATE accounts SET balance=$2 WHERE id=$1", to, toBalance+amount); err != nil {
			if log.V(1) {
				log.Warning(err)
			}
			if err = tx.Rollback(); err != nil {
				log.Fatal(err)
			}
			continue
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

	if aggregate {
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
	db, err := sql.Open("postgres", "dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	if _, err = db.Exec("CREATE DATABASE bank"); err != nil {
		if pqErr, ok := err.(*pq.Error); !ok || pqErr.Code.Name() != "duplicate_database" {
			log.Fatal(err)
		}
	}

	db, err = sql.Open("postgres", "dbname=bank sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(concurrency)

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS accounts (id BIGINT UNIQUE NOT NULL, balance BIGINT NOT NULL)"); err != nil {
		log.Fatal(err)
	}

	if _, err = db.Exec("TRUNCATE TABLE accounts"); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < numAccounts; i++ {
		if _, err = db.Exec("INSERT INTO accounts (id, balance) VALUES ($1, $2)", i, 0); err != nil {
			log.Fatal(err)
		}
	}

	verifyBank(db)

	var lastNumTransfers uint64
	lastNow := time.Now()

	for i := 0; i < concurrency; i++ {
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
