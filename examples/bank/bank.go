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
// Author: Vivek Menezes (vivek.menezes@gmail.com)

package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/log"
)

var useTransaction = flag.Bool("use-transaction", true, "Turn off to disable transaction.")

// Makes an id string from an id int.
func makeAccountID(id int) []byte {
	return []byte(fmt.Sprintf("%09d", id))
}

// Bank stores all the bank related state.
type Bank struct {
	db           *client.DB
	numAccounts  int
	numTransfers int32
}

// Read the balances in all the accounts and return them.
func (bank *Bank) readAllAccounts() []int64 {
	balances := make([]int64, bank.numAccounts)
	err := bank.db.Tx(func(tx *client.Tx) error {
		scan := tx.Scan(makeAccountID(0), makeAccountID(bank.numAccounts), int64(bank.numAccounts))
		if scan.Err != nil {
			log.Fatal(scan.Err)
		}
		if len(scan.Rows) != bank.numAccounts {
			log.Fatalf("Could only read %d of %d rows of the database.\n", len(scan.Rows), bank.numAccounts)
		}
		// Copy responses into balances.
		for i := 0; i < bank.numAccounts; i++ {
			value, err := strconv.ParseInt(string(scan.Rows[i].ValueBytes()), 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			balances[i] = value
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return balances
}

// continuouslyTransferMoney() keeps moving random amounts between
// random accounts.
func (bank *Bank) continuousMoneyTransfer() {
	for {
		from := makeAccountID(rand.Intn(bank.numAccounts))
		to := makeAccountID(rand.Intn(bank.numAccounts))
		// Continue when from == to
		if bytes.Equal(from, to) {
			continue
		}
		exchangeAmount := rand.Int63n(100)
		// transferMoney transfers exchangeAmount between the two accounts
		// using transaction txn if its non nil
		transferMoney := func(tx *client.Tx) error {
			batchRead := &client.Batch{}
			batchRead.Get(from).Get(to)
			if tx != nil {
				if err := tx.Run(batchRead); err != nil {
					return err
				}
			} else {
				if err := bank.db.Run(batchRead); err != nil {
					return err
				}
			}
			// Read from value.
			if batchRead.Results[0].Err != nil {
				return batchRead.Results[0].Err
			}
			fromValue, err := strconv.ParseInt(string(batchRead.Results[0].Rows[0].ValueBytes()), 10, 64)
			if err != nil {
				return err
			}
			// Ensure there is enough cash.
			if fromValue < exchangeAmount {
				return nil
			}
			// Read to value.
			if batchRead.Results[1].Err != nil {
				return batchRead.Results[1].Err
			}
			toValue, errRead := strconv.ParseInt(string(batchRead.Results[1].Rows[0].ValueBytes()), 10, 64)
			if errRead != nil {
				return errRead
			}
			// Update both accounts.
			batchWrite := &client.Batch{}
			batchWrite.Put(from, fmt.Sprintf("%d", fromValue-exchangeAmount))
			batchWrite.Put(to, fmt.Sprintf("%d", toValue+exchangeAmount))
			if tx != nil {
				if err := tx.Run(batchWrite); err != nil {
					return err
				}
			} else {
				return bank.db.Run(batchWrite)
			}
			return nil
		}
		if *useTransaction {
			if err := bank.db.Tx(transferMoney); err != nil {
				log.Fatal(err)
			}
		} else if err := transferMoney(nil); err != nil {
			log.Fatal(err)
		}
		atomic.AddInt32(&bank.numTransfers, 1)
	}
}

// Initialize all the bank accounts with cash.
func (bank *Bank) initBankAccounts(cash int64) {
	batch := &client.Batch{}
	for i := 0; i < bank.numAccounts; i++ {
		batch = batch.Put(makeAccountID(i), fmt.Sprintf("%d", cash))
	}
	if err := bank.db.Run(batch); err != nil {
		log.Fatal(err)
	}
}

func (bank *Bank) periodicallyCheckBalances(initCash int64) {
	for {
		// Sleep for a bit to allow money transfers to happen in the background.
		time.Sleep(time.Second)
		fmt.Printf("%d transfers were executed.\n\n", bank.numTransfers)
		// Check that all the money is accounted for.
		balances := bank.readAllAccounts()
		var totalAmount int64
		for i := 0; i < bank.numAccounts; i++ {
			// fmt.Printf("Account %d contains %d$\n", i, balances[i])
			totalAmount += balances[i]
		}
		if totalAmount != int64(bank.numAccounts)*initCash {
			err := fmt.Sprintf("\nTotal cash in the bank = %d.\n", totalAmount)
			log.Fatal(err)
		}
		fmt.Printf("\nThe bank is in good order\n\n")
	}
}

func main() {
	fmt.Printf("A simple program that keeps moving money between bank accounts.\n\n")
	flag.Parse()
	if !*useTransaction {
		fmt.Printf("Use of a transaction has been disabled.\n")
	}
	// Run a test cockroach instance to represent the bank.
	security.SetReadFileFn(securitytest.Asset)
	serv := server.StartTestServer(nil)
	defer serv.Stop()
	// Initialize the bank.
	var bank Bank
	bank.numAccounts = 1000
	// Create a database handle
	db, err := client.Open("https://root@" + serv.ServingAddr() + "?certs=test_certs")
	if err != nil {
		log.Fatal(err)
	}
	bank.db = db
	// Initialize all the bank accounts.
	const initCash = 1000
	bank.initBankAccounts(initCash)

	// Start all the money transfer routines.
	const numTransferRoutines = 10
	for i := 0; i < numTransferRoutines; i++ {
		go bank.continuousMoneyTransfer()
	}

	bank.periodicallyCheckBalances(initCash)
}
