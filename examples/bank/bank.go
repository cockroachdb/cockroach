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
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/log"
)

var dbName = flag.String("db-name", "", "Name/URL of the distributed database backend.")
var useTransaction = flag.Bool("use-transaction", true, "Turn off to disable transaction.")

// These two flags configure a range of accounts over which the program functions.
var firstAccount = flag.Int("first-account", 0, "First account in the account range.")
var numAccounts = flag.Int("num-accounts", 1000, "Number of accounts in the account range.")

var numParallelTransfers = flag.Int("num-parallel-transfers", 100, "Number of parallel transfers.")

// Bank stores all the bank related state.
type Bank struct {
	db *client.DB
	// First account in the account range.
	firstAccount int
	// Total number of accounts.
	numAccounts  int
	numTransfers int32
}

// Account holds all the customers account information
type Account struct {
	Balance int64
	Err     error
}

func (a Account) encode() []byte {
	result, err := json.Marshal(a)
	if err != nil {
		a.Err = err
	}
	return result
}

func (a *Account) decode(b []byte) {
	err := json.Unmarshal(b, a)
	if err != nil {
		a.Err = err
	}
}

// Makes an id string from an id int.
func (bank *Bank) makeAccountID(id int) []byte {
	return []byte(fmt.Sprintf("%09d", bank.firstAccount+id))
}

// Read the balances in all the accounts and return them.
func (bank *Bank) sumAllAccounts() int64 {
	var result int64
	err := bank.db.Tx(func(tx *client.Tx) error {
		scan, err := tx.Scan(bank.makeAccountID(0), bank.makeAccountID(bank.numAccounts), int64(bank.numAccounts))
		if err != nil {
			return err
		}
		if len(scan.Rows) != bank.numAccounts {
			return fmt.Errorf("Could only read %d of %d rows of the database.\n", len(scan.Rows), bank.numAccounts)
		}
		// Sum up the balances.
		account := &Account{}
		for i := 0; i < bank.numAccounts; i++ {
			account.decode(scan.Rows[i].ValueBytes())
			// fmt.Printf("Account %d contains %d$\n", bank.firstAccount+i, account.Balance)
			result += account.Balance
		}
		return account.Err
	})
	if err != nil {
		log.Fatal(err)
	}
	return result
}

// continuouslyTransferMoney() keeps moving random amounts between
// random accounts.
func (bank *Bank) continuousMoneyTransfer(cash int64) {
	for {
		from := bank.makeAccountID(rand.Intn(bank.numAccounts))
		to := bank.makeAccountID(rand.Intn(bank.numAccounts))
		// Continue when from == to
		if bytes.Equal(from, to) {
			continue
		}
		exchangeAmount := rand.Int63n(cash)
		// transferMoney transfers exchangeAmount between the two accounts
		transferMoney := func(runner client.Runner) error {
			batchRead := &client.Batch{}
			batchRead.Get(from, to)
			if err := runner.Run(batchRead); err != nil {
				return err
			}
			// Read from value.
			fromAccount := &Account{}
			fromAccount.decode(batchRead.Results[0].Rows[0].ValueBytes())
			// Read to value.
			toAccount := &Account{}
			toAccount.decode(batchRead.Results[0].Rows[1].ValueBytes())
			// Update both accounts.
			fromAccount.Balance -= exchangeAmount
			toAccount.Balance += exchangeAmount
			fromValue := fromAccount.encode()
			toValue := toAccount.encode()
			if fromAccount.Err != nil {
				return fromAccount.Err
			}
			if toAccount.Err != nil {
				return toAccount.Err
			}
			// Ensure there is enough cash.
			if fromAccount.Balance < 0 {
				return nil
			}
			batchWrite := &client.Batch{}
			batchWrite.Put(from, fromValue).Put(to, toValue)
			return runner.Run(batchWrite)
		}
		var err error
		if *useTransaction {
			err = bank.db.Tx(func(tx *client.Tx) error { return transferMoney(tx) })
		} else {
			err = transferMoney(bank.db)
		}
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt32(&bank.numTransfers, 1)
	}
}

// Initialize all the bank accounts with cash.
// When multiple instances of the bank app are running,
// only one gets to initialize the bank.
func (bank *Bank) initBankAccounts(cash int64) {
	if err := bank.db.Tx(func(tx *client.Tx) error {
		// Check if the accounts have been initialized by another instance
		if scan, err := tx.Scan(bank.makeAccountID(0), bank.makeAccountID(bank.numAccounts), int64(bank.numAccounts)); err != nil {
			return err
		} else if len(scan.Rows) == bank.numAccounts {
			log.Warning("accounts have already been initialized")
			return nil
		} else if len(scan.Rows) > 0 {
			// TODO(vivek): recover from this error
			return fmt.Errorf("%d of %d accounts of the database are initialized", len(scan.Rows), bank.numAccounts)
		}
		// Let's initialize all the accounts
		batch := &client.Batch{}
		account := Account{Balance: cash}
		var err error
		value := account.encode()
		if account.Err != nil {
			return err
		}
		for i := 0; i < bank.numAccounts; i++ {
			batch.Put(bank.makeAccountID(i), value)
		}
		return tx.Run(batch)
	}); err != nil {
		log.Fatal(err)
	}
	log.Info("done initializing all accounts")
}

func (bank *Bank) periodicallyCheckBalances(initCash int64) {
	for {
		// Sleep for a bit to allow money transfers to happen in the background.
		time.Sleep(time.Second)
		fmt.Printf("%d transfers were executed.\n\n", bank.numTransfers)
		// Check that all the money is accounted for.
		totalAmount := bank.sumAllAccounts()
		if totalAmount != int64(bank.numAccounts)*initCash {
			log.Fatal(fmt.Errorf("\nTotal cash in the bank = %d.\n", totalAmount))
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
	// Initialize the bank.
	var bank Bank
	bank.firstAccount = *firstAccount
	bank.numAccounts = *numAccounts
	if *dbName == "" {
		// Run a test cockroach instance to represent the bank.
		security.SetReadFileFn(securitytest.Asset)
		serv := server.StartTestServer(nil)
		defer serv.Stop()
		*dbName = "https://root@" + serv.ServingAddr() + "?certs=test_certs"
	}
	// Create a database handle
	db, err := client.Open(*dbName)
	if err != nil {
		log.Fatal(err)
	}
	bank.db = db
	// Initialize all the bank accounts.
	const initCash = 1000
	bank.initBankAccounts(initCash)

	// Start all the money transfer routines.
	for i := 0; i < *numParallelTransfers; i++ {
		// Keep transferring upto 10% of initCash between accounts.
		go bank.continuousMoneyTransfer(initCash / 10)
	}

	bank.periodicallyCheckBalances(initCash)
}
