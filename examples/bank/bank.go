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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/log"
)

var useTransaction = flag.Bool("use-transaction", true, "Turn off to disable transaction.")

// Makes an id string from an id int.
func makeAccountID(id int) []byte {
	return []byte(fmt.Sprintf("%09d", id))
}

// Bank stores all the bank related state.
type Bank struct {
	kvClient     *client.KV
	numAccounts  int
	numTransfers int32
}

// Helper function to read bytes received on a Get Call response.
// We could add this to the API
func readBytes(call client.Call) []byte {
	if gr := call.Reply.(*proto.GetResponse); gr.Value != nil {
		return gr.Value.Bytes
	}
	return nil
}

// Helper function to read an int64 received on a Get Call response
func readInt64(call client.Call) (int64, error) {
	if resp := readBytes(call); resp != nil {
		return strconv.ParseInt(string(resp), 10, 64)
	}
	return 0, nil
}

// Read the balances in all the accounts and return them.
func (bank *Bank) readAllAccountsWithScan() []int64 {
	balances := make([]int64, bank.numAccounts)
	txnOpts := &client.TransactionOptions{Name: "Reading all balances"}
	err := bank.kvClient.RunTransaction(txnOpts, func(txn *client.Txn) error {
		call := client.Scan(makeAccountID(0), makeAccountID(bank.numAccounts), int64(bank.numAccounts))
		if err := txn.Run(call); err != nil {
			log.Fatal(err)
		}
		// Copy responses into balances.
		scan := call.Reply.(*proto.ScanResponse)
		if len(scan.Rows) != bank.numAccounts {
			log.Fatalf("Could only read %d of %d rows of the database.\n", len(scan.Rows), bank.numAccounts)
		}
		for i := 0; i < bank.numAccounts; i++ {
			value, err := strconv.ParseInt(string(scan.Rows[i].Value.Bytes), 10, 64)
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

// Read the balances in all the accounts and return them.
func (bank *Bank) readAllAccounts() []int64 {
	balances := make([]int64, bank.numAccounts)
	calls := make([]client.Call, bank.numAccounts)
	txnOpts := &client.TransactionOptions{Name: "Reading all balances"}
	err := bank.kvClient.RunTransaction(txnOpts, func(txn *client.Txn) error {
		for i := 0; i < bank.numAccounts; i++ {
			calls[i] = client.Get(makeAccountID(i))
		}
		if err := txn.Run(calls...); err != nil {
			log.Fatal(err)
		}
		// Copy responses into balances.
		for i := 0; i < bank.numAccounts; i++ {
			if value, err := readInt64(calls[i]); err != nil {
				log.Fatal(err)
			} else {
				balances[i] = value
			}
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
		transferMoney := func(txn *client.Txn) error {
			fromGet := client.Get(from)
			toGet := client.Get(to)
			if txn != nil {
				if err := txn.Run(fromGet, toGet); err != nil {
					return err
				}
			} else {
				if err := bank.kvClient.Run(fromGet, toGet); err != nil {
					return err
				}
			}
			// Read from value.
			fromValue, err := readInt64(fromGet)
			if err != nil {
				return err
			}
			// Ensure there is enough cash.
			if fromValue < exchangeAmount {
				return nil
			}
			// Read to value.
			toValue, errRead := readInt64(toGet)
			if errRead != nil {
				return errRead
			}
			// Update both accounts.
			fromPut := []byte(fmt.Sprintf("%d", fromValue-exchangeAmount))
			toPut := []byte(fmt.Sprintf("%d", toValue+exchangeAmount))
			if txn != nil {
				txn.Prepare(client.Put(from, fromPut), client.Put(to, toPut))
			} else {
				return bank.kvClient.Run(client.Put(from, fromPut), client.Put(to, toPut))
			}
			return nil
		}
		if *useTransaction {
			txnOpts := &client.TransactionOptions{Name: fmt.Sprintf("Transferring %s-%s-%d", from, to, exchangeAmount)}
			if err := bank.kvClient.RunTransaction(txnOpts, transferMoney); err != nil {
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
	calls := make([]client.Call, bank.numAccounts)
	for i := 0; i < bank.numAccounts; i++ {
		calls[i] = client.Put(makeAccountID(i), []byte(fmt.Sprintf("%d", cash)))
	}
	if err := bank.kvClient.Run(calls...); err != nil {
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
	// Key Value Client initialization.
	sender, err := client.NewHTTPSender(serv.ServingAddr(), testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	bank.kvClient = client.NewKV(nil, sender)
	bank.kvClient.User = storage.UserRoot
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
