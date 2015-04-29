// Copyright 2014 The Cockroach Authors.
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
	"fmt"
	"math/rand"
	"strconv"
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

// makes an id string from an id int.
func makeAccountID(id int) []byte {
	return []byte(fmt.Sprintf("%09d", id))
}

// Bank stores all the bank related state.
type Bank struct {
	servingAddr string
	numAccounts int
	// This is not protected, but we really don't care.
	approxNumTransactions int
}

// moveMoney() moves an amount between two accounts if the amount
// is available in the from account. Returns true on success.
func (bank *Bank) moveMoney(kvClient *client.KV, from, to []byte, amount int64) bool {
	txnOpts := &client.TransactionOptions{Name: fmt.Sprintf("Transferring %d", amount)}
	err := kvClient.RunTransaction(txnOpts, func(txn *client.Txn) error {
		call := client.Get(proto.Key(from))
		gr := call.Reply.(*proto.GetResponse)
		if err := txn.Run(call); err != nil {
			return err
		}
		// Read from value.
		var fromValue int64
		if gr.Value != nil && gr.Value.Bytes != nil {
			readValue, err := strconv.ParseInt(string(gr.Value.Bytes), 10, 64)
			if err != nil {
				return err
			}
			fromValue = readValue
		}

		// Ensure there is enough cash.
		if fromValue < amount {
			return nil
		}

		// Read to value.
		call = client.Get(proto.Key(to))
		gr = call.Reply.(*proto.GetResponse)
		if err := txn.Run(call); err != nil {
			return err
		}

		var toValue int64
		if gr.Value != nil && gr.Value.Bytes != nil {
			readValue, err := strconv.ParseInt(string(gr.Value.Bytes), 10, 64)
			if err != nil {
				return err
			}
			toValue = readValue
		}
		// Update both accounts.
		txn.Prepare(client.Put(proto.Key(from), []byte(fmt.Sprintf("%d", fromValue-amount))))
		txn.Prepare(client.Put(proto.Key(to), []byte(fmt.Sprintf("%d", toValue+amount))))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	bank.approxNumTransactions++
	return true
}

// Read the balances in all the accounts and return them.
func (bank *Bank) readAllAccounts() []int64 {
	balances := make([]int64, bank.numAccounts)
	// Key Value Client initialization.
	sender, err := client.NewHTTPSender(bank.servingAddr, testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	kvClient := client.NewKV(nil, sender)
	kvClient.User = storage.UserRoot
	txnOpts := &client.TransactionOptions{Name: "Reading all balances"}
	err = kvClient.RunTransaction(txnOpts, func(txn *client.Txn) error {
		for i := 0; i < bank.numAccounts; i++ {
			call := client.Get(proto.Key(makeAccountID(i)))
			gr := call.Reply.(*proto.GetResponse)
			if err := txn.Run(call); err != nil {
				return err
			}

			if gr.Value != nil && gr.Value.Bytes != nil {
				balances[i], err = strconv.ParseInt(string(gr.Value.Bytes), 10, 64)
				if err != nil {
					return err
				}
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
	// Key Value Client initialization.
	sender, err := client.NewHTTPSender(bank.servingAddr, testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	kvClient := client.NewKV(nil, sender)
	kvClient.User = storage.UserRoot

	for {
		from := makeAccountID(rand.Intn(bank.numAccounts))
		to := makeAccountID(rand.Intn(bank.numAccounts))
		exchange := rand.Int63n(100)
		bank.moveMoney(kvClient, from, to, exchange)
		// time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
	}
}

// Initialize all the bank balances
func initBankAccounts(servingAddr string, numAccounts int, cash int64) {
	// Key Value Client initialization.
	sender, err := client.NewHTTPSender(servingAddr, testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	kvClient := client.NewKV(nil, sender)
	kvClient.User = storage.UserRoot

	putOpts := client.TransactionOptions{Name: "Initialize bank"}
	for i := 0; i < numAccounts; i++ {
		err = kvClient.RunTransaction(&putOpts, func(txn *client.Txn) error {
			key := makeAccountID(i)
			txn.Prepare(client.Put(proto.Key(key), []byte(fmt.Sprintf("%d", cash))))
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	fmt.Printf("Keep moving money in between accounts\n\n")
	security.SetReadFileFn(securitytest.Asset)
	serv := server.StartTestServer(nil)
	defer serv.Stop()
	var bank Bank
	bank.servingAddr = serv.ServingAddr()
	bank.numAccounts = 100
	const initCash int64 = 1000

	// Initialize all the bank accounts
	initBankAccounts(bank.servingAddr, bank.numAccounts, initCash)

	// Start all the money transfer routines
	const numTransferRoutines int = 10
	for i := 0; i < numTransferRoutines; i++ {
		go bank.continuousMoneyTransfer()
	}

	// Sleep for a bit and then exit
	time.Sleep(1 * time.Second)

	fmt.Printf("Approximate number of transactions: %d\n\n", bank.approxNumTransactions)

	// Check that all money is accounted for
	balances := bank.readAllAccounts()
	var totalAmount int64
	for i := 0; i < bank.numAccounts; i++ {
		fmt.Printf("Account %d contains %d$\n", i, balances[i])
		totalAmount += balances[i]
	}
	if totalAmount != int64(bank.numAccounts)*initCash {
		err := fmt.Sprintf("\nTotal cash in the bank = %d\n", totalAmount)
		log.Fatal(err)
	}
}
