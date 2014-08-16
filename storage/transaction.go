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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Jiajia Han (hanjia18@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/util/hlc"
)

// IsolationType TODO(jiajia) Needs documentation.
type IsolationType int

const (
	// SERIALIZABLE TODO(jiajia) Needs documentation.
	SERIALIZABLE IsolationType = iota
	// SNAPSHOT TODO(jiajia) Needs documentation.
	SNAPSHOT
)

// TransactionStatus TODO(jiajia) Needs documentation.
type TransactionStatus int

const (
	// PENDING TODO(jiajia) Needs documentation.
	PENDING TransactionStatus = iota
	// COMMITTED TODO(jiajia) Needs documentation.
	COMMITTED
	// ABORTED TODO(jiajia) Needs documentation.
	ABORTED
)

// A Transaction is a unit of work performed on cockroach db.
// Cockroach transactions support two isolation levels: snapshot
// isolation and serializable snapshot isolation. Each Cockroach
// transaction is assigned a random priority. This priority will
// be used to decide whether a transaction will be aborted during
// contention.
type Transaction struct {
	TxID          string
	Priority      int32
	Isolation     IsolationType
	Status        TransactionStatus
	Epoch         int32         // incremented on txn retry
	Timestamp     hlc.Timestamp // 0 to use timestamp at destination
	MaxTimestamp  hlc.Timestamp // Timestamp + clock skew; set to Timestamp for historical read
	LastHeartbeat hlc.Timestamp // The last hearbeat timestamp
}
