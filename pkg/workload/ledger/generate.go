// Copyright 2018 The Cockroach Authors.
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

package ledger

import (
	"encoding/binary"
	"hash"
	"math"
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	numTxnsPerCustomer    = 2
	numEntriesPerTxn      = 4
	numEntriesPerCustomer = numTxnsPerCustomer * numEntriesPerTxn

	paymentIDPrefix  = "payment:"
	txnTypeReference = 400
	cashMoneyType    = "C"
)

func (w *ledger) ledgerCustomerInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)

	return []interface{}{
		rowIdx,               // id
		strconv.Itoa(rowIdx), // identifier
		nil,                  // name
		randCurrencyCode(rng), // currency_code
		true,               // is_system_customer
		true,               // is_active
		randTimestamp(rng), // created
		0,                  // balance
		nil,                // credit_limit
		-1,                 // sequence
	}
}

func (w *ledger) ledgerCustomerSplitRow(splitIdx int) []interface{} {
	return []interface{}{
		(splitIdx + 1) * (w.customers / w.splits),
	}
}

func (w *ledger) ledgerTransactionInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)

	h := w.hashPool.Get().(hash.Hash64)
	defer w.hashPool.Put(h)
	defer h.Reset()

	return []interface{}{
		w.ledgerStablePaymentID(rowIdx), // external_id
		nil,                // tcomment
		randContext(rng),   // context
		txnTypeReference,   // transaction_type_reference
		randUsername(rng),  // username
		randTimestamp(rng), // created_ts
		randTimestamp(rng), // systimestamp
		nil,                // reversed_by
		randResponse(rng),  // response
	}
}

func (w *ledger) ledgerTransactionSplitRow(splitIdx int) []interface{} {
	rng := rand.New(rand.NewSource(w.seed + int64(splitIdx)))
	u := uuid.FromUint128(uint128.FromInts(rng.Uint64(), rng.Uint64()))
	return []interface{}{
		paymentIDPrefix + u.String(),
	}
}

func (w *ledger) ledgerEntryInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)

	// Alternate.
	debit := rowIdx%2 == 0

	var amount float64
	if debit {
		amount = -float64(rowIdx) / 100
	} else {
		amount = float64(rowIdx-1) / 100
	}

	systemAmount := 88.122259
	if debit {
		systemAmount *= -1
	}

	cRowIdx := rowIdx / numEntriesPerCustomer

	tRowIdx := rowIdx / numEntriesPerTxn
	tID := w.ledgerStablePaymentID(tRowIdx)

	return []interface{}{
		rng.Int(),          // id
		amount,             // amount
		cRowIdx,            // customer_id
		tID,                // transaction_id
		systemAmount,       // system_amount
		randTimestamp(rng), // created_ts
		cashMoneyType,      // money_type
	}
}

func (w *ledger) ledgerEntrySplitRow(splitIdx int) []interface{} {
	return []interface{}{
		(splitIdx + 1) * (int(math.MaxInt64) / w.splits),
	}
}

func (w *ledger) ledgerSessionInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)

	return []interface{}{
		randSessionID(rng),   // session_id
		randTimestamp(rng),   // expiry_timestamp
		randSessionData(rng), // data
		randTimestamp(rng),   // last_update
	}
}

func (w *ledger) ledgerSessionSplitRow(splitIdx int) []interface{} {
	rng := rand.New(rand.NewSource(w.seed + int64(splitIdx)))
	return []interface{}{
		randSessionID(rng),
	}
}

func (w *ledger) ledgerStablePaymentID(tRowIdx int) string {
	h := w.hashPool.Get().(hash.Hash64)
	defer w.hashPool.Put(h)
	defer h.Reset()

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(tRowIdx))
	if _, err := h.Write(b); err != nil {
		panic(err)
	}
	hi := h.Sum64()
	if _, err := h.Write(b); err != nil {
		panic(err)
	}
	low := h.Sum64()

	u := uuid.FromUint128(uint128.FromInts(hi, low))
	return paymentIDPrefix + u.String()
}
