// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ledger

import (
	gosql "database/sql"
	"math/rand"
)

type balance struct{}

var _ ledgerTx = balance{}

func (bal balance) run(config *ledger, db *gosql.DB, rng *rand.Rand) (interface{}, error) {
	cID := config.randCustomer(rng)

	_, err := getBalance(db, config, cID, config.historicalBalance)
	return nil, err
}
