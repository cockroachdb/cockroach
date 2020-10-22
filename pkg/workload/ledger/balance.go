// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
