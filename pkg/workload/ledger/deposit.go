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
	"context"
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type deposit struct{}

var _ ledgerTx = deposit{}

func (bal deposit) run(config *ledger, db *gosql.DB, rng *rand.Rand) (interface{}, error) {
	cID := randInt(rng, 0, config.customers-1)

	err := crdb.ExecuteTx(
		context.Background(),
		db,
		nil, /* txopts */
		func(tx *gosql.Tx) error {
			c, err := getBalance(tx, config, cID, false /* historical */)
			if err != nil {
				return err
			}

			amount := randAmount(rng)
			c.balance += amount
			c.sequence++

			if err := updateBalance(tx, config, c); err != nil {
				return err
			}

			if err := getSession(tx, config, rng); err != nil {
				return err
			}

			tID, err := insertTransaction(tx, config, rng, c.identifier)
			if err != nil {
				return err
			}

			cIDs := [2]int{
				cID,
				randInt(rng, 0, config.customers-1),
			}
			return insertEntries(tx, config, rng, cIDs, tID)
		})
	return nil, err
}
