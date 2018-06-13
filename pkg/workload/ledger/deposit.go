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
	"context"
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type deposit struct{}

var _ ledgerTx = deposit{}

func (bal deposit) run(config *ledger, db *gosql.DB) (interface{}, error) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	cID := randInt(rng, 0, config.customers-1)

	err := crdb.ExecuteTx(
		context.Background(),
		db,
		nil, /* txopts */
		func(tx *gosql.Tx) error {
			c, err := getBalance(config, tx, cID)
			if err != nil {
				return err
			}

			amount := randAmount(rng)
			c.balance += amount
			c.sequence++

			tID, err := insertTransaction(config, tx, rng, c.identifier)
			if err != nil {
				return err
			}

			if err := updateBalance(config, tx, c); err != nil {
				return err
			}

			cIDs := [4]int{
				cID,
				randInt(rng, 0, config.customers-1),
				randInt(rng, 0, config.customers-1),
				randInt(rng, 0, config.customers-1),
			}
			return insertEntries(config, tx, rng, cIDs, tID)
		})
	return nil, err
}
