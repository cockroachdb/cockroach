// Copyright 2017 The Cockroach Authors.
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

package main

import (
	"database/sql"
	"math/rand"

	"context"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// 2.8 The Stock-Level Transaction
//
// The Stock-Level business transaction determines the number of recently sold
// items that have a stock level below a specified threshold. It represents a
// heavy read-only database transaction with a low frequency of execution, a
// relaxed response time requirement, and relaxed consistency requirements.

// 2.8.2.3 states:
// Full serializability and repeatable reads are not required for the
// Stock-Level business transaction. All data read must be committed and no
// older than the most recently committed data prior to the time this business
// transaction was initiated. All other ACID properties must be maintained.
// TODO(jordan): can we take advantage of this?

type stockLevelData struct {
	// This data must all be returned by the transaction. See 2.8.3.4.
	dID       int
	threshold int
	lowStock  int
}

type stockLevel struct{}

var _ tpccTx = stockLevel{}

func (s stockLevel) run(db *sql.DB, wID int) (interface{}, error) {
	// 2.8.1.2: The threshold of minimum quantity in stock is selected at random
	// within [10..20].
	d := stockLevelData{
		threshold: randInt(10, 20),
		dID:       rand.Intn(9) + 1,
	}

	if err := crdb.ExecuteTx(
		context.Background(),
		db,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
		func(tx *sql.Tx) error {
			var dNextOID int
			if err := tx.QueryRow(`
				SELECT d_next_o_id
				FROM district
				WHERE d_w_id = $1 AND d_id = $2`,
				wID, d.dID,
			).Scan(&dNextOID); err != nil {
				return err
			}

			// Count the number of recently sold items that have a stock level below
			// the threshold.
			if err := tx.QueryRow(`
				SELECT COUNT(DISTINCT(s_i_id))
				FROM order_line
				JOIN stock
				ON s_i_id=ol_i_id
				WHERE ol_w_id = $1
				  AND ol_d_id = $2
				  AND ol_o_id BETWEEN $3 - 20 AND $3 - 1
				  AND s_w_id = $1
				  AND s_quantity < $4`,
				wID, d.dID, dNextOID, d.threshold,
			).Scan(&d.lowStock); err != nil {
				return err
			}
			return nil
		}); err != nil {
		return nil, err
	}
	return d, nil
}
