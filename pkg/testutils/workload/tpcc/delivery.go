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
	"time"

	"context"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// 2.7 The Delivery Transaction

// The Delivery business transaction consists of processing a batch of 10 new
// (not yet delivered) orders. Each order is processed (delivered) in full
// within the scope of a read-write database transaction. The number of orders
// delivered as a group (or batched) within the same database transaction is
// implementation specific. The business transaction, comprised of one or more
// (up to 10) database transactions, has a low frequency of execution and must
// complete within a relaxed response time requirement.

// The Delivery transaction is intended to be executed in deferred mode through
// a queuing mechanism, rather than interactively, with terminal response
// indicating transaction completion. The result of the deferred execution is
// recorded into a result file.

type delivery struct{}

var _ tpccTx = newOrder{}

func (del delivery) run(db *sql.DB, wID int) (interface{}, error) {
	oCarrierID := rand.Intn(10) + 1
	olDeliveryD := time.Now()

	if err := crdb.ExecuteTx(
		context.Background(),
		db,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
		func(tx *sql.Tx) error {
			getNewOrder, err := tx.Prepare(`
			SELECT no_o_id
			FROM new_order
			WHERE no_w_id = $1 AND no_d_id = $2
			ORDER BY no_o_id ASC
			LIMIT 1`)
			if err != nil {
				return err
			}
			delNewOrder, err := tx.Prepare(`
			DELETE FROM new_order
			WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3`)
			if err != nil {
				return err
			}
			updateOrder, err := tx.Prepare(`
			UPDATE "order"
			SET o_carrier_id = $1
			WHERE o_w_id = $2 AND o_d_id = $3 AND o_id = $4
			RETURNING o_c_id`)
			if err != nil {
				return err
			}
			updateOrderLine, err := tx.Prepare(`
			UPDATE order_line
			SET ol_delivery_d = $1
			WHERE ol_w_id = $2 AND ol_d_id = $3 AND ol_o_id = $4`)
			if err != nil {
				return err
			}
			sumOrderLine, err := tx.Prepare(`
			SELECT SUM(ol_amount) FROM order_line
			WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3`)
			if err != nil {
				return err
			}
			updateCustomer, err := tx.Prepare(`
			UPDATE customer
			SET (c_balance, c_delivery_cnt) =
				(c_Balance + $1, c_delivery_cnt + 1)
			WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4`)
			if err != nil {
				return err
			}

			// 2.7.4.2. For each district:
			for dID := 1; dID <= 10; dID++ {
				var oID int
				if err := getNewOrder.QueryRow(wID, dID).Scan(&oID); err != nil {
					// If no matching order is found, the delivery of this order is skipped.
					if err != sql.ErrNoRows {
						return err
					}
					continue
				}
				if _, err := delNewOrder.Exec(wID, dID, oID); err != nil {
					return err
				}
				var oCID int
				if err := updateOrder.QueryRow(oCarrierID, wID, dID, oID).Scan(&oCID); err != nil {
					return err
				}
				if _, err := updateOrderLine.Exec(olDeliveryD, wID, dID, oID); err != nil {
					return err
				}
				var olTotal float64
				if err := sumOrderLine.QueryRow(wID, dID, oID).Scan(&olTotal); err != nil {
					return err
				}
				if _, err := updateCustomer.Exec(olTotal, wID, dID, oID); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		return nil, err
	}
	return nil, nil
}
