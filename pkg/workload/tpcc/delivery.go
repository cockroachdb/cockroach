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

package tpcc

import (
	gosql "database/sql"
	"math/rand"
	"sync/atomic"

	"context"

	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
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

var _ tpccTx = delivery{}

func (del delivery) run(config *tpcc, db *gosql.DB, wID int) (interface{}, error) {
	atomic.AddUint64(&config.auditor.deliveryTransactions, 1)

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	oCarrierID := rng.Intn(10) + 1
	olDeliveryD := timeutil.Now()

	err := crdb.ExecuteTx(
		context.Background(),
		db,
		config.txOpts,
		func(tx *gosql.Tx) error {
			// 2.7.4.2. For each district:
			dIDoIDPairs := make(map[int]int)
			dIDolTotalPairs := make(map[int]float64)
			for dID := 1; dID <= 10; dID++ {
				var oID int
				if err := tx.QueryRow(fmt.Sprintf(`
					SELECT no_o_id
					FROM new_order
					WHERE no_w_id = %d AND no_d_id = %d
					ORDER BY no_o_id ASC
					LIMIT 1`,
					wID, dID)).Scan(&oID); err != nil {
					// If no matching order is found, the delivery of this order is skipped.
					if err != gosql.ErrNoRows {
						atomic.AddUint64(&config.auditor.skippedDelivieries, 1)
						return err
					}
					continue
				}
				dIDoIDPairs[dID] = oID

				var olTotal float64
				if err := tx.QueryRow(fmt.Sprintf(`
						SELECT SUM(ol_amount) FROM order_line
						WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d`,
					wID, dID, oID)).Scan(&olTotal); err != nil {
					return err
				}
				dIDolTotalPairs[dID] = olTotal
			}
			dIDoIDPairsStr := makeInTuples(dIDoIDPairs)

			rows, err := tx.Query(fmt.Sprintf(`
					UPDATE "order"
					SET o_carrier_id = %d
					WHERE o_w_id = %d AND (o_d_id, o_id) IN (%s)
					RETURNING o_d_id, o_c_id`,
				oCarrierID, wID, dIDoIDPairsStr))
			if err != nil {
				return err
			}
			dIDcIDPairs := make(map[int]int)
			for rows.Next() {
				var dID, oCID int
				if err := rows.Scan(&dID, &oCID); err != nil {
					rows.Close()
					return err
				}
				dIDcIDPairs[dID] = oCID
			}
			if err := rows.Err(); err != nil {
				return err
			}
			rows.Close()

			if err := checkSameKeys(dIDoIDPairs, dIDcIDPairs); err != nil {
				return err
			}
			dIDcIDPairsStr := makeInTuples(dIDcIDPairs)
			dIDToOlTotalStr := makeWhereCases(dIDolTotalPairs)

			if _, err := tx.Exec(fmt.Sprintf(`
				UPDATE customer
				SET c_delivery_cnt = c_delivery_cnt + 1,
					c_balance = c_balance + CASE c_d_id %s END
				WHERE c_w_id = %d AND (c_d_id, c_id) IN (%s)`,
				dIDToOlTotalStr, wID, dIDcIDPairsStr)); err != nil {
				return err
			}
			if _, err := tx.Exec(fmt.Sprintf(`
				DELETE FROM new_order
				WHERE no_w_id = %d AND (no_d_id, no_o_id) IN (%s)`,
				wID, dIDoIDPairsStr)); err != nil {
				return err
			}
			_, err = tx.Exec(fmt.Sprintf(`
				UPDATE order_line
				SET ol_delivery_d = '%s'
				WHERE ol_w_id = %d AND (ol_d_id, ol_o_id) IN (%s)`,
				olDeliveryD.Format("2006-01-02 15:04:05"), wID, dIDoIDPairsStr))
			return err
		})
	return nil, err
}

func makeInTuples(pairs map[int]int) string {
	tupleStrs := make([]string, 0, len(pairs))
	for k, v := range pairs {
		tupleStrs = append(tupleStrs, fmt.Sprintf("(%d, %d)", k, v))
	}
	return strings.Join(tupleStrs, ", ")
}

func makeWhereCases(cases map[int]float64) string {
	casesStrs := make([]string, 0, len(cases))
	for k, v := range cases {
		casesStrs = append(casesStrs, fmt.Sprintf("WHEN %d THEN %f", k, v))
	}
	return strings.Join(casesStrs, " ")
}

func checkSameKeys(a, b map[int]int) error {
	if len(a) != len(b) {
		return errors.Errorf("different number of keys")
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return errors.Errorf("missing key %v", k)
		}
	}
	return nil
}
