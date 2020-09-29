// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/rand"
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

type delivery struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	selectNewOrders workload.StmtHandle
}

var _ tpccTx = &delivery{}

func createDelivery(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	del := &delivery{
		config: config,
		mcp:    mcp,
	}

	del.selectNewOrders = del.sr.Define(`
		SELECT no_d_id, no_o_id, sum(ol_amount)
		FROM (
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 1  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 2  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 3  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 4  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 5  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 6  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 7  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 8  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 9  ORDER BY no_o_id ASC LIMIT 1)
		UNION ALL
			(SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = $1 AND no_d_id = 10 ORDER BY no_o_id ASC LIMIT 1)
		)
		INNER LOOKUP JOIN order_line
		ON ol_d_id = no_d_id AND ol_o_id = no_o_id
		WHERE ol_w_id = $1
		GROUP BY no_d_id, no_o_id`,
	)

	if err := del.sr.Init(ctx, "delivery", mcp, config.connFlags); err != nil {
		return nil, err
	}

	return del, nil
}

func (del *delivery) run(ctx context.Context, wID int) (interface{}, error) {
	atomic.AddUint64(&del.config.auditor.deliveryTransactions, 1)
	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	oCarrierID := rng.Intn(10) + 1
	olDeliveryD := timeutil.Now()

	// Enter the delivery queue for this warehouse. This ensures that we only
	// run a single delivery transaction at a time, as all devivery transactions
	// within a warehouse contend. See section 2.7.2.
	del.config.deliveryQueue.wait(wID)
	defer del.config.deliveryQueue.release(wID)

	tx, err := del.mcp.Get().BeginEx(ctx, del.config.txOpts)
	if err != nil {
		return nil, err
	}
	err = crdb.ExecuteInTx(
		ctx, (*workload.PgxTx)(tx),
		func() error {
			// 2.7.4.2. For each district, the row in the NEW-ORDER table with
			// matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) and with
			// the lowest NO_O_ID value is selected. This is the oldest
			// undelivered order of that district. NO_O_ID, the order number, is
			// retrieved. If no matching row is found, then the delivery of an
			// order for this district is skipped. The condition in which no
			// outstandin g order is present at a given district must be handled
			// by skipping the delivery of an order for that district only and
			// resuming the delivery of an order from all remaining districts of
			// the selected warehouse.
			rows, err := del.selectNewOrders.QueryTx(ctx, tx, wID)
			if err != nil {
				return err
			}
			dIDoIDPairs := make(map[int]int, 10)
			dIDolTotalPairs := make(map[int]float64, 10)
			for rows.Next() {
				var dID, oID int
				var olTotal float64
				if err := rows.Scan(&dID, &oID, &olTotal); err != nil {
					rows.Close()
					return err
				}
				if _, ok := dIDoIDPairs[dID]; ok {
					return errors.Wrap(err, "duplicate district")
				}
				dIDoIDPairs[dID] = oID
				dIDolTotalPairs[dID] = olTotal
			}
			if err := rows.Err(); err != nil {
				return err
			}
			rows.Close()

			// Handle skipped deliveries.
			atomic.AddUint64(&del.config.auditor.skippedDelivieries, uint64(10-len(dIDoIDPairs)))
			if len(dIDoIDPairs) == 0 {
				return nil
			}

			// The selected rows in the NEW-ORDER table are deleted.
			dIDoIDPairsStr := makeInTuples(dIDoIDPairs)
			if _, err := tx.ExecEx(
				ctx,
				fmt.Sprintf(`
					DELETE FROM new_order
					WHERE no_w_id = %d AND (no_d_id, no_o_id) IN (%s)`,
					wID, dIDoIDPairsStr,
				),
				nil, /* options */
			); err != nil {
				return err
			}

			// The rows in the ORDER table with matching O_W_ID (equals W_ ID),
			// O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) are selected,
			// O_C_ID, the customer number, is retrieved, and O_CARRIER_ID is
			// updated.
			rows, err = tx.QueryEx(
				ctx,
				fmt.Sprintf(`
					UPDATE "order"
					SET o_carrier_id = %d
					WHERE o_w_id = %d AND (o_d_id, o_id) IN (%s)
					RETURNING o_d_id, o_c_id`,
					oCarrierID, wID, dIDoIDPairsStr,
				),
				nil, /* options */
			)
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

			// The row in the CUSTOMER table with matching C_W_ID (equals W_ID),
			// C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected and
			// C_BALANCE is increased by the sum of all order-line amounts
			// (OL_AMOUNT) previously retrieved. C_DELIVERY_CNT is incremented
			// by 1.
			if _, err := tx.ExecEx(
				ctx,
				fmt.Sprintf(`
					UPDATE customer
					SET c_delivery_cnt = c_delivery_cnt + 1,
						c_balance = c_balance + CASE c_d_id %s END
					WHERE c_w_id = %d AND (c_d_id, c_id) IN (%s)`,
					dIDToOlTotalStr, wID, dIDcIDPairsStr,
				),
				nil, /* options */
			); err != nil {
				return err
			}

			// All rows in the ORDER-LINE table with matching OL_W_ID (equals
			// O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are
			// selected. All OL_DELIVERY_D, the delivery dates, are updated to
			// the current system time as returned by the operating system and
			// the sum of all OL_AMOUNT is retrieved.
			_, err = tx.ExecEx(
				ctx,
				fmt.Sprintf(`
					UPDATE order_line
					SET ol_delivery_d = '%s'
					WHERE ol_w_id = %d AND (ol_d_id, ol_o_id) IN (%s)`,
					olDeliveryD.Format("2006-01-02 15:04:05"), wID, dIDoIDPairsStr,
				),
				nil, /* options */
			)
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

// deliveryQueue provides mutual exclusion between all concurrent delivery
// transactions for the same warehouse.
type deliveryQueue []syncutil.Mutex

func newDeliveryQueue(warehouses int) deliveryQueue {
	return make(deliveryQueue, warehouses)
}

func (q deliveryQueue) wait(wID int)    { q[wID].Lock() }
func (q deliveryQueue) release(wID int) { q[wID].Unlock() }
