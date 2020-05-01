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
	gosql "database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach-go/crdb"
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

	selectNewOrder workload.StmtHandle
	sumAmount      workload.StmtHandle
}

var _ tpccTx = &delivery{}

func createDelivery(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	del := &delivery{
		config: config,
		mcp:    mcp,
	}

	del.selectNewOrder = del.sr.Define(`
		SELECT no_o_id
		FROM new_order
		WHERE no_w_id = $1 AND no_d_id = $2
		ORDER BY no_o_id ASC
		LIMIT 1`,
	)

	del.sumAmount = del.sr.Define(`
		SELECT sum(ol_amount) FROM order_line
		WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3`,
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

	tx, err := del.mcp.Get().BeginEx(ctx, del.config.txOpts)
	if err != nil {
		return nil, err
	}
	err = crdb.ExecuteInTx(
		ctx, (*workload.PgxTx)(tx),
		func() error {
			// 2.7.4.2. For each district:
			dIDoIDPairs := make(map[int]int)
			dIDolTotalPairs := make(map[int]float64)
			for dID := 1; dID <= 10; dID++ {
				var oID int
				if err := del.selectNewOrder.QueryRowTx(ctx, tx, wID, dID).Scan(&oID); err != nil {
					// If no matching order is found, the delivery of this order is skipped.
					if !errors.Is(err, gosql.ErrNoRows) {
						atomic.AddUint64(&del.config.auditor.skippedDelivieries, 1)
						return err
					}
					continue
				}
				dIDoIDPairs[dID] = oID

				var olTotal float64
				if err := del.sumAmount.QueryRowTx(
					ctx, tx, wID, dID, oID,
				).Scan(&olTotal); err != nil {
					return err
				}
				dIDolTotalPairs[dID] = olTotal
			}
			dIDoIDPairsStr := makeInTuples(dIDoIDPairs)

			rows, err := tx.QueryEx(
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
