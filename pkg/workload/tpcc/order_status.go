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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/pgtype"
	"golang.org/x/exp/rand"
)

// From the TPCC spec, section 2.6:
//
// The Order-Status business transaction queries the status of a customer's last
// order. It represents a mid-weight read-only database transaction with a low
// frequency of execution and response time requirement to satisfy on-line
// users. In addition, this table includes non-primary key access to the
// CUSTOMER table.

type orderStatusData struct {
	// Return data specified by 2.6.3.3
	dID        int
	cID        int
	cFirst     string
	cMiddle    string
	cLast      string
	cBalance   float64
	oID        int
	oEntryD    time.Time
	oCarrierID pgtype.Int8

	items []orderItem
}

type customerData struct {
	cID      int
	cBalance float64
	cFirst   string
	cMiddle  string
}

type orderStatus struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	selectByCustID   workload.StmtHandle
	selectByLastName workload.StmtHandle
	selectOrder      workload.StmtHandle
	selectItems      workload.StmtHandle

	a bufalloc.ByteAllocator
}

var _ tpccTx = &orderStatus{}

func createOrderStatus(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	o := &orderStatus{
		config: config,
		mcp:    mcp,
	}

	// Select by customer id.
	o.selectByCustID = o.sr.Define(`
		SELECT c_balance, c_first, c_middle, c_last
		FROM customer
		WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3`,
	)

	// Pick the middle row, rounded up, from the selection by last name.
	o.selectByLastName = o.sr.Define(`
		SELECT c_id, c_balance, c_first, c_middle
		FROM customer
		WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
		ORDER BY c_first ASC`,
	)

	// Select the customer's order.
	o.selectOrder = o.sr.Define(`
		SELECT o_id, o_entry_d, o_carrier_id
		FROM "order"
		WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3
		ORDER BY o_id DESC
		LIMIT 1`,
	)

	// Select the items from the customer's order.
	o.selectItems = o.sr.Define(`
		SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
		FROM order_line
		WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3`,
	)

	if err := o.sr.Init(ctx, "order-status", mcp, config.connFlags); err != nil {
		return nil, err
	}

	return o, nil
}

func (o *orderStatus) run(ctx context.Context, wID int) (interface{}, error) {
	atomic.AddUint64(&o.config.auditor.orderStatusTransactions, 1)

	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	d := orderStatusData{
		dID: rng.Intn(10) + 1,
	}

	// 2.6.1.2: The customer is randomly selected 60% of the time by last name
	// and 40% by number.
	if rng.Intn(100) < 60 {
		d.cLast = string(o.config.randCLast(rng, &o.a))
		atomic.AddUint64(&o.config.auditor.orderStatusByLastName, 1)
	} else {
		d.cID = o.config.randCustomerID(rng)
	}

	tx, err := o.mcp.Get().BeginEx(ctx, o.config.txOpts)
	if err != nil {
		return nil, err
	}
	if err := crdb.ExecuteInTx(
		ctx, (*workload.PgxTx)(tx),
		func() error {
			// 2.6.2.2 explains this entire transaction.

			// Select the customer
			if d.cID != 0 {
				// Case 1: select by customer id
				if err := o.selectByCustID.QueryRowTx(
					ctx, tx, wID, d.dID, d.cID,
				).Scan(&d.cBalance, &d.cFirst, &d.cMiddle, &d.cLast); err != nil {
					return errors.Wrap(err, "select by customer idfail")
				}
			} else {
				// Case 2: Pick the middle row, rounded up, from the selection by last name.
				rows, err := o.selectByLastName.QueryTx(ctx, tx, wID, d.dID, d.cLast)
				if err != nil {
					return errors.Wrap(err, "select by last name fail")
				}
				customers := make([]customerData, 0, 1)
				for rows.Next() {
					c := customerData{}
					err = rows.Scan(&c.cID, &c.cBalance, &c.cFirst, &c.cMiddle)
					if err != nil {
						rows.Close()
						return err
					}
					customers = append(customers, c)
				}
				if err := rows.Err(); err != nil {
					return err
				}
				rows.Close()
				if len(customers) == 0 {
					return errors.New("found no customers matching query orderStatus.selectByLastName")
				}
				cIdx := (len(customers) - 1) / 2
				c := customers[cIdx]
				d.cID = c.cID
				d.cBalance = c.cBalance
				d.cFirst = c.cFirst
				d.cMiddle = c.cMiddle
			}

			// Select the customer's order.
			if err := o.selectOrder.QueryRowTx(
				ctx, tx, wID, d.dID, d.cID,
			).Scan(&d.oID, &d.oEntryD, &d.oCarrierID); err != nil {
				return errors.Wrap(err, "select order fail")
			}

			// Select the items from the customer's order.
			rows, err := o.selectItems.QueryTx(ctx, tx, wID, d.dID, d.oID)
			if err != nil {
				return errors.Wrap(err, "select items fail")
			}
			defer rows.Close()

			// On average there's 10 items per order - 2.4.1.3
			d.items = make([]orderItem, 0, 10)
			for rows.Next() {
				item := orderItem{}
				if err := rows.Scan(&item.olIID, &item.olSupplyWID, &item.olQuantity, &item.olAmount, &item.olDeliveryD); err != nil {
					return err
				}
				d.items = append(d.items, item)
			}
			return rows.Err()
		}); err != nil {
		return nil, err
	}
	return d, nil
}
