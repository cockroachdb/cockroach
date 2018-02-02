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
	"time"

	"github.com/pkg/errors"

	"context"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	oCarrierID gosql.NullInt64

	items []orderItem
}

type customerData struct {
	cID      int
	cBalance float64
	cFirst   string
	cMiddle  string
}

type orderStatus struct{}

var _ tpccTx = orderStatus{}

func (o orderStatus) run(_ *tpcc, db *gosql.DB, wID int) (interface{}, error) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	d := orderStatusData{
		dID: rand.Intn(9) + 1,
	}

	// 2.6.1.2: The customer is randomly selected 60% of the time by last name
	// and 40% by number.
	if rand.Intn(9) < 6 {
		d.cLast = randCLast(rng)
	} else {
		d.cID = randCustomerID(rng)
	}

	if err := crdb.ExecuteTx(
		context.Background(),
		db,
		&gosql.TxOptions{Isolation: gosql.LevelSerializable},
		func(tx *gosql.Tx) error {
			// 2.6.2.2 explains this entire transaction.

			// Select the customer
			if d.cID != 0 {
				// Case 1: select by customer id
				if err := tx.QueryRow(`
					SELECT c_balance, c_first, c_middle, c_last
					FROM customer
					WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3`,
					wID, d.dID, d.cID,
				).Scan(&d.cBalance, &d.cFirst, &d.cMiddle, &d.cLast); err != nil {
					return errors.Wrap(err, "select by customer idfail")
				}
			} else {
				// Case 2: Pick the middle row, rounded up, from the selection by last name.
				rows, err := tx.Query(`
					SELECT c_id, c_balance, c_first, c_middle
					FROM customer@customer_idx
					WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
					ORDER BY c_first ASC`,
					wID, d.dID, d.cLast)
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
				rows.Close()
				cIdx := len(customers) / 2
				if len(customers)%2 == 0 {
					cIdx--
				}

				c := customers[cIdx]
				d.cID = c.cID
				d.cBalance = c.cBalance
				d.cFirst = c.cFirst
				d.cMiddle = c.cMiddle
			}

			// Select the customer's order.
			if err := tx.QueryRow(`
				SELECT o_id, o_entry_d, o_carrier_id
				FROM "order"
				WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3
				ORDER BY o_id DESC
				LIMIT 1`,
				wID, d.dID, d.cID,
			).Scan(&d.oID, &d.oEntryD, &d.oCarrierID); err != nil {
				return errors.Wrap(err, "select order fail")
			}

			// Select the items from the customer's order.
			rows, err := tx.Query(`
				SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
				FROM order_line
				WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3`,
				wID, d.dID, d.oID)
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
			return nil
		}); err != nil {
		return nil, err
	}
	return d, nil
}
