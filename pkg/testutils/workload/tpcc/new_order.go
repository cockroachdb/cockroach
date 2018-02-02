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
	"fmt"
	"math/rand"
	"strings"
	"time"

	"context"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// From the TPCC spec, section 2.4:
//
// The New-Order business transaction consists of entering a complete order
// through a single database transaction. It represents a mid-weight, read-write
// transaction with a high frequency of execution and stringent response time
// requirements to satisfy on-line users. This transaction is the backbone of
// the workload. It is designed to place a variable load on the system to
// reflect on-line database activity as typically found in production
// environments.

type orderItem struct {
	olSupplyWID  int    // supplying warehouse id
	olIID        int    // item id
	iName        string // item name
	olQuantity   int    // order quantity
	brandGeneric string
	iPrice       float64 // item price
	olAmount     float64 // order amount
	olDeliveryD  pq.NullTime

	remoteWarehouse bool // internal use - item from a local or remote warehouse?
}

type newOrderData struct {
	// This data must all be returned by the transaction. See 2.4.3.3.
	wID         int // home warehouse ID
	dID         int // district id
	cID         int // customer id
	oID         int // order id
	oOlCnt      int // order line count
	cLast       string
	cCredit     string
	cDiscount   float64
	wTax        float64
	dTax        float64
	oEntryD     time.Time
	totalAmount float64

	items []orderItem
}

var errSimulated = errors.New("simulated user error")

type newOrder struct{}

var _ tpccTx = newOrder{}

func (n newOrder) run(config *tpcc, db *gosql.DB, wID int) (interface{}, error) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	d := newOrderData{
		wID:    wID,
		dID:    randInt(rng, 1, 10),
		cID:    randCustomerID(rng),
		oOlCnt: randInt(rng, 5, 15),
	}
	d.items = make([]orderItem, d.oOlCnt)

	// 2.4.1.4: A fixed 1% of the New-Order transactions are chosen at random to
	// simulate user data entry errors and exercise the performance of rolling
	// back update transactions.
	rollback := rand.Intn(100) == 0

	// allLocal tracks whether any of the items were from a remote warehouse.
	allLocal := 1
	for i := 0; i < d.oOlCnt; i++ {
		item := orderItem{
			// 2.4.1.5.3: order has a quantity [1..10]
			olQuantity: rand.Intn(10) + 1,
		}
		// 2.4.1.5.1 an order item has a random item number, unless rollback is true
		// and it's the last item in the items list.
		if rollback && i == d.oOlCnt-1 {
			item.olIID = -1
		} else {
			item.olIID = randItemID(rng)
		}
		// 2.4.1.5.2: 1% of the time, an item is supplied from a remote warehouse.
		item.remoteWarehouse = rand.Intn(100) == 0
		if item.remoteWarehouse {
			allLocal = 0
			item.olSupplyWID = rand.Intn(config.warehouses)
		} else {
			item.olSupplyWID = wID
		}
		d.items[i] = item
	}

	d.oEntryD = timeutil.Now()

	err := crdb.ExecuteTx(
		context.Background(),
		db,
		&gosql.TxOptions{Isolation: gosql.LevelSerializable},
		func(tx *gosql.Tx) error {
			// Select the warehouse tax rate.
			if err := tx.QueryRow(
				`SELECT w_tax FROM warehouse WHERE w_id = $1`,
				wID,
			).Scan(&d.wTax); err != nil {
				return err
			}

			// Select the district tax rate and next available order number, bumping it.
			var dNextOID int
			if err := tx.QueryRow(`
				UPDATE district
				SET d_next_o_id = d_next_o_id + 1
				WHERE d_w_id = $1 AND d_id = $2
				RETURNING d_tax, d_next_o_id`,
				d.wID, d.dID,
			).Scan(&d.dTax, &dNextOID); err != nil {
				return err
			}

			d.oID = dNextOID - 1

			// Select the customer's discount, last name and credit.
			if err := tx.QueryRow(`
				SELECT c_discount, c_last, c_credit
				FROM customer
				WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3`,
				d.wID, d.dID, d.cID,
			).Scan(&d.cDiscount, &d.cLast, &d.cCredit); err != nil {
				return err
			}

			// Insert row into the orders and new orders table.
			if _, err := tx.Exec(`
				INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				d.oID, d.dID, d.wID, d.cID, d.oEntryD, d.oOlCnt, allLocal); err != nil {
				return err
			}
			if _, err := tx.Exec(`
				INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
				VALUES ($1, $2, $3)`,
				d.oID, d.dID, d.wID); err != nil {
				return err
			}

			selectItem, err := tx.Prepare(`SELECT i_price, i_name, i_data FROM item WHERE i_id=$1`)
			if err != nil {
				return err
			}
			updateStock, err := tx.Prepare(fmt.Sprintf(`
			UPDATE stock
			SET (s_quantity, s_ytd, s_order_cnt, s_remote_cnt) =
				(CASE s_quantity >= $1 + 10 WHEN true THEN s_quantity-$1 ELSE (s_quantity-$1)+91 END,
				 s_ytd + $1,
				 s_order_cnt + 1,
				 s_remote_cnt + (CASE $2::bool WHEN true THEN 1 ELSE 0 END))
			WHERE s_i_id=$3 AND s_w_id=$4
			RETURNING s_dist_%02d, s_data`, d.dID))
			if err != nil {
				return err
			}
			insertOrderLine, err := tx.Prepare(`
			INSERT INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
			if err != nil {
				return err
			}

			var iData string
			// 2.4.2.2: For each o_ol_cnt item in the order, query the relevant item
			// row, update the stock row to account for the order, and insert a new
			// line into the order_line table to reflect the item on the order.
			for i, item := range d.items {
				if err := selectItem.QueryRow(item.olIID).Scan(&item.iPrice, &item.iName, &iData); err != nil {
					if rollback && item.olIID < 0 {
						// 2.4.2.3: roll back when we're expecting a rollback due to
						// simulated user error (invalid item id) and we actually
						// can't find the item. The spec requires us to actually go
						// to the database for this, even though we know earlier
						// that the item has an invalid number.
						return errSimulated
					}
					return err
				}

				var distInfo, sData string
				if err := updateStock.QueryRow(
					item.olQuantity, item.remoteWarehouse, item.olIID, item.olSupplyWID,
				).Scan(&distInfo, &sData); err != nil {
					return err
				}
				if strings.Contains(sData, originalString) && strings.Contains(iData, originalString) {
					item.brandGeneric = "B"
				} else {
					item.brandGeneric = "G"
				}

				item.olAmount = float64(item.olQuantity) * item.iPrice
				d.totalAmount += item.olAmount
				if _, err := insertOrderLine.Exec(
					d.oID, // ol_o_id
					d.dID,
					d.wID,
					i+1, // ol_number is a counter over the items in the order.
					item.olIID,
					item.olSupplyWID,
					item.olQuantity,
					item.olAmount,
					distInfo, // ol_dist_info is set to the contents of s_dist_xx
				); err != nil {
					return err
				}
			}
			// 2.4.2.2: total_amount = sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
			d.totalAmount *= (1 - d.cDiscount) * (1 + d.wTax + d.dTax)

			return nil
		})
	if err == errSimulated {
		return d, nil
	}
	return d, err
}
