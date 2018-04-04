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
	"sort"
	"strings"
	"time"

	"context"

	"sync/atomic"

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
	olNumber     int    // item number in order
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
	atomic.AddUint64(&config.auditor.newOrderTransactions, 1)

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	d := newOrderData{
		wID:    wID,
		dID:    randInt(rng, 1, 10),
		cID:    randCustomerID(rng),
		oOlCnt: randInt(rng, 5, 15),
	}
	d.items = make([]orderItem, d.oOlCnt)

	config.auditor.Lock()
	config.auditor.orderLinesFreq[d.oOlCnt]++
	config.auditor.Unlock()
	atomic.AddUint64(&config.auditor.totalOrderLines, uint64(d.oOlCnt))

	// itemIDs tracks the item ids in the order so that we can prevent adding
	// multiple items with the same ID. This would not make sense because each
	// orderItem already tracks a quantity that can be larger than 1.
	itemIDs := make(map[int]struct{})

	// 2.4.1.4: A fixed 1% of the New-Order transactions are chosen at random to
	// simulate user data entry errors and exercise the performance of rolling
	// back update transactions.
	rollback := rand.Intn(100) == 0

	// allLocal tracks whether any of the items were from a remote warehouse.
	allLocal := 1
	for i := 0; i < d.oOlCnt; i++ {
		item := orderItem{
			olNumber: i + 1,
			// 2.4.1.5.3: order has a quantity [1..10]
			olQuantity: rand.Intn(10) + 1,
		}
		// 2.4.1.5.1 an order item has a random item number, unless rollback is true
		// and it's the last item in the items list.
		if rollback && i == d.oOlCnt-1 {
			item.olIID = -1
		} else {
			// Loop until we find a unique item ID.
			for {
				item.olIID = randItemID(rng)
				if _, ok := itemIDs[item.olIID]; !ok {
					itemIDs[item.olIID] = struct{}{}
					break
				}
			}
		}
		// 2.4.1.5.2: 1% of the time, an item is supplied from a remote warehouse.
		item.remoteWarehouse = rand.Intn(100) == 0
		item.olSupplyWID = wID
		if item.remoteWarehouse && config.warehouses > 1 {
			allLocal = 0
			// To avoid picking the local warehouse again, randomly choose among n-1
			// warehouses and swap in the nth if necessary.
			item.olSupplyWID = rand.Intn(config.warehouses - 1)
			if item.olSupplyWID == wID {
				item.olSupplyWID = config.warehouses - 1
			}
			config.auditor.Lock()
			config.auditor.orderLineRemoteWarehouseFreq[item.olSupplyWID]++
			config.auditor.Unlock()
		} else {
			item.olSupplyWID = wID
		}
		d.items[i] = item
	}

	// Sort the items in the same order that we will require from batch select queries.
	sort.Slice(d.items, func(i, j int) bool {
		return d.items[i].olIID < d.items[j].olIID
	})

	d.oEntryD = timeutil.Now()

	err := crdb.ExecuteTx(
		context.Background(),
		db,
		config.txOpts,
		func(tx *gosql.Tx) error {
			// Select the district tax rate and next available order number, bumping it.
			var dNextOID int
			if err := tx.QueryRow(fmt.Sprintf(`
				UPDATE district
				SET d_next_o_id = d_next_o_id + 1
				WHERE d_w_id = %[1]d AND d_id = %[2]d
				RETURNING d_tax, d_next_o_id`,
				d.wID, d.dID),
			).Scan(&d.dTax, &dNextOID); err != nil {
				return err
			}
			d.oID = dNextOID - 1

			// Select the warehouse tax rate.
			if err := tx.QueryRow(fmt.Sprintf(`
				SELECT w_tax FROM warehouse WHERE w_id = %[1]d`,
				wID),
			).Scan(&d.wTax); err != nil {
				return err
			}

			// Select the customer's discount, last name and credit.
			if err := tx.QueryRow(fmt.Sprintf(`
				SELECT c_discount, c_last, c_credit
				FROM customer
				WHERE c_w_id = %[1]d AND c_d_id = %[2]d AND c_id = %[3]d`,
				d.wID, d.dID, d.cID),
			).Scan(&d.cDiscount, &d.cLast, &d.cCredit); err != nil {
				return err
			}

			// 2.4.2.2: For each o_ol_cnt item in the order, query the relevant item
			// row, update the stock row to account for the order, and insert a new
			// line into the order_line table to reflect the item on the order.
			itemIDs := make([]string, d.oOlCnt)
			for i, item := range d.items {
				itemIDs[i] = fmt.Sprint(item.olIID)
			}
			rows, err := tx.Query(fmt.Sprintf(`
				SELECT i_price, i_name, i_data
				FROM item
				WHERE i_id IN (%[1]s)
				ORDER BY i_id`,
				strings.Join(itemIDs, ", ")),
			)
			if err != nil {
				return err
			}
			iDatas := make([]string, d.oOlCnt)
			for i := range d.items {
				item := &d.items[i]
				iData := &iDatas[i]

				if !rows.Next() {
					if rollback {
						// 2.4.2.3: roll back when we're expecting a rollback due to
						// simulated user error (invalid item id) and we actually
						// can't find the item. The spec requires us to actually go
						// to the database for this, even though we know earlier
						// that the item has an invalid number.
						atomic.AddUint64(&config.auditor.newOrderRollbacks, 1)
						return errSimulated
					}
					return errors.New("missing item row")
				}

				err = rows.Scan(&item.iPrice, &item.iName, iData)
				if err != nil {
					rows.Close()
					return err
				}
			}
			if rows.Next() {
				return errors.New("extra item row")
			}
			if err := rows.Err(); err != nil {
				return err
			}
			rows.Close()

			stockIDs := make([]string, d.oOlCnt)
			for i, item := range d.items {
				stockIDs[i] = fmt.Sprintf("(%d, %d)", item.olIID, item.olSupplyWID)
			}
			rows, err = tx.Query(fmt.Sprintf(`
				SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_%02[1]d
				FROM stock
				WHERE (s_i_id, s_w_id) IN (%[2]s)
				ORDER BY s_i_id`,
				d.dID, strings.Join(stockIDs, ", ")),
			)
			if err != nil {
				return err
			}
			distInfos := make([]string, d.oOlCnt)
			sQuantityUpdateCases := make([]string, d.oOlCnt)
			sYtdUpdateCases := make([]string, d.oOlCnt)
			sOrderCntUpdateCases := make([]string, d.oOlCnt)
			sRemoteCntUpdateCases := make([]string, d.oOlCnt)
			for i := range d.items {
				item := &d.items[i]

				if !rows.Next() {
					return errors.New("missing stock row")
				}

				var sQuantity, sYtd, sOrderCnt, sRemoteCnt int
				var sData string
				err = rows.Scan(&sQuantity, &sYtd, &sOrderCnt, &sRemoteCnt, &sData, &distInfos[i])
				if err != nil {
					rows.Close()
					return err
				}

				if strings.Contains(sData, originalString) && strings.Contains(iDatas[i], originalString) {
					item.brandGeneric = "B"
				} else {
					item.brandGeneric = "G"
				}

				newSQuantity := sQuantity - item.olQuantity
				if sQuantity < item.olQuantity+10 {
					newSQuantity += 91
				}

				newSRemoteCnt := sRemoteCnt
				if item.remoteWarehouse {
					newSRemoteCnt++
				}

				sQuantityUpdateCases[i] = fmt.Sprintf("WHEN %s THEN %d", stockIDs[i], newSQuantity)
				sYtdUpdateCases[i] = fmt.Sprintf("WHEN %s THEN %d", stockIDs[i], sYtd+item.olQuantity)
				sOrderCntUpdateCases[i] = fmt.Sprintf("WHEN %s THEN %d", stockIDs[i], sOrderCnt+1)
				sRemoteCntUpdateCases[i] = fmt.Sprintf("WHEN %s THEN %d", stockIDs[i], newSRemoteCnt)
			}
			if rows.Next() {
				return errors.New("extra stock row")
			}
			if err := rows.Err(); err != nil {
				return err
			}
			rows.Close()

			// Insert row into the orders and new orders table.
			if _, err := tx.Exec(fmt.Sprintf(`
				INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
				VALUES (%[1]d, %[2]d, %[3]d, %[4]d, '%[5]s', %[6]d, %[7]d)`,
				d.oID, d.dID, d.wID, d.cID, d.oEntryD.Format("2006-01-02 15:04:05"),
				d.oOlCnt, allLocal),
			); err != nil {
				return err
			}
			if _, err := tx.Exec(fmt.Sprintf(`
				INSERT INTO new_order (no_o_id, no_d_id, no_w_id) 
				VALUES (%[1]d, %[2]d, %[3]d)`,
				d.oID, d.dID, d.wID)); err != nil {
				return err
			}

			// Update the stock table for each item.
			if _, err := tx.Exec(fmt.Sprintf(`
				UPDATE stock
				SET
					s_quantity = CASE (s_i_id, s_w_id) %[1]s ELSE crdb_internal.force_error('', 'unknown case') END,
					s_ytd = CASE (s_i_id, s_w_id) %[2]s END,
					s_order_cnt = CASE (s_i_id, s_w_id) %[3]s END,
					s_remote_cnt = CASE (s_i_id, s_w_id) %[4]s END
				WHERE (s_i_id, s_w_id) IN (%[5]s)`,
				strings.Join(sQuantityUpdateCases, " "),
				strings.Join(sYtdUpdateCases, " "),
				strings.Join(sOrderCntUpdateCases, " "),
				strings.Join(sRemoteCntUpdateCases, " "),
				strings.Join(stockIDs, ", ")),
			); err != nil {
				return err
			}

			// Insert a new order line for each item in the order.
			olValsStrings := make([]string, d.oOlCnt)
			for i := range d.items {
				item := &d.items[i]
				item.olAmount = float64(item.olQuantity) * item.iPrice
				d.totalAmount += item.olAmount

				olValsStrings[i] = fmt.Sprintf("(%d,%d,%d,%d,%d,%d,%d,%f,'%s')",
					d.oID,            // ol_o_id
					d.dID,            // ol_d_id
					d.wID,            // ol_w_id
					item.olNumber,    // ol_number
					item.olIID,       // ol_i_id
					item.olSupplyWID, // ol_supply_w_id
					item.olQuantity,  // ol_quantity
					item.olAmount,    // ol_amount
					distInfos[i],     // ol_dist_info
				)
			}
			if _, err := tx.Exec(fmt.Sprintf(`
				INSERT INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
				VALUES %s`,
				strings.Join(olValsStrings, ", ")),
			); err != nil {
				return err
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
