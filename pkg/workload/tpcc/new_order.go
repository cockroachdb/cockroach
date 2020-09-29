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
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"golang.org/x/exp/rand"
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

type newOrder struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	updateDistrict     workload.StmtHandle
	selectWarehouseTax workload.StmtHandle
	selectCustomerInfo workload.StmtHandle
	selectItemInfos    workload.StmtHandle
	selectStockInfo    [10]workload.StmtHandle // 0-indexed, by district ID
	updateStock        workload.StmtHandle
	insertOrder        workload.StmtHandle
	insertNewOrder     workload.StmtHandle
	insertOrderLine    workload.StmtHandle
}

var _ tpccTx = &newOrder{}

func createNewOrder(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	n := &newOrder{
		config: config,
		mcp:    mcp,
	}

	// Select the district tax rate and next available order number, bumping it.
	n.updateDistrict = n.sr.Define(`
		UPDATE district
		SET d_next_o_id = d_next_o_id + 1
		WHERE d_w_id = $1 AND d_id = $2
		RETURNING d_tax, d_next_o_id`,
	)

	// Select the warehouse tax rate.
	n.selectWarehouseTax = n.sr.Define(`
		SELECT w_tax FROM warehouse WHERE w_id = $1`,
	)

	// Select the customer's discount, last name, and credit.
	n.selectCustomerInfo = n.sr.Define(`
		SELECT c_discount, c_last, c_credit
		FROM customer
		WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3`,
	)

	// For each o_ol_cnt item in the order, query the relevant item row.
	n.selectItemInfos = n.sr.Define(`
		SELECT i_price, i_name, i_data
		FROM item
		WHERE i_id = ANY ($1)
		ORDER BY i_id`,
	)

	// The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
	// S_W_ID (equals OL_SUPPLY_W_ID) is selected. S_QUANTITY, the quantity in
	// stock, S_DIST_xx, where xx represents the district number, and S_DATA are
	// retrieved.
	for i := range n.selectStockInfo {
		n.selectStockInfo[i] = n.sr.Define(fmt.Sprintf(`
			SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_%02[1]d
			FROM (SELECT unnest($1:::INT[]) AS i_id, unnest($2:::INT[]) AS w_id)
			INNER LOOKUP JOIN stock ON s_i_id = i_id AND s_w_id = w_id
			ORDER BY s_i_id`, i+1,
		))
	}

	n.updateStock = n.sr.Define(`
		UPDATE stock
		SET s_quantity   = new_s_quantity,
			s_ytd        = new_s_ytd,
			s_order_cnt  = new_s_order_cnt,
			s_remote_cnt = new_s_remote_cnt
		FROM (SELECT 
			unnest($1:::INT[]) AS i_id,
			unnest($2:::INT[]) AS w_id,
			unnest($3:::INT[]) AS new_s_quantity,
			unnest($4:::INT[]) AS new_s_ytd,
			unnest($5:::INT[]) AS new_s_order_cnt,
			unnest($6:::INT[]) AS new_s_remote_cnt)
		WHERE s_i_id = i_id AND s_w_id = w_id`,
	)

	n.insertOrder = n.sr.Define(`
		INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
	)

	n.insertNewOrder = n.sr.Define(`
		INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
		VALUES ($1, $2, $3)`,
	)

	n.insertOrderLine = n.sr.Define(`
		INSERT INTO order_line
		(
			ol_o_id,
			ol_d_id,
			ol_w_id,
			ol_i_id,
			ol_supply_w_id,
			ol_number,
			ol_quantity,
			ol_amount,
			ol_dist_info
		)
		(
		 SELECT $1,
				$2,
				$3,
				unnest($4:::INT[]),
				unnest($5:::INT[]),
				unnest($6:::INT[]),
				unnest($7:::INT[]),
				unnest($8:::DECIMAL[]),
				unnest($9:::STRING[])
		)`,
	)

	if err := n.sr.Init(ctx, "new-order", mcp, config.connFlags); err != nil {
		return nil, err
	}

	return n, nil
}

func (n *newOrder) run(ctx context.Context, wID int) (interface{}, error) {
	atomic.AddUint64(&n.config.auditor.newOrderTransactions, 1)

	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	d := newOrderData{
		wID:    wID,
		dID:    int(randInt(rng, 1, 10)),
		cID:    n.config.randCustomerID(rng),
		oOlCnt: int(randInt(rng, 5, 15)),
	}
	d.items = make([]orderItem, d.oOlCnt)

	n.config.auditor.Lock()
	n.config.auditor.orderLinesFreq[d.oOlCnt]++
	n.config.auditor.Unlock()
	atomic.AddUint64(&n.config.auditor.totalOrderLines, uint64(d.oOlCnt))

	// itemIDs tracks the item ids in the order so that we can prevent adding
	// multiple items with the same ID. This would not make sense because each
	// orderItem already tracks a quantity that can be larger than 1.
	itemIDs := make(map[int]struct{})

	// 2.4.1.4: A fixed 1% of the New-Order transactions are chosen at random to
	// simulate user data entry errors and exercise the performance of rolling
	// back update transactions.
	rollback := rng.Intn(100) == 0

	// allLocal tracks whether any of the items were from a remote warehouse.
	allLocal := 1
	for i := 0; i < d.oOlCnt; i++ {
		item := orderItem{
			olNumber: i + 1,
			// 2.4.1.5.3: order has a quantity [1..10]
			olQuantity: rng.Intn(10) + 1,
		}
		// 2.4.1.5.1 an order item has a random item number, unless rollback is true
		// and it's the last item in the items list.
		if rollback && i == d.oOlCnt-1 {
			item.olIID = -1
		} else {
			// Loop until we find a unique item ID.
			for {
				item.olIID = n.config.randItemID(rng)
				if _, ok := itemIDs[item.olIID]; !ok {
					itemIDs[item.olIID] = struct{}{}
					break
				}
			}
		}
		// 2.4.1.5.2: 1% of the time, an item is supplied from a remote warehouse.
		item.remoteWarehouse = rng.Intn(100) == 0
		item.olSupplyWID = wID
		if item.remoteWarehouse && n.config.activeWarehouses > 1 {
			allLocal = 0
			// To avoid picking the local warehouse again, randomly choose among n-1
			// warehouses and swap in the nth if necessary.
			item.olSupplyWID = n.config.wPart.randActive(rng)
			for item.olSupplyWID == wID {
				item.olSupplyWID = n.config.wPart.randActive(rng)
			}
			n.config.auditor.Lock()
			n.config.auditor.orderLineRemoteWarehouseFreq[item.olSupplyWID]++
			n.config.auditor.Unlock()
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

	tx, err := n.mcp.Get().BeginEx(ctx, n.config.txOpts)
	if err != nil {
		return nil, err
	}
	err = crdb.ExecuteInTx(
		ctx, (*workload.PgxTx)(tx),
		func() error {
			// Select the district tax rate and next available order number, bumping it.
			var dNextOID int
			if err := n.updateDistrict.QueryRowTx(
				ctx, tx, d.wID, d.dID,
			).Scan(&d.dTax, &dNextOID); err != nil {
				return err
			}
			d.oID = dNextOID - 1

			// Select the warehouse tax rate.
			if err := n.selectWarehouseTax.QueryRowTx(
				ctx, tx, wID,
			).Scan(&d.wTax); err != nil {
				return err
			}

			// Select the customer's discount, last name, and credit.
			if err := n.selectCustomerInfo.QueryRowTx(
				ctx, tx, d.wID, d.dID, d.cID,
			).Scan(&d.cDiscount, &d.cLast, &d.cCredit); err != nil {
				return err
			}

			// 2.4.2.2: For each o_ol_cnt item in the order, query the relevant item
			// row, update the stock row to account for the order, and insert a new
			// line into the order_line table to reflect the item on the order.
			itemIDs := make([]int64, d.oOlCnt)
			for i, item := range d.items {
				itemIDs[i] = int64(item.olIID)
			}
			rows, err := n.selectItemInfos.QueryTx(ctx, tx, itemIDs)
			if err != nil {
				return err
			}
			iDatas := make([]string, d.oOlCnt)
			for i := range d.items {
				item := &d.items[i]
				iData := &iDatas[i]

				if !rows.Next() {
					if err := rows.Err(); err != nil {
						return err
					}
					if rollback {
						// 2.4.2.3: roll back when we're expecting a rollback due to
						// simulated user error (invalid item id) and we actually
						// can't find the item. The spec requires us to actually go
						// to the database for this, even though we know earlier
						// that the item has an invalid number.
						atomic.AddUint64(&n.config.auditor.newOrderRollbacks, 1)
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

			// The row in the STOCK table with matching S_I_ID (equals OL_I_ID)
			// and S_W_ID (equals OL_SUPPLY_W_ID) is selected. S_QUANTITY, the
			// quantity in stock, S_DIST_xx, where xx represents the district
			// number, and S_DATA are retrieved.
			supplyWIDs := make([]int64, d.oOlCnt)
			for i, item := range d.items {
				supplyWIDs[i] = int64(item.olSupplyWID)
			}
			rows, err = n.selectStockInfo[d.dID-1].QueryTx(ctx, tx, itemIDs, supplyWIDs)
			if err != nil {
				return err
			}

			// If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10
			// or more, then S_QUANTITY is decreased by OL_QUANTITY; otherwise
			// S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91. S_YTD is
			// increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. If
			// the ord er-line is remote, then S_REMOTE_CNT is incremented by 1.
			newStockQuantities := make([]int64, d.oOlCnt)
			newStockYtds := make([]int64, d.oOlCnt)
			newStockOrderCounts := make([]int64, d.oOlCnt)
			newStockRemoteCounts := make([]int64, d.oOlCnt)
			distInfos := make([]string, d.oOlCnt)
			for i := range d.items {
				item := &d.items[i]

				if !rows.Next() {
					if err := rows.Err(); err != nil {
						return err
					}
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

				newStockQuantities[i] = int64(newSQuantity)
				newStockYtds[i] = int64(sYtd + item.olQuantity)
				newStockOrderCounts[i] = int64(sOrderCnt + 1)
				newStockRemoteCounts[i] = int64(newSRemoteCnt)
			}
			if rows.Next() {
				return errors.New("extra stock row")
			}
			if err := rows.Err(); err != nil {
				return err
			}
			rows.Close()

			// Update the stock table for each item.
			if _, err := n.updateStock.ExecTx(
				ctx, tx, itemIDs, supplyWIDs, newStockQuantities,
				newStockYtds, newStockOrderCounts, newStockRemoteCounts,
			); err != nil {
				return err
			}

			// Insert row into the orders table.
			if _, err := n.insertOrder.ExecTx(
				ctx, tx,
				d.oID, d.dID, d.wID, d.cID, d.oEntryD.Format("2006-01-02 15:04:05"), d.oOlCnt, allLocal,
			); err != nil {
				return err
			}

			// Insert row into the new orders table.
			if _, err := n.insertNewOrder.ExecTx(
				ctx, tx, d.oID, d.dID, d.wID,
			); err != nil {
				return err
			}

			// Insert a new order line for each item in the order.
			olNumbers := make([]int64, d.oOlCnt)
			olQuantities := make([]int64, d.oOlCnt)
			olAmounts := make([]float64, d.oOlCnt)
			for i := range d.items {
				item := &d.items[i]
				item.olAmount = float64(item.olQuantity) * item.iPrice
				d.totalAmount += item.olAmount

				olNumbers[i] = int64(item.olNumber)
				olQuantities[i] = int64(item.olQuantity)
				olAmounts[i] = item.olAmount
			}
			if _, err := n.insertOrderLine.ExecTx(
				ctx, tx, d.oID, d.dID, d.wID, itemIDs, supplyWIDs,
				olNumbers, olQuantities, olAmounts, distInfos,
			); err != nil {
				return err
			}

			// 2.4.2.2: total_amount = sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
			d.totalAmount *= (1 - d.cDiscount) * (1 + d.wTax + d.dTax)

			return nil
		})
	if errors.Is(err, errSimulated) {
		return d, nil
	}
	return d, err
}
