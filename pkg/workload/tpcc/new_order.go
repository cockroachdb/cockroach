// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
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

	selectWarehouseTax workload.StmtHandle
	updateDistrict     workload.StmtHandle
	selectCustomerInfo workload.StmtHandle
	insertOrder        workload.StmtHandle
	insertNewOrder     workload.StmtHandle
}

var _ tpccTx = &newOrder{}

func createNewOrder(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	n := &newOrder{
		config: config,
		mcp:    mcp,
	}

	// Select the warehouse tax rate.
	n.selectWarehouseTax = n.sr.Define(`
		SELECT w_tax FROM warehouse WHERE w_id = $1`,
	)

	// Select the district tax rate and next available order number, bumping it.
	n.updateDistrict = n.sr.Define(`
		UPDATE district
		SET d_next_o_id = d_next_o_id + 1
		WHERE d_w_id = $1 AND d_id = $2
		RETURNING d_tax, d_next_o_id`,
	)

	// Select the customer's discount, last name and credit.
	n.selectCustomerInfo = n.sr.Define(`
		SELECT c_discount, c_last, c_credit
		FROM customer
		WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3`,
	)

	n.insertOrder = n.sr.Define(`
		INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
	)

	n.insertNewOrder = n.sr.Define(`
		INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
		VALUES ($1, $2, $3)`,
	)

	if err := n.sr.Init(ctx, "new-order", mcp); err != nil {
		return nil, err
	}

	return n, nil
}

func (n *newOrder) run(ctx context.Context, wID int) (interface{}, time.Duration, error) {
	n.config.auditor.newOrderTransactions.Add(1)

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
	n.config.auditor.totalOrderLines.Add(uint64(d.oOlCnt))

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
		// If we're in localWarehouses mode, keep all items local.
		if n.config.localWarehouses {
			item.remoteWarehouse = false
		} else {
			item.remoteWarehouse = rng.Intn(100) == 0
		}
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

	// This "loop" typically runs only once since we usually `return` at the end
	// but is here to allow retrying in the event that next-order-id repair is
	// enabled and we want to retry the whole new-order txn after a repair.
	for {
		onTxnStartDuration, err := n.config.executeTx(
			ctx, n.mcp.Get(),
			func(tx pgx.Tx) error {
				// Select the warehouse tax rate.
				if err := n.selectWarehouseTax.QueryRowTx(
					ctx, tx, wID,
				).Scan(&d.wTax); err != nil {
					return errors.Wrap(err, "select warehouse failed")
				}

				// Select the district tax rate and next available order number, bumping it.
				var dNextOID int
				if err := n.updateDistrict.QueryRowTx(
					ctx, tx, d.wID, d.dID,
				).Scan(&d.dTax, &dNextOID); err != nil {
					return errors.Wrap(err, "update district failed")
				}
				d.oID = dNextOID - 1

				// Select the customer's discount, last name and credit.
				if err := n.selectCustomerInfo.QueryRowTx(
					ctx, tx, d.wID, d.dID, d.cID,
				).Scan(&d.cDiscount, &d.cLast, &d.cCredit); err != nil {
					return errors.Wrap(err, "select customer failed")
				}

				// 2.4.2.2: For each o_ol_cnt item in the order, query the relevant item
				// row, update the stock row to account for the order, and insert a new
				// line into the order_line table to reflect the item on the order.
				itemIDs := make([]string, d.oOlCnt)
				for i, item := range d.items {
					itemIDs[i] = fmt.Sprint(item.olIID)
				}
				iDatas := make([]string, d.oOlCnt)
				iIDs := strings.Join(itemIDs, ", ")
				err := func() error {
					rows, err := tx.Query(
						ctx,
						fmt.Sprintf(`
						SELECT i_price, i_name, i_data
						FROM item
						WHERE i_id IN (%[1]s)
						ORDER BY i_id`,
							iIDs,
						),
					)
					if err != nil {
						return err
					}
					defer rows.Close()

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
								n.config.auditor.newOrderRollbacks.Add(1)
								return errSimulated
							}
							return errors.New("missing item row")
						}

						if err := rows.Scan(&item.iPrice, &item.iName, iData); err != nil {
							return err
						}
					}
					if rows.Next() {
						return errors.New("extra item row")
					}
					return rows.Err()
				}()
				if err != nil {
					return errors.Wrap(err, "select item failed")
				}

				stockIDs := make([]string, d.oOlCnt)
				for i, item := range d.items {
					stockIDs[i] = fmt.Sprintf("(%d, %d)", item.olIID, item.olSupplyWID)
				}
				distInfos := make([]string, d.oOlCnt)
				sQuantityUpdateCases := make([]string, d.oOlCnt)
				sYtdUpdateCases := make([]string, d.oOlCnt)
				sOrderCntUpdateCases := make([]string, d.oOlCnt)
				sRemoteCntUpdateCases := make([]string, d.oOlCnt)
				if err := func() error {
					rows, err := tx.Query(
						ctx,
						fmt.Sprintf(`
						SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_%02[1]d
						FROM stock
						WHERE (s_i_id, s_w_id) IN (%[2]s)
						ORDER BY s_i_id
						FOR UPDATE`,
							d.dID, strings.Join(stockIDs, ", "),
						),
					)
					if err != nil {
						return err
					}
					defer rows.Close()

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
						if err := rows.Scan(&sQuantity, &sYtd, &sOrderCnt, &sRemoteCnt, &sData, &distInfos[i]); err != nil {
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
					return rows.Err()
				}(); err != nil {
					return errors.Wrap(err, "select stock failed")
				}

				// Insert row into the orders and new orders table.
				if _, err := n.insertOrder.ExecTx(
					ctx, tx,
					d.oID, d.dID, d.wID, d.cID, d.oEntryD.Format("2006-01-02 15:04:05"), d.oOlCnt, allLocal,
				); err != nil {
					return errors.Wrap(err, "insert order failed")
				}
				if _, err := n.insertNewOrder.ExecTx(
					ctx, tx, d.oID, d.dID, d.wID,
				); err != nil {
					return errors.Wrap(err, "insert new_order failed")
				}

				// Update the stock table for each item.
				if _, err := tx.Exec(
					ctx,
					fmt.Sprintf(`
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
						strings.Join(stockIDs, ", "),
					),
				); err != nil {
					return errors.Wrap(err, "update stock failed")
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
				if _, err := tx.Exec(
					ctx,
					fmt.Sprintf(`
					INSERT INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
					VALUES %s`,
						strings.Join(olValsStrings, ", "),
					),
				); err != nil {
					return errors.Wrap(err, "insert order_line failed")
				}

				// 2.4.2.2: total_amount = sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
				d.totalAmount *= (1 - d.cDiscount) * (1 + d.wTax + d.dTax)

				return nil
			})
		if errors.Is(err, errSimulated) {
			return d, 0, nil
		}
		if err != nil && n.config.repairOrderIds {
			var pgErr *pgconn.PgError
			isUniqErr := errors.As(err, &pgErr) && pgcode.MakeCode(pgErr.Code) == pgcode.UniqueViolation
			if isUniqErr {
				fixed := false
				if _, err := n.config.executeTx(ctx, n.mcp.Get(),
					func(tx pgx.Tx) error {
						tag, err := tx.Exec(ctx,
							`UPDATE district SET d_next_o_id = (SELECT max(o_id)+1 FROM "order" WHERE o_w_id = $1 and o_d_id = $2) WHERE d_w_id = $1 AND d_id = $2`,
							d.wID, d.dID,
						)
						if err != nil {
							return err
						}
						if tag.RowsAffected() > 0 {
							fixed = true
						}
						return nil
					}); err == nil && fixed {
					// Updating the next order ID to the computed next did change the row
					// so just go back to the top of the loop and try again.
					continue
				}
			}
		}
		return d, onTxnStartDuration, err
	}
}
