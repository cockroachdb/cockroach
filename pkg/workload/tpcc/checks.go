// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	gosql "database/sql"

	"github.com/cockroachdb/errors"
)

// Check is a tpcc consistency check.
type Check struct {
	Name string
	// If asOfSystemTime is non-empty it will be used to perform the check as
	// a historical query using the provided value as the argument to the
	// AS OF SYSTEM TIME clause.
	Fn func(db *gosql.DB, asOfSystemTime string) error
	// If true, the check is "expensive" and may take a long time to run.
	Expensive bool
	// If true, the check is only valid immediately after loading the dataset.
	// The check may fail if run after the workload.
	LoadOnly bool
	// If true, the check can be skipped for long duration workloads.
	SkipForLongDuration bool
}

// AllChecks returns a slice of all of the checks.
func AllChecks() []Check {
	return []Check{
		{Name: "3.3.2.1", Fn: check3321, SkipForLongDuration: true},
		{Name: "3.3.2.2", Fn: check3322},
		{Name: "3.3.2.3", Fn: check3323},
		{Name: "3.3.2.4", Fn: check3324},
		{Name: "3.3.2.5", Fn: check3325},
		{Name: "3.3.2.6", Fn: check3326, Expensive: true},
		{Name: "3.3.2.7", Fn: check3327},
		{Name: "3.3.2.8", Fn: check3328, SkipForLongDuration: true},
		{Name: "3.3.2.9", Fn: check3329, SkipForLongDuration: true},
		{Name: "3.3.2.10", Fn: check33210, Expensive: true},
		// 3.3.2.11 is LoadOnly. It asserts a relationship between the number of
		// rows in the "order" table and rows in the "new_order" table. Rows are
		// inserted into these tables transactional by the NewOrder transaction.
		// However, only rows in the "new_order" table are deleted by the Delivery
		// transaction. Consequently, the consistency condition will fail after the
		// first Delivery transaction is run by the workload.
		{Name: "3.3.2.11", Fn: check33211, LoadOnly: true},
		{Name: "3.3.2.12", Fn: check33212, Expensive: true},
	}
}

func check3321(db *gosql.DB, asOfSystemTime string) error {
	// 3.3.2.1 Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
	// W_YTD = sum (D_YTD)
	return checkNoRows(db, asOfSystemTime, `
SELECT
    count(*)
FROM
    warehouse
    FULL JOIN (
            SELECT
                d_w_id, sum(d_ytd) AS sum_d_ytd
            FROM
                district
            GROUP BY
                d_w_id
        ) ON w_id = d_w_id
WHERE
    w_ytd != sum_d_ytd
`)
}

func check3322(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
	// D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	districts, err := scanFloats(txn, `
SELECT d_next_o_id FROM district ORDER BY d_w_id, d_id`)
	if err != nil {
		return errors.Wrap(err, "on district")
	}
	newOrders, err := scanFloats(txn, `
SELECT max(no_o_id) FROM new_order GROUP BY no_d_id, no_w_id ORDER BY no_w_id, no_d_id`)
	if err != nil {
		return errors.Wrap(err, "on new_order")
	}
	orders, err := scanFloats(txn, `
SELECT max(o_id) FROM "order" GROUP BY o_d_id, o_w_id ORDER BY o_w_id, o_d_id`)
	if err != nil {
		return errors.Wrap(err, "on order")
	}

	if len(districts) != len(newOrders) || len(districts) != len(orders) {
		return errors.Errorf("length mismatch: district=%d, newOrder=%d, order=%d",
			len(districts), len(newOrders), len(orders))
	}
	if len(districts) == 0 {
		return errors.Errorf("zero rows")
	}
	for i := range districts {
		if (orders[i] != newOrders[i]) || (orders[i] != (districts[i] - 1)) {
			return errors.Errorf(
				"inequality at idx %d: order: %f, newOrder: %f, district-1: %f",
				i, orders[i], newOrders[i], districts[i]-1)
		}
	}
	return nil
}

func check3323(db *gosql.DB, asOfSystemTime string) error {
	// max(NO_O_ID) - min(NO_O_ID) + 1 = # of rows in new_order for each warehouse/district
	return checkNoRows(db, asOfSystemTime, `
SELECT
    count(*)
FROM
    (
        SELECT
            max(no_o_id) - min(no_o_id) - count(*) AS nod
        FROM
            new_order
        GROUP BY
            no_w_id, no_d_id
    )
WHERE
    nod != -1
`)
}

func check3324(db *gosql.DB, asOfSystemTime string) error {
	// sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	orderOLCounts, err := scanInt64s(txn, `
SELECT sum(o_ol_cnt) FROM "order" GROUP BY o_w_id, o_d_id ORDER BY o_w_id, o_d_id`)
	if err != nil {
		return errors.Wrap(err, "on order")
	}
	olCounts, err := scanInt64s(txn, `
SELECT count(*) FROM order_line GROUP BY ol_w_id, ol_d_id ORDER BY ol_w_id, ol_d_id`)
	if err != nil {
		return errors.Wrap(err, "on order_line")
	}

	if len(orderOLCounts) != len(olCounts) {
		return errors.Errorf("length mismatch: order=%d, order_line=%d",
			len(orderOLCounts), len(olCounts))
	}
	if len(orderOLCounts) == 0 {
		return errors.Errorf("0 rows returned")
	}
	for i := range orderOLCounts {
		if orderOLCounts[i] != olCounts[i] {
			return errors.Errorf("order.sum(o_ol_cnt): %d != order_line.count(*): %d",
				orderOLCounts[i], olCounts[i])
		}
	}
	return nil
}

func check3325(db *gosql.DB, asOfSystemTime string) (retErr error) {
	// We want the symmetric difference between the sets:
	// (SELECT no_w_id, no_d_id, no_o_id FROM new_order)
	// (SELECT o_w_id, o_d_id, o_id FROM order@primary WHERE o_carrier_id IS NULL)
	// We achieve this by two EXCEPT ALL queries.
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	firstQuery := txn.QueryRow(`
(SELECT no_w_id, no_d_id, no_o_id FROM new_order)
EXCEPT ALL
(SELECT o_w_id, o_d_id, o_id FROM "order" WHERE o_carrier_id IS NULL)`)
	if err := firstQuery.Scan(); err == nil {
		return errors.Errorf("found at least one row in the new_order table without a corresponding order row")
	} else if !errors.Is(err, gosql.ErrNoRows) {
		return errors.Wrapf(err, "unexpected error during check")
	}
	secondQuery := txn.QueryRow(`
(SELECT o_w_id, o_d_id, o_id FROM "order" WHERE o_carrier_id IS NULL)
EXCEPT ALL
(SELECT no_w_id, no_d_id, no_o_id FROM new_order)`)
	if err := secondQuery.Scan(); err == nil {
		return errors.Errorf("found at least one row in the order table (with o_carrier_id = NULL) without a corresponding new_order row")
	} else if !errors.Is(err, gosql.ErrNoRows) {
		return errors.Wrapf(err, "unexpected error during check")
	}
	return nil
}

func check3326(db *gosql.DB, asOfSystemTime string) (retErr error) {
	// For any row in the ORDER table, O_OL_CNT must equal the number of rows
	// in the ORDER-LINE table for the corresponding order defined by
	// (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	firstQuery := txn.QueryRow(`
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM "order"
  ORDER BY o_w_id, o_d_id, o_id DESC)
EXCEPT ALL
(SELECT ol_w_id, ol_d_id, ol_o_id, count(*) FROM order_line
  GROUP BY (ol_w_id, ol_d_id, ol_o_id)
  ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)`)
	if err := firstQuery.Scan(); err == nil {
		return errors.Errorf("found at least one order count mismatch (using order table on LHS)")
	} else if !errors.Is(err, gosql.ErrNoRows) {
		return errors.Wrapf(err, "unexpected error during check")
	}
	secondQuery := txn.QueryRow(`
(SELECT ol_w_id, ol_d_id, ol_o_id, count(*) FROM order_line
  GROUP BY (ol_w_id, ol_d_id, ol_o_id) ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)
EXCEPT ALL
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM "order"
  ORDER BY o_w_id, o_d_id, o_id DESC)`)
	if err := secondQuery.Scan(); err == nil {
		return errors.Errorf("found at least one order count mismatch (using order table on RHS)")
	} else if !errors.Is(err, gosql.ErrNoRows) {
		return errors.Wrapf(err, "unexpected error during check")
	}
	return nil
}

func check3327(db *gosql.DB, asOfSystemTime string) error {
	// For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null
	// date/time if and only if the corresponding row in the ORDER table defined
	// by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has
	// O_CARRIER_ID set to a null value.
	return checkNoRows(db, asOfSystemTime, `
SELECT count(*) FROM
  (SELECT o_w_id, o_d_id, o_id FROM "order" WHERE o_carrier_id IS NULL)
FULL OUTER JOIN
  (SELECT ol_w_id, ol_d_id, ol_o_id FROM order_line WHERE ol_delivery_d IS NULL)
ON (ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id)
WHERE ol_o_id IS NULL OR o_id IS NULL
`)
}

func check3328(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship:
	// W_YTD = SUM(H_AMOUNT) for each warehouse defined by (W_ID = H _W_ID).
	return checkNoRows(db, asOfSystemTime, `
SELECT count(*) FROM
  (SELECT w_id, w_ytd, sum FROM warehouse
  JOIN
  (SELECT h_w_id, sum(h_amount) FROM history GROUP BY h_w_id)
  ON w_id = h_w_id
  WHERE w_ytd != sum
  )
`)
}

func check3329(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the DISTRICT and HISTORY tables must satisfy the relationship:
	// D_YTD=SUM(H_AMOUNT) for each district defined by (D_W_ID,D_ID)=(H_W_ID,H_D_ID)
	return checkNoRows(db, asOfSystemTime, `
SELECT count(*) FROM
  (SELECT d_id, d_ytd, sum FROM district
  JOIN
  (SELECT h_w_id, h_d_id, sum(h_amount) FROM history GROUP BY (h_w_id, h_d_id))
  ON d_id = h_d_id AND d_w_id = h_w_id
  WHERE d_ytd != sum
  )
`)
}

func check33210(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the CUSTOMER, HISTORY, ORDER, and ORDER-LINE tables must satisfy
	// the relationship:
	//
	//   C_BALANCE = sum(OL_AMOUNT) - sum(H_AMOUNT)
	//
	// where:
	//
	//   H_AMOUNT is selected by (C_W_ID, C_D_ID, C_ID) = (H_C_W_ID, H_C_D_ID, H_C_ID)
	//
	// and
	//
	//   OL_AMOUNT is selected by:
	//     (OL_W_ID, OL_D_ID, OL_O_ID) = (O_W_ID, O_D_ID, O_ID) and
	//     (O_W_ID, O_D_ID, O_C_ID) = (C_W_ID, C_D_ID, C_ID) and
	//     (OL_DELIVERY_D is not a null value)
	//
	return checkNoRows(db, asOfSystemTime, `
SELECT count(*) FROM
  (SELECT c_balance,
          (SELECT coalesce(sum(ol_amount), 0) FROM "order", order_line
            WHERE ol_w_id = o_w_id
            AND ol_d_id = o_d_id
            AND ol_o_id = o_id
            AND o_w_id = c_w_id
            AND o_d_id = c_d_id
            AND o_c_id = c_id
            AND ol_delivery_d IS NOT NULL) sum_ol_amount,
          (SELECT coalesce(sum(h_amount), 0) FROM history
            WHERE h_c_w_id = c_w_id
            AND h_c_d_id = c_d_id
            AND h_c_id = c_id) sum_h_amount
  FROM customer)
WHERE c_balance != sum_ol_amount - sum_h_amount
`)
}

func check33211(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the CUSTOMER, ORDER and NEW-ORDER tables must satisfy the
	// relationship:
	//
	//   (count(*) from ORDER) - (count(*) from NEW-ORDER) = 2100
	//
	// for each district defined by:
	//
	//    (O_W_ID, O_D_ID) = (NO_W_ID, NO_D_ID) = (C_W_ID, C_D_ID)
	//
	return checkNoRows(db, asOfSystemTime, `
SELECT count(*) FROM
  (SELECT order_count, new_order_count FROM
    (SELECT o_w_id, o_d_id, count(*) order_count FROM "order" GROUP BY o_w_id, o_d_id),
    (SELECT no_w_id, no_d_id, count(*) new_order_count FROM new_order GROUP BY no_w_id, no_d_id),
    (SELECT c_w_id, c_d_id FROM customer GROUP BY c_w_id, c_d_id)
	WHERE (o_w_id, o_d_id) = (no_w_id, no_d_id)
	AND   (no_w_id, no_d_id) = (c_w_id, c_d_id))
WHERE order_count - new_order_count != 2100
`)
}

func check33212(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the CUSTOMER and ORDER-LINE tables must satisfy the
	// relationship:
	//
	//   C_BALANCE + C_YTD_PAYMENT = sum(OL_AMOUNT)
	//
	// for any randomly selected customers and where OL_DELIVERY_D is
	// not set to a null date / time.
	return checkNoRows(db, asOfSystemTime, `
SELECT count(*) FROM
  (SELECT c_balance,
          c_ytd_payment,
          (SELECT coalesce(sum(ol_amount), 0) FROM "order", order_line
            WHERE ol_w_id = o_w_id
            AND ol_d_id = o_d_id
            AND ol_o_id = o_id
            AND o_w_id = c_w_id
            AND o_d_id = c_d_id
            AND o_c_id = c_id
            AND ol_delivery_d IS NOT NULL) sum_ol_amount
  FROM customer)
WHERE c_balance + c_ytd_payment != sum_ol_amount
`)
}

func checkNoRows(db *gosql.DB, asOfSystemTime string, q string) error {
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	var i int
	if err := txn.QueryRow(q).Scan(&i); err != nil {
		return err
	}
	if i != 0 {
		return errors.Errorf("%d rows returned, expected zero", i)
	}
	return nil
}

func scanFloats(txn *gosql.Tx, query string) ([]float64, error) {
	rows, err := txn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

func scanInt64s(txn *gosql.Tx, query string) ([]int64, error) {
	rows, err := txn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

// beginAsOfSystemTime starts a transaction and optionally sets it to occur at
// the provided asOfSystemTime. If asOfSystemTime is empty, the transaction will
// not be historical. The asOfSystemTime value will be used as literal SQL in a
// SET TRANSACTION AS OF SYSTEM TIME clause.
func beginAsOfSystemTime(db *gosql.DB, asOfSystemTime string) (txn *gosql.Tx, err error) {
	txn, err = db.Begin()
	if err != nil {
		return nil, err
	}
	// The cluster's default isolation level may be READ COMMITTED, which does
	// not support AS OF SYSTEM TIME. Consistency checks require serializable
	// snapshot reads, so set it explicitly.
	if _, err = txn.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		_ = txn.Rollback()
		return nil, err
	}
	if asOfSystemTime != "" {
		_, err = txn.Exec("SET TRANSACTION AS OF SYSTEM TIME " + asOfSystemTime)
		if err != nil {
			_ = txn.Rollback()
			return nil, err
		}
	}
	return txn, nil
}
