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
	gosql "database/sql"

	"github.com/cockroachdb/errors"
)

// Check is a tpcc consistency check.
type Check struct {
	Name string
	// If asOfSystemTime is non-empty it will be used to perform the check as
	// a historical query using the provided value as the argument to the
	// AS OF SYSTEM TIME clause.
	Fn        func(db *gosql.DB, asOfSystemTime string) error
	Expensive bool
}

// AllChecks returns a slice of all of the checks.
func AllChecks() []Check {
	return []Check{
		{"3.3.2.1", check3321, false},
		{"3.3.2.2", check3322, false},
		{"3.3.2.3", check3323, false},
		{"3.3.2.4", check3324, false},
		{"3.3.2.5", check3325, false},
		{"3.3.2.6", check3326, true},
		{"3.3.2.7", check3327, false},
		{"3.3.2.8", check3328, false},
		{"3.3.2.9", check3329, false},
	}
}

func check3321(db *gosql.DB, asOfSystemTime string) error {
	// 3.3.2.1 Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
	// W_YTD = sum (D_YTD)
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	row := txn.QueryRow(`
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
	var i int
	if err := row.Scan(&i); err != nil {
		return err
	}

	if i != 0 {
		return errors.Errorf("%d rows returned, expected zero", i)
	}

	return nil
}

func check3322(db *gosql.DB, asOfSystemTime string) (err error) {
	// Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
	// D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	ts, err := selectTimestamp(txn)
	_ = txn.Rollback() // close the txn now that we're done with it
	if err != nil {
		return err
	}
	districtRowsQuery := `
SELECT
    d_next_o_id
FROM
    district AS OF SYSTEM TIME '` + ts + `'
ORDER BY
    d_w_id, d_id`
	districtRows, err := db.Query(districtRowsQuery)
	if err != nil {
		return err
	}
	newOrderQuery := `
SELECT
    max(no_o_id)
FROM
    new_order AS OF SYSTEM TIME '` + ts + `'
GROUP BY
    no_d_id, no_w_id
ORDER BY
    no_w_id, no_d_id;`
	newOrderRows, err := db.Query(newOrderQuery)
	if err != nil {
		return err
	}
	orderRowsQuery := `
SELECT
    max(o_id)
FROM
    "order" AS OF SYSTEM TIME '` + ts + `'
GROUP BY
    o_d_id, o_w_id
ORDER BY
    o_w_id, o_d_id`
	orderRows, err := db.Query(orderRowsQuery)
	if err != nil {
		return err
	}

	var district, newOrder, order float64
	var i int
	for ; districtRows.Next() && newOrderRows.Next() && orderRows.Next(); i++ {
		if err := districtRows.Scan(&district); err != nil {
			return err
		}
		if err := newOrderRows.Scan(&newOrder); err != nil {
			return err
		}
		if err := orderRows.Scan(&order); err != nil {
			return err
		}

		if (order != newOrder) || (order != (district - 1)) {
			return errors.Errorf("inequality at idx %d: order: %f, newOrder: %f, district-1: %f",
				i, order, newOrder, district-1)
		}
	}
	if districtRows.Next() || newOrderRows.Next() || orderRows.Next() {
		return errors.New("length mismatch between rows")
	}
	if err := districtRows.Close(); err != nil {
		return err
	}
	if err := newOrderRows.Close(); err != nil {
		return err
	}
	if err := orderRows.Close(); err != nil {
		return err
	}

	if i == 0 {
		return errors.Errorf("zero rows")
	}

	return nil
}

func check3323(db *gosql.DB, asOfSystemTime string) error {
	// max(NO_O_ID) - min(NO_O_ID) + 1 = # of rows in new_order for each warehouse/district
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	row := txn.QueryRow(`
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
    nod != -1`)

	var i int
	if err := row.Scan(&i); err != nil {
		return err
	}

	if i != 0 {
		return errors.Errorf("%d rows returned, expected zero", i)
	}

	return nil
}

func check3324(db *gosql.DB, asOfSystemTime string) (err error) {
	// sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	// Select a timestamp which will be used for the concurrent queries below.
	ts, err := selectTimestamp(txn)
	_ = txn.Rollback() // close txn now that we're done with it.
	if err != nil {
		return err
	}
	leftRows, err := db.Query(`
SELECT
    sum(o_ol_cnt)
FROM
    "order" AS OF SYSTEM TIME '` + ts + `'
GROUP BY
    o_w_id, o_d_id
ORDER BY
    o_w_id, o_d_id`)
	if err != nil {
		return err
	}
	rightRows, err := db.Query(`
SELECT
    count(*)
FROM
    order_line AS OF SYSTEM TIME '` + ts + `'
GROUP BY
    ol_w_id, ol_d_id
ORDER BY
    ol_w_id, ol_d_id`)
	if err != nil {
		return err
	}
	var i int
	var left, right int64
	for ; leftRows.Next() && rightRows.Next(); i++ {
		if err := leftRows.Scan(&left); err != nil {
			return err
		}
		if err := rightRows.Scan(&right); err != nil {
			return err
		}
		if left != right {
			return errors.Errorf("order.sum(o_ol_cnt): %d != order_line.count(*): %d", left, right)
		}
	}
	if i == 0 {
		return errors.Errorf("0 rows returned")
	}
	if leftRows.Next() || rightRows.Next() {
		return errors.Errorf("length of order.sum(o_ol_cnt) != order_line.count(*)")
	}

	if err := leftRows.Close(); err != nil {
		return err
	}
	return rightRows.Close()
}

func check3325(db *gosql.DB, asOfSystemTime string) error {
	// We want the symmetric difference between the sets:
	// (SELECT no_w_id, no_d_id, no_o_id FROM new_order)
	// (SELECT o_w_id, o_d_id, o_id FROM order@primary WHERE o_carrier_id IS NULL)
	// We achieve this by two EXCEPT ALL queries.
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	firstQuery, err := txn.Query(`
(SELECT no_w_id, no_d_id, no_o_id FROM new_order)
EXCEPT ALL
(SELECT o_w_id, o_d_id, o_id FROM "order"@primary WHERE o_carrier_id IS NULL)`)
	if err != nil {
		return err
	}
	if firstQuery.Next() {
		return errors.Errorf("left EXCEPT right returned nonzero results.")
	}
	if err := firstQuery.Close(); err != nil {
		return err
	}
	secondQuery, err := txn.Query(`
(SELECT o_w_id, o_d_id, o_id FROM "order"@primary WHERE o_carrier_id IS NULL)
EXCEPT ALL
(SELECT no_w_id, no_d_id, no_o_id FROM new_order)`)
	if err != nil {
		return err
	}
	if secondQuery.Next() {
		return errors.Errorf("right EXCEPT left returned nonzero results.")
	}
	return secondQuery.Close()
}

func check3326(db *gosql.DB, asOfSystemTime string) (err error) {
	// For any row in the ORDER table, O_OL_CNT must equal the number of rows
	// in the ORDER-LINE table for the corresponding order defined by
	// (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()

	firstQuery, err := txn.Query(`
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM "order"
  ORDER BY o_w_id, o_d_id, o_id DESC)
EXCEPT ALL
(SELECT ol_w_id, ol_d_id, ol_o_id, count(*) FROM order_line
  GROUP BY (ol_w_id, ol_d_id, ol_o_id)
  ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)`)
	if err != nil {
		return err
	}
	if firstQuery.Next() {
		return errors.Errorf("left EXCEPT right returned nonzero results")
	}
	if err := firstQuery.Close(); err != nil {
		return err
	}
	secondQuery, err := txn.Query(`
(SELECT ol_w_id, ol_d_id, ol_o_id, count(*) FROM order_line
  GROUP BY (ol_w_id, ol_d_id, ol_o_id) ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)
EXCEPT ALL
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM "order"
  ORDER BY o_w_id, o_d_id, o_id DESC)`)
	if err != nil {
		return err
	}

	if secondQuery.Next() {
		return errors.Errorf("right EXCEPT left returned nonzero results")
	}
	return secondQuery.Close()
}

func check3327(db *gosql.DB, asOfSystemTime string) error {
	// For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null
	// date/time if and only if the corresponding row in the ORDER table defined
	// by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has
	// O_CARRIER_ID set to a null value.
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	row := txn.QueryRow(`
SELECT count(*) FROM
  (SELECT o_w_id, o_d_id, o_id FROM "order" WHERE o_carrier_id IS NULL)
FULL OUTER JOIN
  (SELECT ol_w_id, ol_d_id, ol_o_id FROM order_line WHERE ol_delivery_d IS NULL)
ON (ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id)
WHERE ol_o_id IS NULL OR o_id IS NULL
`)

	var i int
	if err := row.Scan(&i); err != nil {
		return err
	}

	if i != 0 {
		return errors.Errorf("%d rows returned, expected zero", i)
	}

	return nil
}

func check3328(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship:
	// W_YTD = SUM(H_AMOUNT) for each warehouse defined by (W_ID = H _W_ID).
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	row := txn.QueryRow(`
SELECT count(*) FROM
  (SELECT w_id, w_ytd, sum FROM warehouse
  JOIN
  (SELECT h_w_id, sum(h_amount) FROM history GROUP BY h_w_id)
  ON w_id = h_w_id
  WHERE w_ytd != sum
  )
`)

	var i int
	if err := row.Scan(&i); err != nil {
		return err
	}

	if i != 0 {
		return errors.Errorf("%d rows returned, expected zero", i)
	}

	return nil
}

func check3329(db *gosql.DB, asOfSystemTime string) error {
	// Entries in the DISTRICT and HISTORY tables must satisfy the relationship:
	// D_YTD=SUM(H_AMOUNT) for each district defined by (D_W_ID,D_ID)=(H_W_ID,H_D_ID)
	txn, err := beginAsOfSystemTime(db, asOfSystemTime)
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	row := txn.QueryRow(`
SELECT count(*) FROM
  (SELECT d_id, d_ytd, sum FROM district
  JOIN
  (SELECT h_w_id, h_d_id, sum(h_amount) FROM history GROUP BY (h_w_id, h_d_id))
  ON d_id = h_d_id AND d_w_id = h_w_id
  WHERE d_ytd != sum
  )
`)

	var i int
	if err := row.Scan(&i); err != nil {
		return err
	}

	if i != 0 {
		return errors.Errorf("%d rows returned, expected zero", i)
	}

	return nil
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
	if asOfSystemTime != "" {
		_, err = txn.Exec("SET TRANSACTION AS OF SYSTEM TIME " + asOfSystemTime)
		if err != nil {
			_ = txn.Rollback()
			return nil, err
		}
	}
	return txn, nil
}

// selectTimestamp retreives an unqouted string literal of a decimal value
// representing the hlc timestamp of the provided txn.
func selectTimestamp(txn *gosql.Tx) (ts string, err error) {
	err = txn.QueryRow("SELECT cluster_logical_timestamp()::string").Scan(&ts)
	return ts, err
}
