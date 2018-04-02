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

	"github.com/pkg/errors"
)

func check3321(db *gosql.DB) error {
	// 3.3.2.1 Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
	// W_YTD = sum (D_YTD)

	row := db.QueryRow(`
SELECT COUNT(*)
FROM warehouse
FULL OUTER JOIN
(SELECT d_w_id, SUM(d_ytd) as sum_d_ytd FROM district GROUP BY d_w_id)
ON (w_id = d_w_id)
WHERE w_ytd != sum_d_ytd
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

func check3322(db *gosql.DB) error {
	// Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
	// D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)

	districtRows, err := db.Query("SELECT d_next_o_id FROM district ORDER BY d_w_id, d_id")
	if err != nil {
		return err
	}
	newOrderRows, err := db.Query(`
SELECT MAX(no_o_id) FROM new_order GROUP BY no_d_id, no_w_id ORDER BY no_w_id, no_d_id
`)
	if err != nil {
		return err
	}
	orderRows, err := db.Query(`
SELECT MAX(o_id) FROM "order" GROUP BY o_d_id, o_w_id ORDER BY o_w_id, o_d_id
`)
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

func check3323(db *gosql.DB) error {
	// max(NO_O_ID) - min(NO_O_ID) + 1 = # of rows in new_order for each warehouse/district
	row := db.QueryRow(`
SELECT COUNT(*) FROM
 (SELECT MAX(no_o_id) - MIN(no_o_id) - COUNT(*) AS nod FROM new_order
  GROUP BY no_w_id, no_d_id)
 WHERE nod != -1
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

func check3324(db *gosql.DB) error {
	// SUM(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]

	leftRows, err := db.Query(`
SELECT SUM(o_ol_cnt) FROM "order" GROUP BY o_w_id, o_d_id ORDER BY o_w_id, o_d_id
`)
	if err != nil {
		return err
	}
	rightRows, err := db.Query(`
SELECT COUNT(*) FROM order_line GROUP BY ol_w_id, ol_d_id ORDER BY ol_w_id, ol_d_id
`)
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
			return errors.Errorf("order.SUM(o_ol_cnt): %d != order_line.count(*): %d", left, right)
		}
	}
	if i == 0 {
		return errors.Errorf("0 rows returned")
	}
	if leftRows.Next() || rightRows.Next() {
		return errors.Errorf("length of order.SUM(o_ol_cnt) != order_line.count(*)")
	}

	if err := leftRows.Close(); err != nil {
		return err
	}
	return rightRows.Close()
}

func check3325(db *gosql.DB) error {
	// We want the symmetric difference between the sets:
	// (SELECT no_w_id, no_d_id, no_o_id FROM new_order)
	// (SELECT o_w_id, o_d_id, o_id FROM order@primary WHERE o_carrier_id IS NULL)
	// We achieve this by two EXCEPT ALL queries.

	firstQuery, err := db.Query(`
(SELECT no_w_id, no_d_id, no_o_id FROM new_order)
EXCEPT ALL
(SELECT o_w_id, o_d_id, o_id FROM "order"@primary WHERE o_carrier_id IS NULL)`)
	if err != nil {
		return err
	}
	secondQuery, err := db.Query(`
(SELECT o_w_id, o_d_id, o_id FROM "order"@primary WHERE o_carrier_id IS NULL)
EXCEPT ALL
(SELECT no_w_id, no_d_id, no_o_id FROM new_order)`)
	if err != nil {
		return err
	}

	if firstQuery.Next() {
		return errors.Errorf("left EXCEPT right returned nonzero results.")
	}

	if secondQuery.Next() {
		return errors.Errorf("right EXCEPT left returned nonzero results.")
	}

	if err := firstQuery.Close(); err != nil {
		return err
	}
	return secondQuery.Close()
}

func check3326(db *gosql.DB) error {
	// For any row in the ORDER table, O_OL_CNT must equal the number of rows
	// in the ORDER-LINE table for the corresponding order defined by
	// (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).

	firstQuery, err := db.Query(`
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM "order"
  ORDER BY o_w_id, o_d_id, o_id DESC)
EXCEPT ALL
(SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) FROM order_line
  GROUP BY (ol_w_id, ol_d_id, ol_o_id)
  ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)`)
	if err != nil {
		return err
	}
	secondQuery, err := db.Query(`
(SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) FROM order_line
  GROUP BY (ol_w_id, ol_d_id, ol_o_id) ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)
EXCEPT ALL
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM "order"
  ORDER BY o_w_id, o_d_id, o_id DESC)`)
	if err != nil {
		return err
	}

	if firstQuery.Next() {
		return errors.Errorf("left EXCEPT right returned nonzero results")
	}

	if secondQuery.Next() {
		return errors.Errorf("right EXCEPT left returned nonzero results")
	}

	if err := firstQuery.Close(); err != nil {
		return err
	}
	return secondQuery.Close()
}

func check3327(db *gosql.DB) error {
	// For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null
	// date/time if and only if the corresponding row in the ORDER table defined
	// by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has
	// O_CARRIER_ID set to a null value.

	row := db.QueryRow(`
SELECT COUNT(*) FROM
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

func check3328(db *gosql.DB) error {
	// Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship:
	// W_YTD = SUM(H_AMOUNT) for each warehouse defined by (W_ID = H _W_ID).

	row := db.QueryRow(`
SELECT COUNT(*) FROM
  (SELECT w_id, w_ytd, sum FROM warehouse
  JOIN
  (SELECT h_w_id, SUM(h_amount) FROM history GROUP BY h_w_id)
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

func check3329(db *gosql.DB) error {
	// Entries in the DISTRICT and HISTORY tables must satisfy the relationship:
	// D_YTD=SUM(H_AMOUNT) for each district defined by (D_W_ID,D_ID)=(H_W_ID,H_D_ID)

	row := db.QueryRow(`
SELECT COUNT(*) FROM
  (SELECT d_id, d_ytd, sum FROM district
  JOIN
  (SELECT h_w_id, h_d_id, SUM(h_amount) FROM history GROUP BY (h_w_id, h_d_id))
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

type check struct {
	name      string
	f         func(db *gosql.DB) error
	expensive bool
}

func allChecks() []check {
	return []check{
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
