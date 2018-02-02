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
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// Section 2.5:
//
// The Payment business transaction updates the customer's balance and reflects
// the payment on the district and warehouse sales statistics. It represents a
// light-weight, read-write transaction with a high frequency of execution and
// stringent response time requirements to satisfy on-line users. In addition,
// this transaction includes non-primary key access to the CUSTOMER table.

type paymentData struct {
	// This data must all be returned by the transaction. See 2.5.3.4.
	dID  int
	cID  int
	cDID int
	cWID int

	wStreet1 string
	wStreet2 string
	wCity    string
	wState   string
	wZip     string

	dStreet1 string
	dStreet2 string
	dCity    string
	dState   string
	dZip     string

	cFirst     string
	cMiddle    string
	cLast      string
	cStreet1   string
	cStreet2   string
	cCity      string
	cState     string
	cZip       string
	cPhone     string
	cSince     time.Time
	cCredit    string
	cCreditLim float64
	cDiscount  float64
	cBalance   float64
	cData      string

	hAmount float64
	hDate   time.Time
}

type payment struct{}

var _ tpccTx = payment{}

func (p payment) run(config *tpcc, db *gosql.DB, wID int) (interface{}, error) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	d := paymentData{
		dID: rand.Intn(10) + 1,
		// hAmount is randomly selected within [1.00..5000.00]
		hAmount: float64(randInt(rng, 100, 500000)) / float64(100.0),
		hDate:   timeutil.Now(),
	}

	// 2.5.1.2: 85% chance of paying through home warehouse, otherwise
	// remote.
	if rand.Intn(100) < 85 {
		d.cWID = wID
		d.cDID = d.dID
	} else {
		d.cWID = rand.Intn(config.warehouses)
		// Find a cWID != w_id if there's more than 1 configured warehouse.
		for d.cWID == wID && config.warehouses > 1 {
			d.cWID = rand.Intn(config.warehouses)
		}
		d.cDID = rand.Intn(10) + 1
	}

	// 2.5.1.2: The customer is randomly selected 60% of the time by last name
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
			var wName, dName string
			// Update warehouse with payment
			if err := tx.QueryRow(`
				UPDATE warehouse
				SET w_ytd = w_ytd + $1
				WHERE w_id = $2
				RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip`,
				d.hAmount, wID,
			).Scan(&wName, &d.wStreet1, &d.wStreet2, &d.wCity, &d.wState, &d.wZip); err != nil {
				return err
			}

			// Update district with payment
			if err := tx.QueryRow(`
				UPDATE district
				SET d_ytd = d_ytd + $1
				WHERE d_w_id = $2 AND d_id = $3
				RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip`,
				d.hAmount, wID, d.dID,
			).Scan(&dName, &d.dStreet1, &d.dStreet2, &d.dCity, &d.dState, &d.dZip); err != nil {
				return err
			}

			// If we are selecting by last name, first find the relevant customer id and
			// then proceed.
			if d.cID == 0 {
				// 2.5.2.2 Case 2: Pick the middle row, rounded up, from the selection by last name.
				rows, err := tx.Query(`
					SELECT c_id
					FROM customer@customer_idx
					WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
					ORDER BY c_first ASC`,
					wID, d.dID, d.cLast)
				if err != nil {
					return errors.Wrap(err, "select by last name fail")
				}
				customers := make([]int, 0, 1)
				for rows.Next() {
					var cID int
					err = rows.Scan(&cID)
					if err != nil {
						rows.Close()
						return err
					}
					customers = append(customers, cID)
				}
				rows.Close()
				cIdx := len(customers) / 2
				if len(customers)%2 == 0 {
					cIdx--
				}

				d.cID = customers[cIdx]
			}

			// Update customer with payment.
			if err := tx.QueryRow(`
				UPDATE customer
				SET (c_balance, c_ytd_payment, c_payment_cnt) =
					(c_balance - $1, c_ytd_payment + $1, c_payment_cnt + 1)
				WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4
				RETURNING c_first, c_middle, c_last, c_street_1, c_street_2,
						  c_city, c_state, c_zip, c_phone, c_since, c_credit,
						  c_credit_lim, c_discount, c_balance`,
				d.hAmount, d.cWID, d.cDID, d.cID,
			).Scan(&d.cFirst, &d.cMiddle, &d.cLast, &d.cStreet1, &d.cStreet2,
				&d.cCity, &d.cState, &d.cZip, &d.cPhone, &d.cSince, &d.cCredit,
				&d.cCreditLim, &d.cDiscount, &d.cBalance,
			); err != nil {
				return errors.Wrap(err, "select by customer idfail")
			}

			if d.cCredit == "BC" {
				// If the customer has bad credit, update the customer's C_DATA.
				d.cData = fmt.Sprintf("%d %d %d %d %d %f | %s", d.cID, d.cDID, d.cWID, d.dID, wID, d.hAmount, d.cData)
				if len(d.cData) > 500 {
					d.cData = d.cData[0:500]
				}
			}
			hData := fmt.Sprintf("%s    %s", wName, dName)

			// Insert history line.
			_, err := tx.Exec(`
				INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_date, h_data)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				d.cID, d.cDID, d.cWID, d.dID, wID, d.hAmount, d.hDate, hData,
			)
			return err
		}); err != nil {
		return nil, err
	}
	return d, nil
}
