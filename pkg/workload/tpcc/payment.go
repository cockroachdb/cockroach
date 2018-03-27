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
	"sync/atomic"
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
	atomic.AddUint64(&config.auditor.paymentTransactions, 1)

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
		config.auditor.Lock()
		config.auditor.paymentRemoteWarehouseFreq[d.cWID]++
		config.auditor.Unlock()
		d.cDID = rand.Intn(10) + 1
	}

	// 2.5.1.2: The customer is randomly selected 60% of the time by last name
	// and 40% by number.
	if rand.Intn(100) < 60 {
		d.cLast = randCLast(rng)
		atomic.AddUint64(&config.auditor.paymentsByLastName, 1)
	} else {
		d.cID = randCustomerID(rng)
	}

	if err := crdb.ExecuteTx(
		context.Background(),
		db,
		config.txOpts,
		func(tx *gosql.Tx) error {
			var wName, dName string
			// Update warehouse with payment
			if err := tx.QueryRow(fmt.Sprintf(`
				UPDATE warehouse
				SET w_ytd = w_ytd + %[1]f
				WHERE w_id = %[2]d
				RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip`,
				d.hAmount, wID),
			).Scan(&wName, &d.wStreet1, &d.wStreet2, &d.wCity, &d.wState, &d.wZip); err != nil {
				return err
			}

			// Update district with payment
			if err := tx.QueryRow(fmt.Sprintf(`
				UPDATE district
				SET d_ytd = d_ytd + %[1]f
				WHERE d_w_id = %[2]d AND d_id = %[3]d
				RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip`,
				d.hAmount, wID, d.dID),
			).Scan(&dName, &d.dStreet1, &d.dStreet2, &d.dCity, &d.dState, &d.dZip); err != nil {
				return err
			}

			// If we are selecting by last name, first find the relevant customer id and
			// then proceed.
			if d.cID == 0 {
				// 2.5.2.2 Case 2: Pick the middle row, rounded up, from the selection by last name.
				indexStr := "@customer_idx"
				if config.usePostgres {
					indexStr = ""
				}
				rows, err := tx.Query(fmt.Sprintf(`
					SELECT c_id
					FROM customer%[1]s
					WHERE c_w_id = %[2]d AND c_d_id = %[3]d AND c_last = '%[4]s'
					ORDER BY c_first ASC`,
					indexStr, wID, d.dID, d.cLast))
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
				if err := rows.Err(); err != nil {
					return err
				}
				rows.Close()
				cIdx := len(customers) / 2
				if len(customers)%2 == 0 {
					cIdx--
				}

				d.cID = customers[cIdx]
			}

			// Update customer with payment.
			// If the customer has bad credit, update the customer's C_DATA and return
			// the first 200 characters of it, which is supposed to get displayed by
			// the terminal. See 2.5.3.3 and 2.5.2.2.
			if err := tx.QueryRow(fmt.Sprintf(`
				UPDATE customer
				SET (c_balance, c_ytd_payment, c_payment_cnt, c_data) =
					(c_balance - %[1]f, c_ytd_payment + %[1]f, c_payment_cnt + 1,
					 case c_credit when 'BC' then
					 left(c_id::text || c_d_id::text || c_w_id::text || %[5]d::text || %[6]d::text || %[1]f::text || c_data, 500)
					 else c_data end)
				WHERE c_w_id = %[2]d AND c_d_id = %[3]d AND c_id = %[4]d
				RETURNING c_first, c_middle, c_last, c_street_1, c_street_2,
						  c_city, c_state, c_zip, c_phone, c_since, c_credit,
						  c_credit_lim, c_discount, c_balance, case c_credit when 'BC' then left(c_data, 200) else '' end`,
				d.hAmount, d.cWID, d.cDID, d.cID, d.dID, wID),
			).Scan(&d.cFirst, &d.cMiddle, &d.cLast, &d.cStreet1, &d.cStreet2,
				&d.cCity, &d.cState, &d.cZip, &d.cPhone, &d.cSince, &d.cCredit,
				&d.cCreditLim, &d.cDiscount, &d.cBalance, &d.cData,
			); err != nil {
				return errors.Wrap(err, "select by customer idfail")
			}

			hData := fmt.Sprintf("%s    %s", wName, dName)

			// Insert history line.
			_, err := tx.Exec(fmt.Sprintf(`
				INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_date, h_data)
				VALUES (%[1]d, %[2]d, %[3]d, %[4]d, %[5]d, %[6]f, '%[7]s', '%[8]s')`,
				d.cID, d.cDID, d.cWID, d.dID, wID, d.hAmount,
				d.hDate.Format("2006-01-02 15:04:05"), hData),
			)
			return err
		}); err != nil {
		return nil, err
	}
	return d, nil

}
