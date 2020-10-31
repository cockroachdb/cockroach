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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/rand"
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

type payment struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	updateWarehouse   workload.StmtHandle
	updateDistrict    workload.StmtHandle
	selectByLastName  workload.StmtHandle
	updateWithPayment workload.StmtHandle
	insertHistory     workload.StmtHandle

	a bufalloc.ByteAllocator
}

var _ tpccTx = &payment{}

func createPayment(ctx context.Context, config *tpcc, mcp *workload.MultiConnPool) (tpccTx, error) {
	p := &payment{
		config: config,
		mcp:    mcp,
	}

	// Update warehouse with payment
	p.updateWarehouse = p.sr.Define(`
		UPDATE warehouse
		SET w_ytd = w_ytd + $1
		WHERE w_id = $2
		RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip`,
	)

	// Update district with payment
	p.updateDistrict = p.sr.Define(`
		UPDATE district
		SET d_ytd = d_ytd + $1
		WHERE d_w_id = $2 AND d_id = $3
		RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip`,
	)

	// 2.5.2.2 Case 2: Pick the middle row, rounded up, from the selection by last name.
	p.selectByLastName = p.sr.Define(`
		SELECT c_id
		FROM customer
		WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
		ORDER BY c_first ASC`,
	)

	// Update customer with payment.
	// If the customer has bad credit, update the customer's C_DATA and return
	// the first 200 characters of it, which is supposed to get displayed by
	// the terminal. See 2.5.3.3 and 2.5.2.2.
	p.updateWithPayment = p.sr.Define(`
		UPDATE customer
		SET (c_balance, c_ytd_payment, c_payment_cnt, c_data) =
		(c_balance - ($1:::float)::decimal, c_ytd_payment + ($1:::float)::decimal, c_payment_cnt + 1,
			 case c_credit when 'BC' then
			 left(c_id::text || c_d_id::text || c_w_id::text || ($5:::int)::text || ($6:::int)::text || ($1:::float)::text || c_data, 500)
			 else c_data end)
		WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4
		RETURNING c_first, c_middle, c_last, c_street_1, c_street_2,
					c_city, c_state, c_zip, c_phone, c_since, c_credit,
					c_credit_lim, c_discount, c_balance, case c_credit when 'BC' then left(c_data, 200) else '' end`,
	)

	// Insert history line.

	p.insertHistory = p.sr.Define(`
		INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_date, h_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
	)

	if err := p.sr.Init(ctx, "payment", mcp, config.connFlags); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *payment) run(ctx context.Context, wID int) (interface{}, error) {
	atomic.AddUint64(&p.config.auditor.paymentTransactions, 1)

	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	d := paymentData{
		dID: rng.Intn(10) + 1,
		// hAmount is randomly selected within [1.00..5000.00]
		hAmount: float64(randInt(rng, 100, 500000)) / float64(100.0),
		hDate:   timeutil.Now(),
	}

	// 2.5.1.2: 85% chance of paying through home warehouse, otherwise
	// remote.
	if rng.Intn(100) < 85 {
		d.cWID = wID
		d.cDID = d.dID
	} else {
		d.cWID = p.config.wPart.randActive(rng)
		// Find a cWID != w_id if there's more than 1 configured warehouse.
		for d.cWID == wID && p.config.activeWarehouses > 1 {
			d.cWID = p.config.wPart.randActive(rng)
		}
		p.config.auditor.Lock()
		p.config.auditor.paymentRemoteWarehouseFreq[d.cWID]++
		p.config.auditor.Unlock()
		d.cDID = rng.Intn(10) + 1
	}

	// 2.5.1.2: The customer is randomly selected 60% of the time by last name
	// and 40% by number.
	if rng.Intn(100) < 60 {
		d.cLast = string(p.config.randCLast(rng, &p.a))
		atomic.AddUint64(&p.config.auditor.paymentsByLastName, 1)
	} else {
		d.cID = p.config.randCustomerID(rng)
	}

	tx, err := p.mcp.Get().BeginEx(ctx, p.config.txOpts)
	if err != nil {
		return nil, err
	}
	if err := crdb.ExecuteInTx(
		ctx, (*workload.PgxTx)(tx),
		func() error {
			var wName, dName string
			// Update warehouse with payment
			if err := p.updateWarehouse.QueryRowTx(
				ctx, tx, d.hAmount, wID,
			).Scan(&wName, &d.wStreet1, &d.wStreet2, &d.wCity, &d.wState, &d.wZip); err != nil {
				return err
			}

			// Update district with payment
			if err := p.updateDistrict.QueryRowTx(
				ctx, tx, d.hAmount, wID, d.dID,
			).Scan(&dName, &d.dStreet1, &d.dStreet2, &d.dCity, &d.dState, &d.dZip); err != nil {
				return err
			}

			// If we are selecting by last name, first find the relevant customer id and
			// then proceed.
			if d.cID == 0 {
				// 2.5.2.2 Case 2: Pick the middle row, rounded up, from the selection by last name.
				rows, err := p.selectByLastName.QueryTx(ctx, tx, wID, d.dID, d.cLast)
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
				cIdx := (len(customers) - 1) / 2
				d.cID = customers[cIdx]
			}

			// Update customer with payment.
			// If the customer has bad credit, update the customer's C_DATA and return
			// the first 200 characters of it, which is supposed to get displayed by
			// the terminal. See 2.5.3.3 and 2.5.2.2.
			if err := p.updateWithPayment.QueryRowTx(
				ctx, tx, d.hAmount, d.cWID, d.cDID, d.cID, d.dID, wID,
			).Scan(&d.cFirst, &d.cMiddle, &d.cLast, &d.cStreet1, &d.cStreet2,
				&d.cCity, &d.cState, &d.cZip, &d.cPhone, &d.cSince, &d.cCredit,
				&d.cCreditLim, &d.cDiscount, &d.cBalance, &d.cData,
			); err != nil {
				return errors.Wrap(err, "select by customer idfail")
			}

			hData := fmt.Sprintf("%s    %s", wName, dName)

			// Insert history line.
			_, err := p.insertHistory.ExecTx(
				ctx, tx,
				d.cID, d.cDID, d.cWID, d.dID, wID, d.hAmount, d.hDate.Format("2006-01-02 15:04:05"), hData,
			)
			return err
		}); err != nil {
		return nil, err
	}
	return d, nil
}
