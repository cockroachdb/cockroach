// Copyright 2018 The Cockroach Authors.
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

package ledger

import (
	gosql "database/sql"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func maybeParallelize(config *ledger, stmt string) string {
	if config.parallelStmts {
		return stmt + " RETURNING NOTHING"
	}
	return stmt
}

type customer struct {
	id               int
	identifier       string
	name             gosql.NullString
	currencyCode     string
	isSystemCustomer bool
	isActive         bool
	created          time.Time
	balance          float64
	creditLimit      gosql.NullFloat64
	sequence         int
}

func getBalance(config *ledger, tx *gosql.Tx, id int) (customer, error) {
	rows, err := tx.Query(`
		SELECT
			id,
			identifier,
			"name",
			currency_code,
			is_system_customer,
			is_active,
			created,
			balance,
			credit_limit,
			sequence_number
		FROM customer
		WHERE id = $1 AND IS_ACTIVE = true`,
		id)
	if err != nil {
		return customer{}, err
	}
	defer rows.Close()

	var c customer
	for rows.Next() {
		if err := rows.Scan(
			&c.id,
			&c.identifier,
			&c.name,
			&c.currencyCode,
			&c.isSystemCustomer,
			&c.isActive,
			&c.created,
			&c.balance,
			&c.creditLimit,
			&c.sequence,
		); err != nil {
			return customer{}, err
		}
	}
	return c, rows.Err()
}

func updateBalance(config *ledger, tx *gosql.Tx, c customer) error {
	_, err := tx.Exec(maybeParallelize(config, `
		UPDATE customer SET
			balance         = $1,
			credit_limit    = $2,
			is_active       = $3,
			name            = $4,
			sequence_number = $5
		WHERE id = $6`),
		c.balance, c.creditLimit, c.isActive, c.name, c.sequence, c.id,
	)
	return err
}

func insertTransaction(
	config *ledger, tx *gosql.Tx, rng *rand.Rand, username string,
) (string, error) {
	tID := randPaymentID(rng)
	_, err := tx.Exec(maybeParallelize(config, `
		INSERT INTO transaction (
			tcomment, context, response, reversed_by, created_ts, 
			transaction_type_reference, username, external_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`),
		nil, randContext(rng), randResponse(rng), nil,
		timeutil.Now(), txnTypeReference, username, tID,
	)
	return tID, err
}

func insertEntries(config *ledger, tx *gosql.Tx, rng *rand.Rand, cIDs [4]int, tID string) error {
	amount1, amount2 := randAmount(rng), randAmount(rng)
	sysAmount := 88.433571
	ts := timeutil.Now()

	_, err := tx.Exec(maybeParallelize(config, `
		INSERT INTO entry (
			amount, system_amount, created_ts, transaction_id, customer_id, money_type
		) VALUES
			($1 , $2 , $3 , $4 , $5 , $6 ),
			($7 , $8 , $9 , $10, $11, $12),
			($13, $14, $15, $16, $17, $18),
			($19, $20, $21, $22, $23, $24)`),
		amount1, sysAmount, ts, tID, cIDs[0], cashMoneyType,
		-amount1, -sysAmount, ts, tID, cIDs[1], cashMoneyType,
		amount2, sysAmount, ts, tID, cIDs[2], cashMoneyType,
		-amount2, -sysAmount, ts, tID, cIDs[3], cashMoneyType,
	)
	return err
}

func getSession(config *ledger, tx *gosql.Tx, id int) error {
	rows, err := tx.Query(`
		SELECT
			session_id,
			expiry_timestamp,
			data,
			last_update
		FROM session
		WHERE session_id = ?`,
		id)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// No-op.
	}
	return rows.Err()
}

func insertSession(config *ledger, tx *gosql.Tx, rng *rand.Rand) error {
	_, err := tx.Exec(maybeParallelize(config, `
		INSERT INTO session (
			data, expiry_timestamp, last_update, session_id
		) VALUES (?, ?, ?, ?)`),
		randSessionData(rng), randTimestamp(rng), timeutil.Now(), randSessionID(rng),
	)
	return err
}
