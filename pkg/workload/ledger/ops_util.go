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
	"database/sql/driver"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sync/syncmap"
)

// querier is the common interface to execute queries on a DB, Tx, or Conn.
type querier interface {
	Exec(query string, args ...interface{}) (gosql.Result, error)
	Query(query string, args ...interface{}) (*gosql.Rows, error)
	QueryRow(query string, args ...interface{}) *gosql.Row
}

func maybeParallelize(config *ledger, stmt string) string {
	if config.parallelStmts {
		return stmt + " RETURNING NOTHING"
	}
	return stmt
}

var sqlParamRE = regexp.MustCompile(`\$(\d+)`)
var replacedSQLParams syncmap.Map

func replaceSQLParams(s string) string {
	// Memoize the result.
	if res, ok := replacedSQLParams.Load(s); ok {
		return res.(string)
	}

	res := sqlParamRE.ReplaceAllString(s, "'%[${1}]v'")
	replacedSQLParams.Store(s, res)
	return res
}

func maybeInlineStmtArgs(
	config *ledger, query string, args ...interface{},
) (string, []interface{}) {
	if !config.inlineArgs {
		return query, args
	}
	queryFmt := replaceSQLParams(query)
	for i, arg := range args {
		if v, ok := arg.(driver.Valuer); ok {
			val, err := v.Value()
			if err != nil {
				panic(err)
			}
			args[i] = val
		}
	}
	return strings.Replace(
			strings.Replace(
				fmt.Sprintf(queryFmt, args...),
				" UTC", "", -1), // remove UTC suffix from timestamps.
			`'<nil>'`, `NULL`, -1), // fix NULL values.
		nil
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

func getBalance(q querier, config *ledger, id int, historical bool) (customer, error) {
	aostSpec := ""
	if historical {
		aostSpec = " AS OF SYSTEM TIME '-10s'"
	}
	stmt, args := maybeInlineStmtArgs(config, `
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
		FROM customer`+
		aostSpec+`
		WHERE id = $1 AND IS_ACTIVE = true`,
		id,
	)
	rows, err := q.Query(stmt, args...)
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

func updateBalance(q querier, config *ledger, c customer) error {
	stmt, args := maybeInlineStmtArgs(config, maybeParallelize(config, `
		UPDATE customer SET
			balance         = $1,
			credit_limit    = $2,
			is_active       = $3,
			name            = $4,
			sequence_number = $5
		WHERE id = $6`),
		c.balance, c.creditLimit, c.isActive, c.name, c.sequence, c.id,
	)
	_, err := q.Exec(stmt, args...)
	return err
}

func insertTransaction(q querier, config *ledger, rng *rand.Rand, username string) (string, error) {
	tID := randPaymentID(rng)

	stmt, args := maybeInlineStmtArgs(config, maybeParallelize(config, `
		INSERT INTO transaction (
			tcomment, context, response, reversed_by, created_ts, 
			transaction_type_reference, username, external_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`),
		nil, randContext(rng), randResponse(rng), nil,
		timeutil.Now(), txnTypeReference, username, tID,
	)
	_, err := q.Exec(stmt, args...)
	return tID, err
}

func insertEntries(q querier, config *ledger, rng *rand.Rand, cIDs [2]int, tID string) error {
	amount1 := randAmount(rng)
	sysAmount := 88.433571
	ts := timeutil.Now()

	stmt, args := maybeInlineStmtArgs(config, maybeParallelize(config, `
		INSERT INTO entry (
			amount, system_amount, created_ts, transaction_id, customer_id, money_type
		) VALUES
			($1 , $2 , $3 , $4 , $5 , $6 ),
			($7 , $8 , $9 , $10, $11, $12)`),
		amount1, sysAmount, ts, tID, cIDs[0], cashMoneyType,
		-amount1, -sysAmount, ts, tID, cIDs[1], cashMoneyType,
	)
	_, err := q.Exec(stmt, args...)
	return err
}

func getSession(q querier, config *ledger, rng *rand.Rand) error {
	stmt, args := maybeInlineStmtArgs(config, `
		SELECT
			session_id,
			expiry_timestamp,
			data,
			last_update
		FROM session
		WHERE session_id >= $1
		LIMIT 1`,
		randSessionID(rng),
	)
	rows, err := q.Query(stmt, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// No-op.
	}
	return rows.Err()
}

func insertSession(q querier, config *ledger, rng *rand.Rand) error {
	stmt, args := maybeInlineStmtArgs(config, maybeParallelize(config, `
		INSERT INTO session (
			data, expiry_timestamp, last_update, session_id
		) VALUES ($1, $2, $3, $4)`),
		randSessionData(rng), randTimestamp(rng), timeutil.Now(), randSessionID(rng),
	)
	_, err := q.Exec(stmt, args...)
	return err
}
