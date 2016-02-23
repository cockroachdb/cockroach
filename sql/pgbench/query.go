// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: David Taylor (david@cockroachlabs.com)

package pgbench

import (
	"database/sql"
	"fmt"
	"math/rand"
)

// This is the TPC-B(ish) query that pgbench runs.
// We don't use placeholders since pg doesn't like preparing multiple
// statements at once.
const tpcbQuery = `BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + %d WHERE aid = %d;
SELECT abalance FROM pgbench_accounts WHERE aid = %d;
UPDATE pgbench_tellers SET tbalance = tbalance + %d WHERE tid = %d;
UPDATE pgbench_branches SET bbalance = bbalance + %d WHERE bid = %d;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (%d, %d, %d, %d, CURRENT_TIMESTAMP);
END;` // vars: delta, aid, tid, bid

func RunOne(db *sql.DB, r *rand.Rand, accounts int) error {
	account := r.Intn(accounts)
	delta := r.Intn(5000)
	teller := r.Intn(tellers)
	branch := 1

	q := fmt.Sprintf(tpcbQuery, delta, account, account, delta, teller, delta, branch,
		teller, branch, account, delta)
	_, err := db.Exec(q)
	return err
}
