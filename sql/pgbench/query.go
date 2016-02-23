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
	"math/rand"
)

// This is the TPC-B(ish) query that pgbench runs.
const tpcbQuery = `BEGIN;\n
UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2;
SELECT abalance FROM pgbench_accounts WHERE aid = $2;
UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $3;
UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $4;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES ($3, $4, $2, $1, CURRENT_TIMESTAMP);
END;` // vars: delta, aid, tid, bid

func RunOne(db *sql.DB, r *rand.Rand, accounts int) error {
	account := r.Intn(accounts)
	delta := r.Intn(5000)
	teller := r.Intn(tellers)
	branch := 1

	_, err := db.Exec(tpcbQuery, delta, account, teller, branch)
	return err
}
