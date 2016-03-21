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

package acceptance

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// insertLoad add a very basic load that inserts into a unique table and checks
// that the inserted values are indeed correct.
func insertLoad(dc *dynamicClient) {
	clientNumber := dc.initClient()
	defer dc.closeClient(clientNumber)

	// Initialize the db.
	if _, err := dc.exec(clientNumber, `CREATE DATABASE IF NOT EXISTS Insert`); err != nil {
		dc.t.Fatal(err)
	}

	tableName := fmt.Sprintf("Insert.Table%d", clientNumber)

	createTableStatement := fmt.Sprintf(`
CREATE TABLE %s (
	key INT PRIMARY KEY,
	value INT NOT NULL
)`, tableName)
	insertStatement := fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ($1, $1)`, tableName)
	selectStatement := fmt.Sprintf(`SELECT key-value AS "total" FROM %s WHERE key = $1`, tableName)

	// Init the db for the basic insert.
	if _, err := dc.exec(clientNumber, createTableStatement); err != nil {
		dc.t.Fatal(err)
	}

	var valueCheck, valueInsert int
	nextUpdate := timeutil.Now()

	// Perform inserts and selects
	for dc.running() {
		// Insert some values.
		valueInsert++
		if _, err := dc.exec(clientNumber, insertStatement, valueInsert); err != nil {
			dc.t.Fatal(err)
		}

		// Check that another value is still correct.
		valueCheck--
		if valueCheck < 1 {
			valueCheck = valueInsert
		}

		var total int
		err := dc.queryRowScan(clientNumber, selectStatement, []interface{}{valueCheck}, []interface{}{&total})
		if err != nil {
			dc.t.Fatal(err)
		}
		if total != 0 {
			dc.t.Fatalf("total expected to be 0, is %d", total)
		}

		if timeutil.Now().After(nextUpdate) {
			log.Infof("%d: inserted and checked %d values", clientNumber, valueInsert)
			nextUpdate = timeutil.Now().Add(time.Second)
		}
	}
}
