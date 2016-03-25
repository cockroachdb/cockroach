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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// insertLoad add a very basic load that inserts into a unique table and checks
// that the inserted values are indeed correct.
func insertLoad(t *testing.T, dc *dynamicClient, ID int) {
	// Initialize the db.
	if _, err := dc.exec(`CREATE DATABASE IF NOT EXISTS Insert`); err != nil {
		t.Fatal(err)
	}

	tableName := fmt.Sprintf("Insert.Table%d", ID)
	createTableStatement := fmt.Sprintf(`
CREATE TABLE %s (
	key INT PRIMARY KEY,
	value INT NOT NULL
)`, tableName)
	insertStatement := fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ($1, $1)`, tableName)
	selectStatement := fmt.Sprintf(`SELECT key-value AS "total" FROM %s WHERE key = $1`, tableName)

	// Init the db for the basic insert.
	if _, err := dc.exec(createTableStatement); err != nil {
		t.Fatal(err)
	}

	var valueCheck, valueInsert int
	nextUpdate := timeutil.Now()

	// Perform inserts and selects
	for dc.isRunning() {

		// Insert some values.
		valueInsert++
		if _, err := dc.exec(insertStatement, valueInsert); err != nil {
			if err == errTestFinished {
				return
			}
			t.Fatal(err)
		}

		// Check that another value is still correct.
		valueCheck--
		if valueCheck < 1 {
			valueCheck = valueInsert
		}

		var total int
		if err := dc.queryRowScan(selectStatement, []interface{}{valueCheck}, []interface{}{&total}); err != nil {
			if err == errTestFinished {
				return
			}
			t.Fatal(err)
		}
		if total != 0 {
			t.Fatalf("total expected to be 0, is %d", total)
		}

		if timeutil.Now().After(nextUpdate) {
			log.Infof("Insert %d: inserted and checked %d values", ID, valueInsert)
			nextUpdate = timeutil.Now().Add(time.Second)
		}
	}
}
