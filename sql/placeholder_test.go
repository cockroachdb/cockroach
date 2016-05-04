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
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

// Test that placeholders work.
func TestPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
	k INT,
	v INT, 
	payload INT,
  PRIMARY KEY (k, v)
);
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1, 1, 1), (2, 2, 2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET payload = $1 WHERE (k, v) = (1, 1)`, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET payload = $1 WHERE k = $2 AND v = $3`, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET payload = $1 WHERE (k, v) = ($2, $3)`, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
}
