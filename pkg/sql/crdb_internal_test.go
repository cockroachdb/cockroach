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
// permissions and limitations under the License.
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package sql_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCRDBInternalTableKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	const (
		database = "d"
		table    = "t"
	)

	if _, err := db.Exec(
		fmt.Sprintf(`CREATE DATABASE %[1]s; CREATE TABLE %[1]s.%[2]s ();`, database, table),
	); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(
		`SELECT database_name, name, table_id, start_key, end_key FROM crdb_internal.tables`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var seenSentinel bool
	for rows.Next() {
		var databaseName string
		var tableName string
		var tableID uint32
		var startKey []byte
		var endKey []byte
		if err := rows.Scan(&databaseName, &tableName, &tableID, &startKey, &endKey); err != nil {
			t.Fatal(err)
		}

		seenSentinel = seenSentinel || databaseName == database && tableName == table

		expectedPrefix := roachpb.Key(keys.MakeTablePrefix(tableID))
		expectedSpan := roachpb.Span{
			Key:    expectedPrefix,
			EndKey: expectedPrefix.PrefixEnd(),
		}
		if a, e := (roachpb.Span{Key: startKey, EndKey: endKey}), expectedSpan; !a.Equal(e) {
			t.Errorf("expected table ID %d to have span %v, but got %v", tableID, e, a)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !seenSentinel {
		t.Fatalf("table %s.%s did not appear in crdb_internal.tables", database, table)
	}
}
