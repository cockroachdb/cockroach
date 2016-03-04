// Copyright 2015 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const nonSystemDesc = 2
	const keysPerDesc = 2
	const nonDescKeys = 2

	ms := sql.MakeMetadataSchema()
	kv := ms.GetInitialValues()
	expected := nonDescKeys + keysPerDesc*(nonSystemDesc+sql.NumSystemDescriptors)
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Add an additional table.
	ms.AddTable(keys.MaxSystemConfigDescID+1,
		"CREATE TABLE testdb.x (val INTEGER PRIMARY KEY)",
		privilege.List{privilege.ALL})
	kv = ms.GetInitialValues()
	expected = nonDescKeys + keysPerDesc*ms.DescriptorCount()
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Verify that IDGenerator value is correct.
	found := false
	var idgenkv roachpb.KeyValue
	for _, v := range kv {
		if v.Key.Equal(keys.DescIDGenerator) {
			idgenkv = v
			found = true
			break
		}
	}

	if !found {
		t.Fatal("Could not find descriptor ID generator in initial key set")
	}
	// Expect 2 non-reserved IDs to have been allocated.
	i, err := idgenkv.Value.GetInt()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := i, int64(keys.MaxReservedDescID+1); a != e {
		t.Fatalf("Expected next descriptor ID to be %d, was %d", e, a)
	}
}
