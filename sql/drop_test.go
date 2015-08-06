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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: XisiHuang (cockhuangxh@163.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec("CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR)"); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'),('b', 'd')`); err != nil {
		t.Fatal(err)
	}

	// The first `MaxReservedDescID` (plus 0) are set aside.
	expectedDBCounter := uint32(structured.MaxReservedDescID + 1)
	nameKey := structured.MakeNameMetadataKey(structured.ID(expectedDBCounter), "kv")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}

	if !gr.Exists() {
		t.Fatalf("TableDescriptor %q does not exist", nameKey)
	}

	descKey := gr.ValueBytes()
	desc := structured.TableDescriptor{}
	if err := kvDB.GetProto(descKey, &desc); err != nil {
		t.Fatal(err)
	}

	var tablePrefix []byte
	tablePrefix = append(tablePrefix, keys.TableDataPrefix...)
	tablePrefix = encoding.EncodeUvarint(tablePrefix, uint64(desc.ID))

	tableStartKey := proto.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 6; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if _, err := sqlDB.Exec("DROP TABLE t.kv"); err != nil {
		t.Fatal(err)
	}

	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 0; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if gr, err := kvDB.Get(descKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table descriptor still exists after the table is dropped")
	}

	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table namekey still exists after the table is dropped")
	}
}
