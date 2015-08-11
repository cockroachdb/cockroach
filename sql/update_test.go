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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (
  k CHAR PRIMARY KEY,
  v CHAR,
  CONSTRAINT a INDEX (k, v),
  CONSTRAINT b UNIQUE (v)
);
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// The first `MaxReservedDescID` (plus 0) are set aside.
	nameKey := structured.MakeNameMetadataKey(structured.MaxReservedDescID+1, "kv")
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
	} else if l := 12; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	// Insert that will fail after the index `a` is written.
	if _, err := sqlDB.Exec(`UPDATE t.kv SET v = 'd' WHERE k = 'c'`); !testutils.IsError(err, "duplicate key value .* violates unique constraint") {
		t.Fatalf("unexpected error %s", err)
	}

	// Verify nothing was written.
	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 12; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}
}
