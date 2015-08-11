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
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestRevoke(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	// The first `MaxReservedDescID` (plus 0) are set aside.
	descKey := structured.MakeDescMetadataKey(structured.MaxReservedDescID + 1)
	desc := structured.DatabaseDescriptor{}
	if err := kvDB.GetProto(descKey, &desc); err != nil {
		t.Fatal(err)
	}
	if len(desc.Read) != 1 || desc.Read[0] != security.RootUser {
		t.Fatalf("wrong Read list: %+v", desc.Read)
	}
	if len(desc.Write) != 1 || desc.Write[0] != security.RootUser {
		t.Fatalf("wrong Write list: %+v", desc.Write)
	}

	// Add some permissions.
	if _, err := sqlDB.Exec(`GRANT ALL ON DATABASE TEST TO rw`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`GRANT READ ON DATABASE TEST TO reader`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`GRANT WRITE ON DATABASE TEST TO writer`); err != nil {
		t.Fatal(err)
	}

	if err := kvDB.GetProto(descKey, &desc); err != nil {
		t.Fatal(err)
	}
	if len(desc.Read) != 3 || len(desc.Write) != 3 {
		t.Fatalf("wrong read/write length: %d, %d", len(desc.Read), len(desc.Write))
	}

	// Test some revokes.
	if _, err := sqlDB.Exec(`REVOKE WRITE ON DATABASE TEST FROM writer,reader`); err != nil {
		t.Fatal(err)
	}

	if err := kvDB.GetProto(descKey, &desc); err != nil {
		t.Fatal(err)
	}
	if len(desc.Read) != 3 {
		t.Fatalf("wrong Read list: %+v", desc.Read)
	}
	if len(desc.Write) != 2 || desc.Write[0] != security.RootUser || desc.Write[1] != "rw" {
		t.Fatalf("wrong Write list: %+v", desc.Write)
	}

	// Remove ALL Permissions.
	if _, err := sqlDB.Exec(`REVOKE ALL ON DATABASE TEST FROM rw`); err != nil {
		t.Fatal(err)
	}

	if err := kvDB.GetProto(descKey, &desc); err != nil {
		t.Fatal(err)
	}
	if len(desc.Read) != 2 || desc.Read[0] != "reader" || desc.Read[1] != security.RootUser {
		t.Fatalf("wrong Read list: %+v", desc.Read)
	}
	if len(desc.Write) != 1 || desc.Write[0] != security.RootUser {
		t.Fatalf("wrong Write list: %+v", desc.Write)
	}

	// Removing permissions for "root" fails.
	if _, err := sqlDB.Exec(`REVOKE READ ON DATABASE TEST FROM root`); err == nil {
		t.Fatal("unexpected success")
	}
}
