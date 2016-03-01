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

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestMakeDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOneTraditional("CREATE DATABASE test")
	if err != nil {
		t.Fatal(err)
	}
	desc := makeDatabaseDesc(stmt.(*parser.CreateDatabase))
	if desc.Name != "test" {
		t.Fatalf("expected Name == test, got %s", desc.Name)
	}
	// ID is not set yet.
	if desc.ID != 0 {
		t.Fatalf("expected ID == 0, got %d", desc.ID)
	}
	if len(desc.GetPrivileges().Users) != 1 {
		t.Fatalf("wrong number of privilege users, expected 1, got: %d", len(desc.GetPrivileges().Users))
	}
}
