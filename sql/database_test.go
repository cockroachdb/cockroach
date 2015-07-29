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

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestMakeDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)

	stmt, err := parser.Parse("CREATE DATABASE test")
	if err != nil {
		t.Fatal(err)
	}
	desc := makeDatabaseDesc(stmt[0].(*parser.CreateDatabase))
	if desc.Name != "test" {
		t.Fatalf("expected Name == test, got %s", desc.Name)
	}
	// ID is not set yet.
	if desc.ID != 0 {
		t.Fatalf("expected ID == 0, got %d", desc.ID)
	}
	if len(desc.Read) != 1 || desc.Read[0] != security.RootUser {
		t.Fatalf("expected read == [root], got: %v", desc.Read)
	}
	if len(desc.Write) != 1 || desc.Write[0] != security.RootUser {
		t.Fatalf("expected write == [root], got: %v", desc.Write)
	}
}
