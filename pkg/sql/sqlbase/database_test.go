// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMakeDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOne("CREATE DATABASE test")
	if err != nil {
		t.Fatal(err)
	}
	const id = 17
	desc := NewInitialDatabaseDescriptor(id, string(stmt.AST.(*tree.CreateDatabase).Name))
	if desc.GetName() != "test" {
		t.Fatalf("expected Name == test, got %s", desc.GetName())
	}
	// ID is not set yet.
	if desc.GetID() != id {
		t.Fatalf("expected ID == %d, got %d", id, desc.GetID())
	}
	if len(desc.GetPrivileges().Users) != 2 {
		t.Fatalf("wrong number of privilege users, expected 2, got: %d", len(desc.GetPrivileges().Users))
	}
}
