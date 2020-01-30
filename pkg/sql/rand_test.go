// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestGenerateRandInterestingTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Ensure that we can create the random table.
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	if _, err := db.Exec("CREATE DATABASE d"); err != nil {
		t.Fatal(err)
	}
	err := sqlbase.GenerateRandInterestingTable(db, "d", "t")
	if err != nil {
		t.Fatal(err)
	}
}
