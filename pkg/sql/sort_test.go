// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestOrderByRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	seenOne := false
	seenTwo := false
	for {
		row := sqlDB.QueryRow("SELECT * FROM (VALUES (1),(2)) ORDER BY random() LIMIT 1")
		var val int
		if err := row.Scan(&val); err != nil {
			t.Fatal(err)
		}
		switch val {
		case 1:
			seenOne = true
		case 2:
			seenTwo = true
		}
		if seenOne && seenTwo {
			break
		}
	}
}
