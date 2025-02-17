// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestOrderByRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

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
