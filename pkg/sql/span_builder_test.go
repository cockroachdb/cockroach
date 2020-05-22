// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// These tests are in the sql package rather than span package
// so that we have easy access to table descriptor creation.

func TestSpanBuilderCanSplitSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	tcs := []struct {
		sql               string
		index             string
		prefixLen         int
		numNeededFamilies int
		containsNull      bool
		canSplit          bool
	}{
		{
			sql:               "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c), FAMILY (d)",
			index:             "primary",
			prefixLen:         2,
			numNeededFamilies: 1,
			canSplit:          true,
		},
		{
			sql:               "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c), FAMILY (d)",
			index:             "primary",
			prefixLen:         1,
			numNeededFamilies: 1,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c, d)",
			index:             "primary",
			prefixLen:         2,
			numNeededFamilies: 1,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:             "i",
			prefixLen:         1,
			numNeededFamilies: 1,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, UNIQUE INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:             "i",
			prefixLen:         1,
			numNeededFamilies: 1,
			containsNull:      true,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, UNIQUE INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:             "i",
			prefixLen:         1,
			numNeededFamilies: 1,
			containsNull:      false,
			canSplit:          true,
		},
	}
	if _, err := sqlDB.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range tcs {
		t.Run(tc.sql, func(t *testing.T) {
			if _, err := sqlDB.Exec("DROP TABLE IF EXISTS t.t"); err != nil {
				t.Fatal(err)
			}
			sql := fmt.Sprintf("CREATE TABLE t.t (%s)", tc.sql)
			if _, err := sqlDB.Exec(sql); err != nil {
				t.Fatal(err)
			}
			desc := sqlbase.GetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t")
			idx, _, err := desc.FindIndexByName(tc.index)
			if err != nil {
				t.Fatal(err)
			}
			builder := span.MakeBuilder(execCfg.Codec, desc, idx)
			if res := builder.CanSplitSpanIntoSeparateFamilies(
				tc.numNeededFamilies, tc.prefixLen, tc.containsNull); res != tc.canSplit {
				t.Errorf("expected result to be %v, but found %v", tc.canSplit, res)
			}
		})
	}
}
