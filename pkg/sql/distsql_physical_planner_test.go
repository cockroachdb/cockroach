// Copyright 2016 The Cockroach Authors.

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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDistSQLPlanner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	args := base.TestClusterArgs{ReplicationMode: base.ReplicationManual}
	tc := serverutils.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop()

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t",
		"num INT PRIMARY KEY, str STRING, mod INT, INDEX(mod)",
		10, sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowEnglishFn, sqlutils.RowModuloFn(3)),
	)

	r := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	r.Exec("SET DIST_SQL = ALWAYS")
	r.CheckQueryResults(
		"SELECT 5, 2 + num, * FROM test.t ORDER BY str",
		[][]string{
			strings.Fields("5 10  8 eight    2"),
			strings.Fields("5  7  5 five     2"),
			strings.Fields("5  6  4 four     1"),
			strings.Fields("5 11  9 nine     0"),
			strings.Fields("5  3  1 one      1"),
			strings.Fields("5 12 10 one-zero 1"),
			strings.Fields("5  9  7 seven    1"),
			strings.Fields("5  8  6 six      0"),
			strings.Fields("5  5  3 three    0"),
			strings.Fields("5  4  2 two      2"),
		},
	)
	r.CheckQueryResults(
		"SELECT str FROM test.t WHERE mod=0",
		[][]string{
			{"three"},
			{"six"},
			{"nine"},
		},
	)
}
