// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
)

// TestStatsAnyType verifies that table stats collection on a column of any type
// works.
func TestStatsAnyType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	st := srv.ApplicationLayer().ClusterSettings()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	var typ *types.T
loop:
	for {
		typ = RandType(rng)
		switch typ.Oid() {
		case oid.T_int2vector, oid.T_oidvector:
			// We can't create a table with a column of this type.
			continue loop
		case oid.T_regtype:
			// Casting random integers to REGTYPE might fail.
			continue loop
		}
		if typ.Family() == types.ArrayFamily &&
			(typ.ArrayContents().Family() == types.FloatFamily || typ.ArrayContents().Family() == types.DecimalFamily) {
			// Skip arrays of floats and decimals since they might contain
			// infinities as elements which are a bit annoying to insert below.
			continue loop
		}
		if err := colinfo.ValidateColumnDefType(ctx, st.Version, typ); err == nil {
			break
		}
	}
	numRows := 1 + rng.Intn(10)
	t.Logf("picked %s and %d rows", typ.SQLString(), numRows)
	runner.Exec(t, fmt.Sprintf("CREATE TABLE t(c %s)", typ.SQLString()))
	for i := 0; i < numRows; i++ {
		d := RandDatum(rng, typ, false /* nullOk */)
		dString := d.String()
		switch typ.Family() {
		case types.IntFamily, types.FloatFamily, types.DecimalFamily:
			dString = "'" + dString + "'"
		case types.TSQueryFamily, types.TSVectorFamily:
			dString = "'" + strings.ReplaceAll(dString, "'", "''") + "'"
		}
		runner.Exec(t, fmt.Sprintf("INSERT INTO t VALUES (%s::%s)", dString, typ.SQLString()))
	}
	runner.Exec(t, "ANALYZE t")
}
