// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestNewSchemaChangerKnob ensures that the new schema changer cannot be
// enabled without a testing knob being set.
func TestNewSchemaChangerKnob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "knob", func(t *testing.T, enabled bool) {
		ctx := context.Background()
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					AllowNewSchemaChanger: enabled,
				},
			},
		})
		defer s.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		tdb.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY, j INT)")
		// The session variable is allowed but requires the knob to do anything.
		tdb.Exec(t, "SET experimental_use_new_schema_changer = 'unsafe_always'")

		_, err := sqlDB.Exec("ALTER TABLE t DROP COLUMN j")
		if enabled {
			require.Regexp(t,
				`pq: \*tree.AlterTableDropColumn not implemented in the new schema changer`,
				err)
		} else {
			require.NoError(t, err)
		}
	})
}
