// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestUWIConstraintReferencingTypes tests that adding/dropping
// unique without index constraints that reference other type descriptors
// properly adds/drops back-references in the type descriptor.
func TestUWIConstraintReferencingTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "test-in-both-legacy-and-declarative-schema-changer", func(
		t *testing.T, useDeclarativeSchemaChanger bool,
	) {
		s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		if useDeclarativeSchemaChanger {
			tdb.Exec(t, "SET use_declarative_schema_changer = on;")
		} else {
			tdb.Exec(t, "SET use_declarative_schema_changer = off;")
		}
		tdb.Exec(t, "SET experimental_enable_unique_without_index_constraints = true;")
		tdb.Exec(t, "CREATE TYPE typ AS ENUM ('a', 'b');")
		tdb.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY, j STRING);")
		tdb.Exec(t, "ALTER TABLE t ADD UNIQUE WITHOUT INDEX (j) WHERE (j::typ != 'a');")

		// Ensure that `typ` has a back-reference to table `t`.
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec,
			"defaultdb", "t")
		typDesc := desctestutils.TestingGetPublicTypeDescriptor(kvDB, keys.SystemSQLCodec,
			"defaultdb", "typ")
		require.Equal(t, 1, typDesc.NumReferencingDescriptors())
		require.Equal(t, tableDesc.GetID(), typDesc.GetReferencingDescriptorID(0))

		// Ensure that dropping `typ` fails because `typ` is referenced by the constraint.
		tdb.ExpectErr(t, `pq: cannot drop type "typ" because other objects \(\[defaultdb.public.t\]\) still depend on it`, "DROP TYPE typ")

		// Ensure that dropping the constraint removes the back-reference from `typ`.
		tdb.Exec(t, "ALTER TABLE t DROP CONSTRAINT unique_j")
		typDesc = desctestutils.TestingGetPublicTypeDescriptor(kvDB, keys.SystemSQLCodec,
			"defaultdb", "typ")
		require.Zero(t, typDesc.NumReferencingDescriptors())

		// Ensure that now we can succeed dropping `typ`.
		tdb.Exec(t, "DROP TYPE typ")
	})
}
