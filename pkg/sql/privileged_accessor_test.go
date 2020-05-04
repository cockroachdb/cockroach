// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupNamespaceID tests the lookup namespace id falls back
// onto system.namespace_deprecated.
func TestLookupNamespaceIDFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.Put(
			ctx,
			sqlbase.NewDeprecatedTableKey(999, "bob").Key(keys.SystemSQLCodec),
			9999,
		)
	})
	require.NoError(t, err)

	// Assert the row exists in the database.
	var id int64
	err = sqlDB.QueryRow(
		`SELECT crdb_internal.get_namespace_id($1, $2)`,
		999,
		"bob",
	).Scan(&id)
	require.NoError(t, err)
	assert.Equal(t, int64(9999), id)
}
