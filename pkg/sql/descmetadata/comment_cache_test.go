// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descmetadata

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/stretchr/testify/require"
)

func TestCommentCacheShouldLoadFromDB(t *testing.T) {
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `INSERT INTO system.comments (type, object_id, sub_id, comment) VALUES (1, 1, 0, 'hi')`)

	// Test when there are less than 1000 comments (cache warmup size)
	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		cache := NewCommentCache(txn, s.InternalExecutor().(sqlutil.InternalExecutor)).(*metadataCache)
		err := cache.LoadCommentsForObjects(ctx, []descpb.ID{})
		require.NoError(t, err)

		require.False(t, cache.shouldLoadFromDB(1))
		require.False(t, cache.shouldLoadFromDB(math.MaxUint32))
		return nil
	})
	require.NoError(t, err)

	var insertSQLBuf strings.Builder
	insertSQLBuf.WriteString(`INSERT INTO system.comments (type, object_id, sub_id, comment) VALUES (1, 2, 0, 'hi')`)
	for i := 3; i <= 1001; i++ {
		_, err = fmt.Fprintf(&insertSQLBuf, ", (1, %d, 0, 'hi')", i)
		require.NoError(t, err)
	}
	sqlRunner.Exec(t, insertSQLBuf.String())

	// Test when there are more than 1000 comments (cache warmup size)
	err = kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		cache := NewCommentCache(txn, s.InternalExecutor().(sqlutil.InternalExecutor)).(*metadataCache)
		err := cache.LoadCommentsForObjects(ctx, []descpb.ID{})
		require.NoError(t, err)

		require.False(t, cache.shouldLoadFromDB(999))
		comment, ok, err := cache.GetTableComment(ctx, 999)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "hi", comment)

		require.True(t, cache.shouldLoadFromDB(1000))
		err = cache.LoadCommentsForObjects(ctx, []descpb.ID{1000})
		require.NoError(t, err)
		require.False(t, cache.shouldLoadFromDB(1000))
		comment, ok, err = cache.GetTableComment(ctx, 1000)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "hi", comment)

		require.True(t, cache.shouldLoadFromDB(1001))
		comment, ok, err = cache.GetTableComment(ctx, 1001)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "hi", comment)
		require.False(t, cache.shouldLoadFromDB(1001))

		require.True(t, cache.shouldLoadFromDB(1002))
		comment, ok, err = cache.GetTableComment(ctx, 1002)
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, "", comment)
		require.False(t, cache.shouldLoadFromDB(1002))
		return nil
	})
	require.NoError(t, err)
}
