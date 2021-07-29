// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCreateSystemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	table := tabledesc.NewBuilder(systemschema.NamespaceTable.TableDesc()).BuildExistingMutableTable()
	table.ID = keys.MaxReservedDescID

	prevPrivileges, ok := descpb.SystemAllowedPrivileges[table.ID]
	defer func() {
		if ok {
			// Restore value of privileges.
			descpb.SystemAllowedPrivileges[table.ID] = prevPrivileges
		} else {
			delete(descpb.SystemAllowedPrivileges, table.ID)
		}
	}()
	descpb.SystemAllowedPrivileges[table.ID] = descpb.SystemAllowedPrivileges[keys.NamespaceTableID]

	table.Name = "dummy"
	nameKey := catalogkeys.MakePublicObjectNameKey(keys.SystemSQLCodec, table.ParentID, table.Name)
	descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, table.ID)
	descVal := table.DescriptorProto()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Verify that the keys were not written.
	checkNotExists := func(t *testing.T, key roachpb.Key) {
		kv, err := kvDB.Get(ctx, key)
		require.NoError(t, err)
		require.False(t, kv.Exists(),
			"expected %q not to exist, got %v", nameKey, kv)
	}
	checkNotExists(t, nameKey)
	checkNotExists(t, descKey)

	require.NoError(t, CreateSystemTable(ctx, kvDB, keys.SystemSQLCodec, table))

	// Verify that the appropriate keys were written.
	{
		kv, err := kvDB.Get(ctx, nameKey)
		require.NoError(t, err)
		require.Truef(t, kv.Exists(),
			"expected %q to exist, got that it doesn't exist", nameKey)
	}
	{
		var descriptor descpb.Descriptor
		err := kvDB.GetProto(ctx, descKey, &descriptor)
		require.NoError(t, err)
		require.Truef(t, !descVal.Equal(&descriptor),
			"expected %v for key %q, got %v", descVal, descKey, descriptor)
	}

	// Verify the idempotency of the call.
	require.NoError(t, CreateSystemTable(ctx, kvDB, keys.SystemSQLCodec, table))
}
