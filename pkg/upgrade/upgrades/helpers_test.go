// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	HasColumn         = hasColumn
	HasIndex          = hasIndex
	DoesNotHaveIndex  = doesNotHaveIndex
	HasColumnFamily   = hasColumnFamily
	CreateSystemTable = createSystemTable
)

type Schema struct {
	// Schema name.
	Name string
	// Function that validates the schema.
	ValidationFn func(catalog.TableDescriptor, catalog.TableDescriptor, string) (bool, error)
}

// Upgrade runs cluster upgrade by changing the 'version' cluster setting.
func Upgrade(
	t *testing.T, sqlDB *gosql.DB, key clusterversion.Key, done chan struct{}, expectError bool,
) {
	UpgradeToVersion(t, sqlDB, clusterversion.ByKey(key), done, expectError)
}

func UpgradeToVersion(
	t *testing.T, sqlDB *gosql.DB, v roachpb.Version, done chan struct{}, expectError bool,
) {
	defer func() {
		if done != nil {
			done <- struct{}{}
		}
	}()
	_, err := sqlDB.Exec(`SET CLUSTER SETTING version = $1`,
		v.String())
	if expectError {
		assert.Error(t, err)
		return
	}
	assert.NoError(t, err)
}

// InjectLegacyTable overwrites the existing table descriptor with the previous table descriptor.
func InjectLegacyTable(
	ctx context.Context,
	t *testing.T,
	s serverutils.TestServerInterface,
	table catalog.TableDescriptor,
	getDeprecatedDescriptor func() *descpb.TableDescriptor,
) {
	err := s.InternalExecutorFactory().(descs.TxnManager).DescsTxn(ctx, s.DB(), func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		id := table.GetID()
		tab, err := descriptors.GetMutableTableByID(ctx, txn, id, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}
		builder := tabledesc.NewBuilder(getDeprecatedDescriptor())
		if err := builder.RunPostDeserializationChanges(); err != nil {
			return err
		}
		tab.TableDescriptor = builder.BuildCreatedMutableTable().TableDescriptor
		tab.Version = tab.ClusterVersion().Version + 1
		return descriptors.WriteDesc(ctx, false /* kvTrace */, tab, txn)
	})
	require.NoError(t, err)
}

// ValidateSchemaExists validates whether the schema changes of the system table exist or not.
func ValidateSchemaExists(
	ctx context.Context,
	t *testing.T,
	s serverutils.TestServerInterface,
	sqlDB *gosql.DB,
	storedTableID descpb.ID,
	expectedTable catalog.TableDescriptor,
	stmts []string,
	schemas []Schema,
	expectExists bool,
) {
	// First validate by reading the columns and the index.
	for _, stmt := range stmts {
		_, err := sqlDB.Exec(stmt)
		if expectExists {
			require.NoErrorf(
				t, err, "expected schema to exist, but unable to query it, using statement: %s", stmt,
			)
		} else {
			require.Errorf(
				t, err, "expected schema to not exist, but queried it successfully, using statement: %s", stmt,
			)
		}
	}

	// Manually verify the table descriptor.
	storedTable := GetTable(ctx, t, s, storedTableID)
	str := "not have"
	if expectExists {
		str = "have"
	}
	for _, schema := range schemas {
		updated, err := schema.ValidationFn(storedTable, expectedTable, schema.Name)
		require.NoError(t, err)
		require.Equal(t, expectExists, updated,
			"expected table to %s %s (name=%s)", str, schema, schema.Name)
	}
}

// GetTable returns the system table descriptor, reading it from storage.
func GetTable(
	ctx context.Context, t *testing.T, s serverutils.TestServerInterface, tableID descpb.ID,
) catalog.TableDescriptor {
	var table catalog.TableDescriptor
	// Retrieve the table.
	err := s.InternalExecutorFactory().(descs.TxnManager).DescsTxn(ctx, s.DB(), func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		table, err = descriptors.ByID(txn).WithoutNonPublic().WithoutLeased().Immutable().Table(ctx, tableID)
		return err
	})
	require.NoError(t, err)
	return table
}

// WaitForJobStatement is exported so that it can be detected by a testing knob.
const WaitForJobStatement = waitForJobStatement
