// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	gosql "database/sql"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	HasColumn           = hasColumn
	HasIndex            = hasIndex
	DoesNotHaveIndex    = doesNotHaveIndex
	HasColumnFamily     = hasColumnFamily
	CreateSystemTable   = createSystemTable
	OnlyHasColumnFamily = onlyHasColumnFamily
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
	UpgradeToVersion(t, sqlDB, key.Version(), done, expectError)
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
	err := s.InternalDB().(descs.DB).DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		deprecatedDesc := getDeprecatedDescriptor()
		var tab *tabledesc.Mutable
		switch id := table.GetID(); id {
		// If the table descriptor does not have a valid ID, it must be a system
		// table with a dynamically-allocated ID.
		case descpb.InvalidID:
			var err error
			tab, err = txn.Descriptors().MutableByName(txn.KV()).Table(ctx,
				systemschema.SystemDB, schemadesc.GetPublicSchema(), table.GetName())
			if err != nil {
				return err
			}
			deprecatedDesc.ID = tab.GetID()
		default:
			var err error
			tab, err = txn.Descriptors().MutableByID(txn.KV()).Table(ctx, id)
			if err != nil {
				return err
			}
		}
		builder := tabledesc.NewBuilder(deprecatedDesc)
		if err := builder.RunPostDeserializationChanges(); err != nil {
			return err
		}
		tab.TableDescriptor = builder.BuildCreatedMutableTable().TableDescriptor
		tab.Version = tab.ClusterVersion().Version + 1
		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, tab, txn.KV())
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
	err := s.InternalDB().(descs.DB).DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) (err error) {
		table, err = txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
		return err
	})
	require.NoError(t, err)
	return table
}

// WaitForJobStatement is exported so that it can be detected by a testing knob.
const WaitForJobStatement = waitForJobStatement

// ExecForCountInTxns allows statements to be repeatedly run on a database
// in transactions of a specified size.
func ExecForCountInTxns(
	ctx context.Context,
	t *testing.T,
	db *gosql.DB,
	count int,
	txnSize int,
	fn func(tx *gosql.Tx, i int) error,
) {
	numTxns := int(math.Ceil(float64(count) / float64(txnSize)))
	for txnNum := 0; txnNum < numTxns; txnNum++ {
		iterEnd := (txnNum + 1) * txnSize
		if count < iterEnd {
			iterEnd = count
		}
		err := crdb.ExecuteTx(ctx, db, nil /* opts */, func(tx *gosql.Tx) error {
			for i := txnNum * txnSize; i < iterEnd; i++ {
				if err := fn(tx, i); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}
}

// ValidateSystemDatabaseSchemaVersionBumped validates that the system database
// schema version has been bumped to the expected version.
func ValidateSystemDatabaseSchemaVersionBumped(
	t *testing.T, sqlDB *gosql.DB, expectedVersion clusterversion.Key,
) {
	expectedSchemaVersion := expectedVersion.Version()

	var actualSchemaVersionBytes []byte
	require.NoError(t, sqlDB.QueryRow(
		`SELECT crdb_internal.json_to_pb(
    'cockroach.roachpb.Version',
    crdb_internal.pb_to_json(
        'cockroach.sql.sqlbase.Descriptor',
        descriptor,
        false
    )->'database'->'systemDatabaseSchemaVersion')
FROM system.descriptor WHERE id = $1`,
		keys.SystemDatabaseID,
	).Scan(&actualSchemaVersionBytes))

	actualSchemaVersion := roachpb.Version{}
	require.NoError(t, protoutil.Unmarshal(actualSchemaVersionBytes, &actualSchemaVersion))

	require.Equal(t, expectedSchemaVersion, actualSchemaVersion)
}
