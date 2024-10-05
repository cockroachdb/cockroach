// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestNoOpGrant tests that if a GRANT privilege statement is a no-op
// (e.g. GRANT SELECT on a table that a user already has SELECT privilege on)
// then the statement is actually a no-op (i.e. no schema change will happen).
func TestNoOpGrant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, "CREATE DATABASE db")
	tdb.Exec(t, "CREATE SCHEMA db.sc")
	tdb.Exec(t, "CREATE TABLE db.sc.tbl(a int)")
	tdb.Exec(t, "CREATE TYPE db.sc.typ AS ENUM('a', 'b')")
	tdb.Exec(t, "CREATE USER roach")

	// Assert that user `roach` should not have any privilege on the database, schema, and table.
	dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "db")
	scDesc := desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
	tblDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "db", "sc", "tbl")
	typDesc := desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "db", "sc", "typ")
	userRoach, err := username.MakeSQLUsernameFromUserInput("roach", username.PurposeValidation)
	require.NoError(t, err)
	_, ok := dbDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)
	_, ok = scDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)
	_, ok = tblDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)
	_, ok = typDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)

	retrieveDescriptorByObjectType := func(objectType privilege.ObjectType) catalog.Descriptor {
		var desc catalog.Descriptor
		switch objectType {
		case privilege.Database:
			desc = desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetName())
		case privilege.Schema:
			desc = desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), scDesc.GetName())
		case privilege.Table:
			desc = desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetName(), scDesc.GetName(), tblDesc.GetName())
		case privilege.Type:
			desc = desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetName(), scDesc.GetName(), typDesc.GetName())
		}
		return desc
	}

	testCases := []struct {
		objectType   privilege.ObjectType
		allowedPrivs []privilege.Kind
		objectName   string
		retrieveDesc func(objectType privilege.ObjectType) catalog.Descriptor
	}{
		{
			objectType:   privilege.Database,
			allowedPrivs: privilege.DBPrivileges,
			objectName:   "db",
		},
		{
			objectType:   privilege.Schema,
			allowedPrivs: privilege.SchemaPrivileges,
			objectName:   "db.sc",
		},
		{
			objectType:   privilege.Table,
			allowedPrivs: privilege.TablePrivileges,
			objectName:   "db.sc.tbl",
		},
		{
			objectType:   privilege.Type,
			allowedPrivs: privilege.TypePrivileges,
			objectName:   "db.sc.typ",
		},
	}

	for _, testCase := range testCases {
		objectType := testCase.objectType
		objectName := testCase.objectName

		for _, priv := range testCase.allowedPrivs {
			if priv == privilege.ALL {
				// We exclude `ALL` privilege here because granting `ALL` here will disturb testing for other privileges.
				continue
			}

			// Grant privilege `privilege` on `objectType` `objectName` to user `roach`.
			tdb.Exec(t, fmt.Sprintf("GRANT %v ON %v %v TO %v", priv.DisplayName(), objectType, objectName, userRoach.Normalized()))
			desc := retrieveDescriptorByObjectType(objectType)
			userPriv, ok := desc.GetPrivileges().FindUser(userRoach)
			require.True(t, ok)
			require.True(t, priv.IsSetIn(userPriv.Privileges))
			descVersion := desc.GetVersion()

			// Repeat and check we no-oped this GRANT by asserting that the privilege remains there and
			// the table version remains the same.
			tdb.Exec(t, fmt.Sprintf("GRANT %v ON %v %v TO %v", priv.DisplayName(), objectType, objectName, userRoach.Normalized()))
			desc = retrieveDescriptorByObjectType(objectType)
			userPriv, ok = desc.GetPrivileges().FindUser(userRoach)
			require.True(t, ok)
			require.True(t, priv.IsSetIn(userPriv.Privileges))
			require.Equal(t, descVersion, desc.GetVersion())
		}

		// Now we can safely test granting `ALL` to `roach` with the same approach.
		tdb.Exec(t, fmt.Sprintf("GRANT ALL ON %v %v TO %v", objectType, objectName, userRoach.Normalized()))
		desc := retrieveDescriptorByObjectType(objectType)
		userPriv, ok := desc.GetPrivileges().FindUser(userRoach)
		require.True(t, ok)
		require.True(t, privilege.ALL.IsSetIn(userPriv.Privileges))
		descVersion := desc.GetVersion()

		tdb.Exec(t, fmt.Sprintf("GRANT ALL ON %v %v TO %v", objectType, objectName, userRoach.Normalized()))
		desc = retrieveDescriptorByObjectType(objectType)
		userPriv, ok = desc.GetPrivileges().FindUser(userRoach)
		require.True(t, ok)
		require.True(t, privilege.ALL.IsSetIn(userPriv.Privileges))
		require.Equal(t, descVersion, desc.GetVersion())
	}
}

// TestNoOpRevoke tests that if a REVOKE privilege statement is a no-op
// (e.g. REVOKE SELECT on a table that a user does not have SELECT privilege on)
// then the statement is actually a no-op (i.e. no schema change will happen).
func TestNoOpRevoke(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, "CREATE DATABASE db")
	tdb.Exec(t, "CREATE SCHEMA db.sc")
	tdb.Exec(t, "CREATE TABLE db.sc.tbl(a int)")
	tdb.Exec(t, "CREATE TYPE db.sc.typ AS ENUM('a', 'b')")
	tdb.Exec(t, "CREATE USER roach")

	// Assert that user `roach` should not have any privilege on the database, schema, and table.
	dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "db")
	scDesc := desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
	tblDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "db", "sc", "tbl")
	typDesc := desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "db", "sc", "typ")
	userRoach, err := username.MakeSQLUsernameFromUserInput("roach", username.PurposeValidation)
	require.NoError(t, err)
	_, ok := dbDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)
	_, ok = scDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)
	_, ok = tblDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)
	_, ok = typDesc.GetPrivileges().FindUser(userRoach)
	require.False(t, ok)

	retrieveDescriptorByObjectType := func(objectType privilege.ObjectType) catalog.Descriptor {
		var desc catalog.Descriptor
		switch objectType {
		case privilege.Database:
			desc = desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetName())
		case privilege.Schema:
			desc = desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), scDesc.GetName())
		case privilege.Table:
			desc = desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetName(), scDesc.GetName(), tblDesc.GetName())
		case privilege.Type:
			desc = desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetName(), scDesc.GetName(), typDesc.GetName())
		}
		return desc
	}

	testCases := []struct {
		objectType privilege.ObjectType
		// allowedPrivs will contain all allowed privileges for this object type except for 'ALL' and 'GRANT'
		allowedPrivs []privilege.Kind
		objectName   string
		retrieveDesc func(privilege.ObjectType) catalog.Descriptor
	}{
		{
			objectType:   privilege.Database,
			allowedPrivs: privilege.DBPrivileges,
			objectName:   "db",
		},
		{
			objectType:   privilege.Schema,
			allowedPrivs: privilege.SchemaPrivileges,
			objectName:   "db.sc",
		},
		{
			objectType:   privilege.Table,
			allowedPrivs: privilege.TablePrivileges,
			objectName:   "db.sc.tbl",
		},
		{
			objectType:   privilege.Type,
			allowedPrivs: privilege.TypePrivileges,
			objectName:   "db.sc.typ",
		},
	}

	for _, testCase := range testCases {
		objectType := testCase.objectType
		objectName := testCase.objectName
		objectVersionBeforeRevoke := retrieveDescriptorByObjectType(objectType).GetVersion()

		for _, priv := range testCase.allowedPrivs {
			// Revoke privilege `privilege` on `objectType` `objectName` from user `roach`.
			// Since `roach` has no privileges at all, those revokes should be treated as no-ops.
			tdb.Exec(t, fmt.Sprintf("REVOKE %v ON %v %v FROM %v", priv.DisplayName(), objectType, objectName, userRoach.Normalized()))
			desc := retrieveDescriptorByObjectType(objectType)
			require.Equal(t, objectVersionBeforeRevoke, desc.GetVersion())
		}
	}
}
