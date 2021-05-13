// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package userfile

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestPutUserFileTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qualifiedTableName := "defaultdb.public.user_file_table_test"
	filename := "path/to/file"

	testSettings := cluster.MakeTestingClusterSettings()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, _, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	dest := MakeUserFileStorageURI(qualifiedTableName, filename)

	ie := s.InternalExecutor().(sqlutil.InternalExecutor)
	cloudtestutils.CheckExportStore(t, dest, false, security.RootUserName(), ie, kvDB, testSettings)

	cloudtestutils.CheckListFiles(t, "userfile://defaultdb.public.file_list_table/listing-test/basepath",
		security.RootUserName(), ie, kvDB, testSettings)

	t.Run("empty-qualified-table-name", func(t *testing.T) {
		dest := MakeUserFileStorageURI("", filename)

		ie := s.InternalExecutor().(sqlutil.InternalExecutor)
		cloudtestutils.CheckExportStore(t, dest, false, security.RootUserName(), ie, kvDB, testSettings)

		cloudtestutils.CheckListFilesCanonical(t, "userfile:///listing-test/basepath", "userfile://defaultdb.public.userfiles_root/listing-test/basepath",
			security.RootUserName(), ie, kvDB, testSettings)
	})

	t.Run("reject-normalized-basename", func(t *testing.T) {
		testfile := "listing-test/../basepath"
		userfileURL := url.URL{Scheme: "userfile", Host: qualifiedTableName, Path: ""}

		store, err := cloud.ExternalStorageFromURI(ctx, userfileURL.String()+"/",
			base.ExternalIODirConfig{}, cluster.NoSettings, blobs.TestEmptyBlobClientFactory,
			security.RootUserName(), ie, kvDB)
		require.NoError(t, err)
		defer store.Close()

		err = cloud.WriteFile(ctx, store, testfile, bytes.NewReader([]byte{0}))
		require.True(t, testutils.IsError(err, "does not permit such constructs"))
	})
}

func createUserGrantAllPrivieleges(
	username security.SQLUsername, database string, sqlDB *gosql.DB,
) error {
	_, err := sqlDB.Exec(fmt.Sprintf("CREATE USER %s", username.SQLIdentifier()))
	if err != nil {
		return err
	}
	dbName := tree.Name(database)
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", &dbName, username.SQLIdentifier()))
	if err != nil {
		return err
	}

	return nil
}

func TestUserScoping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qualifiedTableName := "defaultdb.public.user_file_table_test"
	filename := "path/to/file"

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	dest := MakeUserFileStorageURI(qualifiedTableName, "")
	ie := s.InternalExecutor().(sqlutil.InternalExecutor)

	// Create two users and grant them all privileges on defaultdb.
	user1 := security.MakeSQLUsernameFromPreNormalizedString("foo")
	require.NoError(t, createUserGrantAllPrivieleges(user1, "defaultdb", sqlDB))
	user2 := security.MakeSQLUsernameFromPreNormalizedString("bar")
	require.NoError(t, createUserGrantAllPrivieleges(user2, "defaultdb", sqlDB))

	// Write file as user1.
	fileTableSystem1, err := cloud.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user1, ie, kvDB)
	require.NoError(t, err)
	require.NoError(t, cloud.WriteFile(ctx, fileTableSystem1, filename, bytes.NewReader([]byte("aaa"))))

	// Attempt to read/write file as user2 and expect to fail.
	fileTableSystem2, err := cloud.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user2, ie, kvDB)
	require.NoError(t, err)
	_, err = fileTableSystem2.ReadFile(ctx, filename)
	require.Error(t, err)
	require.Error(t, cloud.WriteFile(ctx, fileTableSystem2, filename, bytes.NewReader([]byte("aaa"))))

	// Read file as root and expect to succeed.
	fileTableSystem3, err := cloud.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, security.RootUserName(), ie, kvDB)
	require.NoError(t, err)
	_, err = fileTableSystem3.ReadFile(ctx, filename)
	require.NoError(t, err)
}
