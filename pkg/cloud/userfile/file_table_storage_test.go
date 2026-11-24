// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package userfile_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/cloud/userfile"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPutUserFileTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	qualifiedTableName := "defaultdb.public.user_file_table_test"
	filename := "path/to/file"

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	dest := userfile.MakeUserFileStorageURI(qualifiedTableName, filename)

	db := s.InternalDB().(isql.DB)
	info := cloudtestutils.StoreInfo{
		URI:  dest,
		User: username.RootUserName(),
		DB:   db,
	}
	cloudtestutils.CheckExportStore(t, info)

	info.URI = "userfile://defaultdb.public.file_list_table/listing-test/basepath"
	cloudtestutils.CheckListFiles(t, info)

	t.Run("empty-qualified-table-name", func(t *testing.T) {
		info.URI = userfile.MakeUserFileStorageURI("", filename)
		cloudtestutils.CheckExportStore(t, info)

		info.URI = "userfile:///listing-test/basepath"
		cloudtestutils.CheckListFilesCanonical(t, info, "userfile://defaultdb.public.userfiles_root/listing-test/basepath")
	})

	t.Run("reject-normalized-basename", func(t *testing.T) {
		testfile := "listing-test/../basepath"
		userfileURL := url.URL{Scheme: "userfile", Host: qualifiedTableName, Path: ""}

		store, err := cloud.ExternalStorageFromURI(ctx, userfileURL.String()+"/",
			base.ExternalIODirConfig{}, cluster.NoSettings, blobs.TestEmptyBlobClientFactory,
			username.RootUserName(), db, nil, cloud.NilMetrics)
		require.NoError(t, err)
		defer store.Close()

		err = cloud.WriteFile(ctx, store, testfile, bytes.NewReader([]byte{0}))
		require.True(t, testutils.IsError(err, "does not permit such constructs"))
	})
}

func createUserGrantAllPrivieleges(
	username username.SQLUsername, database string, sqlDB *gosql.DB,
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
	defer log.Scope(t).Close(t)

	qualifiedTableName := "defaultdb.public.user_file_table_test"
	filename := "path/to/file"

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	dest := userfile.MakeUserFileStorageURI(qualifiedTableName, "")
	db := s.InternalDB().(isql.DB)

	// Create two users and grant them all privileges on defaultdb.
	user1 := username.MakeSQLUsernameFromPreNormalizedString("foo")
	require.NoError(t, createUserGrantAllPrivieleges(user1, "defaultdb", sqlDB))
	user2 := username.MakeSQLUsernameFromPreNormalizedString("bar")
	require.NoError(t, createUserGrantAllPrivieleges(user2, "defaultdb", sqlDB))

	// Write file as user1.
	fileTableSystem1, err := cloud.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user1, db, nil,
		cloud.NilMetrics)
	require.NoError(t, err)
	require.NoError(t, cloud.WriteFile(ctx, fileTableSystem1, filename, bytes.NewReader([]byte("aaa"))))

	// Attempt to read/write file as user2 and expect to fail.
	fileTableSystem2, err := cloud.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user2, db, nil,
		cloud.NilMetrics)
	require.NoError(t, err)
	_, _, err = fileTableSystem2.ReadFile(ctx, filename, cloud.ReadOptions{NoFileSize: true})
	require.Error(t, err)
	require.Error(t, cloud.WriteFile(ctx, fileTableSystem2, filename, bytes.NewReader([]byte("aaa"))))

	// Read file as root and expect to succeed.
	fileTableSystem3, err := cloud.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, username.RootUserName(), db,
		nil, cloud.NilMetrics)
	require.NoError(t, err)
	_, _, err = fileTableSystem3.ReadFile(ctx, filename, cloud.ReadOptions{NoFileSize: true})
	require.NoError(t, err)
}
