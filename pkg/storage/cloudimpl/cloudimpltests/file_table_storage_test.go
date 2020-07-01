// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cloudimpltests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestPutUserFileTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qualifiedTableName := "defaultdb.public.user_file_table_test"
	filename := "path/to/file"

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, _, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	dest := cloudimpl.MakeUserFileStorageURI(qualifiedTableName, filename)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	testExportStore(t, dest, false, security.RootUser, ie, kvDB)

	testListFiles(t, "userfile://defaultdb.public.file_list_table/listing-test/basepath",
		security.RootUser, ie, kvDB)
}

func createUserGrantAllPrivieleges(username, database string, sqlDB *gosql.DB) error {
	_, err := sqlDB.Exec(fmt.Sprintf("CREATE USER %s", username))
	if err != nil {
		return err
	}
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", database, username))
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

	dest := cloudimpl.MakeUserFileStorageURI(qualifiedTableName, "")
	ie := s.InternalExecutor().(*sql.InternalExecutor)

	// Create two users and grant them all privileges on defaultdb.
	user1 := "foo"
	require.NoError(t, createUserGrantAllPrivieleges(user1, "defaultdb", sqlDB))
	user2 := "bar"
	require.NoError(t, createUserGrantAllPrivieleges(user2, "defaultdb", sqlDB))

	// Write file as user1.
	fileTableSystem1, err := cloudimpl.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user1, ie, kvDB)
	require.NoError(t, err)
	require.NoError(t, fileTableSystem1.WriteFile(ctx, filename, bytes.NewReader([]byte("aaa"))))

	// Attempt to read/write file as user2 and expect to fail.
	fileTableSystem2, err := cloudimpl.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user2, ie, kvDB)
	require.NoError(t, err)
	_, err = fileTableSystem2.ReadFile(ctx, filename)
	require.Error(t, err)
	require.Error(t, fileTableSystem2.WriteFile(ctx, filename, bytes.NewReader([]byte("aaa"))))

	// Read file as root and expect to succeed.
	fileTableSystem3, err := cloudimpl.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
		cluster.NoSettings, blobs.TestEmptyBlobClientFactory, security.RootUser, ie, kvDB)
	require.NoError(t, err)
	_, err = fileTableSystem3.ReadFile(ctx, filename)
	require.NoError(t, err)
}
