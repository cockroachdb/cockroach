// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package filetable

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

const database = "defaultdb"

// uploadFile generates random data and copies it to the FileTableSystem via
// the FileWriter.
func uploadFile(
	t *testing.T,
	ctx context.Context,
	filename string,
	fileSize, chunkSize int,
	ft *FileToTableSystem,
) []byte {
	data := make([]byte, fileSize)
	randGen, _ := randutil.NewPseudoRand()
	randutil.ReadTestdataBytes(randGen, data)

	writer, err := ft.NewFileWriter(ctx, filename, chunkSize)
	require.NoError(t, err)
	_, err = io.Copy(writer, bytes.NewReader(data))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	return data
}

func TestListAndDeleteFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, _, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	fileTableReadWriter, err := NewFileToTableSystem(ctx, database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"root")
	require.NoError(t, err)

	// Create first test file with multiple chunks.
	const size = 1024
	_ = uploadFile(t, ctx, "file1", size, chunkDefaultSize, fileTableReadWriter)

	// Create second test file with multiple chunks.
	_ = uploadFile(t, ctx, "file2", size, chunkDefaultSize, fileTableReadWriter)

	// Create third test file with multiple chunks.
	_ = uploadFile(t, ctx, "file3", size, chunkDefaultSize, fileTableReadWriter)

	// List files before delete.
	files, err := fileTableReadWriter.ListFiles(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2", "file3"}, files)

	// Delete file1.
	require.NoError(t, fileTableReadWriter.DeleteFile(ctx, "file1"))

	// List files.
	files, err = fileTableReadWriter.ListFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, []string{"file2", "file3"}, files)

	// Destroy the filesystem.
	require.NoError(t, DestroyUserFileSystem(ctx, fileTableReadWriter))
}

func TestReadWriteFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, _, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	fileTableReadWriter, err := NewFileToTableSystem(ctx, database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"root")
	require.NoError(t, err)

	testFileName := "testfile"

	isContentEqual := func(filename string, expected []byte, ft *FileToTableSystem) bool {
		reader, err := fileTableReadWriter.ReadFile(ctx, filename)
		require.NoError(t, err)
		got, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		return bytes.Equal(got, expected)
	}
	t.Run("empty-file", func(t *testing.T) {
		expected := uploadFile(t, ctx, testFileName, 0, 1024, fileTableReadWriter)
		size, err := fileTableReadWriter.FileSize(ctx, testFileName)
		require.NoError(t, err)
		require.Equal(t, size, int64(0))
		require.True(t, isContentEqual(testFileName, expected, fileTableReadWriter))
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("single-byte-chunk", func(t *testing.T) {
		expected := uploadFile(t, ctx, testFileName, 1024, 1, fileTableReadWriter)
		size, err := fileTableReadWriter.FileSize(ctx, testFileName)
		require.NoError(t, err)
		require.Equal(t, size, int64(1024))
		require.True(t, isContentEqual(testFileName, expected, fileTableReadWriter))
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("file-size-chunk", func(t *testing.T) {
		expected := uploadFile(t, ctx, testFileName, 1024, 1024, fileTableReadWriter)
		size, err := fileTableReadWriter.FileSize(ctx, testFileName)
		require.NoError(t, err)
		require.Equal(t, size, int64(1024))
		require.True(t, isContentEqual(testFileName, expected, fileTableReadWriter))
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("large-file", func(t *testing.T) {
		expected := uploadFile(t, ctx, testFileName, 1024*1024, 1024, fileTableReadWriter)
		size, err := fileTableReadWriter.FileSize(ctx, testFileName)
		require.NoError(t, err)
		require.Equal(t, size, int64(1024*1024))
		require.True(t, isContentEqual(testFileName, expected, fileTableReadWriter))
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("one-extra-chunk", func(t *testing.T) {
		expected := uploadFile(t, ctx, testFileName, 11, 2, fileTableReadWriter)
		size, err := fileTableReadWriter.FileSize(ctx, testFileName)
		require.NoError(t, err)
		require.Equal(t, size, int64(11))
		require.True(t, isContentEqual(testFileName, expected, fileTableReadWriter))
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("file-already-exists", func(t *testing.T) {
		_ = uploadFile(t, ctx, testFileName, 11, 2, fileTableReadWriter)
		_, err := fileTableReadWriter.NewFileWriter(ctx, testFileName, 2)
		require.Error(t, err)
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("write-delete-write", func(t *testing.T) {
		write1 := uploadFile(t, ctx, testFileName, 11, 2, fileTableReadWriter)
		require.True(t, isContentEqual(testFileName, write1, fileTableReadWriter))

		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))

		// Write same file name but different size configuration.
		write2 := uploadFile(t, ctx, testFileName, 1024, 4, fileTableReadWriter)
		require.True(t, isContentEqual(testFileName, write2, fileTableReadWriter))
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})
}

// TestUserGrants tests that a new user with only CREATE privileges can use all
// the FileToTableSystem methods after creating the FileToTableSystem, which is
// responsible for granting SELECT, INSERT, DELETE and DROP privileges on the
// file and payload tables.
func TestUserGrants(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create non-admin user with only CREATE privilege on the database.
	_, err := sqlDB.Exec("CREATE USER john")
	require.NoError(t, err)
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT CREATE ON DATABASE %s TO john", database))
	require.NoError(t, err)

	// Switch to non-admin user.
	pgURL, cleanupGoDB := sqlutils.PGUrlWithOptionalClientCerts(
		t, s.ServingSQLAddr(), "notAdmin", url.User("john"), false, /* withCerts */
	)
	defer cleanupGoDB()
	pgURL.RawQuery = "sslmode=disable"
	userDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer userDB.Close()

	fileTableReadWriter, err := NewFileToTableSystem(ctx, database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	require.NoError(t, err)

	// Upload a file to test INSERT privilege.
	expected := uploadFile(t, ctx, "file1", 1024, 1, fileTableReadWriter)

	// Read file to test SELECT privilege.
	reader, err := fileTableReadWriter.ReadFile(ctx, "file1")
	require.NoError(t, err)
	got, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(got, expected))

	// Delete file to test DELETE privilege.
	require.NoError(t, fileTableReadWriter.DeleteFile(ctx, "file1"))

	// Delete all files to test DROP privilege.
	require.NoError(t, DestroyUserFileSystem(ctx, fileTableReadWriter))
}

func getTableGrantees(ctx context.Context, tablename string, conn *gosql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SELECT grantee FROM [SHOW GRANTS ON %s]`,
		tablename))
	if err != nil {
		return nil, err
	}

	var grantees []string
	for rows.Next() {
		var grantee string
		err = rows.Scan(&grantee)
		if err != nil {
			return nil, err
		}
		grantees = append(grantees, grantee)
	}

	sort.Strings(grantees)
	return grantees, nil
}

// TestDifferentUserDisallowed tests that a user who does not own the file and
// payload tables but has ALL privileges on the database cannot access the
// tables once they have been created/written to.
func TestDifferentUserDisallowed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)

	// Create non-admin user with only CREATE privilege on the database.
	_, err = sqlDB.Exec("CREATE USER john")
	require.NoError(t, err)
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT CREATE ON DATABASE %s TO john", database))
	require.NoError(t, err)

	// Create non-admin user with ALL privileges on the database.
	_, err = sqlDB.Exec("CREATE USER doe")
	require.NoError(t, err)
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT ALL ON DATABASE %s TO doe", database))
	require.NoError(t, err)

	// Switch to non-admin user john.
	pgURL, cleanupGoDB := sqlutils.PGUrlWithOptionalClientCerts(
		t, s.ServingSQLAddr(), "notAdmin", url.User("john"), false, /* withCerts */
	)
	defer cleanupGoDB()
	pgURL.RawQuery = "sslmode=disable"
	userDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer userDB.Close()

	fileTableReadWriter, err := NewFileToTableSystem(ctx, database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	require.NoError(t, err)

	_ = uploadFile(t, ctx, "file1", 1024, 10, fileTableReadWriter)

	// Switch to non-admin user doe who should not have access to john's tables.
	pgURL, cleanupGoDB = sqlutils.PGUrlWithOptionalClientCerts(
		t, s.ServingSQLAddr(), "notAdmin", url.User("doe"), false, /* withCerts */
	)
	defer cleanupGoDB()
	pgURL.RawQuery = "sslmode=disable"
	userDB, err = gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer userDB.Close()

	// Under normal circumstances Doe should have ALL privileges on the file and
	// payload tables created by john above. FileToTableSystem should have
	// revoked these privileges.
	fileTableName := fileTableNamePrefix + "john"
	payloadTableName := payloadTableNamePrefix + "john"

	// Only grantees on the table should be admin, root and john (5 privileges).
	grantees, err := getTableGrantees(ctx, fileTableName, conn)
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "john", "john", "john", "john", "john", "root"}, grantees)

	grantees, err = getTableGrantees(ctx, payloadTableName, conn)
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "john", "john", "john", "john", "john", "root"}, grantees)
}

// TestDifferentRoleDisallowed tests that a user who does not own the file and
// payload tables but has a role with ALL privileges on the database cannot
// access the tables once they have been created/written to.
func TestDifferentRoleDisallowed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)

	// Create non-admin user with only CREATE privilege on the database.
	_, err = sqlDB.Exec("CREATE USER john")
	require.NoError(t, err)
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT CREATE ON DATABASE %s TO john", database))
	require.NoError(t, err)

	// Create role with ALL privileges on the database.
	_, err = sqlDB.Exec("CREATE ROLE allprivilege")
	require.NoError(t, err)
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT ALL ON DATABASE %s TO allprivilege", database))
	require.NoError(t, err)

	// Create non-admin user and assign above role.
	_, err = sqlDB.Exec("CREATE USER doe")
	require.NoError(t, err)
	_, err = sqlDB.Exec(`GRANT allprivilege TO doe`)
	require.NoError(t, err)

	// Switch to non-admin user john.
	pgURL, cleanupGoDB := sqlutils.PGUrlWithOptionalClientCerts(
		t, s.ServingSQLAddr(), "notAdmin", url.User("john"), false, /* withCerts */
	)
	defer cleanupGoDB()
	pgURL.RawQuery = "sslmode=disable"
	userDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer userDB.Close()

	fileTableReadWriter, err := NewFileToTableSystem(ctx, database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	require.NoError(t, err)

	_ = uploadFile(t, ctx, "file1", 1024, 10, fileTableReadWriter)

	// Switch to non-admin user doe.
	pgURL, cleanupGoDB = sqlutils.PGUrlWithOptionalClientCerts(
		t, s.ServingSQLAddr(), "notAdmin", url.User("doe"), false, /* withCerts */
	)
	defer cleanupGoDB()
	pgURL.RawQuery = "sslmode=disable"
	userDB, err = gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer userDB.Close()

	// Under normal circumstances Doe should have ALL privileges on the file and
	// payload tables created by john above. FileToTableSystem should have
	// revoked these privileges.
	fileTableName := fileTableNamePrefix + "john"
	payloadTableName := payloadTableNamePrefix + "john"

	// Only grantees on the table should be admin, root and john (5 privileges).
	grantees, err := getTableGrantees(ctx, fileTableName, conn)
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "john", "john", "john", "john", "john", "root"}, grantees)

	grantees, err = getTableGrantees(ctx, payloadTableName, conn)
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "john", "john", "john", "john", "john", "root"}, grantees)
}

// TestDatabaseScope tests that the FileToTableSystem executes all of its
// internal queries wrt the database it is given.
func TestDatabaseScope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	fileTableReadWriter, err := NewFileToTableSystem(ctx, database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"root")
	require.NoError(t, err)

	// Verify defaultdb has the file we wrote.
	uploadedContent := uploadFile(t, ctx, "file1", 1024, 10, fileTableReadWriter)
	oldDBReader, err := fileTableReadWriter.ReadFile(ctx, "file1")
	require.NoError(t, err)
	oldDBContent, err := ioutil.ReadAll(oldDBReader)
	require.True(t, bytes.Equal(uploadedContent, oldDBContent))

	// Switch database and attempt to read the file.
	_, err = sqlDB.Exec(`CREATE DATABASE newdb`)
	require.NoError(t, err)
	newFileTableReadWriter, err := NewFileToTableSystem(ctx, "newdb",
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"root")
	require.NoError(t, err)
	reader, err := newFileTableReadWriter.ReadFile(ctx, "file1")
	require.NoError(t, err)
	newDBContent, err := ioutil.ReadAll(reader)
	require.Empty(t, newDBContent)
}
