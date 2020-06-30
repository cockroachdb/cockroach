// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package filetabletest

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl/filetable"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

const database = "defaultdb"
const qualifiedTableName = database + ".public.file_table_read_writer"

// uploadFile generates random data and copies it to the FileTableSystem via
// the FileWriter.
func uploadFile(
	ctx context.Context, filename string, fileSize, chunkSize int, ft *filetable.FileToTableSystem,
) ([]byte, error) {
	data := make([]byte, fileSize)
	randGen, _ := randutil.NewPseudoRand()
	randutil.ReadTestdataBytes(randGen, data)

	writer, err := ft.NewFileWriter(ctx, filename, chunkSize)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(writer, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Checks that filename has been divided into the expected number of chunks
// before being written to the Payload table.
func checkNumberOfPayloadChunks(
	ctx context.Context,
	t *testing.T,
	payloadTableName, filename string,
	expectedNumChunks int,
	sqlDB *gosql.DB,
) {
	var count int
	err := sqlDB.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE filename='%s'`,
		payloadTableName, filename)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, expectedNumChunks, count)
}

// Checks that a metadata entry exists for the given filename in the File table.
func checkMetadataEntryExists(
	ctx context.Context, t *testing.T, fileTableName, filename string, sqlDB *gosql.DB,
) {
	var count int
	err := sqlDB.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE filename='%s'`,
		fileTableName, filename)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, count, 1)
}

func TestListAndDeleteFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, _, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	fileTableReadWriter, err := filetable.NewFileToTableSystem(ctx, qualifiedTableName,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		security.RootUser)
	require.NoError(t, err)

	// Create first test file with multiple chunks.
	const size = 1024
	const chunkSize = 8
	_, err = uploadFile(ctx, "file1", size, chunkSize, fileTableReadWriter)
	require.NoError(t, err)

	// Create second test file with multiple chunks.
	_, err = uploadFile(ctx, "file2", size, chunkSize, fileTableReadWriter)
	require.NoError(t, err)

	// Create third test file with multiple chunks.
	_, err = uploadFile(ctx, "file3", size, chunkSize, fileTableReadWriter)
	require.NoError(t, err)

	// List files before delete.
	files, err := fileTableReadWriter.ListFiles(ctx, "")
	require.NoError(t, err)
	require.Equal(t, []string{"file1", "file2", "file3"}, files)

	// Delete file1.
	require.NoError(t, fileTableReadWriter.DeleteFile(ctx, "file1"))

	// List files.
	files, err = fileTableReadWriter.ListFiles(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, []string{"file2", "file3"}, files)

	// Destroy the filesystem.
	require.NoError(t, filetable.DestroyUserFileSystem(ctx, fileTableReadWriter))

	// Attempt to write after the user system has been destroyed.
	_, err = uploadFile(ctx, "file4", size, chunkSize, fileTableReadWriter)
	require.Error(t, err)
}

func TestReadWriteFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	fileTableReadWriter, err := filetable.NewFileToTableSystem(ctx, qualifiedTableName,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		security.RootUser)
	require.NoError(t, err)

	testFileName := "testfile"

	isContentEqual := func(filename string, expected []byte, ft *filetable.FileToTableSystem) bool {
		reader, err := fileTableReadWriter.ReadFile(ctx, filename)
		require.NoError(t, err)
		got, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		return bytes.Equal(got, expected)
	}

	testCases := []struct {
		name      string
		fileSize  int
		chunkSize int
	}{
		{"empty-file", 0, 1024},
		{"single-byte-chunk", 1024, 1},
		{"file-size-chunk", 1024, 1024},
		{"large-file", 1024 * 1024, 1024},
		{"one-extra-chunk", 11, 2},
	}

	for _, testCase := range testCases {
		expected, err := uploadFile(ctx, testFileName, testCase.fileSize, testCase.chunkSize,
			fileTableReadWriter)
		require.NoError(t, err)

		// Check size.
		size, err := fileTableReadWriter.FileSize(ctx, testFileName)
		require.NoError(t, err)
		require.Equal(t, size, int64(testCase.fileSize))

		// Check content.
		require.True(t, isContentEqual(testFileName, expected, fileTableReadWriter))

		// Check chunking and metadata entry.
		checkMetadataEntryExists(ctx, t, fileTableReadWriter.GetFQFileTableName(), testFileName,
			sqlDB)
		expectedNumChunks := (testCase.fileSize / testCase.chunkSize) +
			(testCase.fileSize % testCase.chunkSize)
		checkNumberOfPayloadChunks(ctx, t, fileTableReadWriter.GetFQPayloadTableName(), testFileName,
			expectedNumChunks, sqlDB)

		// Delete file.
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	}

	t.Run("file-already-exists", func(t *testing.T) {
		_, err = uploadFile(ctx, testFileName, 11, 2, fileTableReadWriter)
		require.NoError(t, err)
		_, err := fileTableReadWriter.NewFileWriter(ctx, testFileName, 2)
		require.NoError(t, err)

		// Upload the same file again, and expect a PK violation.
		_, err = uploadFile(ctx, testFileName, 11, 2, fileTableReadWriter)
		require.Error(t, err)

		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("write-delete-write", func(t *testing.T) {
		write1, err := uploadFile(ctx, testFileName, 11, 2, fileTableReadWriter)
		require.NoError(t, err)
		require.True(t, isContentEqual(testFileName, write1, fileTableReadWriter))

		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))

		// Write same file name but different size configuration.
		write2, err := uploadFile(ctx, testFileName, 1024, 4, fileTableReadWriter)
		require.NoError(t, err)
		require.True(t, isContentEqual(testFileName, write2, fileTableReadWriter))
		require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFileName))
	})

	t.Run("call-write-many-times", func(t *testing.T) {
		chunkSize := 4
		fileSize := 1024

		data := make([]byte, fileSize)
		randGen, _ := randutil.NewPseudoRand()
		randutil.ReadTestdataBytes(randGen, data)

		writer, err := fileTableReadWriter.NewFileWriter(ctx, testFileName, chunkSize)
		require.NoError(t, err)

		// Write two 1 Kib files using the same writer.
		for i := 0; i < 2; i++ {
			_, err = io.Copy(writer, bytes.NewReader(data))
			require.NoError(t, err)
		}

		require.NoError(t, writer.Close())

		// Check content.
		expectedContent := append(data, data...)
		require.True(t, isContentEqual(testFileName, expectedContent, fileTableReadWriter))

		// Check chunking and metadata entry.
		expectedFileSize := fileSize * 2
		checkMetadataEntryExists(ctx, t, fileTableReadWriter.GetFQFileTableName(), testFileName, sqlDB)
		expectedNumChunks := (expectedFileSize / chunkSize) +
			(expectedFileSize % chunkSize)
		checkNumberOfPayloadChunks(ctx, t, fileTableReadWriter.GetFQPayloadTableName(), testFileName,
			expectedNumChunks, sqlDB)

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
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	require.NoError(t, err)

	// Create non-admin user with only CREATE privilege on the database.
	_, err = sqlDB.Exec("CREATE USER john")
	require.NoError(t, err)
	_, err = sqlDB.Exec(fmt.Sprintf("GRANT CREATE ON DATABASE %s TO john", database))
	require.NoError(t, err)

	// Operate under non-admin user.
	fileTableReadWriter, err := filetable.NewFileToTableSystem(ctx, qualifiedTableName,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	require.NoError(t, err)

	// Upload a file to test INSERT privilege.
	expected, err := uploadFile(ctx, "file1", 1024, 1, fileTableReadWriter)
	require.NoError(t, err)

	// Read file to test SELECT privilege.
	reader, err := fileTableReadWriter.ReadFile(ctx, "file1")
	require.NoError(t, err)
	got, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(got, expected))

	// Delete file to test DELETE privilege.
	require.NoError(t, fileTableReadWriter.DeleteFile(ctx, "file1"))

	// Delete all files to test DROP privilege.
	require.NoError(t, filetable.DestroyUserFileSystem(ctx, fileTableReadWriter))

	// Check that there are no grantees on the File and Payload tables as they
	// should have been dropped above.
	_, err = getTableGrantees(ctx, fileTableReadWriter.GetFQFileTableName(), conn)
	require.Error(t, err)

	_, err = getTableGrantees(ctx, fileTableReadWriter.GetFQPayloadTableName(), conn)
	require.Error(t, err)
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
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	require.NoError(t, err)

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

	// Operate under non-admin user john.
	fileTableReadWriter, err := filetable.NewFileToTableSystem(ctx, qualifiedTableName,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	require.NoError(t, err)

	_, err = uploadFile(ctx, "file1", 1024, 10, fileTableReadWriter)
	require.NoError(t, err)

	// Under normal circumstances Doe should have ALL privileges on the file and
	// payload tables created by john above. FileToTableSystem should have revoked
	// these privileges.
	//
	// Only grantees on the table should be admin, root and john (5 privileges).
	grantees, err := getTableGrantees(ctx, fileTableReadWriter.GetFQFileTableName(), conn)
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "john", "john", "john", "john", "john", "root"}, grantees)

	grantees, err = getTableGrantees(ctx, fileTableReadWriter.GetFQPayloadTableName(), conn)
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
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	require.NoError(t, err)

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

	// Operate under non-admin user john.
	fileTableReadWriter, err := filetable.NewFileToTableSystem(ctx, qualifiedTableName,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	require.NoError(t, err)

	_, err = uploadFile(ctx, "file1", 1024, 10, fileTableReadWriter)
	require.NoError(t, err)

	// Under normal circumstances Doe should have ALL privileges on the file and
	// payload tables created by john above. FileToTableSystem should have
	// revoked these privileges.
	//
	// Only grantees on the table should be admin, root and john (5 privileges).
	grantees, err := getTableGrantees(ctx, fileTableReadWriter.GetFQFileTableName(), conn)
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "john", "john", "john", "john", "john", "root"}, grantees)

	grantees, err = getTableGrantees(ctx, fileTableReadWriter.GetFQPayloadTableName(), conn)
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "john", "john", "john", "john", "john", "root"}, grantees)
}

// TestDatabaseScope tests that the FileToTableSystem executes all of its
// internal queries wrt the database it is given.
func TestDatabaseScope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	fileTableReadWriter, err := filetable.NewFileToTableSystem(ctx, qualifiedTableName,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		security.RootUser)
	require.NoError(t, err)

	// Verify defaultdb has the file we wrote.
	uploadedContent, err := uploadFile(ctx, "file1", 1024, 10, fileTableReadWriter)
	require.NoError(t, err)
	oldDBReader, err := fileTableReadWriter.ReadFile(ctx, "file1")
	require.NoError(t, err)
	oldDBContent, err := ioutil.ReadAll(oldDBReader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(uploadedContent, oldDBContent))

	// Switch database and attempt to read the file.
	_, err = sqlDB.Exec(`CREATE DATABASE newdb`)
	require.NoError(t, err)
	newFileTableReadWriter, err := filetable.NewFileToTableSystem(ctx,
		"newdb.file_table_read_writer",
		s.InternalExecutor().(*sql.InternalExecutor), kvDB, security.RootUser)
	require.NoError(t, err)
	_, err = newFileTableReadWriter.ReadFile(ctx, "file1")
	require.True(t, os.IsNotExist(err))
}
