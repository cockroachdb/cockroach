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
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

const filename = "file_to_table_test.csv"
const database = "defaultdb"

func writeFile(t *testing.T, testSendFile string, fileContent []byte) {
	err := os.MkdirAll(filepath.Dir(testSendFile), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(testSendFile, fileContent, 0644)
	if err != nil {
		t.Fatal(err)
	}
}

type FileTableRow struct {
	Filename string
	User     string

	// TODO(adityamaru): Figure out how to compare timestamps.
}

type PayloadTableRow struct {
	Filename   string
	ByteOffset int
	Payload    []byte
}

func (p PayloadTableRow) Equal(g PayloadTableRow) bool {
	return p.Filename == g.Filename && p.ByteOffset == g.ByteOffset && bytes.Equal(p.Payload,
		g.Payload)
}

func checkFileTableRows(
	ctx context.Context, username string, conn *gosql.Conn, expectedRows []FileTableRow,
) error {
	fileTableName := fileTableNamePrefix + username
	var rows *gosql.Rows
	var err error
	if rows, err = conn.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s`,
		fileTableName)); err != nil {
		return err
	}

	var rowIndex int
	for rows.Next() {
		// Check that the number of rows we got is not more than the number we
		// expect.
		if rowIndex >= len(expectedRows) {
			return errors.New("got more rows than we expected")
		}

		var fileName string
		var user string
		var uploadTime string
		if err = rows.Scan(&fileName, &user, &uploadTime); err != nil {
			return err
		}
		actualRow := FileTableRow{Filename: fileName, User: user}
		if !cmp.Equal(expectedRows[rowIndex], actualRow) {
			return errors.Newf("expected %+v but got %+v", expectedRows[rowIndex], actualRow)
		}
		rowIndex++
	}

	return nil
}

func checkPayloadTableRows(
	ctx context.Context, username string, conn *gosql.Conn, expectedRows []PayloadTableRow,
) error {
	payloadTableName := payloadTableNamePrefix + username
	var rows *gosql.Rows
	var err error
	if rows, err = conn.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s`,
		payloadTableName)); err != nil {
		return err
	}

	var rowIndex int
	for rows.Next() {
		// Check that the number of rows we got is not more than the number we
		// expect.
		if rowIndex >= len(expectedRows) {
			return errors.New("got more rows than we expected")
		}

		var fileName string
		var byteOffset int
		var payload []byte
		if err = rows.Scan(&fileName, &byteOffset, &payload); err != nil {
			return err
		}
		actualRow := PayloadTableRow{Filename: fileName, ByteOffset: byteOffset, Payload: payload}
		if !bytes.Equal(expectedRows[rowIndex].Payload, actualRow.Payload) {
			return errors.Newf("expected %+v but got %+v", expectedRows[rowIndex], actualRow)
		}
		rowIndex++
	}

	return nil
}

func TestListAndDeleteFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, _, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create first test file with multiple chunks.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath1 := filepath.Join(testFileDir, filename)
	const size = 10 * 1024 * 1024 // 10 MiB
	fileContent1 := make([]byte, size)
	if _, err := rand.Read(fileContent1); err != nil {
		t.Fatal(err)
	}
	writeFile(t, testFilePath1, fileContent1)

	// Create second test file with multiple chunks.
	testFileDir2, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testFilePath2 := filepath.Join(testFileDir2, filename)
	fileContent2 := make([]byte, size)
	if _, err := rand.Read(fileContent2); err != nil {
		t.Fatal(err)
	}
	writeFile(t, testFilePath2, fileContent2)

	// Create third test file with multiple chunks.
	testFileDir3, cleanup3 := testutils.TempDir(t)
	defer cleanup3()
	testFilePath3 := filepath.Join(testFileDir3, filename)
	fileContent3 := make([]byte, size)
	if _, err := rand.Read(fileContent3); err != nil {
		t.Fatal(err)
	}
	writeFile(t, testFilePath3, fileContent3)

	// Write the files to the file and payload tables.
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"root")
	reader1, err := os.Open(testFilePath1)
	require.NoError(t, err)
	defer reader1.Close()
	require.NoError(t, fileTableReadWriter.WriteFile(ctx, testFilePath1, reader1))

	reader2, err := os.Open(testFilePath2)
	require.NoError(t, err)
	defer reader2.Close()
	require.NoError(t, fileTableReadWriter.WriteFile(ctx, testFilePath2, reader2))

	reader3, err := os.Open(testFilePath3)
	require.NoError(t, err)
	defer reader3.Close()
	require.NoError(t, fileTableReadWriter.WriteFile(ctx, testFilePath3, reader3))

	// List files before delete.
	files, err := fileTableReadWriter.ListFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, []string{testFilePath1, testFilePath2, testFilePath3}, files)

	// Delete file1.
	require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFilePath1))

	// List files.
	files, err = fileTableReadWriter.ListFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, []string{testFilePath2, testFilePath3}, files)

	// Delete all files.
	require.NoError(t, fileTableReadWriter.DeleteAllFiles(ctx))

	// List files and expect a table not found error.
	files, err = fileTableReadWriter.ListFiles(ctx)
	require.Error(t, err)
}

func TestReadFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, _, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create first test file with multiple chunks.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath1 := filepath.Join(testFileDir, filename)
	const size = 10 * 1024 * 1024 // 10 MiB
	fileContent1 := make([]byte, size)
	if _, err := rand.Read(fileContent1); err != nil {
		t.Fatal(err)
	}
	writeFile(t, testFilePath1, fileContent1)

	// Create second test file with multiple chunks.
	testFileDir2, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testFilePath2 := filepath.Join(testFileDir2, filename)
	fileContent2 := make([]byte, size)
	if _, err := rand.Read(fileContent2); err != nil {
		t.Fatal(err)
	}
	writeFile(t, testFilePath2, fileContent2)

	// Write the files to the file and payload tables.
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"root")
	reader1, err := os.Open(testFilePath1)
	require.NoError(t, err)
	defer reader1.Close()
	require.NoError(t, fileTableReadWriter.WriteFile(ctx, testFilePath1, reader1))

	reader2, err := os.Open(testFilePath2)
	require.NoError(t, err)
	defer reader2.Close()
	require.NoError(t, fileTableReadWriter.WriteFile(ctx, testFilePath2, reader2))

	// Read the first file and verify size and contents.
	fileSize1, err := fileTableReadWriter.FileSize(ctx, testFilePath1)
	require.NoError(t, err)
	require.Equal(t, int64(len(fileContent1)), fileSize1)
	fileGotContent1, err := fileTableReadWriter.ReadFile(ctx, testFilePath1)
	require.NoError(t, err)
	require.True(t, bytes.Equal(fileGotContent1, fileContent1))

	// Read the second file and verify size and contents.
	fileSize2, err := fileTableReadWriter.FileSize(ctx, testFilePath2)
	require.NoError(t, err)
	require.Equal(t, int64(len(fileContent2)), fileSize2)
	fileGotContent2, err := fileTableReadWriter.ReadFile(ctx, testFilePath2)
	require.NoError(t, err)
	require.True(t, bytes.Equal(fileGotContent2, fileContent2))
}

func TestWriteSingleFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)

	// Create test file with multiple chunks.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath := filepath.Join(testFileDir, filename)
	const size = 10 * 1024 * 1024 // 10 MiB
	fileContent := make([]byte, size)
	if _, err := rand.Read(fileContent); err != nil {
		t.Fatal(err)
	}
	writeFile(t, testFilePath, fileContent)

	// Write to file and payload tables.
	username := "root"
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		username)
	reader, err := os.Open(testFilePath)
	require.NoError(t, err)
	err = fileTableReadWriter.WriteFile(ctx, testFilePath, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	reader, err = os.Open(testFilePath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify rows of the file and payload tables.
	var expectedFileTableRows = []FileTableRow{{testFilePath, username}}
	require.NoError(t, checkFileTableRows(ctx, username, conn, expectedFileTableRows))

	var expectedPayloadTableRows []PayloadTableRow
	var byteOffset int
	for {
		chunk := make([]byte, chunkSize)
		n, err := reader.Read(chunk)
		if n > 0 {
			row := PayloadTableRow{
				Filename:   testFilePath,
				ByteOffset: byteOffset,
				Payload:    make([]byte, n),
			}
			copy(row.Payload, chunk[:n])
			expectedPayloadTableRows = append(expectedPayloadTableRows, row)
			byteOffset += n
		} else if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

	}
	require.NoError(t, checkPayloadTableRows(ctx, username, conn, expectedPayloadTableRows))

	// Check that the size returned by the FileToTableSystem matches that of the
	// test file.
	actualSize, err := fileTableReadWriter.FileSize(ctx, testFilePath)
	require.NoError(t, err)
	require.Equal(t, int64(len(fileContent)), actualSize)
}

func TestWriteEmptyFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = database
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)

	// Create empty test file.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath := filepath.Join(testFileDir, filename)
	fileContent := []byte("")
	writeFile(t, testFilePath, fileContent)

	// Write to file and payload tables.
	username := "root"
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		username)
	reader, err := os.Open(testFilePath)
	defer reader.Close()
	require.NoError(t, err)
	err = fileTableReadWriter.WriteFile(ctx, testFilePath, reader)
	require.NoError(t, err)

	// Verify rows of the file and payload tables.
	var expectedFileTableRows = []FileTableRow{{testFilePath, username}}
	require.NoError(t, checkFileTableRows(ctx, username, conn, expectedFileTableRows))

	var expectedPayloadTableRows = []PayloadTableRow{{Filename: testFilePath, ByteOffset: 0,
		Payload: fileContent}}
	require.NoError(t, checkPayloadTableRows(ctx, username, conn, expectedPayloadTableRows))

	// Check that the size returned by the FileToTableSystem matches that of the
	// test file.
	actualSize, err := fileTableReadWriter.FileSize(ctx, testFilePath)
	require.NoError(t, err)
	require.Equal(t, int64(len(fileContent)), actualSize)
}

//
// TestUserGrants tests that a new user with only CREATE privileges can use all
// the FileToTableSystem methods after the first write, which is responsible
// for granting SELECT, INSERT, DELETE and DROP privileges on the file and
// payload tables.
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

	// Create test file.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath := filepath.Join(testFileDir, filename)
	fileContent := []byte("small file")
	writeFile(t, testFilePath, fileContent)

	// Trigger first write to get SELECT, INSERT, DELETE and DROP privileges on
	// the payload and file tables.
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	reader, err := os.Open(testFilePath)
	defer reader.Close()
	require.NoError(t, err)
	err = fileTableReadWriter.WriteFile(ctx, testFilePath, reader)
	require.NoError(t, err)

	// Read file to test SELECT privilege.
	readFileContent, err := fileTableReadWriter.ReadFile(ctx, testFilePath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(readFileContent, fileContent))

	// Delete file to test DELETE privilege.
	require.NoError(t, fileTableReadWriter.DeleteFile(ctx, testFilePath))

	// Delete all files to test DROP privilege.
	require.NoError(t, fileTableReadWriter.DeleteAllFiles(ctx))
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

	// Create test file.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath := filepath.Join(testFileDir, filename)
	fileContent := []byte("small file")
	writeFile(t, testFilePath, fileContent)

	// Write file to CREATE tables and update privileges.
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	reader, err := os.Open(testFilePath)
	defer reader.Close()
	require.NoError(t, err)
	err = fileTableReadWriter.WriteFile(ctx, testFilePath, reader)
	require.NoError(t, err)

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
	// revoked these privileges after the first write.
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

	// Create test file.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath := filepath.Join(testFileDir, filename)
	fileContent := []byte("small file")
	writeFile(t, testFilePath, fileContent)

	// Write file to CREATE tables and update privileges.
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"john")
	reader, err := os.Open(testFilePath)
	defer reader.Close()
	require.NoError(t, err)
	err = fileTableReadWriter.WriteFile(ctx, testFilePath, reader)
	require.NoError(t, err)

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
	// revoked these privileges after the first write.
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

	// Create test file.
	testFileDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	testFilePath := filepath.Join(testFileDir, filename)
	fileContent := []byte("small file")
	writeFile(t, testFilePath, fileContent)

	// Write file.
	fileTableReadWriter := NewFileToTableSystem(database,
		s.InternalExecutor().(*sql.InternalExecutor), kvDB,
		"root")
	reader, err := os.Open(testFilePath)
	defer reader.Close()
	require.NoError(t, err)
	err = fileTableReadWriter.WriteFile(ctx, testFilePath, reader)
	require.NoError(t, err)

	// Switch database and attempt to read the file.
	_, err = sqlDB.Exec(`CREATE DATABASE newdb`)
	require.NoError(t, err)
	newFileTableReadWriter := NewFileToTableSystem("newdb",
		s.InternalExecutor().(*sql.InternalExecutor), kvDB, "root")
	_, err = newFileTableReadWriter.ReadFile(ctx, testFilePath)
	require.Error(t, err)
}
