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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const chunkSize = 1024 * 1024 * 4 // 4 Mib

var fileTableNamePrefix = "upload_files_"
var payloadTableNamePrefix = "upload_payload_"

// FileToTableSystem can be used to store, retrieve and delete the blobs and
// metadata of files, from user scoped tables. Access to these tables is
// restricted to the root/admin user and the user responsible for triggering
// table creation in the first place.
// All methods operate within the scope of the provided database db, as the user
// with the provided username.
//
// Refer to the method headers for more details about the user scoped tables.
type FileToTableSystem struct {
	databaseName string
	ie           *sql.InternalExecutor
	db           *kv.DB
	username     string
}

// NewFileToTableSystem returns a FileToTableSystem object.
func NewFileToTableSystem(
	databaseName string, ie *sql.InternalExecutor, db *kv.DB, username string,
) *FileToTableSystem {
	return &FileToTableSystem{
		databaseName: databaseName, ie: ie, db: db, username: username,
	}
}

// FileSize returns the size of the filename blob in bytes.
func (f *FileToTableSystem) FileSize(ctx context.Context, filename string) (int64, error) {
	payloadTableName := payloadTableNamePrefix + f.username

	// Byte offset of the last file chunk, plus the size of the last file chunk
	// will give us the size of the whole file.
	getLastChunkQuery := fmt.Sprintf(`SELECT byte_offset, 
payload FROM %s WHERE filename='%s' ORDER BY byte_offset DESC LIMIT 1`,
		payloadTableName, filename)
	rows, err := f.ie.QueryRowEx(ctx, "payload-table-storage-size", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		getLastChunkQuery)
	if err != nil {
		return 0, errors.Wrap(err, "failed to calculate size of file from the payload table")
	}

	size := int(tree.MustBeDInt(rows[0])) + len([]byte(tree.MustBeDBytes(rows[1])))
	return int64(size), nil
}

// ListFiles returns a list of all the files which are currently stored in the
// user scoped tables.
func (f *FileToTableSystem) ListFiles(ctx context.Context) ([]string, error) {
	var files []string
	fileTableName := fileTableNamePrefix + f.username
	listFilesQuery := fmt.Sprintf(`SELECT filename FROM %s ORDER BY upload_time`, fileTableName)
	rows, err := f.ie.QueryEx(ctx, "file-table-storage-list", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		listFilesQuery)
	if err != nil {
		return files, errors.Wrap(err, "failed to list files from file table")
	}

	// Verify that all the filenames are strings and aggregate them.
	for _, row := range rows {
		files = append(files, string(tree.MustBeDString(row[0])))
	}

	return files, nil
}

// DeleteAllFiles drops the user scoped tables effectively deleting the blobs
// and metadata of every file.
func (f *FileToTableSystem) DeleteAllFiles(ctx context.Context) error {
	txn := f.db.NewTxn(ctx, "file-table-storage-drop")

	payloadTableName := payloadTableNamePrefix + f.username
	dropPayloadTableQuery := fmt.Sprintf(`DROP TABLE %s`, payloadTableName)
	_, err := f.ie.QueryEx(ctx, "drop-payload-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		dropPayloadTableQuery)
	if err != nil {
		return errors.Wrap(err, "failed to drop payload table")
	}

	fileTableName := fileTableNamePrefix + f.username
	dropFileTableQuery := fmt.Sprintf(`DROP TABLE %s`, fileTableName)
	_, err = f.ie.QueryEx(ctx, "drop-file-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		dropFileTableQuery)
	if err != nil {
		return errors.Wrap(err, "failed to drop file table")
	}

	if err = txn.CommitOrCleanup(ctx); err != nil {
		return errors.Wrap(err, "failed to commit or cleanup delete all files txn")
	}

	return nil
}

// DeleteFile deletes the blobs and metadata of filename from the user scoped
// tables.
func (f *FileToTableSystem) DeleteFile(ctx context.Context, filename string) error {
	txn := f.db.NewTxn(ctx, "file-table-storage-file-deletion")

	fileTableName := fileTableNamePrefix + f.username
	deleteFileQuery := fmt.Sprintf(`DELETE FROM %s WHERE filename='%s'`, fileTableName, filename)
	_, err := f.ie.QueryEx(ctx, "delete-file-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		deleteFileQuery)
	if err != nil {
		return errors.Wrap(err, "failed to delete from the file table")
	}

	payloadTableName := payloadTableNamePrefix + f.username
	deletePayloadQuery := fmt.Sprintf(`DELETE FROM %s WHERE filename='%s'`, payloadTableName, filename)
	_, err = f.ie.QueryEx(ctx, "delete-payload-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		deletePayloadQuery)
	if err != nil {
		return errors.Wrap(err, "failed to delete from the payload table")
	}

	if err = txn.CommitOrCleanup(ctx); err != nil {
		return errors.Wrap(err, "failed to commit or cleanup delete all files txn")
	}

	return nil
}

type payloadWriter struct {
	username   string
	database   string
	filename   string
	ie         *sql.InternalExecutor
	ctx        context.Context
	txn        *kv.Txn
	byteOffset int
}

var _ io.Writer = &payloadWriter{}

// Write implements the io.Writer interface by inserting a single row into the
// Payload table.
func (p *payloadWriter) Write(buf []byte) (int, error) {
	payloadTableName := payloadTableNamePrefix + p.username
	insertChunkQuery := fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, payloadTableName)
	_, err := p.ie.QueryEx(p.ctx, "insert-file-chunk", p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: p.username, Database: p.database},
		insertChunkQuery, p.filename, p.byteOffset, buf)
	if err != nil {
		return 0, err
	}

	bytesWritten := len(buf)
	p.byteOffset += bytesWritten

	return bytesWritten, nil
}

type chunkWriter struct {
	writer   *bufio.Writer
	filename string
	username string
	database string
	ie       *sql.InternalExecutor
	ctx      context.Context
	txn      *kv.Txn
}

var _ io.WriteCloser = &chunkWriter{}

func newChunkWriter(
	ctx context.Context, filename, username, database string, ie *sql.InternalExecutor, txn *kv.Txn,
) *chunkWriter {
	return &chunkWriter{
		bufio.NewWriterSize(&payloadWriter{
			username, database, filename, ie, ctx, txn,
			0}, chunkSize), filename, username, database, ie, ctx, txn,
	}
}

// Write implements the io.Writer interface by chunking the file into chunkSize
// bytes before sending it to the underlying io.Writer.
func (w *chunkWriter) Write(p []byte) (n int, err error) {
	// Write file chunks.
	var byteOffset int
	buf := bytes.NewBuffer(p)
	for {
		send := make([]byte, chunkSize)
		n, err := buf.Read(send)
		if n > 0 {
			written, err := w.writer.Write(send[:n])
			if err != nil {
				return byteOffset, err
			}
			byteOffset += written
		} else if err == io.EOF {
			// If no rows were previously inserted then we insert a row with an empty
			// payload.
			if byteOffset == 0 {
				payloadTableName := payloadTableNamePrefix + w.username
				insertEmptyChunk := fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, payloadTableName)
				_, err := w.ie.QueryEx(w.ctx, "insert-empty-file-chunk", w.txn,
					sqlbase.InternalExecutorSessionDataOverride{User: w.username, Database: w.database},
					insertEmptyChunk, w.filename, byteOffset, []byte(""))
				if err != nil {
					return byteOffset, err
				}
			}
			break
		} else if err != nil {
			return byteOffset, err
		}
	}

	return byteOffset, nil
}

// Close implements the io.Closer interface by flushing the underlying writer
// thereby writing remaining data to the Payload table, and committing the
// overarching txn.
func (w *chunkWriter) Close() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.txn.CommitOrCleanup(w.ctx); err != nil {
		return errors.Wrap(err, "failed to commit or cleanup file table txn")
	}
	return nil
}

func (f *FileToTableSystem) newFileReader(ctx context.Context, filename string) (io.Reader, error) {
	payloadTableName := payloadTableNamePrefix + f.username
	query := fmt.Sprintf(`SELECT payload FROM %s WHERE filename='%s'`, payloadTableName, filename)
	rows, err := f.ie.QueryEx(
		ctx, "get-filename-payload", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName}, query,
	)
	if err != nil {
		return nil, err
	}

	// Verify that all the payloads are bytes and assemble bytes of filename.
	var fileBytes []byte
	for _, row := range rows {
		fileBytes = append(fileBytes, []byte(tree.MustBeDBytes(row[0]))...)
	}

	return bufio.NewReader(bytes.NewBuffer(fileBytes)), nil
}

// ReadFile returns the blob for filename using a FileTableReader.
// TODO(adityamaru): Reading currently involves aggregating all chunks of a
// file from the Payload table. In the future we might want to implement a pull
// x rows system, or a scan based interface.
func (f *FileToTableSystem) ReadFile(ctx context.Context, filename string) ([]byte, error) {
	var reader io.Reader
	var err error
	if reader, err = f.newFileReader(ctx, filename); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}

func (f *FileToTableSystem) checkIfFileExists(
	ctx context.Context, fileName, fileTableName string, txn *kv.Txn,
) (bool, error) {
	fileExistenceQuery := fmt.Sprintf(`SELECT filename FROM %s WHERE filename='%s'`, fileTableName,
		fileName)
	rows, err := f.ie.QueryEx(ctx, "file-exists", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		fileExistenceQuery)
	if err != nil {
		return false, err
	}
	if len(rows) != 0 {
		return true, nil
	}

	return false, nil
}

func (f *FileToTableSystem) checkIfFileAndPayloadTableExist(
	ctx context.Context, fileTableName,
	payloadTableName string,
) (bool, error) {
	tableExistenceQuery := fmt.Sprintf(`SELECT table_name FROM [SHOW TABLES] WHERE table_name='%s' OR table_name='%s'`,
		fileTableName, payloadTableName)
	rows, err := f.ie.QueryEx(ctx, "tables-exist", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		tableExistenceQuery)
	if err != nil {
		return false, err
	}

	if len(rows) == 1 {
		return false, errors.New("expected both File and Payload tables to exist, " +
			"but one of them has been dropped")
	}
	return len(rows) == 2, nil
}

func (f *FileToTableSystem) createFileAndPayloadTables(
	ctx context.Context, txn *kv.Txn, fileTableName, payloadTableName string,
) error {
	// Create the File and Payload tables to hold the file chunks.
	fileTableCreateQuery := fmt.Sprintf(`CREATE TABLE %s (filename STRING PRIMARY KEY, 
username STRING, upload_time TIMESTAMP)`,
		fileTableName)
	_, err := f.ie.QueryEx(ctx, "create-file-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		fileTableCreateQuery)
	if err != nil {
		return errors.Wrap(err, "failed to create file table to store uploaded file names")
	}

	// The Payload table is interleaved in the File table to prevent repetition
	// of filename for every chunk at the KV level.
	payloadTableCreateQuery := fmt.Sprintf(`CREATE TABLE %s (filename STRING, byte_offset INT, 
payload BYTES, PRIMARY KEY(filename, byte_offset)) INTERLEAVE IN PARENT %s(filename)`,
		payloadTableName, fileTableName)
	_, err = f.ie.QueryEx(ctx, "create-payload-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		payloadTableCreateQuery)
	if err != nil {
		return errors.Wrap(err, "failed to create interleaved table to store chunks of uploaded files")
	}

	return nil
}

// Grant the current user all read/edit privileges for the file and payload
// tables.
func (f *FileToTableSystem) grantCurrentUserTablePrivileges(
	ctx context.Context, fileTableName,
	payloadTableName string, txn *kv.Txn,
) error {
	grantQuery := fmt.Sprintf(`GRANT SELECT, INSERT, DROP, DELETE ON TABLE %s, %s TO %s`,
		fileTableName, payloadTableName, f.username)
	_, err := f.ie.QueryEx(ctx, "grant-user-file-payload-table-access", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser, Database: f.databaseName},
		grantQuery)
	if err != nil {
		return errors.Wrap(err, "failed to grant access privileges to file and payload tables")
	}

	return nil
}

// Revoke all privileges from every user and role except root/admin and the
// current user.
func (f *FileToTableSystem) revokeOtherUserTablePrivileges(
	ctx context.Context, fileTableName,
	payloadTableName string, txn *kv.Txn,
) error {
	getUsersQuery := fmt.Sprintf(`SELECT username FROM system.
users WHERE NOT "username" = 'root' AND NOT "username" = 'admin' AND NOT "username" = '%s'`,
		f.username)
	rows, err := f.ie.QueryEx(
		ctx, "get-users", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		getUsersQuery,
	)
	if err != nil {
		return errors.Wrap(err, "failed to get all the users of the cluster")
	}

	var users []string
	for _, row := range rows {
		users = append(users, string(tree.MustBeDString(row[0])))
	}

	for _, user := range users {
		revokeQuery := fmt.Sprintf(`REVOKE ALL ON TABLE %s, %s FROM %s`,
			fileTableName, payloadTableName, user)
		_, err = f.ie.QueryEx(ctx, "revoke-user-privileges", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser, Database: f.databaseName},
			revokeQuery)
		if err != nil {
			return errors.Wrap(err, "failed to revoke privileges")
		}
	}

	return nil
}

// WriteFile has a few different responsibilities:
// - 	Creates the File and Payload tables which are responsible for storing the
// 		metadata and blob chunks for filename, respectively.
// - 	Grants the creating user SELECT, INSERT, DELETE, DROP privileges on these
// 		tables.
// - 	Revokes all privileges of every other user and role, except root/admin.
// -	Writes the filename metadata to File table.
// - 	Write the filename chunks to the Payload table.
//
// All the above stages are performed within a txn to guarantee atomicity.
func (f *FileToTableSystem) WriteFile(
	ctx context.Context, filename string, content io.Reader,
) error {
	// Check if the File and Payload tables need to be created.
	fileTableName := fileTableNamePrefix + f.username
	payloadTableName := payloadTableNamePrefix + f.username
	tablesExist, err := f.checkIfFileAndPayloadTableExist(ctx, fileTableName, payloadTableName)
	if err != nil {
		return err
	}

	txn := f.db.NewTxn(ctx, "file-table-storage-write-file")
	// TODO(adityamaru): Handle scenario where the user has already created tables
	// with the same names. Not sure if we want to error out or work around it.
	if !tablesExist {
		// Create the File and Payload tables.
		if err = f.createFileAndPayloadTables(ctx, txn, fileTableName, payloadTableName); err != nil {
			return err
		}

		if err = f.grantCurrentUserTablePrivileges(ctx, fileTableName, payloadTableName,
			txn); err != nil {
			return err
		}

		if err = f.revokeOtherUserTablePrivileges(ctx, fileTableName, payloadTableName,
			txn); err != nil {
			return err
		}
	}

	if exists, err := f.checkIfFileExists(ctx, filename, fileTableName, txn); exists || err != nil {
		if err != nil {
			return err
		}
		return errors.Newf("file %s already exists in the File and Payload tables", filename)
	}

	// Write file metadata.
	fileNameQuery := fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, fileTableName)
	_, err = f.ie.QueryEx(ctx, "insert-file-name", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username, Database: f.databaseName},
		fileNameQuery, filename, f.username, timeutil.Now())
	if err != nil {
		return err
	}

	// Write the file payloads.
	writer := newChunkWriter(ctx, filename, f.username, f.databaseName, f.ie, txn)
	var contentBytes []byte
	if contentBytes, err = ioutil.ReadAll(content); err != nil {
		return err
	}
	_, err = writer.Write(contentBytes)
	if err != nil {
		return err
	}
	if err = writer.Close(); err != nil {
		return err
	}

	return nil
}
