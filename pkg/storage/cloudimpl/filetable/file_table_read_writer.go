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
	"os"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// ChunkDefaultSize is the default size of each chunk a file will be broken into
// before being written to the Payload table.
const ChunkDefaultSize = 1024 * 1024 * 4 // 4 Mib

var fileTableNameSuffix = "_upload_files"
var payloadTableNameSuffix = "_upload_payload"

// FileToTableSystem can be used to store, retrieve and delete the blobs and
// metadata of files, from user scoped tables. Access to these tables is
// restricted to the root/admin user and the user responsible for triggering
// table creation in the first place.
// All methods operate within the scope of the provided database db, as the user
// with the provided username.
//
// Refer to the method headers for more details about the user scoped tables.
type FileToTableSystem struct {
	qualifiedTableName string
	ie                 *sql.InternalExecutor
	db                 *kv.DB
	username           string
}

// FileTable which contains records for every uploaded file.
const fileTableSchema = `CREATE TABLE %s (filename STRING PRIMARY KEY, 
file_size INT NOT NULL, 
username STRING NOT NULL, 
upload_time TIMESTAMP DEFAULT now())`

// PayloadTable contains the chunked payloads of each file.
// The Payload table is interleaved in the File table to prevent repetition
// of filename for every chunk at the KV level.
const payloadTableSchema = `CREATE TABLE %s (filename STRING, 
byte_offset INT, 
payload BYTES, 
PRIMARY KEY(filename, byte_offset)) 
INTERLEAVE IN PARENT %s(filename)`

// GetFQFileTableName returns the qualified File table name.
func (f *FileToTableSystem) GetFQFileTableName() string {
	return f.qualifiedTableName + fileTableNameSuffix
}

// GetFQPayloadTableName returns the qualified Payload table name.
func (f *FileToTableSystem) GetFQPayloadTableName() string {
	return f.qualifiedTableName + payloadTableNameSuffix
}

// GetSimpleFileTableName returns the non-qualified File table name.
func (f *FileToTableSystem) GetSimpleFileTableName() (string, error) {
	tableName, err := parser.ParseQualifiedTableName(f.qualifiedTableName)
	if err != nil {
		return "", err
	}

	return tableName.ObjectName.String() + fileTableNameSuffix, nil
}

// GetSimplePayloadTableName returns the non-qualified Payload table name.
func (f *FileToTableSystem) GetSimplePayloadTableName() (string, error) {
	tableName, err := parser.ParseQualifiedTableName(f.qualifiedTableName)
	if err != nil {
		return "", err
	}

	return tableName.ObjectName.String() + payloadTableNameSuffix, nil
}

// GetDatabaseAndSchema returns the database.schema of the current
// FileToTableSystem.
func (f *FileToTableSystem) GetDatabaseAndSchema() (string, error) {
	tableName, err := parser.ParseQualifiedTableName(f.qualifiedTableName)
	if err != nil {
		return "", err
	}

	return tableName.ObjectNamePrefix.String(), nil
}

// NewFileToTableSystem returns a FileToTableSystem object. It creates the File
// and Payload user tables, grants the current user all read/edit privileges on
// the tables and revokes access of every other user and role (except
// root/admin).
func NewFileToTableSystem(
	ctx context.Context,
	qualifiedTableName string,
	ie *sql.InternalExecutor,
	db *kv.DB,
	username string,
) (*FileToTableSystem, error) {
	f := FileToTableSystem{
		qualifiedTableName: qualifiedTableName, ie: ie, db: db, username: username,
	}

	if err := f.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// TODO(adityamaru): Handle scenario where the user has already created
		// tables with the same names not via the FileToTableSystem object. Not sure
		// if we want to error out or work around it.
		tablesExist, err := f.checkIfFileAndPayloadTableExist(ctx)
		if err != nil {
			return err
		}

		if !tablesExist {
			if err := f.createFileAndPayloadTables(ctx, txn); err != nil {
				return err
			}

			if err := f.grantCurrentUserTablePrivileges(ctx, txn); err != nil {
				return err
			}

			if err := f.revokeOtherUserTablePrivileges(ctx, txn); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &f, nil
}

// FileSize returns the size of the filename blob in bytes.
func (f *FileToTableSystem) FileSize(ctx context.Context, filename string) (int64, error) {
	getFileSizeQuery := fmt.Sprintf(`SELECT file_size FROM %s WHERE filename='%s'`,
		f.GetFQFileTableName(), filename)
	rows, err := f.ie.QueryRowEx(ctx, "payload-table-storage-size", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username},
		getFileSizeQuery)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get size of file from the payload table")
	}

	return int64(tree.MustBeDInt(rows[0])), nil
}

// ListFiles returns a list of all the files which are currently stored in the
// user scoped tables.
func (f *FileToTableSystem) ListFiles(ctx context.Context, pattern string) ([]string, error) {
	var files []string
	listFilesQuery := fmt.Sprintf(`SELECT filename FROM %s WHERE filename LIKE '%s' ORDER BY
filename`, f.GetFQFileTableName(), pattern+"%")
	rows, err := f.ie.QueryEx(ctx, "file-table-storage-list", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username},
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

// DestroyUserFileSystem drops the user scoped tables effectively deleting the
// blobs and metadata of every file.
// The FileToTableSystem object is unusable after this method returns.
func DestroyUserFileSystem(ctx context.Context, f *FileToTableSystem) error {
	if err := f.db.Txn(ctx,
		func(ctx context.Context, txn *kv.Txn) error {
			dropPayloadTableQuery := fmt.Sprintf(`DROP TABLE %s`, f.GetFQPayloadTableName())
			_, err := f.ie.QueryEx(ctx, "drop-payload-table", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: f.username},
				dropPayloadTableQuery)
			if err != nil {
				return errors.Wrap(err, "failed to drop payload table")
			}

			dropFileTableQuery := fmt.Sprintf(`DROP TABLE %s CASCADE`, f.GetFQFileTableName())
			_, err = f.ie.QueryEx(ctx, "drop-file-table", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: f.username},
				dropFileTableQuery)
			if err != nil {
				return errors.Wrap(err, "failed to drop file table")
			}

			return nil
		}); err != nil {
		return err
	}

	return nil
}

// DeleteFile deletes the blobs and metadata of filename from the user scoped
// tables.
func (f *FileToTableSystem) DeleteFile(ctx context.Context, filename string) error {
	if err := f.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		deleteFileQuery := fmt.Sprintf(`DELETE FROM %s WHERE filename='%s'`, f.GetFQFileTableName(),
			filename)
		_, err := f.ie.QueryEx(ctx, "delete-file-table", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: f.username},
			deleteFileQuery)
		if err != nil {
			return errors.Wrap(err, "failed to delete from the file table")
		}

		deletePayloadQuery := fmt.Sprintf(`DELETE FROM %s WHERE filename='%s'`,
			f.GetFQPayloadTableName(), filename)
		_, err = f.ie.QueryEx(ctx, "delete-payload-table", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: f.username},
			deletePayloadQuery)
		if err != nil {
			return errors.Wrap(err, "failed to delete from the payload table")
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

// payloadWriter is responsible for writing the file data (payload) to the user
// Payload table. It implements the io.Writer interface.
type payloadWriter struct {
	filename                string
	ie                      *sql.InternalExecutor
	ctx                     context.Context
	txn                     *kv.Txn
	byteOffset              int
	execSessionDataOverride sqlbase.InternalExecutorSessionDataOverride
	fileTableName           string
	payloadTableName        string
}

var _ io.Writer = &payloadWriter{}

// Write implements the io.Writer interface by inserting a single row into the
// Payload table.
func (p *payloadWriter) Write(buf []byte) (int, error) {
	insertChunkQuery := fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, p.payloadTableName)
	_, err := p.ie.QueryEx(p.ctx, "insert-file-chunk", p.txn, p.execSessionDataOverride,
		insertChunkQuery, p.filename, p.byteOffset, buf)
	if err != nil {
		p.txn.CleanupOnError(p.ctx, err)
		return 0, err
	}

	bytesWritten := len(buf)
	p.byteOffset += bytesWritten

	return bytesWritten, nil
}

// chunkWriter is responsible for buffering chunk of chunkSize and passing them
// on to the underlying payloadWriter to be written to the Payload table.
// On Close() chunkWriter inserts a file metadata entry in the File table once
// all the chunks have been written.
type chunkWriter struct {
	buf                     *bytes.Buffer
	pw                      *payloadWriter
	execSessionDataOverride sqlbase.InternalExecutorSessionDataOverride
	fileTableName           string
	payloadTableName        string
	chunkSize               int
}

var _ io.WriteCloser = &chunkWriter{}

func newChunkWriter(
	ctx context.Context,
	chunkSize int,
	filename, username, fileTableName, payloadTableName string,
	ie *sql.InternalExecutor,
	txn *kv.Txn,
) *chunkWriter {
	execSessionDataOverride := sqlbase.InternalExecutorSessionDataOverride{User: username}
	pw := &payloadWriter{
		filename, ie, ctx, txn, 0,
		execSessionDataOverride, fileTableName,
		payloadTableName}
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, chunkSize))
	return &chunkWriter{
		bytesBuffer, pw, execSessionDataOverride,
		fileTableName, payloadTableName,
		chunkSize,
	}
}

// fillAvailableBufferSpace fills the remaining space in the bytes buffer with
// data from payload, and returns the remainder of payload which has not been
// buffered.
func (w *chunkWriter) fillAvailableBufferSpace(payload []byte) ([]byte, error) {
	available := w.buf.Cap() - w.buf.Len()
	if available > len(payload) {
		available = len(payload)
	}
	if _, err := w.buf.Write(payload[:available]); err != nil {
		return nil, err
	}
	return payload[available:], nil
}

// Write is responsible for filling up the bytes buffer upto chunkSize, and
// then forwarding the bytes to the payloadWriter to be written into the SQL
// tables.
// Any bytes remaining in the bytes buffer at the end of Write() will be flushed
// in Close().
func (w *chunkWriter) Write(buf []byte) (int, error) {
	bufLen := len(buf)
	for len(buf) > 0 {
		var err error
		buf, err = w.fillAvailableBufferSpace(buf)
		if err != nil {
			return 0, err
		}

		// If the buffer has been filled to capacity, write the chunk.
		if w.buf.Len() == w.buf.Cap() {
			if n, err := w.pw.Write(w.buf.Bytes()); err != nil || n != w.buf.Len() {
				return 0, err
			}
			w.buf.Reset()
		}
	}

	return bufLen, nil
}

// Close implements the io.Closer interface by flushing the underlying writer
// thereby writing remaining data to the Payload table. It also inserts a file
// metadata entry into the File table.
//
// The chunkWriter must be Close()'d, and the error returned should be checked
// to ensure that the buffer has been flushed and the txn committed. Not
// handling the error could lead to unexpected behavior.
func (w *chunkWriter) Close() error {
	// If an error is encountered when writing the final chunk in the
	// payloadWriter Write() method, then the txn is aborted and the error is
	// propagated here.
	if w.buf.Len() > 0 {
		if n, err := w.pw.Write(w.buf.Bytes()); err != nil || n != w.buf.Len() {
			w.pw.txn.CleanupOnError(w.pw.ctx, err)
			return err
		}
	}

	// Insert file metadata entry into File table.
	fileNameQuery := fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, w.fileTableName)

	_, err := w.pw.ie.QueryEx(w.pw.ctx, "insert-file-name", w.pw.txn,
		w.execSessionDataOverride, fileNameQuery, w.pw.filename, w.pw.byteOffset,
		w.execSessionDataOverride.User)
	if err != nil {
		w.pw.txn.CleanupOnError(w.pw.ctx, err)
		return err
	}

	// Commit the txn after all the payload bytes have been written and the
	// metadata entry has been inserted.
	return w.pw.txn.CommitOrCleanup(w.pw.ctx)
}

// fileReader reads the file payload from the underlying Payload table.
type fileReader struct {
	io.Reader
}

var _ io.ReadCloser = &fileReader{}

// Close implements the io.Closer interface.
func (f *fileReader) Close() error {
	return nil
}

func newFileReader(
	ctx context.Context,
	filename, username, fileTableName, payloadTableName string,
	ie *sql.InternalExecutor,
) (io.ReadCloser, error) {
	fileTableReader, err := newFileTableReader(ctx, filename, username, fileTableName,
		payloadTableName, ie)
	if err != nil {
		return nil, err
	}
	return &fileReader{fileTableReader}, nil
}

func newFileTableReader(
	ctx context.Context,
	filename, username, fileTableName, payloadTableName string,
	ie *sql.InternalExecutor,
) (io.Reader, error) {
	query := fmt.Sprintf(`SELECT payload FROM %s WHERE filename='%s'`, payloadTableName, filename)
	rows, err := ie.QueryEx(
		ctx, "get-filename-payload", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: username}, query,
	)
	if err != nil {
		return nil, err
	}

	// If no payload entries were found, check for a metadata entry. If that does
	// not exist either, return an does not exist error.
	if len(rows) == 0 {
		query := fmt.Sprintf(`SELECT filename FROM %s WHERE filename='%s'`, fileTableName, filename)
		metadataRows, err := ie.QueryEx(
			ctx, "get-filename-metadata", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{User: username}, query,
		)
		if err != nil {
			return nil, err
		}

		if len(metadataRows) == 0 {
			return nil, os.ErrNotExist
		}
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
func (f *FileToTableSystem) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	var reader, err = newFileReader(ctx, filename, f.username, f.GetFQFileTableName(),
		f.GetFQPayloadTableName(), f.ie)
	return reader, err
}

func (f *FileToTableSystem) checkIfFileAndPayloadTableExist(ctx context.Context) (bool, error) {
	fileTableName, err := f.GetSimpleFileTableName()
	if err != nil {
		return false, err
	}
	payloadTableName, err := f.GetSimplePayloadTableName()
	if err != nil {
		return false, err
	}
	databaseSchema, err := f.GetDatabaseAndSchema()
	if err != nil {
		return false, err
	}

	tableExistenceQuery := fmt.Sprintf(`SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name='%s' OR table_name='%s'`,
		databaseSchema, fileTableName, payloadTableName)
	rows, err := f.ie.QueryEx(ctx, "tables-exist", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
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

func (f *FileToTableSystem) createFileAndPayloadTables(ctx context.Context, txn *kv.Txn) error {
	// Create the File and Payload tables to hold the file chunks.
	fileTableCreateQuery := fmt.Sprintf(fileTableSchema, f.GetFQFileTableName())
	_, err := f.ie.QueryEx(ctx, "create-file-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username},
		fileTableCreateQuery)
	if err != nil {
		return errors.Wrap(err, "failed to create file table to store uploaded file names")
	}

	payloadTableCreateQuery := fmt.Sprintf(payloadTableSchema, f.GetFQPayloadTableName(),
		f.GetFQFileTableName())
	_, err = f.ie.QueryEx(ctx, "create-payload-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username},
		payloadTableCreateQuery)
	if err != nil {
		return errors.Wrap(err, "failed to create interleaved table to store chunks of uploaded files")
	}

	return nil
}

// Grant the current user all read/edit privileges for the file and payload
// tables.
func (f *FileToTableSystem) grantCurrentUserTablePrivileges(
	ctx context.Context, txn *kv.Txn,
) error {
	grantQuery := fmt.Sprintf(`GRANT SELECT, INSERT, DROP, DELETE ON TABLE %s, %s TO %s`,
		f.GetFQFileTableName(), f.GetFQPayloadTableName(), f.username)
	_, err := f.ie.QueryEx(ctx, "grant-user-file-payload-table-access", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		grantQuery)
	if err != nil {
		return errors.Wrap(err, "failed to grant access privileges to file and payload tables")
	}

	return nil
}

// Revoke all privileges from every user and role except root/admin and the
// current user.
func (f *FileToTableSystem) revokeOtherUserTablePrivileges(ctx context.Context, txn *kv.Txn) error {
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
			f.GetFQFileTableName(), f.GetFQPayloadTableName(), user)
		_, err = f.ie.QueryEx(ctx, "revoke-user-privileges", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			revokeQuery)
		if err != nil {
			return errors.Wrap(err, "failed to revoke privileges")
		}
	}

	return nil
}

// NewFileWriter returns a io.WriteCloser which can be used to write files to
// the user File and Payload tables. The io.WriteCloser must be closed to flush
// the last chunk and commit the txn within which all writes occur.
// An error at any point of the write aborts the txn.
func (f *FileToTableSystem) NewFileWriter(
	ctx context.Context, filename string, chunkSize int,
) (io.WriteCloser, error) {
	return newChunkWriter(ctx, chunkSize, filename, f.username, f.GetFQFileTableName(),
		f.GetFQPayloadTableName(), f.ie,
		f.db.NewTxn(ctx, f.qualifiedTableName)), nil
}
