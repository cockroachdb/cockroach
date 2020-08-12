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
	"database/sql/driver"
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/errors"
)

// ChunkDefaultSize is the default size of each chunk a file will be broken into
// before being written to the Payload table.
const ChunkDefaultSize = 1024 * 1024 * 4 // 4 Mib

var fileTableNameSuffix = "_upload_files"
var payloadTableNameSuffix = "_upload_payload"

// FileToTableExecutorRows encompasses the two formats in which the
// InternalFileToTableExecutor and SQLConnFileToTableExecutor output their rows.
type FileToTableExecutorRows struct {
	internalExecResults []tree.Datums
	sqlConnExecResults  driver.Rows
}

// FileToTableSystemExecutor is the interface which defines the methods for the
// SQL query executor used by the FileToTableSystem
type FileToTableSystemExecutor interface {
	Query(ctx context.Context, opName, query, username string,
		qargs ...interface{}) (*FileToTableExecutorRows, error)
	Exec(ctx context.Context, opName, query, username string,
		qargs ...interface{}) error
}

// InternalFileToTableExecutor is the SQL query executor which uses an internal
// SQL connection to interact with the database.
type InternalFileToTableExecutor struct {
	ie *sql.InternalExecutor
	db *kv.DB
}

var _ FileToTableSystemExecutor = &InternalFileToTableExecutor{}

// MakeInternalFileToTableExecutor returns an instance of a
// InternalFileToTableExecutor.
func MakeInternalFileToTableExecutor(
	ie *sql.InternalExecutor, db *kv.DB,
) *InternalFileToTableExecutor {
	return &InternalFileToTableExecutor{ie, db}
}

// Query implements the FileToTableSystemExecutor interface.
func (i *InternalFileToTableExecutor) Query(
	ctx context.Context, opName, query, username string, qargs ...interface{},
) (*FileToTableExecutorRows, error) {
	result := FileToTableExecutorRows{}
	var err error
	result.internalExecResults, err = i.ie.QueryEx(ctx, opName, nil,
		sqlbase.InternalExecutorSessionDataOverride{User: username}, query, qargs...)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Exec implements the FileToTableSystemExecutor interface.
func (i *InternalFileToTableExecutor) Exec(
	ctx context.Context, opName, query, username string, qargs ...interface{},
) error {
	_, err := i.ie.ExecEx(ctx, opName, nil,
		sqlbase.InternalExecutorSessionDataOverride{User: username}, query, qargs...)
	return err
}

// SQLConnFileToTableExecutor is the SQL query executor which uses a network
// backed SQL connection to interact with the database.
type SQLConnFileToTableExecutor struct {
	executor cloud.SQLConnI
}

var _ FileToTableSystemExecutor = &SQLConnFileToTableExecutor{}

// MakeSQLConnFileToTableExecutor returns an instance of a
// SQLConnFileToTableExecutor.
func MakeSQLConnFileToTableExecutor(executor cloud.SQLConnI) *SQLConnFileToTableExecutor {
	return &SQLConnFileToTableExecutor{executor: executor}
}

// Query implements the FileToTableSystemExecutor interface.
func (i *SQLConnFileToTableExecutor) Query(
	ctx context.Context, _, query, _ string, qargs ...interface{},
) (*FileToTableExecutorRows, error) {
	result := FileToTableExecutorRows{}

	argVals := make([]driver.NamedValue, len(qargs))
	for i, qarg := range qargs {
		namedVal := driver.NamedValue{
			// Ordinal position is 1 indexed.
			Ordinal: i + 1,
			Value:   qarg,
		}
		argVals[i] = namedVal
	}

	var err error
	result.sqlConnExecResults, err = i.executor.QueryContext(ctx, query, argVals)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Exec implements the FileToTableSystemExecutor interface.
func (i *SQLConnFileToTableExecutor) Exec(
	ctx context.Context, _, query, _ string, qargs ...interface{},
) error {
	argVals := make([]driver.NamedValue, len(qargs))
	for i, qarg := range qargs {
		namedVal := driver.NamedValue{
			// Ordinal position is 1 indexed.
			Ordinal: i + 1,
			Value:   qarg,
		}
		argVals[i] = namedVal
	}
	_, err := i.executor.ExecContext(ctx, query, argVals)
	return err
}

// FileToTableSystem can be used to store, retrieve and delete the
// blobs and metadata of files, from user scoped tables. Access to these tables
// is restricted to the root/admin user and the user responsible for triggering
// table creation in the first place.
// All methods operate within the scope of the provided database db, as the user
// with the provided username.
//
// Refer to the method headers for more details about the user scoped tables.
type FileToTableSystem struct {
	qualifiedTableName string
	executor           FileToTableSystemExecutor
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

func resolveInternalFileToTableExecutor(
	executor FileToTableSystemExecutor,
) (*InternalFileToTableExecutor, error) {
	var e *InternalFileToTableExecutor
	var ok bool
	if e, ok = executor.(*InternalFileToTableExecutor); !ok {
		return nil, errors.New("unable to resolve to a supported executor type")
	}

	return e, nil
}

// NewFileToTableSystem returns a FileToTableSystem object. It creates the File
// and Payload user tables, grants the current user all read/edit privileges on
// the tables and revokes access of every other user and role (except
// root/admin).
func NewFileToTableSystem(
	ctx context.Context,
	qualifiedTableName string,
	executor FileToTableSystemExecutor,
	username string,
) (*FileToTableSystem, error) {
	f := FileToTableSystem{
		qualifiedTableName: qualifiedTableName, executor: executor, username: username,
	}

	// A SQLConnFileToTableExecutor should not perform any of the init steps as it
	// can only be used to interact with the existing user scoped SQL tables.
	if _, ok := executor.(*SQLConnFileToTableExecutor); ok {
		return &f, nil
	}

	e, err := resolveInternalFileToTableExecutor(executor)
	if err != nil {
		return nil, err
	}
	if err := e.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// TODO(adityamaru): Handle scenario where the user has already created
		// tables with the same names not via the FileToTableSystem
		// object. Not sure if we want to error out or work around it.
		tablesExist, err := f.checkIfFileAndPayloadTableExist(ctx, e.ie)
		if err != nil {
			return err
		}

		if !tablesExist {
			if err := f.createFileAndPayloadTables(ctx, txn, e.ie); err != nil {
				return err
			}

			if err := f.grantCurrentUserTablePrivileges(ctx, txn, e.ie); err != nil {
				return err
			}

			if err := f.revokeOtherUserTablePrivileges(ctx, txn, e.ie); err != nil {
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
	e, err := resolveInternalFileToTableExecutor(f.executor)
	if err != nil {
		return 0, err
	}

	getFileSizeQuery := fmt.Sprintf(`SELECT file_size FROM %s WHERE filename=$1`,
		f.GetFQFileTableName())
	rows, err := e.ie.QueryRowEx(ctx, "payload-table-storage-size", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username},
		getFileSizeQuery, filename)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get size of file from the payload table")
	}

	if len(rows) == 0 {
		return 0, errors.Newf("file %s does not exist in the UserFileStorage", filename)
	}

	return int64(tree.MustBeDInt(rows[0])), nil
}

// ListFiles returns a list of all the files which are currently stored in the
// user scoped tables.
func (f *FileToTableSystem) ListFiles(ctx context.Context, pattern string) ([]string, error) {
	var files []string
	listFilesQuery := fmt.Sprintf(`SELECT filename FROM %s WHERE filename LIKE $1 ORDER BY
filename`, f.GetFQFileTableName())

	rows, err := f.executor.Query(ctx, "file-table-storage-list", listFilesQuery, f.username,
		pattern+"%")
	if err != nil {
		return files, errors.Wrap(err, "failed to list files from file table")
	}

	// Based on the executor type we must process the outputted rows differently.
	switch f.executor.(type) {
	case *InternalFileToTableExecutor:
		// Verify that all the filenames are strings and aggregate them.
		for _, row := range rows.internalExecResults {
			files = append(files, string(tree.MustBeDString(row[0])))
		}
	case *SQLConnFileToTableExecutor:
		vals := make([]driver.Value, 1)
		for {
			if err := rows.sqlConnExecResults.Next(vals); err == io.EOF {
				break
			} else if err != nil {
				return files, errors.Wrap(err, "failed to list files from file table")
			}
			filename := vals[0].(string)
			files = append(files, filename)
		}

		if err = rows.sqlConnExecResults.Close(); err != nil {
			return nil, err
		}
	default:
		return []string{}, errors.New("unsupported executor type in FileSize")
	}

	return files, nil
}

// DestroyUserFileSystem drops the user scoped tables effectively deleting the
// blobs and metadata of every file.
// The FileToTableSystem object is unusable after this method returns.
func DestroyUserFileSystem(ctx context.Context, f *FileToTableSystem) error {
	e, err := resolveInternalFileToTableExecutor(f.executor)
	if err != nil {
		return err
	}

	if err := e.db.Txn(ctx,
		func(ctx context.Context, txn *kv.Txn) error {
			dropPayloadTableQuery := fmt.Sprintf(`DROP TABLE %s`, f.GetFQPayloadTableName())
			_, err := e.ie.QueryEx(ctx, "drop-payload-table", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: f.username},
				dropPayloadTableQuery)
			if err != nil {
				return errors.Wrap(err, "failed to drop payload table")
			}

			dropFileTableQuery := fmt.Sprintf(`DROP TABLE %s CASCADE`, f.GetFQFileTableName())
			_, err = e.ie.QueryEx(ctx, "drop-file-table", txn,
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
	defer func() {
		_ = f.executor.Exec(ctx, "commit", `COMMIT`, f.username)
	}()

	txnErr := f.executor.Exec(ctx, "delete-file", `BEGIN`, f.username)
	if txnErr != nil {
		return txnErr
	}

	deleteFileQuery := fmt.Sprintf(`DELETE FROM %s WHERE filename=$1`, f.GetFQFileTableName())
	txnErr = f.executor.Exec(ctx, "delete-file-table", deleteFileQuery,
		f.username, filename)
	if txnErr != nil {
		return errors.Wrap(txnErr, "failed to delete from the file table")
	}

	deletePayloadQuery := fmt.Sprintf(`DELETE FROM %s WHERE filename=$1`,
		f.GetFQPayloadTableName())

	txnErr = f.executor.Exec(ctx, "delete-payload-table", deletePayloadQuery, f.username,
		filename)
	if txnErr != nil {
		return errors.Wrap(txnErr, "failed to delete from the payload table")
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
			return err
		}
	}

	// Insert file metadata entry into File table.
	fileNameQuery := fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3)`, w.fileTableName)

	_, err := w.pw.ie.QueryEx(w.pw.ctx, "insert-file-name", w.pw.txn,
		w.execSessionDataOverride, fileNameQuery, w.pw.filename, w.pw.byteOffset,
		w.execSessionDataOverride.User)
	return err
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
	query := fmt.Sprintf(`SELECT payload FROM %s WHERE filename=$1`, payloadTableName)
	rows, err := ie.QueryEx(
		ctx, "get-filename-payload", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: username}, query, filename,
	)
	if err != nil {
		return nil, err
	}

	// If no payload entries were found, check for a metadata entry. If that does
	// not exist either, return an does not exist error.
	if len(rows) == 0 {
		query := fmt.Sprintf(`SELECT filename FROM %s WHERE filename=$1`, fileTableName)
		metadataRows, err := ie.QueryEx(
			ctx, "get-filename-metadata", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{User: username}, query, filename,
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
	e, err := resolveInternalFileToTableExecutor(f.executor)
	if err != nil {
		return nil, err
	}

	var reader, readerErr = newFileReader(ctx, filename, f.username, f.GetFQFileTableName(),
		f.GetFQPayloadTableName(), e.ie)
	return reader, readerErr
}

func (f *FileToTableSystem) checkIfFileAndPayloadTableExist(
	ctx context.Context, ie *sql.InternalExecutor,
) (bool, error) {
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

	tableExistenceQuery := fmt.Sprintf(
		`SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name=$1 OR table_name=$2`,
		databaseSchema)
	rows, err := ie.QueryEx(ctx, "tables-exist", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		tableExistenceQuery, fileTableName, payloadTableName)
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
	ctx context.Context, txn *kv.Txn, ie *sql.InternalExecutor,
) error {
	// Create the File and Payload tables to hold the file chunks.
	fileTableCreateQuery := fmt.Sprintf(fileTableSchema, f.GetFQFileTableName())
	_, err := ie.QueryEx(ctx, "create-file-table", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: f.username},
		fileTableCreateQuery)
	if err != nil {
		return errors.Wrap(err, "failed to create file table to store uploaded file names")
	}

	payloadTableCreateQuery := fmt.Sprintf(payloadTableSchema, f.GetFQPayloadTableName(),
		f.GetFQFileTableName())
	_, err = ie.QueryEx(ctx, "create-payload-table", txn,
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
	ctx context.Context, txn *kv.Txn, ie *sql.InternalExecutor,
) error {
	grantQuery := fmt.Sprintf(`GRANT SELECT, INSERT, DROP, DELETE ON TABLE %s, %s TO %s`,
		f.GetFQFileTableName(), f.GetFQPayloadTableName(), f.username)
	_, err := ie.QueryEx(ctx, "grant-user-file-payload-table-access", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		grantQuery)
	if err != nil {
		return errors.Wrap(err, "failed to grant access privileges to file and payload tables")
	}

	return nil
}

// Revoke all privileges from every user and role except root/admin and the
// current user.
func (f *FileToTableSystem) revokeOtherUserTablePrivileges(
	ctx context.Context, txn *kv.Txn, ie *sql.InternalExecutor,
) error {
	getUsersQuery := fmt.Sprintf(`SELECT username FROM system.
users WHERE NOT "username" = 'root' AND NOT "username" = 'admin' AND NOT "username" = $1`)
	rows, err := ie.QueryEx(
		ctx, "get-users", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		getUsersQuery, f.username,
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
		_, err = ie.QueryEx(ctx, "revoke-user-privileges", txn,
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
	ctx context.Context, filename string, chunkSize int, txn *kv.Txn,
) (io.WriteCloser, error) {
	e, err := resolveInternalFileToTableExecutor(f.executor)
	if err != nil {
		return nil, err
	}

	return newChunkWriter(ctx, chunkSize, filename, f.username, f.GetFQFileTableName(),
		f.GetFQPayloadTableName(), e.ie, txn), nil
}
