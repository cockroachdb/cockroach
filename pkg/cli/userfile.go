// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const (
	defaultUserfileScheme      = "userfile"
	defaultQualifiedNamePrefix = "defaultdb.public.userfiles_"
	tmpSuffix                  = ".tmp"
	fileTableNameSuffix        = "_upload_files"
)

var userFileUploadCmd = &cobra.Command{
	Use:   "upload <source> <destination>",
	Short: "upload file from source to destination",
	Long: `
Uploads a file to the user scoped file storage using a SQL connection.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: maybeShoutError(runUserFileUpload),
}

var userFileListCmd = &cobra.Command{
	Use:   "list <file|dir glob>",
	Short: "list files matching the provided pattern",
	Long: `
Lists the files stored in the user scoped file storage which match the provided pattern,
using a SQL connection. If no pattern is provided, all files in the specified
(or default, if unspecified) user scoped file storage will be listed.
`,
	Args:    cobra.MinimumNArgs(0),
	RunE:    maybeShoutError(runUserFileList),
	Aliases: []string{"ls"},
}

var userFileDeleteCmd = &cobra.Command{
	Use:   "delete <file|dir glob>",
	Short: "delete files matching the provided pattern",
	Long: `
Deletes the files stored in the user scoped file storage which match the provided pattern,
using a SQL connection. If passed pattern '*', all files in the specified
(or default, if unspecified) user scoped file storage will be deleted. Deletions are not
atomic, and all deletions prior to the first failure will occur. 
`,
	Args:    cobra.MinimumNArgs(1),
	RunE:    maybeShoutError(runUserFileDelete),
	Aliases: []string{"rm"},
}

func runUserFileDelete(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	glob := args[0]

	if err := deleteUserFile(context.Background(), conn, glob); err != nil {
		return err
	}

	telemetry.Count("userfile.command.delete")
	return nil
}

func runUserFileList(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	var glob string
	if len(args) > 0 {
		glob = args[0]
	}

	if err := listUserFile(context.Background(), conn, glob); err != nil {
		return err
	}

	telemetry.Count("userfile.command.list")
	return nil
}

func runUserFileUpload(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	source := args[0]

	var destination string
	if len(args) == 2 {
		destination = args[1]
	}

	reader, err := openUserFile(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	if err := uploadUserFile(context.Background(), conn, reader, source, destination); err != nil {
		return err
	}

	telemetry.Count("userfile.command.upload")
	return nil
}

func openUserFile(source string) (io.ReadCloser, error) {
	f, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get source file stats for %s", source)
	}
	if stat.IsDir() {
		return nil, fmt.Errorf("source file %s is a directory, not a file", source)
	}
	return f, nil
}

// Construct the userfile ExternalStorage URI from CLI args.
func constructUserfileDestinationURI(source, destination, user string) string {
	// User has not specified a destination URI/path. We use the default URI
	// scheme and host, and the basename from the source arg as the path.
	if destination == "" {
		sourceFilename := path.Base(source)
		userFileURL := url.URL{
			Scheme: defaultUserfileScheme,
			Host:   defaultQualifiedNamePrefix + user,
			Path:   sourceFilename,
		}
		return userFileURL.String()
	}

	// If the destination is a well-formed userfile URI of the form
	// userfile://db.schema.tablename_prefix/path/to/file, then we
	// use that as the final URI.
	//
	// A URI without a host will default to searching in
	// `defaultdb.public.userfiles_username`.
	var userfileURI *url.URL
	var err error
	if userfileURI, err = url.ParseRequestURI(destination); err == nil {
		if userfileURI.Scheme == defaultUserfileScheme {
			if userfileURI.Host == "" {
				userfileURI.Host = defaultQualifiedNamePrefix + user
			}
			return userfileURI.String()
		}
	}

	// If destination is not a well formed userfile URI, we use the default
	// userfile URI schema and host, and the destination as the path.
	userFileURL := url.URL{
		Scheme: defaultUserfileScheme,
		Host:   defaultQualifiedNamePrefix + user,
		Path:   destination,
	}
	return userFileURL.String()
}

func constructUserfileListURI(glob, user string) string {
	// User has not specified a glob pattern and so we construct a URI which will
	// list all the files stored in the UserFileTableStorage.
	if glob == "" || glob == "*" {
		userFileURL := url.URL{
			Scheme: defaultUserfileScheme,
			Host:   defaultQualifiedNamePrefix + user,
			Path:   "",
		}
		return userFileURL.String()
	}

	// If the destination is a well-formed userfile URI of the form
	// userfile://db.schema.tablename_prefix/glob/pattern, then we
	// use that as the final URI.
	if userfileURL, err := url.ParseRequestURI(glob); err == nil {
		if userfileURL.Scheme == defaultUserfileScheme {
			return userfileURL.String()
		}
	}

	// If destination is not a well formed userfile URI, we use the default
	// userfile URI schema and host, and the glob as the path.
	userfileURL := url.URL{
		Scheme: defaultUserfileScheme,
		Host:   defaultQualifiedNamePrefix + user,
		Path:   glob,
	}

	return userfileURL.String()
}

func listUserFile(ctx context.Context, conn *sqlConn, glob string) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return err
	}

	userfileListURI := constructUserfileListURI(glob, connURL.User.Username())
	unescapedUserfileListURI, err := url.PathUnescape(userfileListURI)
	if err != nil {
		return err
	}

	userFileTableConf, err := cloudimpl.ExternalStorageConfFromURI(unescapedUserfileListURI,
		connURL.User.Username())
	if err != nil {
		return err
	}

	f, err := cloudimpl.MakeSQLConnFileTableStorage(ctx, userFileTableConf.FileTableConfig,
		conn.conn.(cloud.SQLConnI))
	if err != nil {
		return err
	}

	files, err := f.ListFiles(ctx, "")
	if err != nil {
		return err
	}

	for _, file := range files {
		fmt.Println(file)
	}

	return nil
}

func deleteUserFile(ctx context.Context, conn *sqlConn, glob string) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return err
	}

	userfileListURI := constructUserfileListURI(glob, connURL.User.Username())
	unescapedUserfileListURI, err := url.PathUnescape(userfileListURI)
	if err != nil {
		return err
	}

	userFileTableConf, err := cloudimpl.ExternalStorageConfFromURI(unescapedUserfileListURI,
		connURL.User.Username())
	if err != nil {
		return err
	}

	// We zero out the path so that we can provide explicit glob patterns to the
	// ListFiles call below. Explicit glob patterns allows us to use the same
	// ExternalStorage for both the ListFiles() and Delete() methods.
	userFileTableConf.FileTableConfig.Path = ""
	f, err := cloudimpl.MakeSQLConnFileTableStorage(ctx, userFileTableConf.FileTableConfig,
		conn.conn.(cloud.SQLConnI))
	if err != nil {
		return err
	}

	userfileParsedURL, err := url.ParseRequestURI(unescapedUserfileListURI)
	if err != nil {
		return err
	}
	files, err := f.ListFiles(ctx, userfileParsedURL.Path)
	if err != nil {
		return err
	}

	for _, file := range files {
		var deleteFileBasename string
		if userfileParsedURL.Path == "" {
			// ListFiles will return absolute userfile URIs which will require
			// parsing.
			parsedFile, err := url.ParseRequestURI(file)
			if err != nil {
				return errors.WithDetailf(err, "deletion failed at %s", file)
			}
			deleteFileBasename = parsedFile.Path
		} else {
			// ListFiles returns relative filepaths without a leading /. All files are
			// stored with a prefix / in the underlying user scoped tables.
			deleteFileBasename = path.Join("/", file)
		}
		err = f.Delete(ctx, deleteFileBasename)
		if err != nil {
			return errors.WithDetail(err, fmt.Sprintf("deletion failed at %s", file))
		}

		resolvedHost := defaultQualifiedNamePrefix + connURL.User.Username()
		if userfileParsedURL.Host != "" {
			resolvedHost = userfileParsedURL.Host
		}
		fmt.Printf("deleted userfile://%s%s\n", resolvedHost, deleteFileBasename)
	}
	return nil
}

func renameUserFile(
	ctx context.Context, conn *sqlConn, oldFilename,
	newFilename, qualifiedTableName string,
) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	ex := conn.conn.(driver.ExecerContext)
	if _, err := ex.ExecContext(ctx, `BEGIN`, nil); err != nil {
		return err
	}

	stmt, err := conn.conn.Prepare(fmt.Sprintf(`UPDATE %s SET filename=$1 WHERE filename=$2`,
		qualifiedTableName+fileTableNameSuffix))
	if err != nil {
		return err
	}

	defer func() {
		if stmt != nil {
			_ = stmt.Close()
			_, _ = ex.ExecContext(ctx, `ROLLBACK`, nil)
		}
	}()

	_, err = stmt.Exec([]driver.Value{newFilename, oldFilename})
	if err != nil {
		return err
	}

	if err := stmt.Close(); err != nil {
		return err
	}
	stmt = nil

	if _, err := ex.ExecContext(ctx, `COMMIT`, nil); err != nil {
		return err
	}

	return nil
}

func uploadUserFile(
	ctx context.Context, conn *sqlConn, reader io.Reader, source, destination string,
) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	ex := conn.conn.(driver.ExecerContext)
	if _, err := ex.ExecContext(ctx, `BEGIN`, nil); err != nil {
		return err
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return err
	}

	// Construct the userfile URI as the destination for the CopyIn stmt.
	// Currently we hardcode the db.schema prefix, in the future we might allow
	// users to specify this.
	userfileURI := constructUserfileDestinationURI(source, destination, connURL.User.Username())

	// Accounts for filenames with arbitrary unicode characters. url.URL escapes
	// these characters by default when setting the Path above.
	unescapedUserfileURL, err := url.PathUnescape(userfileURI)

	// We append a tmp suffix to the filename being uploaded to indicate that the
	// upload is still ongoing. This suffix is dropped once the copyTxn running
	// the upload commits.
	unescapedUserfileURL = unescapedUserfileURL + tmpSuffix
	if err != nil {
		return err
	}
	stmt, err := conn.conn.Prepare(sql.CopyInFileStmt(unescapedUserfileURL, sql.CrdbInternalName,
		sql.UserFileUploadTable))
	if err != nil {
		return err
	}

	defer func() {
		if stmt != nil {
			_ = stmt.Close()
			_, _ = ex.ExecContext(ctx, `ROLLBACK`, nil)
		}
	}()

	send := make([]byte, chunkSize)
	for {
		n, err := reader.Read(send)
		if n > 0 {
			// TODO(adityamaru): Switch to StmtExecContext once the copyin driver
			// supports it.
			_, err = stmt.Exec([]driver.Value{string(send[:n])})
			if err != nil {
				return err
			}
		} else if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	if err := stmt.Close(); err != nil {
		return err
	}
	stmt = nil

	if _, err := ex.ExecContext(ctx, `COMMIT`, nil); err != nil {
		return err
	}

	// Drop the .tmp suffix from the filename uploaded to userfile, thereby
	// indicating all chunks have been uploaded successfully.
	tmpURL, err := url.Parse(unescapedUserfileURL)
	if err != nil {
		return err
	}
	err = renameUserFile(ctx, conn, tmpURL.Path, strings.TrimSuffix(tmpURL.Path, tmpSuffix),
		tmpURL.Host)
	if err != nil {
		return err
	}

	fmt.Printf("successfully uploaded to %s\n",
		strings.TrimSuffix(unescapedUserfileURL, tmpSuffix))
	return nil
}

var userFileCmds = []*cobra.Command{
	userFileUploadCmd,
	userFileListCmd,
	userFileDeleteCmd,
}

var userFileCmd = &cobra.Command{
	Use:   "userfile [command]",
	Short: "upload, list and delete user scoped files",
	Long:  "Upload, list and delete files from the user scoped file storage.",
	RunE:  usageAndErr,
}

func init() {
	userFileCmd.AddCommand(userFileCmds...)
}
