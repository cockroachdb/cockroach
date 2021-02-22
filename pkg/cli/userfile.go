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
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const (
	defaultUserfileScheme         = "userfile"
	defaultQualifiedNamePrefix    = "defaultdb.public.userfiles_"
	defaultQualifiedHexNamePrefix = "defaultdb.public.userfilesx_"
	tmpSuffix                     = ".tmp"
	fileTableNameSuffix           = "_upload_files"
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

var userFileGetCmd = &cobra.Command{
	Use:   "get <file|dir glob> [destination]",
	Short: "get file(s) matching the provided pattern",
	Long: `
Fetch the files stored in the user scoped file storage which match the provided pattern,
using a SQL connection, to the current directory or 'destination' if provided.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: maybeShoutError(runUserFileGet),
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

	var deletedFiles []string
	if deletedFiles, err = deleteUserFile(context.Background(), conn, glob); err != nil {
		return err
	}

	telemetry.Count("userfile.command.delete")
	for _, file := range deletedFiles {
		fmt.Printf("successfully deleted %s\n", file)
	}

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

	var files []string
	if files, err = listUserFile(context.Background(), conn, glob); err != nil {
		return err
	}

	telemetry.Count("userfile.command.list")
	for _, file := range files {
		fmt.Println(file)
	}

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

	var uploadedFile string
	if uploadedFile, err = uploadUserFile(context.Background(), conn, reader, source,
		destination); err != nil {
		return err
	}

	telemetry.Count("userfile.command.upload")
	fmt.Printf("successfully uploaded to %s\n", uploadedFile)
	return nil
}

func runUserFileGet(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx := context.Background()

	var dest string
	if len(args) > 1 {
		dest = args[len(args)-1]
	}

	conf, err := getUserfileConf(ctx, conn, args[0])
	if err != nil {
		return err
	}
	glob := conf.Path
	conf.Path = "/"
	f, err := cloudimpl.MakeSQLConnFileTableStorage(ctx, conf, conn.conn.(cloud.SQLConnI))
	if err != nil {
		return err
	}
	defer f.Close()

	files, err := f.ListFiles(ctx, glob)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return errors.New("no files matched requested path or path pattern")
	}

	for _, file := range files {
		u, err := url.Parse(file)
		if err != nil {
			return err
		}
		var fileDest string
		if len(files) > 1 {
			// If we matched multiple files, write their name in to dest or cwd.
			fileDest = filepath.Join(dest, filepath.FromSlash(u.Path))
		} else {
			filename := path.Base(u.Path)
			// If we matched just one file and do not have explicit dest, write it to
			// its file name in cwd.
			if dest == "" {
				fileDest = filename
			} else {
				// If we have an explicit destination, write the file to it, or in it if
				// that destination is a directory.
				stat, err := os.Stat(dest)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				if err == nil && stat.IsDir() { // dest is dir in which to put the file.
					fileDest = filepath.Join(dest, filename)
				} else { // not a directory, so dest is the name for this file.
					fileDest = dest
				}
			}
		}
		fmt.Printf("downloading %s... ", file)
		sz, err := downloadUserfile(ctx, f, u.Path, fileDest)
		if err != nil {
			return err
		}
		fmt.Printf("\rdownloaded %s to %s (%s)\n", file, fileDest, humanizeutil.IBytes(sz))
	}

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

// getDefaultQualifiedTableName returns the default table name prefix for the
// tables backing userfile.
// To account for all supported usernames, we adopt a naming scheme whereby if
// the normalized username remains unquoted after encoding to a SQL identifier,
// we use it as is. Otherwise we use its hex representation.
//
// This schema gives us the two properties we desire from this table name prefix:
// - Uniqueness amongst users with different usernames.
// - Support for all current and future valid usernames.
func getDefaultQualifiedTableName(user security.SQLUsername) string {
	normalizedUsername := user.Normalized()
	if lexbase.IsBareIdentifier(normalizedUsername) {
		return defaultQualifiedNamePrefix + normalizedUsername
	}
	return defaultQualifiedHexNamePrefix + fmt.Sprintf("%x", normalizedUsername)
}

// Construct the userfile ExternalStorage URI from CLI args.
func constructUserfileDestinationURI(source, destination string, user security.SQLUsername) string {
	// User has not specified a destination URI/path. We use the default URI
	// scheme and host, and the basename from the source arg as the path.
	if destination == "" {
		sourceFilename := path.Base(source)
		userFileURL := url.URL{
			Scheme: defaultUserfileScheme,
			Host:   getDefaultQualifiedTableName(user),
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
				userfileURI.Host = getDefaultQualifiedTableName(user)
			}
			return userfileURI.String()
		}
	}

	// If destination is not a well formed userfile URI, we use the default
	// userfile URI schema and host, and the destination as the path.
	userFileURL := url.URL{
		Scheme: defaultUserfileScheme,
		Host:   getDefaultQualifiedTableName(user),
		Path:   destination,
	}
	return userFileURL.String()
}

func constructUserfileListURI(glob string, user security.SQLUsername) string {
	// User has not specified a glob pattern and so we construct a URI which will
	// list all the files stored in the UserFileTableStorage.
	if glob == "" || glob == "*" {
		userFileURL := url.URL{
			Scheme: defaultUserfileScheme,
			Host:   getDefaultQualifiedTableName(user),
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
		Host:   getDefaultQualifiedTableName(user),
		Path:   glob,
	}

	return userfileURL.String()
}

func getUserfileConf(
	ctx context.Context, conn *sqlConn, glob string,
) (roachpb.ExternalStorage_FileTable, error) {
	if err := conn.ensureConn(); err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}

	reqUsername, _ := security.MakeSQLUsernameFromUserInput(connURL.User.Username(), security.UsernameValidation)

	userfileListURI := constructUserfileListURI(glob, reqUsername)
	unescapedUserfileListURI, err := url.PathUnescape(userfileListURI)
	if err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}

	userFileTableConf, err := cloudimpl.ExternalStorageConfFromURI(unescapedUserfileListURI, reqUsername)
	if err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}
	return userFileTableConf.FileTableConfig, nil

}

func listUserFile(ctx context.Context, conn *sqlConn, glob string) ([]string, error) {
	conf, err := getUserfileConf(ctx, conn, glob)
	if err != nil {
		return nil, err
	}
	prefix := conf.Path
	conf.Path = ""
	f, err := cloudimpl.MakeSQLConnFileTableStorage(ctx, conf, conn.conn.(cloud.SQLConnI))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return f.ListFiles(ctx, prefix)
}

func downloadUserfile(
	ctx context.Context, store cloud.ExternalStorage, src, dst string,
) (int64, error) {
	remoteFile, err := store.ReadFile(ctx, src)
	if err != nil {
		return 0, err
	}
	defer remoteFile.Close()

	localDir := path.Dir(dst)
	if err := os.MkdirAll(localDir, 0700); err != nil {
		return 0, err
	}

	// os.Create uses a permissive 0666 mode so use OpenFile directly.
	localFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer localFile.Close()

	return io.Copy(localFile, remoteFile)
}

func deleteUserFile(ctx context.Context, conn *sqlConn, glob string) ([]string, error) {
	if err := conn.ensureConn(); err != nil {
		return nil, err
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return nil, err
	}

	reqUsername, _ := security.MakeSQLUsernameFromUserInput(connURL.User.Username(), security.UsernameValidation)

	userfileListURI := constructUserfileListURI(glob, reqUsername)
	unescapedUserfileListURI, err := url.PathUnescape(userfileListURI)
	if err != nil {
		return nil, err
	}

	userFileTableConf, err := cloudimpl.ExternalStorageConfFromURI(unescapedUserfileListURI, reqUsername)
	if err != nil {
		return nil, err
	}

	// We zero out the path so that we can provide explicit glob patterns to the
	// ListFiles call below. Explicit glob patterns allows us to use the same
	// ExternalStorage for both the ListFiles() and Delete() methods.
	userFileTableConf.FileTableConfig.Path = ""
	f, err := cloudimpl.MakeSQLConnFileTableStorage(ctx, userFileTableConf.FileTableConfig,
		conn.conn.(cloud.SQLConnI))
	if err != nil {
		return nil, err
	}

	userfileParsedURL, err := url.ParseRequestURI(unescapedUserfileListURI)
	if err != nil {
		return nil, err
	}

	files, err := f.ListFiles(ctx, userfileParsedURL.Path)
	if err != nil {
		return nil, err
	}

	for i := range files {
		if err = f.Delete(ctx, files[i]); err != nil {
			return files[:i], errors.WithDetailf(err, "deletion failed at %s", files[i])
		}
	}
	return files, nil
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

// uploadUserFile is responsible for uploading the local source file to the user
// scoped storage referenced by destination.
// This method returns the complete userfile URI representation to which the
// file is uploaded to.
func uploadUserFile(
	ctx context.Context, conn *sqlConn, reader io.Reader, source, destination string,
) (string, error) {
	if err := conn.ensureConn(); err != nil {
		return "", err
	}

	ex := conn.conn.(driver.ExecerContext)
	if _, err := ex.ExecContext(ctx, `BEGIN`, nil); err != nil {
		return "", err
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return "", err
	}

	// Validate the username for creation. We need to do this because
	// there is no guarantee that the username in the connection string
	// is the same one on the remote machine, and it may contain special
	// characters.
	// See also: https://github.com/cockroachdb/cockroach/issues/55389
	username, err := security.MakeSQLUsernameFromUserInput(connURL.User.Username(), security.UsernameCreation)
	if err != nil {
		return "", err
	}
	// Construct the userfile URI as the destination for the CopyIn stmt.
	// Currently we hardcode the db.schema prefix, in the future we might allow
	// users to specify this.
	userfileURI := constructUserfileDestinationURI(source, destination, username)

	// Accounts for filenames with arbitrary unicode characters. url.URL escapes
	// these characters by default when setting the Path above.
	unescapedUserfileURL, err := url.PathUnescape(userfileURI)

	// We append a tmp suffix to the filename being uploaded to indicate that the
	// upload is still ongoing. This suffix is dropped once the copyTxn running
	// the upload commits.
	unescapedUserfileURL = unescapedUserfileURL + tmpSuffix
	if err != nil {
		return "", err
	}
	stmt, err := conn.conn.Prepare(sql.CopyInFileStmt(unescapedUserfileURL, sql.CrdbInternalName,
		sql.UserFileUploadTable))
	if err != nil {
		return "", err
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
				return "", err
			}
		} else if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
	}
	if err := stmt.Close(); err != nil {
		return "", err
	}
	stmt = nil

	if _, err := ex.ExecContext(ctx, `COMMIT`, nil); err != nil {
		return "", err
	}

	// Drop the .tmp suffix from the filename uploaded to userfile, thereby
	// indicating all chunks have been uploaded successfully.
	tmpURL, err := url.Parse(unescapedUserfileURL)
	if err != nil {
		return "", err
	}
	err = renameUserFile(ctx, conn, tmpURL.Path, strings.TrimSuffix(tmpURL.Path, tmpSuffix),
		tmpURL.Host)
	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(unescapedUserfileURL, tmpSuffix), nil
}

var userFileCmds = []*cobra.Command{
	userFileUploadCmd,
	userFileListCmd,
	userFileGetCmd,
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
