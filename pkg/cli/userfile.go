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
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/userfile"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
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
Uploads a single file, or, with the -r flag, all the files in the subtree rooted
at a directory, to the user-scoped file storage using a SQL connection.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: clierrorplus.MaybeShoutError(runUserFileUpload),
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
	RunE:    clierrorplus.MaybeShoutError(runUserFileList),
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
	RunE: clierrorplus.MaybeShoutError(runUserFileGet),
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
	RunE:    clierrorplus.MaybeShoutError(runUserFileDelete),
	Aliases: []string{"rm"},
}

func runUserFileDelete(cmd *cobra.Command, args []string) (resErr error) {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

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

func runUserFileList(cmd *cobra.Command, args []string) (resErr error) {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

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

func uploadUserFileRecursive(conn clisqlclient.Conn, srcDir, dstDir string) error {
	srcHasTrailingSlash := strings.HasSuffix(srcDir, "/")
	var err error
	srcDir, err = filepath.Abs(srcDir)
	if err != nil {
		return err
	}
	dstDir = strings.TrimSuffix(dstDir, "/")
	// We append the last element of the (absolute) source path, i.e. the source
	// directory name, to the destination path in the following two cases:
	//   1. The user has not specified a destination, i.e. it is empty.
	//   2. The source has no trailing slash (à la rsync).
	srcDirBase := filepath.Base(srcDir)
	if dstDir == "" {
		dstDir = srcDirBase
	} else if !srcHasTrailingSlash {
		dstDir = dstDir + "/" + srcDirBase
	}

	ctx := context.Background()

	err = filepath.WalkDir(srcDir,
		func(path string, info fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				relativePath := strings.TrimPrefix(path, srcDir+"/")
				fmt.Printf("uploading: %s\n", relativePath)

				uploadedFile, err := uploadUserFile(ctx, conn, path, dstDir+"/"+relativePath)
				if err != nil {
					return err
				}
				fmt.Printf("successfully uploaded to %s\n", uploadedFile)
			}
			return nil
		})
	if err != nil {
		return err
	}

	fmt.Printf("successfully uploaded all files in the subtree rooted at %s\n", filepath.Base(srcDir))
	return nil
}

func runUserFileUpload(cmd *cobra.Command, args []string) (resErr error) {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	source := args[0]

	var destination string
	if len(args) == 2 {
		destination = args[1]
	}

	if userfileCtx.recursive {
		if err := uploadUserFileRecursive(conn, source, destination); err != nil {
			return err
		}
	} else {
		uploadedFile, err := uploadUserFile(context.Background(), conn, source,
			destination)
		if err != nil {
			return err
		}
		fmt.Printf("successfully uploaded to %s\n", uploadedFile)
	}

	telemetry.Count("userfile.command.upload")
	return nil
}

func runUserFileGet(cmd *cobra.Command, args []string) (resErr error) {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()
	ctx := context.Background()

	var dest string
	if len(args) > 1 {
		dest = args[len(args)-1]
	}

	conf, err := getUserfileConf(ctx, conn, args[0])
	if err != nil {
		return err
	}

	fullPath := conf.Path
	conf.Path = cloud.GetPrefixBeforeWildcard(fullPath)
	pattern := fullPath[len(conf.Path):]
	displayPath := strings.TrimPrefix(conf.Path, "/")

	f, err := userfile.MakeSQLConnFileTableStorage(ctx, conf, conn.GetDriverConn().(cloud.SQLConnI))
	if err != nil {
		return err
	}
	defer f.Close()

	var files []string
	if err := f.List(ctx, "", "", func(s string) error {
		if pattern != "" {
			if ok, err := path.Match(pattern, s); err != nil || !ok {
				return err
			}
		}
		files = append(files, s)
		return nil
	}); err != nil {
		return err
	}

	if len(files) == 0 {
		return errors.New("no files matched requested path or path pattern")
	}

	for _, src := range files {
		file := displayPath + src
		var fileDest string
		if len(files) > 1 {
			// If we matched multiple files, write their name in to dest or cwd.
			fileDest = filepath.Join(dest, filepath.FromSlash(file))
		} else {
			filename := path.Base(file)
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
		sz, err := downloadUserfile(ctx, f, src, fileDest)
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
	ctx context.Context, conn clisqlclient.Conn, glob string,
) (roachpb.ExternalStorage_FileTable, error) {
	if err := conn.EnsureConn(); err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}

	connURL, err := url.Parse(conn.GetURL())
	if err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}

	reqUsername, _ := security.MakeSQLUsernameFromUserInput(connURL.User.Username(), security.UsernameValidation)

	userfileListURI := constructUserfileListURI(glob, reqUsername)
	unescapedUserfileListURI, err := url.PathUnescape(userfileListURI)
	if err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}

	userFileTableConf, err := cloud.ExternalStorageConfFromURI(unescapedUserfileListURI, reqUsername)
	if err != nil {
		return roachpb.ExternalStorage_FileTable{}, err
	}
	return userFileTableConf.FileTableConfig, nil

}

func listUserFile(ctx context.Context, conn clisqlclient.Conn, glob string) ([]string, error) {
	conf, err := getUserfileConf(ctx, conn, glob)
	if err != nil {
		return nil, err
	}

	fullPath := conf.Path
	conf.Path = cloud.GetPrefixBeforeWildcard(fullPath)
	pattern := fullPath[len(conf.Path):]

	f, err := userfile.MakeSQLConnFileTableStorage(ctx, conf, conn.GetDriverConn().(cloud.SQLConnI))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	displayPrefix := strings.TrimPrefix(conf.Path, "/")
	var res []string
	if err := f.List(ctx, "", "", func(s string) error {
		if pattern != "" {
			ok, err := path.Match(pattern, s)
			if err != nil || !ok {
				return err
			}
		}
		res = append(res, displayPrefix+s)
		return nil
	}); err != nil {
		return nil, err
	}
	return res, nil
}

func downloadUserfile(
	ctx context.Context, store cloud.ExternalStorage, src, dst string,
) (int64, error) {
	remoteFile, err := store.ReadFile(ctx, src)
	if err != nil {
		return 0, err
	}
	defer remoteFile.Close(ctx)

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

	return io.Copy(localFile, ioctx.ReaderCtxAdapter(ctx, remoteFile))
}

func deleteUserFile(ctx context.Context, conn clisqlclient.Conn, glob string) ([]string, error) {
	if err := conn.EnsureConn(); err != nil {
		return nil, err
	}

	connURL, err := url.Parse(conn.GetURL())
	if err != nil {
		return nil, err
	}

	reqUsername, _ := security.MakeSQLUsernameFromUserInput(connURL.User.Username(), security.UsernameValidation)

	userfileListURI := constructUserfileListURI(glob, reqUsername)
	unescapedUserfileListURI, err := url.PathUnescape(userfileListURI)
	if err != nil {
		return nil, err
	}

	userFileTableConf, err := cloud.ExternalStorageConfFromURI(unescapedUserfileListURI, reqUsername)
	if err != nil {
		return nil, err
	}

	// We truncate the path so that we can open one store to first do a listing
	// with our actual pattern, then pass the found names to delete them using the
	// same store.
	fullPath := userFileTableConf.FileTableConfig.Path
	userFileTableConf.FileTableConfig.Path = cloud.GetPrefixBeforeWildcard(fullPath)
	pattern := fullPath[len(userFileTableConf.FileTableConfig.Path):]

	f, err := userfile.MakeSQLConnFileTableStorage(ctx, userFileTableConf.FileTableConfig,
		conn.GetDriverConn().(cloud.SQLConnI))
	if err != nil {
		return nil, err
	}

	displayRoot := strings.TrimPrefix(userFileTableConf.FileTableConfig.Path, "/")
	var deleted []string

	if err := f.List(ctx, "", "", func(s string) error {
		if pattern != "" {
			ok, err := path.Match(pattern, s)
			if err != nil || !ok {
				return err
			}
		}
		if err := errors.WithDetailf(f.Delete(ctx, s), "deleting %s failed", s); err != nil {
			return err
		}
		deleted = append(deleted, displayRoot+s)
		return nil
	}); err != nil {
		return nil, err
	}

	return deleted, nil
}

func renameUserFile(
	ctx context.Context, conn clisqlclient.Conn, oldFilename,
	newFilename, qualifiedTableName string,
) error {
	if err := conn.EnsureConn(); err != nil {
		return err
	}

	ex := conn.GetDriverConn()
	if _, err := ex.ExecContext(ctx, `BEGIN`, nil); err != nil {
		return err
	}

	stmt, err := conn.GetDriverConn().Prepare(fmt.Sprintf(`UPDATE %s SET filename=$1 WHERE filename=$2`,
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
	//lint:ignore SA1019 DriverConn doesn't support go 1.8 API
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
	ctx context.Context, conn clisqlclient.Conn, source, destination string,
) (string, error) {
	reader, err := openUserFile(source)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	if err := conn.EnsureConn(); err != nil {
		return "", err
	}

	ex := conn.GetDriverConn()
	if _, err := ex.ExecContext(ctx, `BEGIN`, nil); err != nil {
		return "", err
	}

	connURL, err := url.Parse(conn.GetURL())
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
	stmt, err := conn.GetDriverConn().Prepare(sql.CopyInFileStmt(unescapedUserfileURL, sql.CrdbInternalName,
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
			//lint:ignore SA1019 DriverConn doesn't support go 1.8 API
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
	RunE:  UsageAndErr,
}

func init() {
	userFileCmd.AddCommand(userFileCmds...)
}
