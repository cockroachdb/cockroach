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
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const (
	defaultUserfileScheme      = "userfile"
	defaultQualifiedNamePrefix = "defaultdb.public.userfiles_"
)

var userFileUploadCmd = &cobra.Command{
	Use:   "upload <source> <destination>",
	Short: "Upload file from source to destination",
	Long: `
Uploads a file to the user scoped file storage using a SQL connection.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: maybeShoutError(runUserFileUpload),
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

	return uploadUserFile(context.Background(), conn, reader, source, destination)
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
		sourceFilename := filepath.Base(source)
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
	var userfileURI *url.URL
	var err error
	if userfileURI, err = url.ParseRequestURI(destination); err == nil {
		if userfileURI.Scheme == defaultUserfileScheme && userfileURI.Host != "" {
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

	fmt.Printf("successfully uploaded to %s\n", unescapedUserfileURL)
	return nil
}

var userFileCmds = []*cobra.Command{
	userFileUploadCmd,
}

var userFileCmd = &cobra.Command{
	Use:   "userfile [command]",
	Short: "upload and delete user scoped files",
	Long:  "Upload and delete files from the user scoped file storage.",
	RunE:  usageAndErr,
}

func init() {
	userFileCmd.AddCommand(userFileCmds...)
}
