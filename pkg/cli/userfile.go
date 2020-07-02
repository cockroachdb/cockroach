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
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const defaultQualifiedDBSchemaName = "defaultdb.public."

var userFileUploadCmd = &cobra.Command{
	Use:   "upload <source> <destination>",
	Short: "Upload file from source to destination",
	Long: `
Uploads a file to the user scoped file storage using a SQL connection.
`,
	Args: cobra.MinimumNArgs(2),
	RunE: maybeShoutError(runUserFileUpload),
}

func runUserFileUpload(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach userfile", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	source := args[0]
	destination := args[1]
	reader, err := openUserFile(source)
	if err != nil {
		return err
	}
	defer reader.Close()
	return uploadUserFile(conn, reader, destination)
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

func uploadUserFile(conn *sqlConn, reader io.Reader, destination string) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	if _, err := conn.conn.Exec(`BEGIN`, nil); err != nil {
		return err
	}

	// TODO(adityamaru): In the future we may want to allow users to specify a
	// fully qualified db.schema.table where their underlying SQL file tables will
	// be created. Enforcing the filepath to begin with a / allows for easy
	// disambiguation between the qualified name and the filepath.
	if !strings.HasPrefix(destination, "/") {
		return errors.Newf("userfile upload destination path must begin with /")
	}

	// TODO(adityamaru): We reject any destination filepath's with `..` in them.
	// This is because as the UserFileTableSystem is not a real file system, when
	// you upload a file to a destination such as test/../../test.csv, we write
	// its contents to a SQL table with filename set to test/../../test.csv. This
	// is strange and we should come up with a better scheme of enforcing
	// "sensible" filenames.
	if strings.Contains(destination, "..") {
		return errors.Newf("path %s has a `.."+
			"` in its path which is an invalid construct for userfile upload destinations", destination)
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return err
	}

	// Construct the userfile URI as the destination for the CopyIn stmt.
	// Currently we hardcode the db.schema prefix, in the future we might allow
	// users to specify this.
	var userfileURI string
	userfileURI = fmt.Sprintf("userfile://%s%s",
		defaultQualifiedDBSchemaName+connURL.User.Username(), destination)

	stmt, err := conn.conn.Prepare(sql.CopyInFileStmt(userfileURI, "crdb_internal",
		"user_file_upload"))
	if err != nil {
		return err
	}

	defer func() {
		if stmt != nil {
			_ = stmt.Close()
			_, _ = conn.conn.Exec(`ROLLBACK`, nil)
		}
	}()

	send := make([]byte, chunkSize)
	for {
		n, err := reader.Read(send)
		if n > 0 {
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

	if _, err := conn.conn.Exec(`COMMIT`, nil); err != nil {
		return err
	}

	fmt.Printf("successfully uploaded to %s\n", userfileURI)
	return nil
}

var userFileCmds = []*cobra.Command{
	userFileUploadCmd,
}

var userFileCmd = &cobra.Command{
	Use:   "userfile [command]",
	Short: "upload and delete user files",
	Long:  "Upload and delete files from the user scoped file storage.",
	RunE:  usageAndErr,
}

func init() {
	userFileCmd.AddCommand(userFileCmds...)
}
