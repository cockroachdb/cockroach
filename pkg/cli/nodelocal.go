// Copyright 2019 The Cockroach Authors.
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
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const (
	chunkSize = 4 * 1024
)

var nodeLocalUploadCmd = &cobra.Command{
	Use:   "upload <source> <destination>",
	Short: "Upload file from source to destination",
	Long: `
Uploads a file to a gateway node's local file system using a SQL connection.
`,
	Args: cobra.MinimumNArgs(2),
	RunE: maybeShoutError(runUpload),
}

func runUpload(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach nodelocal", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	source := args[0]
	destination := args[1]
	reader, err := openSourceFile(source)
	if err != nil {
		return err
	}
	defer reader.Close()
	return uploadFile(conn, reader, destination)
}

func openSourceFile(source string) (io.ReadCloser, error) {
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

func uploadFile(conn *sqlConn, reader io.Reader, destination string) error {
	return conn.ExecTxn(func(cn *sqlConn) error {
		stmt, err := cn.conn.Prepare(sql.CopyInFileStmt(destination, "crdb_internal", "file_upload"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		send := make([]byte, chunkSize)
		for {
			n, err := reader.Read(send)
			if err == io.EOF {
				if n == 0 {
					break
				}
			} else if err != nil {
				return err
			}
			_, err = stmt.Exec([]driver.Value{string(send[:n])})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

var nodeLocalCmds = []*cobra.Command{
	nodeLocalUploadCmd,
}

var nodeLocalCmd = &cobra.Command{
	Use:   "nodelocal [command]",
	Short: "Upload and delete nodelocal files.",
	Long:  "Upload and delete files on the gateway node's local file system.",
	RunE:  usageAndErr,
}

func init() {
	nodeLocalCmd.AddCommand(nodeLocalCmds...)
}
