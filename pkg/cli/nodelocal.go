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

//var verUploadFile = version.MustParse("")

func runUpload(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach nodelocal", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()
	// TODO: which version should we be checking for?
	//if err := conn.requireServerVersion(verUploadFile); err != nil {
	//	return err
	//}

	source := args[0]
	destination := args[1]
	reader, err := openSourceFile(source)
	if err != nil {
		return err
	}
	return uploadFile(conn, reader, destination)
}

func openSourceFile(source string) (io.ReadCloser, error) {
	stat, err := os.Stat(source)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get source file stats for %s", source)
	}
	if stat.IsDir() {
		return nil, fmt.Errorf("source file %s is a directory, not a file", source)
	}
	return os.Open(source)
}

func uploadFile(conn *sqlConn, reader io.ReadCloser, destination string) error {
	return conn.ExecTxn(func(cn *sqlConn) error {
		stmt, err := cn.conn.Prepare(sql.CopyInFileStmt(destination, "crdb_internal", "file_upload"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for {
			send := make([]byte, chunkSize)
			n, err := reader.Read(send)
			if err == io.EOF {
				break
			}
			if err != nil {
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
