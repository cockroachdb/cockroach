// Copyright 2021 The Cockroach Authors.
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
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// dumpCmd dumps SQL tables.
var dumpCmd = &cobra.Command{
	Use:   "dump [options] <database>",
	Short: "dump sql tables in a database\n",
	Long: `
Dump SQL tables of a cockroach database.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDumpSchema),
	Deprecated: `cockroach dump only supports --dump-mode=schema in v21.1. 
Please use SHOW CREATE ALL TABLES moving forward.
cockroach dump will be completely removed in v21.2.
For details, see: https://github.com/cockroachdb/cockroach/issues/54040`,
}

func runDumpSchema(cmd *cobra.Command, args []string) error {
	// Version gate to v21.1.
	sqlConn, err := makeSQLClient("cockroach dump", useSystemDb)
	if err != nil {
		return err
	}
	defer sqlConn.Close()

	if dumpCtx.dumpMode != dumpSchemaOnly {
		return errors.Newf("only cockroach dump --dump-mode=schema is supported")
	}

	if len(args) == 0 {
		return fmt.Errorf("must specify database")
	}

	dbName := args[0]

	if _, err := sqlConn.Query(fmt.Sprintf(`USE %s`, dbName), nil); err != nil {
		return err
	}
	rows, err := sqlConn.Query("SHOW CREATE ALL TABLES", nil)
	if err != nil {
		return err
	}
	w := os.Stdout
	iter := newRowIter(rows, true /* showMoreChars */)
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if _, err := w.Write([]byte(row[0])); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}
