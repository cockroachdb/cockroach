// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/workload"

	// Register the relevant examples
	_ "github.com/cockroachdb/cockroach/pkg/workload/examples"
)

var genExamplesCmd = &cobra.Command{
	Use:   "example-data",
	Short: "generate example SQL code suitable for use with CockroachDB",
	Long: `This command generates example SQL code that shows various CockroachDB features and
is suitable to populate an example database for demonstration and education purposes.
`,
}

func init() {
	for _, meta := range workload.Registered() {
		gen := meta.New()
		genExampleCmd := &cobra.Command{
			Use:   meta.Name,
			Short: meta.Description,
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				runGenExamplesCmd(gen)
				return nil
			},
		}
		if f, ok := gen.(workload.Flagser); ok {
			genExampleCmd.Flags().AddFlagSet(f.Flags().FlagSet)
		}
		genExamplesCmd.AddCommand(genExampleCmd)
	}
}

func runGenExamplesCmd(gen workload.Generator) {
	w := os.Stdout

	meta := gen.Meta()
	fmt.Fprintf(w, "CREATE DATABASE IF NOT EXISTS %s;\n", meta.Name)
	fmt.Fprintf(w, "SET DATABASE=%s;\n", meta.Name)
	for _, table := range gen.Tables() {
		fmt.Fprintf(w, "DROP TABLE IF EXISTS \"%s\";\n", table.Name)
		fmt.Fprintf(w, "CREATE TABLE \"%s\" %s;\n", table.Name, table.Schema)
		for rowIdx := 0; rowIdx < table.InitialRows.NumBatches; rowIdx++ {
			for _, row := range table.InitialRows.Batch(rowIdx) {
				rowTuple := strings.Join(workload.StringTuple(row), `,`)
				fmt.Fprintf(w, "INSERT INTO \"%s\" VALUES (%s);\n", table.Name, rowTuple)
			}
		}
	}

	fmt.Fprintf(w, footerComment)
}

const footerComment = `--
--
-- If you can see this message, you probably want to redirect the output of
-- 'cockroach gen example-data' to a file, or pipe it as input to 'cockroach sql'.
`
