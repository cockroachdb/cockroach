// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package cli

import (
	"io"
	"os"

	"github.com/cockroachdb/cockroach/util/log"

	"github.com/spf13/cobra"
)

// A logCmd makes cockroach protobuf-encoded log files human-readable
// by outputting the specified files to standard out.
var logCmd = &cobra.Command{
	Use:   "log logfile1 [logfile2 ...]",
	Short: "make log files human-readable\n",
	Long: `
Cockroach log files are stored encoded as protobuf messages. The
log command parses the specified log files and outputs the result to
standard out. If the terminal supports colors, the output is colorized
unless --color=off is specified.
`,
	Run: runLog,
}

// runLog accesses creates a term log entry reader for each
// log file named in arguments.
func runLog(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		reader, err := log.GetLogReader(arg, true /* allowAbsolute */)
		if err != nil {
			log.Error(err)
			break
		}
		if _, err := io.Copy(os.Stdout, log.NewTermEntryReader(reader)); err != nil {
			log.Error(err)
			break
		}
		reader.Close()
	}
}

var logCmds = []*cobra.Command{
	logCmd,
}
