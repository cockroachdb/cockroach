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
// Author: Peter Mattis (peter@cockroachlabs.com)
//
// TODO(pmattis): ConditionalPut, DeleteRange.

package cli

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"

	"github.com/spf13/cobra"
)

var osExit = os.Exit
var osStderr = os.Stderr

func makeDBClient() *client.DB {
	// TODO(pmattis): Initialize the user to something more
	// reasonable. Perhaps Context.Addr should be considered a URL.
	db, err := client.Open(Context.RequestScheme() +
		"://root@" + util.EnsureHost(Context.Addr) +
		"?certs=" + Context.Certs)
	if err != nil {
		fmt.Fprintf(osStderr, "failed to initialize KV client: %s", err)
		osExit(1)
	}
	return db
}

// unquoteArg unquotes the provided argument using Go double-quoted
// string literal rules.
func unquoteArg(arg string, disallowSystem bool) string {
	s, err := strconv.Unquote(`"` + arg + `"`)
	if err != nil {
		fmt.Fprintf(osStderr, "invalid argument %q: %s\n", arg, err)
		osExit(1)
	}
	if disallowSystem && strings.HasPrefix(s, "\x00") {
		fmt.Fprintf(osStderr, "cannot specify system key %q\n", s)
		osExit(1)
	}
	return s
}

// A getCmd command gets the value for the specified key.
var getCmd = &cobra.Command{
	Use:   "get [options] <key>",
	Short: "gets the value for a key",
	Long: `
Fetches and displays the value for <key>.
`,
	Run: runGet,
}

func runGet(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	key := proto.Key(unquoteArg(args[0], false))
	r, err := kvDB.Get(key)
	if err != nil {
		fmt.Fprintf(osStderr, "get failed: %s\n", err)
		osExit(1)
		return
	}
	if !r.Rows[0].Exists() {
		fmt.Fprintf(osStderr, "%s not found\n", key)
		osExit(1)
		return
	}
	if i, ok := r.Rows[0].Value.(*int64); ok {
		fmt.Printf("%d\n", *i)
	} else {
		fmt.Printf("%s\n", r.Rows[0].Value)
	}
}

// A putCmd command sets the value for one or more keys.
var putCmd = &cobra.Command{
	Use:   "put [options] <key> <value> [<key2> <value2>...]",
	Short: "sets the value for a key",
	Long: `
Sets the value for one or more keys. Keys and values must be provided
in pairs on the command line.
`,
	Run: runPut,
}

func runPut(cmd *cobra.Command, args []string) {
	if len(args) == 0 || len(args)%2 == 1 {
		cmd.Usage()
		return
	}

	// Do not allow system keys to be put.
	var unquoted []string
	for i := 0; i < len(args); i++ {
		unquoted = append(unquoted, unquoteArg(args[i], i%2 == 0 /* disallow system keys */))
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	b := &client.Batch{}
	for i := 0; i < len(unquoted); i += 2 {
		b.Put(unquoted[i], unquoted[i+1])
	}
	if err := kvDB.Run(b); err != nil {
		fmt.Fprintf(osStderr, "put failed: %s\n", err)
		osExit(1)
		return
	}
}

// A incCmd command increments the value for one or more keys.
var incCmd = &cobra.Command{
	Use:   "inc [options] <key> [<amount>]",
	Short: "increments the value for a key",
	Long: `
Increments the value for a key. The increment amount defaults to 1 if
not specified. Displays the incremented value upon success.
`,
	Run: runInc,
}

func runInc(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		cmd.Usage()
		return
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	amount := 1
	if len(args) >= 2 {
		var err error
		if amount, err = strconv.Atoi(args[1]); err != nil {
			fmt.Fprintf(osStderr, "invalid increment: %s: %s\n", args[1], err)
			osExit(1)
			return
		}
	}

	key := proto.Key(unquoteArg(args[0], true /* disallow system keys */))
	if r, err := kvDB.Inc(key, int64(amount)); err != nil {
		fmt.Fprintf(osStderr, "increment failed: %s\n", err)
		osExit(1)
	} else {
		fmt.Printf("%d\n", r.Rows[0].ValueInt())
	}
}

// A delCmd command sets the value for one or more keys.
var delCmd = &cobra.Command{
	Use:   "del [options] <key> [<key2>...]",
	Short: "deletes the value for a key",
	Long: `
Deletes the value for one or more keys.
`,
	Run: runDel,
}

func runDel(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Usage()
		return
	}

	// Do not allow system keys to be deleted.
	var unquoted []string
	for i := 0; i < len(args); i++ {
		unquoted = append(unquoted, unquoteArg(args[0], i%2 == 0 /* disallow system keys */))
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	b := &client.Batch{}
	for i := 0; i < len(unquoted); i++ {
		b.Del(unquoted[i])
	}
	if err := kvDB.Run(b); err != nil {
		fmt.Fprintf(osStderr, "delete failed: %s\n", err)
		osExit(1)
		return
	}
}

// A scanCmd command fetches the key/value pairs for a specified
// range.
var scanCmd = &cobra.Command{
	Use:   "scan [options] [<start-key> [<end-key>]]",
	Short: "scans a range of keys\n",
	Long: `
Fetches and display the key/value pairs for a range. If no <start-key>
is specified then all (non-system) key/value pairs are retrieved. If no
<end-key> is specified then all keys greater than or equal to <start-key>
are retrieved.

Caveat: Currently only retrieves up to 1000 keys.
`,
	Run: runScan,
}

func runScan(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		cmd.Usage()
		return
	}
	var (
		startKey proto.Key
		endKey   proto.Key
	)
	if len(args) >= 1 {
		startKey = proto.Key(unquoteArg(args[0], false))
	} else {
		// Start with the first key after the system key range.
		startKey = keys.SystemMax
	}
	if len(args) >= 2 {
		endKey = proto.Key(unquoteArg(args[1], false))
	} else {
		endKey = proto.KeyMax
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	// TODO(pmattis): Add a flag for the number of results to scan.
	r, err := kvDB.Scan(startKey, endKey, 1000)
	if err != nil {
		fmt.Fprintf(osStderr, "scan failed: %s\n", err)
		osExit(1)
		return
	}
	for _, row := range r.Rows {
		if bytes.HasPrefix(row.Key, []byte{0}) {
			// TODO(pmattis): Pretty-print system keys.
			fmt.Printf("%s\n", row.Key)
			continue
		}

		key := proto.Key(row.Key)
		if i, ok := row.Value.(*int64); ok {
			fmt.Printf("%s\t%d\n", key, *i)
		} else {
			fmt.Printf("%s\t%q\n", key, row.Value)
		}
	}
}

var kvCmds = []*cobra.Command{
	getCmd,
	putCmd,
	incCmd,
	delCmd,
	scanCmd,
}

var kvCmd = &cobra.Command{
	Use:   "kv",
	Short: "get, put, increment, delete and scan key/value pairs",
	Long: `
Special characters in keys or values should be specified according to
the double-quoted Go string literal rules (see
https://golang.org/ref/spec#String_literals).
`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

func init() {
	kvCmd.AddCommand(kvCmds...)
}
