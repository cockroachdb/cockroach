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
// TODO(pmattis): ConditionalPut.

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

	"github.com/spf13/cobra"
)

const defaultMaxResults = 1000

var maxResults int64

var delRange bool

var osExit = os.Exit
var osStderr = os.Stderr

func makeDBClient() *client.DB {
	// TODO(pmattis): Initialize the user to something more
	// reasonable. Perhaps Context.Addr should be considered a URL.
	db, err := client.Open(Context.RequestScheme() +
		"://root@" + Context.Addr +
		"?certs=" + Context.Certs)
	if err != nil {
		fmt.Fprintf(osStderr, "failed to initialize KV client: %s\n", err)
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
	if !r.Exists() {
		fmt.Fprintf(osStderr, "%s not found\n", key)
		osExit(1)
		return
	}
	fmt.Printf("%q\n", r.Value)
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

	count := len(args) / 2

	keys := make([]string, 0, count)
	values := make([]string, 0, count)
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, unquoteArg(args[i], true /* disallow system keys */))
		values = append(values, unquoteArg(args[i+1], false))
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	var b client.Batch
	for i := 0; i < count; i++ {
		b.Put(keys[i], values[i])
	}
	if err := kvDB.Run(&b); err != nil {
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
Negative values need to be prefixed with -- to not get interpreted as
flags.
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
		fmt.Printf("%d\n", r.ValueInt())
	}
}

// A delCmd command sets the value for one or more keys.
var delCmd = &cobra.Command{
	Use:   "del [options] <key> [<key2>...]",
	Short: "deletes the value for a key",
	Long: `
Deletes the value for one or more keys. Only two keys are expected when
--range flag is specified. If the --range flag is specified then all the keys in
the range of <key1> and <key2> is deleted including <key1> and excluding <key2>.
`,
	Run: runDel,
}

func runDel(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Usage()
		return
	}

	count := len(args)

	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, unquoteArg(args[i], true /* disallow system keys */))
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}

	if delRange {
		if len(keys) != 2 {
			cmd.Usage()
			return
		}
		if err := kvDB.DelRange(keys[0], keys[1]); err != nil {
			fmt.Fprintf(osStderr, "del with --range failed: %s\n", err)
			osExit(1)
		}
		return
	}

	var b client.Batch
	for i := 0; i < count; i++ {
		b.Del(keys[i])
	}
	if err := kvDB.Run(&b); err != nil {
		fmt.Fprintf(osStderr, "delete failed: %s\n", err)
		osExit(1)
		return
	}
}

// A scanCmd command fetches the key/value pairs for a specified
// range.
var scanCmd = &cobra.Command{
	Use:   "scan [options] [<start-key> [<end-key>]]",
	Short: "scans a range of keys",
	Long: `
Fetches and display the key/value pairs for a range. If no <start-key>
is specified then all (non-system) key/value pairs are retrieved. If no
<end-key> is specified then all keys greater than or equal to <start-key>
are retrieved.
`,
	Run: runScan,
}

func runScan(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		cmd.Usage()
		return
	}
	startKey, endKey := initScanArgs(args)

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	rows, err := kvDB.Scan(startKey, endKey, maxResults)
	if err != nil {
		fmt.Fprintf(osStderr, "scan failed: %s\n", err)
		osExit(1)
		return
	}
	showResult(rows)
}

// A reverseScanCmd command fetches the key/value pairs for a specified
// range.
var reverseScanCmd = &cobra.Command{
	Use:   "revscan [options] [<start-key> [<end-key>]]",
	Short: "scans a range of keys in reverse order",
	Long: `
Fetches and display the key/value pairs for a range. If no <start-key>
is specified then all (non-system) key/value pairs are retrieved. If no
<end-key> is specified then all keys greater than or equal to <start-key>
are retrieved.
`,
	Run: runReverseScan,
}

func runReverseScan(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		cmd.Usage()
		return
	}
	startKey, endKey := initScanArgs(args)
	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	rows, err := kvDB.ReverseScan(startKey, endKey, maxResults)
	if err != nil {
		fmt.Fprintf(osStderr, "reverse scan failed: %s\n", err)
		osExit(1)
		return
	}
	showResult(rows)
}

func initScanArgs(args []string) (startKey, endKey proto.Key) {
	if len(args) >= 1 {
		startKey = proto.Key(unquoteArg(args[0], false))
	} else {
		// Start with the first key after the system key range.
		startKey = keys.SystemMax
	}
	if len(args) >= 2 {
		endKey = proto.Key(unquoteArg(args[1], false))
	} else {
		// Exclude table data keys by default. The user can explicitly request them
		// by passing \xff\xff for the end key.
		endKey = keys.TableDataPrefix
	}
	return startKey, endKey
}

func showResult(rows []client.KeyValue) {
	for _, row := range rows {
		if bytes.HasPrefix(row.Key, []byte{0}) {
			// TODO(pmattis): Pretty-print system keys.
			fmt.Printf("%s\n", row.Key)
			continue
		}

		key := proto.Key(row.Key)
		fmt.Printf("%s\t%q\n", key, row.Value)
	}
}

var kvCmds = []*cobra.Command{
	getCmd,
	putCmd,
	incCmd,
	delCmd,
	scanCmd,
	reverseScanCmd,
}

var kvCmd = &cobra.Command{
	Use:   "kv",
	Short: "get, put, increment, delete, scan and reverse scan key/value pairs",
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
