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

package cli

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"

	"github.com/spf13/cobra"
)

func makeDBClient() *client.DB {
	// TODO(marc): KV endpoints are now restricted to node users.
	// This should probably be made more explicit.
	db, err := client.Open(fmt.Sprintf("%s://%s@%s?certs=%s",
		context.RequestScheme(),
		security.NodeUser,
		context.Addr,
		context.Certs))
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

// A getCmd gets the value for the specified key.
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

// A putCmd sets the value for one or more keys.
var putCmd = &cobra.Command{
	Use:   "put [options] <key> <value> [<key2> <value2>...]",
	Short: "sets the value for one or more keys",
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

	var b client.Batch
	for i := 0; i < len(args); i += 2 {
		b.Put(
			unquoteArg(args[i], true /* disallow system keys */),
			unquoteArg(args[i+1], false),
		)
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}

	if err := kvDB.Run(&b); err != nil {
		fmt.Fprintf(osStderr, "put failed: %s\n", err)
		osExit(1)
		return
	}
}

// A cPutCmd conditionally sets a value for a key.
var cPutCmd = &cobra.Command{
	Use:   "cput [options] <key> <value> [<expValue>]",
	Short: "conditionally sets a value for a key",
	Long: `
Conditionally sets a value for a key if the existing value is equal
to expValue. To conditionally set a value only if there is no existing entry
pass nil for expValue. The expValue defaults to 1 if not specified.
`,
	Run: runCPut,
}

func runCPut(cmd *cobra.Command, args []string) {
	if len(args) != 2 && len(args) != 3 {
		cmd.Usage()
		return
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}

	key := unquoteArg(args[0], true /* disallow system keys */)
	value := unquoteArg(args[1], false)
	var err error
	if len(args) == 3 {
		err = kvDB.CPut(key, value, unquoteArg(args[2], false))
	} else {
		err = kvDB.CPut(key, value, nil)
	}

	if err != nil {
		fmt.Fprintf(osStderr, "conditionally put failed: %s\n", err)
		osExit(1)
	}
}

// A incCmd command increments the value for a key.
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

// A delCmd deletes the values of one or more keys.
var delCmd = &cobra.Command{
	Use:   "del [options] <key> [<key2>...]",
	Short: "deletes the values of one or more keys",
	Long: `
Deletes the values of one or more keys.
`,
	Run: runDel,
}

func runDel(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Usage()
		return
	}

	var b client.Batch
	for _, arg := range args {
		b.Del(unquoteArg(arg, true /* disallow system keys */))
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}

	if err := kvDB.Run(&b); err != nil {
		fmt.Fprintf(osStderr, "delete failed: %s\n", err)
		osExit(1)
		return
	}
}

// A delRangeCmd deletes the values for a range of keys.
// [startKey, endKey).
var delRangeCmd = &cobra.Command{
	Use:   "delrange [options] <startKey> <endKey>",
	Short: "deletes the values for a range of keys",
	Long: `
Deletes the values for the range of keys [startKey, endKey).
`,
	Run: runDelRange,
}

func runDelRange(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Usage()
		return
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}

	if err := kvDB.DelRange(
		unquoteArg(args[0], true /* disallow system keys */),
		unquoteArg(args[1], true /* disallow system keys */),
	); err != nil {
		fmt.Fprintf(osStderr, "delrange failed: %s\n", err)
		osExit(1)
	}
}

// A scanCmd fetches the key/value pairs for a specified
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

// A reverseScanCmd fetches the key/value pairs for a specified
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
		startKey = keys.UserDataSpan.Start
	}
	if len(args) >= 2 {
		endKey = proto.Key(unquoteArg(args[1], false))
	} else {
		// Exclude table data keys by default. The user can explicitly request them
		// by passing \xff\xff for the end key.
		endKey = keys.UserDataSpan.End
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
	fmt.Printf("%d result(s)\n", len(rows))
}

var kvCmds = []*cobra.Command{
	getCmd,
	putCmd,
	cPutCmd,
	incCmd,
	delCmd,
	delRangeCmd,
	scanCmd,
	reverseScanCmd,
}

var kvCmd = &cobra.Command{
	Use:   "kv",
	Short: "get, put, conditional put, increment, delete, scan, and reverse scan key/value pairs",
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
