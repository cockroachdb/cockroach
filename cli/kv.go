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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/stop"

	"github.com/spf13/cobra"
)

func makeDBClient() (*client.DB, *stop.Stopper) {
	stopper := stop.NewStopper()
	context := &base.Context{
		User:       security.NodeUser,
		SSLCA:      cliContext.SSLCA,
		SSLCert:    cliContext.SSLCert,
		SSLCertKey: cliContext.SSLCertKey,
		Insecure:   cliContext.Insecure,
	}
	sender, err := client.NewSender(rpc.NewContext(context, nil, stopper), cliContext.Addr)
	if err != nil {
		stopper.Stop()
		panicf("failed to initialize KV client: %s", err)
	}
	return client.NewDB(sender), stopper
}

// unquoteArg unquotes the provided argument using Go double-quoted
// string literal rules.
func unquoteArg(arg string, disallowSystem bool) string {
	s, err := strconv.Unquote(`"` + arg + `"`)
	if err != nil {
		panicf("invalid argument %q: %s", arg, err)
	}
	if disallowSystem && strings.HasPrefix(s, "\x00") {
		panicf("cannot specify system key %q", s)
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
	SilenceUsage: true,
	RunE:         panicGuard(runGet),
}

func runGet(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}
	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	key := roachpb.Key(unquoteArg(args[0], false))
	r, err := kvDB.Get(key)
	if err != nil {
		panicf("get failed: %s", err)
	}
	if !r.Exists() {
		panicf("%s not found", key)
	}
	fmt.Printf("%s\n", r.PrettyValue())
}

// A putCmd sets the value for one or more keys.
var putCmd = &cobra.Command{
	Use:   "put [options] <key> <value> [<key2> <value2>...]",
	Short: "sets the value for one or more keys",
	Long: `
Sets the value for one or more keys. Keys and values must be provided
in pairs on the command line.

WARNING: Modifying system or table keys can corrupt your cluster.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runPut),
}

func runPut(cmd *cobra.Command, args []string) {
	if len(args) == 0 || len(args)%2 == 1 {
		mustUsage(cmd)
		return
	}

	var b client.Batch
	for i := 0; i < len(args); i += 2 {
		b.Put(
			unquoteArg(args[i], true /* disallow system keys */),
			unquoteArg(args[i+1], false),
		)
	}

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	if err := kvDB.Run(&b); err != nil {
		panicf("put failed: %s", err)
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

WARNING: Modifying system or table keys can corrupt your cluster.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runCPut),
}

func runCPut(cmd *cobra.Command, args []string) {
	if len(args) != 2 && len(args) != 3 {
		mustUsage(cmd)
		return
	}

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	key := unquoteArg(args[0], true /* disallow system keys */)
	value := unquoteArg(args[1], false)
	var pErr *roachpb.Error
	if len(args) == 3 {
		pErr = kvDB.CPut(key, value, unquoteArg(args[2], false))
	} else {
		pErr = kvDB.CPut(key, value, nil)
	}

	if pErr != nil {
		panicf("conditional put failed: %s", pErr)
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

WARNING: Modifying system or table keys can corrupt your cluster.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runInc),
}

func runInc(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		mustUsage(cmd)
		return
	}

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	amount := 1
	if len(args) >= 2 {
		var err error
		if amount, err = strconv.Atoi(args[1]); err != nil {
			panicf("invalid increment: %s: %s", args[1], err)
		}
	}

	key := roachpb.Key(unquoteArg(args[0], true /* disallow system keys */))
	if r, err := kvDB.Inc(key, int64(amount)); err != nil {
		panicf("increment failed: %s", err)
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

WARNING: Modifying system or table keys can corrupt your cluster.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runDel),
}

func runDel(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		mustUsage(cmd)
		return
	}

	var b client.Batch
	for _, arg := range args {
		b.Del(unquoteArg(arg, true /* disallow system keys */))
	}

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	if err := kvDB.Run(&b); err != nil {
		panicf("delete failed: %s", err)
	}
}

// A delRangeCmd deletes the values for a range of keys.
// [startKey, endKey).
var delRangeCmd = &cobra.Command{
	Use:   "delrange [options] <startKey> <endKey>",
	Short: "deletes the values for a range of keys",
	Long: `
Deletes the values for the range of keys [startKey, endKey).

WARNING: Modifying system or table keys can corrupt your cluster.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runDelRange),
}

func runDelRange(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		mustUsage(cmd)
		return
	}

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	if err := kvDB.DelRange(
		unquoteArg(args[0], true /* disallow system keys */),
		unquoteArg(args[1], true /* disallow system keys */),
	); err != nil {
		panicf("delrange failed: %s", err)
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
	SilenceUsage: true,
	RunE:         panicGuard(runScan),
}

func runScan(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		mustUsage(cmd)
		return
	}
	startKey, endKey := initScanArgs(args)

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	rows, err := kvDB.Scan(startKey, endKey, maxResults)
	if err != nil {
		panicf("scan failed: %s", err)
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
	SilenceUsage: true,
	RunE:         panicGuard(runReverseScan),
}

func runReverseScan(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		mustUsage(cmd)
		return
	}
	startKey, endKey := initScanArgs(args)
	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	rows, err := kvDB.ReverseScan(startKey, endKey, maxResults)
	if err != nil {
		panicf("reverse scan failed: %s", err)
	}
	showResult(rows)
}

func initScanArgs(args []string) (startKey, endKey roachpb.Key) {
	if len(args) >= 1 {
		startKey = roachpb.Key(unquoteArg(args[0], false))
	} else {
		// Start with the first key after the system key range.
		startKey = keys.UserDataSpan.Key
	}
	if len(args) >= 2 {
		endKey = roachpb.Key(unquoteArg(args[1], false))
	} else {
		// Exclude table data keys by default. The user can explicitly request them
		// by passing \xff\xff for the end key.
		endKey = keys.UserDataSpan.EndKey
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

		key := roachpb.Key(row.Key)
		fmt.Printf("%s\t%s\n", key, row.PrettyValue())
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
	Short: "get, put, delete, and scan key/value pairs",
	Long: `
Special characters in keys or values should be specified according to
the double-quoted Go string literal rules (see
https://golang.org/ref/spec#String_literals).
`,
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}

func init() {
	kvCmd.AddCommand(kvCmds...)
}
