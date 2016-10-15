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

	"golang.org/x/net/context"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func makeDBClient() (*client.DB, *stop.Stopper, error) {
	stopper := stop.NewStopper()
	cfg := &base.Config{
		User:       security.NodeUser,
		SSLCA:      baseCfg.SSLCA,
		SSLCert:    baseCfg.SSLCert,
		SSLCertKey: baseCfg.SSLCertKey,
		Insecure:   baseCfg.Insecure,
	}

	sender, err := client.NewSender(
		rpc.NewContext(log.AmbientContext{}, cfg, nil, stopper),
		baseCfg.Addr,
	)
	if err != nil {
		stopper.Stop()
		return nil, nil, errors.Wrap(err, "failed to initialize KV client")
	}
	return client.NewDB(sender), stopper, nil
}

// unquoteArg unquotes the provided argument using Go double-quoted
// string literal rules.
func unquoteArg(arg string, disallowSystem bool) (string, error) {
	s, err := strconv.Unquote(`"` + arg + `"`)
	if err != nil {
		return "", errors.Wrapf(err, "invalid argument %q", arg)
	}
	if disallowSystem && strings.HasPrefix(s, "\x00") {
		return "", errors.Errorf("cannot specify system key %q", s)
	}
	return s, nil
}

// A getCmd gets the value for the specified key.
var getCmd = &cobra.Command{
	Use:   "get [options] <key>",
	Short: "gets the value for a key",
	Long: `
Fetches and displays the value for <key>.
`,
	SilenceUsage: true,
	RunE:         maybeDecorateGRPCError(runGet),
}

func runGet(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}
	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	unquoted, err := unquoteArg(args[0], false)
	if err != nil {
		return err
	}
	key := roachpb.Key(unquoted)
	r, err := kvDB.Get(context.Background(), key)
	if err != nil {
		return err
	}
	if !r.Exists() {
		return errors.Errorf("%s not found", key)
	}
	fmt.Printf("%s\n", r.PrettyValue())
	return nil
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
	RunE:         maybeDecorateGRPCError(runPut),
}

func runPut(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || len(args)%2 == 1 {
		return usageAndError(cmd)
	}

	b := &client.Batch{}
	for i := 0; i < len(args); i += 2 {
		k, err := unquoteArg(args[i], true /* disallow system keys */)
		if err != nil {
			return err
		}
		v, err := unquoteArg(args[i+1], false)
		if err != nil {
			return err
		}
		b.Put(k, v)
	}

	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	return kvDB.Run(context.Background(), b)
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
	RunE:         maybeDecorateGRPCError(runCPut),
}

func runCPut(cmd *cobra.Command, args []string) error {
	if len(args) != 2 && len(args) != 3 {
		return usageAndError(cmd)
	}

	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	key, err := unquoteArg(args[0], true /* disallow system keys */)
	if err != nil {
		return err
	}
	value, err := unquoteArg(args[1], false)
	if err != nil {
		return err
	}
	if len(args) == 3 {
		existing, err := unquoteArg(args[2], false)
		if err != nil {
			return err
		}
		return kvDB.CPut(context.Background(), key, value, existing)
	}
	return kvDB.CPut(context.Background(), key, value, nil)
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
	RunE:         maybeDecorateGRPCError(runInc),
}

func runInc(cmd *cobra.Command, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return usageAndError(cmd)
	}

	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	amount := 1
	if len(args) == 2 {
		var err error
		if amount, err = strconv.Atoi(args[1]); err != nil {
			return errors.Wrap(err, "invalid increment")
		}
	}

	unquoted, err := unquoteArg(args[0], true /* disallow system keys */)
	if err != nil {
		return err
	}
	key := roachpb.Key(unquoted)
	r, err := kvDB.Inc(context.TODO(), key, int64(amount))
	if err != nil {
		return errors.Wrap(err, "increment failed")
	}
	fmt.Printf("%d\n", r.ValueInt())
	return nil
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
	RunE:         maybeDecorateGRPCError(runDel),
}

func runDel(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return usageAndError(cmd)
	}

	b := &client.Batch{}
	for _, arg := range args {
		key, err := unquoteArg(arg, true /* disallow system keys */)
		if err != nil {
			return err
		}
		b.Del(key)
	}

	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	return kvDB.Run(context.Background(), b)
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
	RunE:         maybeDecorateGRPCError(runDelRange),
}

func runDelRange(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return usageAndError(cmd)
	}

	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	uqFrom, err := unquoteArg(args[0], true /* disallow system keys */)
	if err != nil {
		return err
	}
	uqTo, err := unquoteArg(args[1], true /* disallow system keys */)
	if err != nil {
		return err
	}

	return kvDB.DelRange(
		context.TODO(), roachpb.Key(uqFrom), roachpb.Key(uqTo),
	)
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
	RunE:         maybeDecorateGRPCError(runScan),
}

func runScan(cmd *cobra.Command, args []string) error {
	if len(args) > 2 {
		return usageAndError(cmd)
	}
	startKey, endKey, err := initScanArgs(args)
	if err != nil {
		return err
	}

	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	rows, err := kvDB.Scan(context.Background(), startKey, endKey, maxResults)
	if err != nil {
		return err
	}
	showResult(rows)
	return nil
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
	RunE:         maybeDecorateGRPCError(runReverseScan),
}

func runReverseScan(cmd *cobra.Command, args []string) error {
	if len(args) > 2 {
		return usageAndError(cmd)
	}
	startKey, endKey, err := initScanArgs(args)
	if err != nil {
		return err
	}
	kvDB, stopper, err := makeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	rows, err := kvDB.ReverseScan(context.TODO(), startKey, endKey, maxResults)
	if err != nil {
		return err
	}
	showResult(rows)
	return nil
}

func initScanArgs(args []string) (startKey, endKey roachpb.Key, _ error) {
	if len(args) >= 1 {
		unquoted, err := unquoteArg(args[0], false)
		if err != nil {
			return nil, nil, errors.Wrap(err, "invalid start key")
		}
		startKey = roachpb.Key(unquoted)
	} else {
		// Start with the first key after the system key range.
		startKey = keys.UserDataSpan.Key
	}
	if len(args) >= 2 {
		unquoted, err := unquoteArg(args[1], false)
		if err != nil {
			return nil, nil, errors.Wrap(err, "invalid end key")
		}
		endKey = roachpb.Key(unquoted)
	} else {
		// Exclude table data keys by default. The user can explicitly request them
		// by passing \xff\xff for the end key.
		endKey = keys.UserDataSpan.EndKey
	}
	if bytes.Compare(startKey, endKey) >= 0 {
		return nil, nil, errors.New("start key must be smaller than end key")
	}
	return startKey, endKey, nil
}

func showResult(rows []client.KeyValue) {
	for _, row := range rows {
		if bytes.HasPrefix(row.Key, []byte{0}) {
			// TODO(pmattis): Pretty-print system keys.
			fmt.Printf("%s\n", row.Key)
			continue
		}
		fmt.Printf("%s\t%s\n", row.Key, row.PrettyValue())
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
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

func init() {
	kvCmd.AddCommand(kvCmds...)
}
