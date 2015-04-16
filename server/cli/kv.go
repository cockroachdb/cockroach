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
// Author: Peter Mattis (peter.mattis@gmail.com)
//
// TODO(pmattis): ConditionalPut, DeleteRange.

package cli

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"

	commander "code.google.com/p/go-commander"
)

var osExit = os.Exit
var osStderr = os.Stderr

func makeKVClient() *client.KV {
	transport := &http.Transport{
		TLSClientConfig: rpc.LoadInsecureTLSConfig().Config(),
	}
	kv := client.NewKV(nil, client.NewHTTPSender(
		util.EnsureHost(Context.Addr), transport))
	// TODO(pmattis): Initialize this to something more reasonable
	kv.User = "root"
	return kv
}

// A getCmd command gets the value for the specified key.
var getCmd = &commander.Command{
	UsageLine: "get [options] <key>",
	Short:     "gets the value for a key",
	Long: `
Fetches and display the value for <key>.
`,
	Run:  runGet,
	Flag: *flag.CommandLine,
}

func runGet(cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	kv := makeKVClient()
	key := proto.Key(args[0])
	resp := &proto.GetResponse{}
	if err := kv.Run(client.GetCall(key, resp)); err != nil {
		fmt.Fprintf(osStderr, "get failed: %s\n", err)
		osExit(1)
		return
	}
	if resp.Value == nil {
		fmt.Fprintf(osStderr, "%s not found\n", key)
		osExit(1)
		return
	}
	if resp.Value.Integer != nil {
		fmt.Printf("%d\n", *resp.Value.Integer)
	} else {
		fmt.Printf("%s\n", resp.Value.Bytes)
	}
}

// A putCmd command sets the value for one or more keys.
var putCmd = &commander.Command{
	UsageLine: "put [options] <key> <value> [<key2> <value2>...]",
	Short:     "sets the value for a key",
	Long: `
Sets the value for one or more keys. Keys and values must be provided
in pairs on the command line. All of the key/value pairs are set within
a transaction.
`,
	Run:  runPut,
	Flag: *flag.CommandLine,
}

func runPut(cmd *commander.Command, args []string) {
	if len(args) == 0 || len(args)%2 == 1 {
		cmd.Usage()
		return
	}

	// TODO(pmattis): Investigate allowing keys/values to be quoted and
	// to unquote them using strconv.Unquote.

	// Do not allow system keys to be put.
	for i := 0; i < len(args); i += 2 {
		if strings.HasPrefix(args[i], "\x00") {
			fmt.Fprintf(osStderr, "unable to put system key: %s\n", proto.Key(args[i]))
			osExit(1)
			return
		}
	}

	kv := makeKVClient()
	opts := &client.TransactionOptions{Name: "test", Isolation: proto.SERIALIZABLE}
	err := kv.RunTransaction(opts, func(txn *client.KV) error {
		for i := 0; i < len(args); i += 2 {
			key := proto.Key(args[i])
			value := []byte(args[i+1])
			txn.Prepare(client.PutCall(key, value, nil))
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(osStderr, "put failed: %s\n", err)
		osExit(1)
		return
	}
}

// A incCmd command increments the value for one or more keys.
var incCmd = &commander.Command{
	UsageLine: "inc [options] <key> [<amount>]",
	Short:     "increments the value for a key",
	Long: `
Increments the value for a key. The increment amount defaults to 1 if
not specified. Displays the incremented value upon success.
`,
	Run:  runInc,
	Flag: *flag.CommandLine,
}

func runInc(cmd *commander.Command, args []string) {
	if len(args) > 2 {
		cmd.Usage()
		return
	}

	if strings.HasPrefix(args[0], "\x00") {
		fmt.Fprintf(osStderr, "unable to increment system key: %s\n", proto.Key(args[0]))
		osExit(1)
		return
	}

	kv := makeKVClient()
	amount := 1
	if len(args) >= 2 {
		var err error
		if amount, err = strconv.Atoi(args[1]); err != nil {
			fmt.Fprintf(osStderr, "invalid increment: %s: %s\n", args[1], err)
			osExit(1)
			return
		}
	}

	key := proto.Key(args[0])
	resp := &proto.IncrementResponse{}
	if err := kv.Run(client.IncrementCall(key, int64(amount), resp)); err != nil {
		fmt.Fprintf(osStderr, "increment failed: %s\n", err)
		osExit(1)
		return
	}
	fmt.Printf("%d\n", resp.NewValue)
}

// A delCmd command sets the value for one or more keys.
var delCmd = &commander.Command{
	UsageLine: "del [options] <key> [<key2>...]",
	Short:     "deletes the value for a key",
	Long: `
Deletes the value for one or more keys.
`,
	Run:  runDel,
	Flag: *flag.CommandLine,
}

func runDel(cmd *commander.Command, args []string) {
	if len(args) == 0 {
		cmd.Usage()
		return
	}

	// Do not allow system keys to be deleted.
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "\x00") {
			fmt.Fprintf(osStderr, "unable to delete system key: %s\n", proto.Key(args[i]))
			osExit(1)
			return
		}
	}

	kv := makeKVClient()
	opts := &client.TransactionOptions{Name: "test", Isolation: proto.SERIALIZABLE}
	err := kv.RunTransaction(opts, func(txn *client.KV) error {
		for i := 0; i < len(args); i++ {
			key := proto.Key(args[i])
			txn.Prepare(client.DeleteCall(key, nil))
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(osStderr, "delete failed: %s\n", err)
		osExit(1)
		return
	}
}

// A scanCmd command fetches the key/value pairs for a specified
// range.
var scanCmd = &commander.Command{
	UsageLine: "scan [options] [<start-key> [<end-key>]]",
	Short:     "scans a range of keys\n",
	Long: `
Fetches and display the key/value pairs for a range. If no <start-key>
is specified then all (non-system) key/value pairs are retrieved. If no
<end-key> is specified then all keys greater than or equal to <start-key>
are retrieved.

Caveat: Currently only retrieves up to 1000 keys.
`,
	Run:  runScan,
	Flag: *flag.CommandLine,
}

func runScan(cmd *commander.Command, args []string) {
	if len(args) > 2 {
		cmd.Usage()
		return
	}
	var (
		startKey proto.Key
		endKey   proto.Key
	)
	if len(args) >= 1 {
		startKey = proto.Key(args[0])
	} else {
		// Start with the first key after the system key range.
		//
		// TODO(pmattis): Add a flag for retrieving system keys as well.
		startKey = engine.KeySystemMax
	}
	if len(args) >= 2 {
		endKey = proto.Key(args[1])
	} else {
		endKey = proto.KeyMax
	}

	kv := makeKVClient()
	// TODO(pmattis): Add a flag for the number of results to scan.
	resp := &proto.ScanResponse{}
	if err := kv.Run(client.ScanCall(startKey, endKey, 1000, resp)); err != nil {
		fmt.Fprintf(osStderr, "scan failed: %s\n", err)
		osExit(1)
		return
	}

	for _, r := range resp.Rows {
		if bytes.HasPrefix(r.Key, []byte{0}) {
			// TODO(pmattis): Pretty-print system keys.
			fmt.Printf("%s\n", r.Key)
			continue
		}

		if r.Value.Integer != nil {
			fmt.Printf("%s\t%d\n", r.Key, *r.Value.Integer)
		} else {
			fmt.Printf("%s\t%s\n", r.Key, r.Value.Bytes)
		}
	}
}
