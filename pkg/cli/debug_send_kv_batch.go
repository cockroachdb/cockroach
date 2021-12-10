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
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugSendKVBatchCmd = &cobra.Command{
	Use:   "send-kv-batch <jsonfile>",
	Short: "sends a KV BatchRequest via the connected node",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runSendKVBatch,
	Long: `
Submits a JSON-encoded roachpb.BatchRequest (file or stdin) to a node which
proxies it into the KV API, and outputs the JSON-encoded roachpb.BatchResponse.
Requires the admin role. The request is logged to the system event log.

This command can modify internal system state. Incorrect use can cause severe
irreversible damage including permanent data loss.

For more information on requests, see roachpb/api.proto. Unknown or invalid
fields will error. Binary fields ([]byte) are base64-encoded. Requests spanning
multiple ranges are wrapped in a transaction.

The following BatchRequest example sets the key "foo" to the value "bar", and
reads the value back (as a base64-encoded roachpb.Value.RawBytes with checksum):

{"requests": [
	{"put": {
		"header": {"key": "Zm9v"},
		"value": {"raw_bytes": "DMEB5ANiYXI="}
	}},
	{"get": {
		"header": {"key": "Zm9v"}
	}}
]}

This would yield the following response:

{"responses": [
	{"put": {
		"header": {}
	}},
	{"get": {
		"header": {"numKeys": "1", "numBytes": "8"},
		"value": {"rawBytes": "DMEB5ANiYXI=", "timestamp": {"wallTime": "1636898055143103168"}}
	}}
]}

To generate JSON requests with Go (see also debug_send_kv_batch_test.go):

func TestSendKVBatchExample(t *testing.T) {
	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(roachpb.Key("foo"), roachpb.MakeValueFromString("bar")))
	ba.Add(roachpb.NewGet(roachpb.Key("foo"), false /* forUpdate */))

	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	require.NoError(t, err)

	fmt.Println(string(jsonProto))
}
	`,
}

func runSendKVBatch(cmd *cobra.Command, args []string) error {
	jsonpb := protoutil.JSONPb{Indent: "  "}

	// Parse and validate BatchRequest JSON.
	var baJSON []byte
	var err error
	if len(args) > 0 {
		baJSON, err = ioutil.ReadFile(args[0])
	} else {
		baJSON, err = ioutil.ReadAll(os.Stdin)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to read input")
	}

	var ba roachpb.BatchRequest
	if err := jsonpb.Unmarshal(baJSON, &ba); err != nil {
		return errors.Wrap(err, "invalid JSON")
	}

	if len(ba.Requests) == 0 {
		return errors.New("BatchRequest contains no requests")
	}

	// Send BatchRequest.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to the node")
	}
	defer finish()
	br, err := serverpb.NewAdminClient(conn).SendKVBatch(ctx, &ba)
	if err != nil {
		return errors.Wrap(err, "request failed")
	}

	// Display BatchResponse.
	brJSON, err := jsonpb.Marshal(br)
	if err != nil {
		return errors.Wrap(err, "failed to format BatchResponse as JSON")
	}
	fmt.Printf("%s\n", brJSON)
	if br.Error != nil {
		// If there's a KV-level error, it will be in br.Error. We made sure to
		// marshal it above (so that we can get the structured error out if needed)
		// but we still want to fail the invocation (which in particular renders
		// the error).
		return br.Error.GoError()
	}

	return nil
}
