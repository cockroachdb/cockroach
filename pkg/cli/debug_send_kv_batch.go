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
Submits a JSON-formatted roachpb.BatchRequest, given as a file or via stdin,
to the specified node which will proxy it into KV. Outputs the JSON-formatted
roachpb.BatchResponse, or an error on failure. The user must have the admin
role, and the request will be logged to the system event log.

This command can access and mutate internal system state. Incorrect use can
cause severe irreversible damage including permanent data loss. Users must
take extreme caution.

For more information on available requests and fields, see roachpb/api.proto.
If an unknown or invalid field is specified, the command returns an error.
Binary fields (i.e. []byte) are given and returned as base64. The request
will be wrapped in a transaction if it spans multiple ranges.

For example, the following will submit the BatchRequest specified in
batch.json to node01 using the root certificate from the certs/ directory, and
pipe the JSON output to jq for better formatting:

cockroach debug send-kv-batch --host node01 --certs-dir certs/ batch.json | jq

The following is an example BatchRequest which sets the key "foo" to the value
"bar", and then reads the value back (as a base64-encoded
roachpb.Value.RawBytes including a checksum):

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

{
	"header": {
		"Timestamp": {"wallTime": "1636898055147324643"},
		"now": {"wallTime": "1636898055149924844"}
	},
	"responses": [
		{"put": {
			"header": {}
		}},
		{"get": {
			"header": {"numKeys": "1", "numBytes": "8"},
			"value": {"rawBytes": "DMEB5ANiYXI=", "timestamp": {"wallTime": "1636898055143103168"}}
		}}
	]
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

	return nil
}
