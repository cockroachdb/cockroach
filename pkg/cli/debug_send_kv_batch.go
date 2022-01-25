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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// TODO(knz): this struct belongs elsewhere.
// See: https://github.com/cockroachdb/cockroach/issues/49509
var debugSendKVBatchOpts = struct {
	// Whether to set the verbose flag in the trace span before the
	// request.
	verboseTracing bool
	// Whether to print out the recording at the end.
	printRecording bool
	// Whether to preserve the collected spans in the batch response.
	keepCollectedSpans bool
}{
	verboseTracing:     false,
	printRecording:     false,
	keepCollectedSpans: false,
}

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
	if debugSendKVBatchOpts.printRecording {
		// Not useful unless verbose is set.
		debugSendKVBatchOpts.verboseTracing = true
	}

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
	conn, tracer, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to the node")
	}
	defer finish()
	admin := serverpb.NewAdminClient(conn)

	br, rec, err := setupTraceThenSendKVBatchRequest(ctx, tracer, admin, jsonpb, &ba)
	if err != nil {
		return err
	}

	if !debugSendKVBatchOpts.keepCollectedSpans {
		// The most common use is with -print-recording, in which case the
		// collected spans are redundant output.
		br.CollectedSpans = nil
	}

	// Display BatchResponse.
	brJSON, err := jsonpb.Marshal(br)
	if err != nil {
		return errors.Wrap(err, "failed to format BatchResponse as JSON")
	}
	fmt.Println(string(brJSON))

	if debugSendKVBatchOpts.printRecording {
		// Print out the trace. We use stderr so that the
		// user can redirect the response (on stdout)
		// from the trace (on stderr) to different files.
		fmt.Fprintln(stderr, rec)
	}

	return nil
}

func setupTraceThenSendKVBatchRequest(
	ctx context.Context,
	tracer *tracing.Tracer,
	admin serverpb.AdminClient,
	jsonpb protoutil.JSONPb,
	ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, tracing.Recording, error) {
	// Set up a tracing span and enable verbose tracing if requested by
	// configuration.
	ctx, rootSpan := tracing.EnsureChildSpan(ctx, tracer, "debug-send-kv-batch", tracing.WithForceRealSpan())
	if debugSendKVBatchOpts.verboseTracing {
		rootSpan.SetVerbose(true)
	}
	defer rootSpan.Finish()

	// Run the request and collect the trace.
	br, err := doSendKVBatchRequestInternal(ctx, admin, jsonpb, ba)

	// Extract the recording.
	rec := rootSpan.GetRecording(tracing.RecordingVerbose)
	return br, rec, err
}

func doSendKVBatchRequestInternal(
	ctx context.Context,
	admin serverpb.AdminClient,
	jsonpb protoutil.JSONPb,
	ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	// Attach the span metadata to the request.
	sp := tracing.SpanFromContext(ctx)
	if sp != nil && !sp.IsNoop() {
		// Inject the span metadata into the KV request.
		ba.TraceInfo = sp.Meta().ToProto()
	}

	// Do the request server-side.
	br, err := admin.SendKVBatch(ctx, ba)

	// Import the remotely collected spans, if any.
	if sp != nil && len(br.CollectedSpans) != 0 {
		sp.ImportRemoteSpans(br.CollectedSpans)
	}

	return br, errors.Wrap(err, "request failed")
}
