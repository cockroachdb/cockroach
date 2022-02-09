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
	"bufio"
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
var debugSendKVBatchContext = struct {
	// Whether to request verbose tracing and which
	// format to use to emit the trace.
	traceFormat string
	// The output file to use.
	traceFile string
	// Whether to preserve the collected spans in the batch response.
	keepCollectedSpans bool
}{}

func setDebugSendKVBatchContextDefaults() {
	debugSendKVBatchContext.traceFormat = "off"
	debugSendKVBatchContext.traceFile = ""
	debugSendKVBatchContext.keepCollectedSpans = false
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
	enableTracing := false
	fmtJaeger := false
	switch debugSendKVBatchContext.traceFormat {
	case "on", "text":
		// NB: even though the canonical value is "text", it's a common
		// mistake to use "on" instead. Let's be friendly to mistakes.
		enableTracing = true
		fmtJaeger = false
	case "jaeger":
		enableTracing = true
		fmtJaeger = true
	case "off":
	default:
		return errors.New("unknown --trace value")
	}
	var traceFile *os.File
	if enableTracing {
		fileName := debugSendKVBatchContext.traceFile
		if fileName == "" {
			// We use stderr by default so that the user can redirect the
			// response (on stdout) from the trace (on stderr) to different
			// files.
			traceFile = stderr
		} else {
			var err error
			traceFile, err = os.OpenFile(
				fileName,
				os.O_TRUNC|os.O_CREATE|os.O_WRONLY,
				// Note: traces can contain sensitive information so we ensure
				// new trace files are created user-readable.
				0600)
			if err != nil {
				return err
			}
			defer func() {
				if err := traceFile.Close(); err != nil {
					fmt.Fprintf(stderr, "warning: error while closing trace output: %v\n", err)
				}
			}()
		}
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
	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to the node")
	}
	defer finish()
	admin := serverpb.NewAdminClient(conn)

	br, rec, err := sendKVBatchRequestWithTracingOption(ctx, enableTracing, admin, &ba)
	if err != nil {
		return err
	}

	if !debugSendKVBatchContext.keepCollectedSpans {
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

	if enableTracing {
		out := bufio.NewWriter(traceFile)
		if fmtJaeger {
			// Note: we cannot fill in the "node ID" string (3rd argument)
			// here, for example with the string "CLI", because somehow this
			// causes the Jaeger visualizer to override the node prefix on
			// all the sub-spans. With an empty string, the node ID of the
			// node that processes the request is properly annotated in the
			// Jaeger UI.
			j, err := rec.ToJaegerJSON(ba.Summary(), "", "")
			if err != nil {
				return err
			}
			if _, err = fmt.Fprintln(out, j); err != nil {
				return err
			}
		} else {
			if _, err = fmt.Fprintln(out, rec); err != nil {
				return err
			}
		}
		if err := out.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func sendKVBatchRequestWithTracingOption(
	ctx context.Context, verboseTrace bool, admin serverpb.AdminClient, ba *roachpb.BatchRequest,
) (br *roachpb.BatchResponse, rec tracing.Recording, err error) {
	var sp *tracing.Span
	if verboseTrace {
		// Set up a tracing span and enable verbose tracing if requested by
		// configuration.
		//
		// Note: we define the span conditionally under verboseTrace, instead of
		// defining a span unconditionally and then conditionally setting the verbose flag,
		// because otherwise the unit test TestSendKVBatch becomes non-deterministic
		// on the contents of the traceInfo JSON field in the request.
		_, sp = tracing.NewTracer().StartSpanCtx(ctx, "debug-send-kv-batch",
			tracing.WithRecording(tracing.RecordingVerbose))
		defer sp.Finish()

		// Inject the span metadata into the KV request.
		ba.TraceInfo = sp.Meta().ToProto()
	}

	// Do the request server-side.
	br, err = admin.SendKVBatch(ctx, ba)

	if sp != nil {
		// Import the remotely collected spans, if any.
		sp.ImportRemoteSpans(br.CollectedSpans)

		// Extract the recording.
		rec = sp.GetRecording(tracing.RecordingVerbose)
	}

	return br, rec, errors.Wrap(err, "request failed")
}
