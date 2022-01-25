// Copyright 2017 The Cockroach Authors.
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestSendKVBatchExample is a simple example of generating Protobuf-compatible
// JSON for a BatchRequest doing a Put and then Get of a key.
func TestSendKVBatchExample(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(roachpb.Key("foo"), roachpb.MakeValueFromString("bar")))
	ba.Add(roachpb.NewGet(roachpb.Key("foo"), false /* forUpdate */))

	// NOTE: This cannot be marshaled using the standard Go JSON marshaler,
	// since it does not correctly (un)marshal the JSON as mandated by the
	// Protobuf spec. Instead, use the JSON marshaler shipped with Protobuf.
	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	require.NoError(t, err)

	fmt.Println(string(jsonProto))
}

func TestSendKVBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This sets the key "foo" to the value "bar", and reads back the result.
	jsonManual := []byte(`
		{"requests": [
			{"put": {
				"header": {"key": "Zm9v"},
				"value": {"raw_bytes": "DMEB5ANiYXI="}
			}},
			{"get": {
				"header": {"key": "Zm9v"}
			}}
		]}`)

	// Alternatively, build a BatchRequest and marshal it. This might be
	// preferable for more complex requests.
	//
	// NOTE: This cannot be marshaled using the standard Go JSON marshaler,
	// since it does not correctly (un)marshal the JSON as mandated by the
	// Protobuf spec. Instead, use the JSON marshaler shipped with Protobuf.
	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(roachpb.Key("foo"), roachpb.MakeValueFromString("bar")))
	ba.Add(roachpb.NewGet(roachpb.Key("foo"), false /* forUpdate */))

	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	require.NoError(t, err)

	// This is the expected response. We zero out any HLC timestamps before comparing.
	jsonResponse := `
		{
			"header": {
				"Timestamp": {},
				"now": {}
			},
			"responses": [
				{"put": {
					"header": {}
				}},
				{"get": {
					"header": {"numKeys": "1", "numBytes": "8"},
					"value": {"rawBytes": "DMEB5ANiYXI=", "timestamp": {}}
				}}
			]
		}`

	// Run test both with manual and Protobuf-generated JSON.
	testutils.RunTrueAndFalse(t, "fromProto", func(t *testing.T, fromProto bool) {
		defer log.Scope(t).Close(t)
		start := timeutil.Now()

		// Save the JSON BatchRequest to batch.json.
		jsonRequest := jsonManual
		if fromProto {
			jsonRequest = jsonProto
		}
		path := filepath.Join(t.TempDir(), "batch.json")
		require.NoError(t, ioutil.WriteFile(path, jsonRequest, 0644))

		// Start a CLI test server and run 'debug send-kv-batch batch.json'.
		c := NewCLITest(TestCLIParams{T: t})
		defer c.Cleanup()

		output, err := c.RunWithCapture("debug send-kv-batch " + path)
		require.NoError(t, err)

		// Clean and check the BatchResponse output, by removing first line
		// (contains input command) and emptying out all HLC timestamp objects.
		output = strings.SplitN(output, "\n", 2)[1]
		output = regexp.MustCompile(`(?s)\{\s*"wallTime":.*?\}`).ReplaceAllString(output, "{}")
		require.JSONEq(t, jsonResponse, output)

		// Check that a structured log event was emitted.
		log.Flush()
		entries, err := log.FetchEntriesFromFiles(start.UnixNano(), timeutil.Now().UnixNano(), 1,
			regexp.MustCompile("debug_send_kv_batch"), log.WithFlattenedSensitiveData)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		entry := entries[0]
		require.Equal(t, logpb.Severity_INFO, entry.Severity)
		require.Equal(t, logpb.Channel_OPS, entry.Channel)

		event := map[string]interface{}{}
		require.NoError(t, json.Unmarshal([]byte(entry.Message[entry.StructuredStart:]), &event))
		require.EqualValues(t, "debug_send_kv_batch", event["EventType"])
		require.EqualValues(t, "root", event["User"])
		require.EqualValues(t, 1, event["NodeID"])

		// Check that the log entry contains the BatchRequest as JSON, following
		// a Protobuf marshaling roundtrip (for normalization).
		var ba roachpb.BatchRequest
		require.NoError(t, jsonpb.Unmarshal(jsonRequest, &ba))
		expectLogJSON, err := jsonpb.Marshal(&ba)
		require.NoError(t, err)
		require.JSONEq(t, string(expectLogJSON), event["BatchRequest"].(string),
			"structured log entry contains unexpected BatchRequest")
	})
}

func TestSendKVBatchTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	reqJSON := `{"requests": [{"get": {"header": {"key": "Zm9v"}}}]}`
	path := filepath.Join(t.TempDir(), "batch.json")
	require.NoError(t, ioutil.WriteFile(path, []byte(reqJSON), 0644))

	// text mode, output to stderr.
	output, err := c.RunWithCapture("debug send-kv-batch --trace=text " + path)
	require.NoError(t, err)
	require.Contains(t, output, "=== operation:/cockroach.roachpb.Internal/Batch")

	// jaeger mode, output to stderr.
	output, err = c.RunWithCapture("debug send-kv-batch --trace=jaeger " + path)
	require.NoError(t, err)
	require.Contains(t, output, `"operationName": "/cockroach.roachpb.Internal/Batch",`)

	traceOut := filepath.Join(t.TempDir(), "trace.out")
	// text mode, output to file.
	_, err = c.RunWithCapture("debug send-kv-batch --trace=text --trace-output=" + traceOut + " " + path)
	require.NoError(t, err)
	b, err := ioutil.ReadFile(traceOut)
	require.NoError(t, err)
	require.Contains(t, string(b), "=== operation:/cockroach.roachpb.Internal/Batch")

	// jaeger mode, output to file.
	_, err = c.RunWithCapture("debug send-kv-batch --trace=jaeger --trace-output=" + traceOut + " " + path)
	require.NoError(t, err)
	b, err = ioutil.ReadFile(traceOut)
	require.NoError(t, err)
	require.Contains(t, string(b), `"operationName": "/cockroach.roachpb.Internal/Batch",`)
}

func TestSendKVBatchErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	reqJSON := `{"requests": [{"get": {"header": {"key": "Zm9v"}}}]}`
	path := filepath.Join(t.TempDir(), "batch.json")
	require.NoError(t, ioutil.WriteFile(path, []byte(reqJSON), 0644))

	// Insecure connection should error.
	output, err := c.RunWithCapture("debug send-kv-batch --insecure " + path)
	require.NoError(t, err)
	require.Contains(t, output, "ERROR: failed to connect")

	// Invalid trace mode should error.
	output, err = c.RunWithCapture("debug send-kv-batch --trace=unknown " + path)
	require.NoError(t, err)
	require.Contains(t, output, "ERROR: unknown --trace value")

	// Invalid trace output file should error.
	output, err = c.RunWithCapture("debug send-kv-batch --trace=on --trace-output=invalid/. " + path)
	require.NoError(t, err)
	require.Contains(t, output, "ERROR: open invalid/.: no such file or directory")

	// Invalid JSON should error.
	require.NoError(t, ioutil.WriteFile(path, []byte("{invalid"), 0644))
	output, err = c.RunWithCapture("debug send-kv-batch " + path)
	require.NoError(t, err)
	require.Contains(t, output, "ERROR: invalid JSON")

	// Unknown JSON field should error.
	require.NoError(t, ioutil.WriteFile(path, []byte(`{"unknown": null}`), 0644))
	output, err = c.RunWithCapture("debug send-kv-batch " + path)
	require.NoError(t, err)
	require.Contains(t, output, "ERROR: invalid JSON")
}
