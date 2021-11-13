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
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestSendKVBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	// Protobuf spec. Instead, use the JSON marshaler shipped with Protobuf, and
	// name it 'json' to get around the protoutil.Marshal linter.
	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(roachpb.Key("foo"), roachpb.MakeValueFromString("bar")))
	ba.Add(roachpb.NewGet(roachpb.Key("foo"), false /* forUpdate */))

	json := protoutil.JSONPb{}
	jsonProto, err := json.Marshal(&ba)
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
		jsonRequest := jsonManual
		if fromProto {
			jsonRequest = jsonProto
		}
		path := filepath.Join(t.TempDir(), "batch.json")
		require.NoError(t, ioutil.WriteFile(path, jsonRequest, 0644))

		c := NewCLITest(TestCLIParams{T: t})
		defer c.Cleanup()

		output, err := c.RunWithCapture("debug send-kv-batch " + path)
		require.NoError(t, err)

		// Remove first line containing the input command.
		output = strings.SplitN(output, "\n", 2)[1]
		// Zero out all HLC timestamp objects.
		output = regexp.MustCompile(`(?s)\{\s*"wallTime":.*?\}`).ReplaceAllString(output, "{}")

		require.JSONEq(t, jsonResponse, output)
	})
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
