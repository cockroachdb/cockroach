// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSendKVBatchExample is a simple example of generating Protobuf-compatible
// JSON for a BatchRequest doing a Put and then Get of a key.
func TestSendKVBatchExample(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(roachpb.Key("foo"), roachpb.MakeValueFromString("bar")))
	ba.Add(roachpb.NewGet(roachpb.Key("foo"), false /* forUpdate */))
	ba.Add(roachpb.NewConditionalPut(roachpb.Key("foo"), roachpb.MakeValueFromString("bar"), []byte("foo"), false /* allowNotExist */))

	// NOTE: This cannot be marshaled using the standard Go JSON marshaler,
	// since it does not correctly (un)marshal the JSON as mandated by the
	// Protobuf spec. Instead, use the JSON marshaler shipped with Protobuf.
	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	require.NoError(t, err)

	fmt.Println(string(jsonProto))
}

func TestCliSendKVBatch(t *testing.T) {
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
		c := cli.NewCLITest(cli.TestCLIParams{T: t})
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

func TestCliSendKVBatchErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := cli.NewCLITest(cli.TestCLIParams{T: t})
	defer c.Cleanup()

	path := filepath.Join(t.TempDir(), "batch.json")

	// Successful request.
	reqJSON := `{"requests": [{"get": {"header": {"key": "Zm9v"}}}]}`
	require.NoError(t, ioutil.WriteFile(path, []byte(reqJSON), 0644))

	// Request that fails with a KV error (for example ConditionFailed).
	require.NoError(t, ioutil.WriteFile(path, []byte(`
{"requests":[{"conditionalPut":{"header":{"key":"Zm9v"},"value":{"rawBytes":"DMEB5ANiYXI="},"expBytes":"Zm9v"}}]}`,
	), 0644))
	{
		output, err := c.RunWithCapture("debug send-kv-batch " + path)
		require.NoError(t, err)
		// Check that
		// 1. output contains marshaled br.Error,
		require.Contains(t, output, `"encodedError":`)
		// 2. marshaled br.Error describes underlying error's type (gogoproto.Any),
		require.Contains(t, output, "*roachpb.ConditionFailedError")
		// 3. but also command fails and prints the error.
		require.Contains(t, output, "ERROR: unexpected value: <nil>")
	}

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

// TestSendKVBatch_RemoveIntentOnMeta2 sets up a situation in which an intent on
// meta2 needs to be removed without going through the standard conflict
// resolution machinery. It does so by using SendKVBatch to send a
// ResolveIntent(ABORTED) to the meta2 key.
//
// This scenario is of interest during recovery from loss of quorum.
// Transactions that leave intents on the meta ranges are always anchored on
// their home range, and if that range is unavailable, the intent cannot be
// removed by legal means. `reset-quorum` needs to be able to read from meta2 to
// learn the descriptor, so having a way to remove the intent is helpful. In
// such a case, we might adapt this test into a one-off binary and run it
// against the target cluster, or go through a read-modify-write cycle via
// `debug send-kv-batch`.
func TestSendKVBatch_RemoveIntentOnMeta2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	ac, err := tc.GetAdminClient(ctx, t, 0)
	require.NoError(t, err)

	metaKey := keys.RangeMetaKey(tc.LookupRangeOrFatal(t, tc.ScratchRange(t)).EndKey)

	err = tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var desc roachpb.RangeDescriptor
		if err := txn.GetProto(ctx, metaKey, &desc); err != nil {
			return err
		}
		if desc.RangeID == 0 {
			return errors.New("descriptor not found")
		}
		desc.IncrementGeneration()
		if err := txn.Put(ctx, metaKey, &desc); err != nil {
			return err
		}

		// Now we have the intent where we want it, use SendKVBatch to manipulate it.
		txnMeta := func() enginepb.TxnMeta {
			var ba roachpb.BatchRequest
			ba.Header.WaitPolicy = lock.WaitPolicy_Error
			ba.Add(roachpb.NewGet(metaKey.AsRawKey(), false /* forUpdate */))
			br, err := ac.SendKVBatch(ctx, &ba)
			// We expect an error, but not an RPC error. The one we want is in `br.Error`.
			require.NoError(t, err)
			require.NotNil(t, br.Error)
			err = br.Error.GoError()
			wiErr := &roachpb.WriteIntentError{}
			require.True(t, errors.As(err, &wiErr), "%+v", err)
			require.Len(t, wiErr.Intents, 1)
			txnID := wiErr.Intents[0].Txn.ID
			require.Equal(t, txn.ID(), txnID)
			return wiErr.Intents[0].Txn
		}()

		{
			var ba roachpb.BatchRequest
			ba.Add(&roachpb.ResolveIntentRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: metaKey.AsRawKey(),
				},
				IntentTxn: txnMeta,
				Status:    roachpb.ABORTED,
				Poison:    true, // sure, why not
			})
			br, err := ac.SendKVBatch(ctx, &ba)
			require.NoError(t, err)
			require.NoError(t, br.Error.GoError())
		}

		// Can now read the key outside of txn without blocking.
		{
			var ba roachpb.BatchRequest
			ba.Add(roachpb.NewGet(metaKey.AsRawKey(), false /* forUpdate */))
			br, err := ac.SendKVBatch(ctx, &ba)
			require.NoError(t, err)
			require.NoError(t, br.Error.GoError())
		}

		return txn.Rollback(ctx)
	})
	// Because we poisoned the txn, it won't be able to touch its txn record at
	// all and will get a TransactionStatusError. These details are not relevant
	// for this test and are thus not asserted.
	t.Log(err)
}
