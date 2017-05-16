// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func createTestClient(t *testing.T, s serverutils.TestServerInterface) *client.DB {
	return createTestClientForUser(t, s, security.NodeUser)
}

func createTestClientForUser(
	t *testing.T, s serverutils.TestServerInterface, user string,
) *client.DB {
	var ctx base.Config
	ctx.InitDefaults()
	ctx.User = user
	testutils.FillCerts(&ctx)

	conn, err := rpc.NewContext(log.AmbientContext{}, &ctx, s.Clock(), s.Stopper()).GRPCDial(s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}
	return client.NewDB(client.NewSender(conn), s.Clock())
}

// TestKVDBCoverage verifies that all methods may be invoked on the
// key value database.
func TestKVDBCoverage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	db := createTestClient(t, s)
	key := roachpb.Key("a")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Put first value at key.
	if pErr := db.Put(context.TODO(), key, value1); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify put.
	if gr, pErr := db.Get(ctx, key); pErr != nil {
		t.Fatal(pErr)
	} else if !gr.Exists() {
		t.Error("expected key to exist")
	}

	// Conditional put should succeed, changing value1 to value2.
	if pErr := db.CPut(context.TODO(), key, value2, value1); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify get by looking up conditional put value.
	if gr, pErr := db.Get(ctx, key); pErr != nil {
		t.Fatal(pErr)
	} else if !bytes.Equal(gr.ValueBytes(), value2) {
		t.Errorf("expected get to return %q; got %q", value2, gr.ValueBytes())
	}

	// Increment.
	if ir, pErr := db.Inc(ctx, "i", 10); pErr != nil {
		t.Fatal(pErr)
	} else if ir.ValueInt() != 10 {
		t.Errorf("expected increment new value of %d; got %d", 10, ir.ValueInt())
	}

	// Delete conditional put value.
	if pErr := db.Del(ctx, key); pErr != nil {
		t.Fatal(pErr)
	}
	if gr, pErr := db.Get(ctx, key); pErr != nil {
		t.Fatal(pErr)
	} else if gr.Exists() {
		t.Error("expected key to not exist after delete")
	}

	// Put values in anticipation of scan & delete range.
	keyValues := []roachpb.KeyValue{
		{Key: roachpb.Key("a"), Value: roachpb.MakeValueFromBytes(value1)},
		{Key: roachpb.Key("b"), Value: roachpb.MakeValueFromBytes(value2)},
		{Key: roachpb.Key("c"), Value: roachpb.MakeValueFromBytes(value3)},
	}
	for _, kv := range keyValues {
		valueBytes, pErr := kv.Value.GetBytes()
		if pErr != nil {
			t.Fatal(pErr)
		}
		if pErr := db.Put(context.TODO(), kv.Key, valueBytes); pErr != nil {
			t.Fatal(pErr)
		}
	}
	if rows, pErr := db.Scan(context.TODO(), "a", "d", 0); pErr != nil {
		t.Fatal(pErr)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			valueBytes, pErr := kv.Value.GetBytes()
			if pErr != nil {
				t.Fatal(pErr)
			}
			if !bytes.Equal(rows[i].ValueBytes(), valueBytes) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[i].ValueBytes(), valueBytes)
			}
		}
	}

	// Test reverse scan.
	if rows, pErr := db.ReverseScan(context.TODO(), "a", "d", 0); pErr != nil {
		t.Fatal(pErr)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			valueBytes, pErr := kv.Value.GetBytes()
			if pErr != nil {
				t.Fatal(pErr)
			}
			if !bytes.Equal(rows[len(keyValues)-1-i].ValueBytes(), valueBytes) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[len(keyValues)-i].ValueBytes(), valueBytes)
			}
		}
	}

	if pErr := db.DelRange(context.TODO(), "a", "c"); pErr != nil {
		t.Fatal(pErr)
	}
}

func TestKVDBInternalMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	testCases := []roachpb.Request{
		&roachpb.HeartbeatTxnRequest{},
		&roachpb.GCRequest{},
		&roachpb.PushTxnRequest{},
		&roachpb.QueryTxnRequest{},
		&roachpb.ResolveIntentRequest{},
		&roachpb.ResolveIntentRangeRequest{},
		&roachpb.MergeRequest{},
		&roachpb.TruncateLogRequest{},
		&roachpb.RequestLeaseRequest{},

		&roachpb.EndTransactionRequest{
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{},
		},
	}
	// Verify internal methods experience bad request errors.
	db := createTestClient(t, s)
	for i, args := range testCases {
		{
			header := args.Header()
			header.Key = roachpb.Key("a")
			args.SetHeader(header)
		}
		if roachpb.IsRange(args) {
			header := args.Header()
			header.EndKey = args.Header().Key.Next()
			args.SetHeader(header)
		}
		b := &client.Batch{}
		b.AddRawRequest(args)
		err := db.Run(context.TODO(), b)
		if !testutils.IsError(err, "contains an internal request|contains commit trigger") {
			t.Errorf("%d: unexpected error for %s: %v", i, args.Method(), err)
		}
	}
}

// TestKVDBTransaction verifies that transactions work properly over
// the KV DB endpoint.
func TestKVDBTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db := createTestClient(t, s)

	key := roachpb.Key("db-txn-test")
	value := []byte("value")
	err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		// Use snapshot isolation so non-transactional read can always push.
		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			return err
		}

		if err := txn.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}

		// Attempt to read outside of txn.
		if gr, err := db.Get(ctx, key); err != nil {
			t.Fatal(err)
		} else if gr.Exists() {
			t.Errorf("expected nil value; got %+v", gr.Value)
		}

		// Read within the transaction.
		if gr, err := txn.Get(ctx, key); err != nil {
			t.Fatal(err)
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
			t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
		}
		return nil
	})
	if err != nil {
		t.Errorf("expected success on commit; got %s", err)
	}

	// Verify the value is now visible after commit.
	if gr, err := db.Get(context.TODO(), key); err != nil {
		t.Errorf("expected success reading value; got %s", err)
	} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
		t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
	}
}

// TestAuthentication tests authentication for the KV endpoint.
func TestAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	b1 := &client.Batch{}
	b1.Put("a", "b")

	// Create a node user client and call Run() on it which lets us build our own
	// request, specifying the user.
	db1 := createTestClientForUser(t, s, security.NodeUser)
	if err := db1.Run(context.TODO(), b1); err != nil {
		t.Fatal(err)
	}

	b2 := &client.Batch{}
	b2.Put("c", "d")

	// Try again, but this time with certs for a non-node user (even the root
	// user has no KV permissions).
	db2 := createTestClientForUser(t, s, security.RootUser)
	if err := db2.Run(context.TODO(), b2); !testutils.IsError(err, "is not allowed") {
		t.Fatal(err)
	}
}

// TestTxnDelRangeIntentResolutionCounts ensures that intents left behind by a
// DelRange with a MaxSpanRequestKeys limit are resolved correctly and by
// using the minimal span of keys.
func TestTxnDelRangeIntentResolutionCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var intentResolutionCount int64
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				NumKeysEvaluatedForRangeIntentResolution: &intentResolutionCount,
				TestingEvalFilter: func(filterArgs storagebase.FilterArgs) *roachpb.Error {
					req, ok := filterArgs.Req.(*roachpb.ResolveIntentRequest)
					if ok {
						key := req.Header().Key.String()
						// Check if the intent is from the range being
						// scanned below.
						if key >= "a" && key < "d" {
							t.Errorf("resolving intent on key %s", key)
						}
					}
					return nil
				},
			},
		},
	}
	s, _, db := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	for _, abortTxn := range []bool{false, true} {
		spanSize := int64(10)
		prefixes := []string{"a", "b", "c"}
		for i := int64(0); i < spanSize; i++ {
			for _, prefix := range prefixes {
				if err := db.Put(context.TODO(), fmt.Sprintf("%s%d", prefix, i), "v"); err != nil {
					t.Fatal(err)
				}
			}
		}
		totalNumKeys := int64(len(prefixes)) * spanSize

		atomic.StoreInt64(&intentResolutionCount, 0)
		limit := totalNumKeys / 2
		if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			var b client.Batch
			// Fully deleted.
			b.DelRange("a", "b", false)
			// Partially deleted.
			b.DelRange("b", "c", false)
			// Not deleted.
			b.DelRange("c", "d", false)
			b.Header.MaxSpanRequestKeys = limit
			if err := txn.Run(ctx, &b); err != nil {
				return err
			}
			if abortTxn {
				return errors.New("aborting txn")
			}
			return nil
		}); err != nil && !abortTxn {
			t.Fatal(err)
		}

		// Ensure that the correct number of keys were evaluated for intents.
		if numKeys := atomic.LoadInt64(&intentResolutionCount); numKeys != limit {
			t.Fatalf("abortTxn: %v, resolved %d keys, expected %d", abortTxn, numKeys, limit)
		}

		// Ensure no intents are left behind. Scan the entire span to resolve
		// any intents that were left behind; intents are resolved internally
		// through ResolveIntentRequest(s).
		kvs, err := db.Scan(context.TODO(), "a", "d", totalNumKeys)
		if err != nil {
			t.Fatal(err)
		}
		expectedNumKeys := totalNumKeys / 2
		if abortTxn {
			expectedNumKeys = totalNumKeys
		}
		if int64(len(kvs)) != expectedNumKeys {
			t.Errorf("abortTxn: %v, %d keys left behind, expected=%d",
				abortTxn, len(kvs), expectedNumKeys)
		}
	}
}

// Test that, for non-transactional requests, low-level retryable errors get
// transformed to an UnhandledRetryableError.
func TestNonTransactionalRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	key := []byte("key-restart")
	value := []byte("value")
	params := base.TestServerArgs{}
	testingKnobs := &storage.StoreTestingKnobs{
		TestingEvalFilter: func(args storagebase.FilterArgs) *roachpb.Error {
			if resArgs, ok := args.Req.(*roachpb.PutRequest); ok {
				if resArgs.Key.Equal(key) {
					return roachpb.NewError(&roachpb.WriteTooOldError{})
				}
			}
			return nil
		},
	}
	params.Knobs.Store = testingKnobs
	s, _, db := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	err := db.Put(context.TODO(), key, value)
	if _, ok := err.(*roachpb.UnhandledRetryableError); !ok {
		t.Fatalf("expected UnhandledRetryableError, got: %T - %v", err, err)
	}
}
