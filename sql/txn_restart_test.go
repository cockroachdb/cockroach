// Copyright 2016 The Cockroach Authors.
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
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/leaktest"
	_ "github.com/lib/pq"
)

func injectRetriableErrors(
	_ roachpb.StoreID, req roachpb.Request, hdr roachpb.Header,
	magicVals []string, restarts map[string]int) error {
	cput, ok := req.(*roachpb.ConditionalPutRequest)
	if !ok {
		return nil
	}
	for _, val := range magicVals {
		if restarts[val] < 2 && bytes.Contains(cput.Value.RawBytes, []byte(val)) {
			restarts[val]++
			return &roachpb.ReadWithinUncertaintyIntervalError{
				Timestamp:         hdr.Timestamp,
				ExistingTimestamp: hdr.Timestamp,
			}
		}
	}
	return nil
}

func TestTxnRestart(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer func() { storage.TestingCommandFilter = nil }()
	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v TEXT);
`); err != nil {
		t.Fatal(err)
	}

	restarts := make(map[string]int)
	magicVals := []string{"boulanger", "dromedary", "fajita", "hooly", "josephine", "laureal"}
	storage.TestingCommandFilter =
		func(sid roachpb.StoreID, req roachpb.Request, hdr roachpb.Header) error {
			return injectRetriableErrors(sid, req, hdr, magicVals, restarts)
		}

	if _, err := sqlDB.Exec(`
INSERT INTO t.test (k, v) VALUES ('a', 'boulanger');
BEGIN;
INSERT INTO t.test (k, v) VALUES ('c', 'dromedary');
INSERT INTO t.test (k, v) VALUES ('e', 'fajita');
END;
INSERT INTO t.test (k, v) VALUES ('g', 'hooly');
BEGIN;
INSERT INTO t.test (k, v) VALUES ('i', 'josephine');
INSERT INTO t.test (k, v) VALUES ('k', 'laureal');
END;
`); err != nil {
		t.Fatal(err)
	}
	for _, val := range magicVals {
		if restarts[val] != 2 {
			t.Errorf("INSERT for %s has been retried %d times, instead of 2",
				val, restarts[val])
		}
	}

	// Now insert an error into a txn we can't automatically retry (because
	// it spans requests).

	magicVals = []string{"hooly"}
	restarts = make(map[string]int)
	storage.TestingCommandFilter =
		func(sid roachpb.StoreID, req roachpb.Request, hdr roachpb.Header) error {
			return injectRetriableErrors(sid, req, hdr, magicVals, restarts)
		}

	// Start a txn.
	if _, err := sqlDB.Exec(`
DELETE FROM t.test WHERE true;
BEGIN;
`); err != nil {
		t.Fatal(err)
	}

	// Continue the txn in a new request, which is not retriable.
	_, err := sqlDB.Exec("INSERT INTO t.test (k, v) VALUES ('g', 'hooly')")
	if err == nil || !strings.Contains(
		err.Error(), "encountered previous write with future timestamp") {
		t.Errorf("didn't get expected injected error. Got: %s", err)
	}
}
