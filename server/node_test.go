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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/storage"
)

func formatKeys(keys []storage.Key) string {
	var buf bytes.Buffer
	for i, key := range keys {
		buf.WriteString(fmt.Sprintf("%d: %s\n", i, key))
	}
	return buf.String()
}

// TestBootstrap verifies the results of bootstrapping a cluster. Uses
// an in memory engine.
func TestBootstrapCluster(t *testing.T) {
	engine := storage.NewInMem(1 << 20)
	localDB, err := BootstrapCluster("cluster-1", engine)
	if err != nil {
		t.Fatal(err)
	}
	// Scan the complete contents of the local database.
	sr := <-localDB.Scan(&storage.ScanRequest{
		StartKey:   storage.KeyMin,
		EndKey:     storage.KeyMax,
		MaxResults: math.MaxInt64,
	})
	if sr.Error != nil {
		t.Fatal(sr.Error)
	}
	var keys []storage.Key
	for _, kv := range sr.Rows {
		keys = append(keys, kv.Key)
	}
	var expectedKeys = []storage.Key{
		storage.Key("\x00\x00\x00range-1"),
		storage.Key("\x00\x00\x00range-id-generator"),
		storage.Key("\x00\x00\x00store-ident"),
		storage.Key("\x00\x00meta1\xff"),
		storage.Key("\x00\x00meta2\xff"),
		storage.Key("\x00node-id-generator"),
		storage.Key("\x00store-id-generator-1"),
	}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Errorf("expected keys mismatch:\n%s\n  -- vs. -- \n\n%s",
			formatKeys(keys), formatKeys(expectedKeys))
	}

	// TODO(spencer): check values.
}
