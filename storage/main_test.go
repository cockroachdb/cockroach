// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package storage_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/randutil"
)

//go:generate ../util/leaktest/add-leaktest.sh *_test.go

func init() {
	security.SetReadFileFn(securitytest.Asset)
}

var verifyBelowRaftProtos bool

func TestMain(m *testing.M) {
	randutil.SeedForTests()

	// Create a set of all protos we believe to be marshalled downstream of raft.
	// After the tests are run, we'll subtract the encountered protos from this
	// set.
	notBelowRaftProtos := make(map[reflect.Type]struct{}, len(belowRaftGoldenProtos))
	for typ := range belowRaftGoldenProtos {
		notBelowRaftProtos[typ] = struct{}{}
	}

	// Before running the tests, enable instrumentation that tracks protos which
	// are marshalled downstream of raft.
	stopTrackingAndGetTypes := storage.TrackRaftProtos()

	code := m.Run()

	// Only do this verification if the associated test was run. Without this
	// condition, the verification here would spuriously fail when running a
	// small subset of tests e.g. as we often do with `stress`.
	if verifyBelowRaftProtos {
		failed := false
		// Retrieve all the observed downstream-of-raft protos and confirm that they
		// are all present in our expected set.
		for _, typ := range stopTrackingAndGetTypes() {
			if _, ok := belowRaftGoldenProtos[typ]; ok {
				delete(notBelowRaftProtos, typ)
			} else {
				failed = true
				fmt.Printf("%s: missing fixture!\n", typ)
			}
		}

		// Confirm that our expected set is now empty; we don't want to cement any
		// protos needlessly.
		for typ := range notBelowRaftProtos {
			failed = true
			fmt.Printf("%s: not observed below raft!\n", typ)
		}

		// Make sure our error messages make it out.
		if failed && code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}
