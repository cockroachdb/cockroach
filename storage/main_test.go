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
	"bytes"
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

func TestMain(m *testing.M) {
	stopTrackingAndGetTypes := storage.TrackRaftProtos()

	randutil.SeedForTests()

	code := m.Run()

	notBelowRaftProtos := make(map[reflect.Type]struct{}, len(goldenProtos))
	for typ := range goldenProtos {
		notBelowRaftProtos[typ] = struct{}{}
	}

	var buf bytes.Buffer
	for _, typ := range stopTrackingAndGetTypes() {
		if _, ok := goldenProtos[typ]; ok {
			delete(notBelowRaftProtos, typ)
		} else {
			fmt.Fprintf(&buf, "%s: missing fixture!\n", typ)
		}
	}

	for typ := range notBelowRaftProtos {
		fmt.Fprintf(&buf, "%s: not observed below raft!\n", typ)
	}

	if out := buf.String(); len(out) != 0 {
		fmt.Print(out)

		// In case the rest of the test suite passed, fail it.
		if code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}
