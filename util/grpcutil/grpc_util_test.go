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

package grpcutil

import (
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"

	"golang.org/x/net/context"

	"google.golang.org/grpc/metadata"
)

func TestVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// no metadata
	ctx := context.Background()
	if err := CheckVersionNumber(ctx); err == nil || err != errNoMetadata {
		t.Fatalf("expected err '%s', got '%s'", errNoMetadata, err)
	}

	// no version number
	md := metadata.Pairs("something", "something")
	ctx = metadata.NewContext(context.Background(), md)
	if err := CheckVersionNumber(ctx); err == nil || err != errNoVersionNumber {
		t.Fatalf("expected err '%s', got '%s'", errNoVersionNumber, err)
	}

	// too many version numbers
	md = metadata.Pairs(RPCVersionKey, RPCVersion, RPCVersionKey, RPCVersion)
	ctx = metadata.NewContext(context.Background(), md)
	if err := CheckVersionNumber(ctx); !testutils.IsError(err, "expected only 1 RPC context version number, got 2") {
		t.Fatalf("expected err 'expected only 1 RPC context version number, got 2', got '%s'", err)
	}

	// incorrect version number
	md = metadata.Pairs(RPCVersionKey, "wrong")
	ctx = metadata.NewContext(context.Background(), md)
	if err := CheckVersionNumber(ctx); !testutils.IsError(err, "RPC version numbers do not match expected: 1, received: wrong") {
		t.Fatalf("expected err 'RPC version numbers do not match expected: 1, received: wrong', got %s", err)
	}

	// the correct version number
	ctx = AddVersionNumber(context.Background())
	if err := CheckVersionNumber(ctx); err != nil {
		t.Fatal(err)
	}
}
