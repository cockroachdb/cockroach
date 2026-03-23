// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCanVirtuallyResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeRequest := func(reqs ...kvpb.Request) Request {
		var reqUnions []kvpb.RequestUnion
		for _, req := range reqs {
			var ru kvpb.RequestUnion
			ru.MustSetInner(req)
			reqUnions = append(reqUnions, ru)
		}
		return Request{Requests: reqUnions}
	}

	testCases := []struct {
		name string
		req  Request
		exp  bool
	}{{
		name: "empty batch",
		req:  Request{},
		exp:  true,
	}, {
		name: "single non-locking read",
		req:  makeRequest(&kvpb.GetRequest{}),
		exp:  true,
	}, {
		name: "single non-locking scan",
		req:  makeRequest(&kvpb.ScanRequest{}),
		exp:  true,
	}, {
		name: "multiple non-locking reads",
		req: makeRequest(
			&kvpb.GetRequest{},
			&kvpb.ScanRequest{},
			&kvpb.ReverseScanRequest{},
		),
		exp: true,
	}, {
		name: "single write",
		req:  makeRequest(&kvpb.PutRequest{}),
		exp:  false,
	}, {
		name: "single locking read with shared strength",
		req:  makeRequest(&kvpb.GetRequest{KeyLockingStrength: lock.Shared}),
		exp:  false,
	}, {
		name: "single locking read with exclusive strength",
		req:  makeRequest(&kvpb.GetRequest{KeyLockingStrength: lock.Exclusive}),
		exp:  false,
	}, {
		name: "single locking scan",
		req:  makeRequest(&kvpb.ScanRequest{KeyLockingStrength: lock.Exclusive}),
		exp:  false,
	}, {
		name: "mixed batch with read and write",
		req: makeRequest(
			&kvpb.GetRequest{},
			&kvpb.PutRequest{},
		),
		exp: false,
	}, {
		name: "mixed batch with non-locking and locking read",
		req: makeRequest(
			&kvpb.GetRequest{},
			&kvpb.GetRequest{KeyLockingStrength: lock.Shared},
		),
		exp: false,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.req.canVirtuallyResolve())
		})
	}
}
