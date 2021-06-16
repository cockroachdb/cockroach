// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestBackendLookupAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Note that this list isn't exhaustive. We only cover the common cases.
	// backendLookupAddr is only temporary, and will go away soon.
	testData := []struct {
		in          string
		expected    string
		expectedErr string
	}{
		// Invalid routing rules.
		{"foobar", "", "missing port in address"},
		{"foobar:", "", "no port was provided for 'foobar:'"},
		{":80", "", "no host was provided for ':80'"},
		{":", "", "no host was provided for ':'"},

		// Invalid hosts.
		{"nonexistent.example.com:26257", "", "no such host"},
		{"333.333.333.333:26257", "", "no such host"},

		// Valid hosts.
		{"127.0.0.1:80", "127.0.0.1:80", ""},
		{"localhost:80", "127.0.0.1:80", ""},
	}
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d/%s", i, test.in), func(t *testing.T) {
			addr, err := backendLookupAddr(ctx, test.in)
			if err != nil {
				if !testutils.IsError(err, test.expectedErr) {
					t.Fatalf("expected error %q, got %v", test.expectedErr, err)
				}
				return
			}
			if test.expectedErr != "" {
				t.Fatalf("expected error %q, got success", test.expectedErr)
			}
			if addr != test.expected {
				t.Fatalf("expected %q,\ngot %q", test.expected, addr)
			}
		})
	}
}
