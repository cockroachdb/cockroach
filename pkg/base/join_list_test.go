// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestJoinListType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		join string
		exp  string
		err  string
	}{
		{"", "", "no address specified in --join"},
		{":", "--join=:" + base.DefaultPort, ""},
		{"a", "--join=a:" + base.DefaultPort, ""},
		{"a,b", "--join=a:" + base.DefaultPort + " --join=b:" + base.DefaultPort, ""},
		{"a,,b", "--join=a:" + base.DefaultPort + " --join=b:" + base.DefaultPort, ""},
		{",a", "--join=a:" + base.DefaultPort, ""},
		{"a,", "--join=a:" + base.DefaultPort, ""},
		{"a:123,b", "--join=a:123 --join=b:" + base.DefaultPort, ""},
		{"[::1]:123,b", "--join=[::1]:123 --join=b:" + base.DefaultPort, ""},
		{"[::1,b", "", `address \[::1: missing ']' in address`},
	}

	for _, test := range testData {
		t.Run(test.join, func(t *testing.T) {
			var jls base.JoinListType
			err := jls.Set(test.join)
			if !testutils.IsError(err, test.err) {
				t.Fatalf("error: expected %q, got: %+v", test.err, err)
			}
			if test.err != "" {
				return
			}
			actual := jls.String()
			if actual != test.exp {
				t.Errorf("expected: %q, got: %q", test.exp, actual)
			}
		})
	}
}
