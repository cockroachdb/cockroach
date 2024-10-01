// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRedactStackTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		log                 string
		expectedRedactedLog string
	}{
		{"labels: {\"tags\":\"n1,rnode=1,raddr=localhost:26257,class=default,rpc\"}", "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›,class=default,rpc\"}"},
		{"labels: {\"tags\":\"n1,rnode=1,raddr=248.123.55.1:26257,class=default,rpc\"}", "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›,class=default,rpc\"}"},
		{"labels: {\"tags\":\"n1,rnode=1,raddr=abc.def.com:78484,class=default,rpc\"}", "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›,class=default,rpc\"}"},
		{"labels: {\"tags\":\"n1,rnode=1,raddr=0.0.0:26257\"}", "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›\"}"},
		{"labels: {\"tags\":\"n1,rnode=1,raddr=2001:db8:3333:4444:5555:6666:7777:8888:78484,class=default,rpc\"}", "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›,class=default,rpc\"}"},
	}

	for _, test := range tests {
		redactedData := redactStackTrace([]byte(test.log))
		redactedDataStr := bytes.NewBuffer(redactedData).String()
		require.Equal(t, test.expectedRedactedLog, redactedDataStr)
	}
}
