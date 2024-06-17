// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	}

	for _, test := range tests {
		redactedData := redactStackTrace([]byte(test.log))
		redactedDataStr := bytes.NewBuffer(redactedData).String()
		require.Equal(t, test.expectedRedactedLog, redactedDataStr)
	}
}
