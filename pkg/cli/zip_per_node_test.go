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

	"github.com/stretchr/testify/require"
)

func TestRedactStackTrace(t *testing.T) {
	stackTrace := "labels: {\"tags\":\"n1,rnode=1,raddr=localhost:26257,class=default,rpc\"}"
	redactedData := redactStackTrace([]byte(stackTrace))
	redactedDataStr := bytes.NewBuffer(redactedData).String()

	require.Equal(t, redactedDataStr, "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›,class=default,rpc\"}")
}

func TestRedactStackTraceWithNumericIP(t *testing.T) {
	stackTrace := "labels: {\"tags\":\"n1,rnode=1,raddr=248.123.55.1:26257,class=default,rpc\"}"
	redactedData := redactStackTrace([]byte(stackTrace))
	redactedDataStr := bytes.NewBuffer(redactedData).String()

	require.Equal(t, redactedDataStr, "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›,class=default,rpc\"}")
}

func TestRedactStackTraceWithNonnumericHost(t *testing.T) {
	stackTrace := "labels: {\"tags\":\"n1,rnode=1,raddr=abc.def.com:78484,class=default,rpc\"}"
	redactedData := redactStackTrace([]byte(stackTrace))
	redactedDataStr := bytes.NewBuffer(redactedData).String()

	require.Equal(t, redactedDataStr, "labels: {\"tags\":\"n1,rnode=1,raddr=‹×›,class=default,rpc\"}")
}
