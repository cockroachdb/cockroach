// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestBufferedServerStream() *BufferedStreamSender {
	return &BufferedStreamSender{
		ServerStreamSender: newTestServerStream(),
	}
}

func TestBufferedStreamSender(t *testing.T) {
	var serverStream ServerStreamSender = newTestBufferedServerStream()
	require.NotNil(t, serverStream)
	bs, ok := serverStream.(*BufferedStreamSender)
	require.True(t, ok)
	require.ErrorContains(t, bs.SendBuffered(nil, nil),
		"unimplemented: buffered stream sender")
}
