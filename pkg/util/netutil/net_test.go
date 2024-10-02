// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package netutil

import (
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestIsClosedConnection(t *testing.T) {
	for _, tc := range []struct {
		err           error
		isClosedError bool
	}{
		{
			fmt.Errorf("an error"),
			false,
		},
		{
			net.ErrClosed,
			true,
		},
		{
			cmux.ErrListenerClosed,
			true,
		},
		{
			grpc.ErrServerStopped,
			true,
		},
		{
			io.EOF,
			true,
		},
		{
			// TODO(rafi): should this be treated the same as EOF?
			io.ErrUnexpectedEOF,
			false,
		},
		{
			&net.AddrError{Err: "addr", Addr: "err"},
			true,
		},
		{
			syscall.ECONNRESET,
			true,
		},
		{
			syscall.EADDRINUSE,
			true,
		},
		{
			syscall.ECONNABORTED,
			true,
		},
		{
			syscall.ECONNREFUSED,
			true,
		},
		{
			syscall.EBADMSG,
			true,
		},
		{
			syscall.EINTR,
			false,
		},
		{
			&timeutil.TimeoutError{},
			false,
		},
	} {
		assert.Equalf(t, tc.isClosedError, IsClosedConnection(tc.err),
			"expected %q to be evaluated as %v", tc.err, tc.isClosedError,
		)
	}
}
