// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errorutil

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
)

// Renumber lines so they're stable no matter what changes above. (We
// could make the regexes accept any string of digits, but we also
// want to make sure that the correct line numbers get captured).
//
//line error_test.go:1000

func TestUnexpectedWithIssueErrorf(t *testing.T) {
	err := UnexpectedWithIssueErrorf(1234, "args: %d %s %f", 1, "two", 3.0)
	exp := "unexpected error: args: 1 two 3.000000"
	if err.Error() != exp {
		t.Errorf("expected message:\n  %s\ngot:\n  %s", exp, err.Error())
	}

	safeMsg := fmt.Sprintf("%+v", err)
	reqHint := "We've been trying to track this particular issue down. Please report your " +
		"reproduction at https://github.com/cockroachdb/cockroach/issues/1234 unless " +
		"that issue seems to have been resolved (in which case you might want to " +
		"update crdb to a newer version)."
	if !strings.Contains(safeMsg, reqHint) {
		t.Errorf("expected substring in error\n%s\ngot:\n%s", exp, safeMsg)
	}

	// Check that the issue number is present in the safe details.
	exp = "issue #1234"
	if !strings.Contains(safeMsg, exp) {
		t.Errorf("expected substring in error\n%s\ngot:\n%s", exp, safeMsg)
	}
}

func BenchmarkErrorsIs(b *testing.B) {
	b.Run("SimpleError", func(b *testing.B) {
		err := errors.New("test")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("WrappedError", func(b *testing.B) {
		baseErr := errors.New("test")
		err := errors.Wrap(baseErr, "wrapped error")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("WrappedWithStack", func(b *testing.B) {
		baseErr := errors.New("test")
		err := errors.WithStack(baseErr)
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("NetworkError", func(b *testing.B) {
		netErr := &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257},
			Err:  fmt.Errorf("connection refused"),
		}
		err := errors.Wrap(netErr, "network connection failed")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("DeeplyWrappedNetworkError", func(b *testing.B) {
		netErr := &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257},
			Err:  fmt.Errorf("connection refused"),
		}
		err := errors.WithStack(netErr)
		err = errors.Wrap(err, "failed to connect to database")
		err = errors.Wrap(err, "unable to establish connection")
		err = errors.WithStack(err)
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("MultipleWrappedErrors", func(b *testing.B) {
		baseErr := errors.New("internal error")
		err := errors.WithStack(baseErr)
		err = errors.Wrap(err, "operation failed")
		err = errors.WithStack(err)
		err = errors.Wrap(err, "transaction failed")
		err = errors.WithStack(err)
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("NetworkErrorWithLongAddress", func(b *testing.B) {
		netErr := &net.OpError{
			Op:  "read",
			Net: "tcp",
			Addr: &net.TCPAddr{
				IP:   net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
				Port: 26257,
			},
			Err: fmt.Errorf("i/o timeout"),
		}
		err := errors.WithStack(netErr)
		err = errors.Wrap(err, "failed to read from connection")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("WithMessage", func(b *testing.B) {
		baseErr := errors.New("test")
		err := errors.WithMessage(baseErr, "additional context")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("MultipleWithMessage", func(b *testing.B) {
		baseErr := errors.New("internal error")
		err := errors.WithMessage(baseErr, "first message")
		err = errors.WithMessage(err, "second message")
		err = errors.WithMessage(err, "third message")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("WithMessageAndStack", func(b *testing.B) {
		baseErr := errors.New("test")
		err := errors.WithStack(baseErr)
		err = errors.WithMessage(err, "operation context")
		err = errors.WithStack(err)
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("NetworkErrorWithMessage", func(b *testing.B) {
		netErr := &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257},
			Err:  fmt.Errorf("connection refused"),
		}
		err := errors.WithMessage(netErr, "database connection failed")
		err = errors.WithMessage(err, "unable to reach server")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("NetworkErrorWithEverything", func(b *testing.B) {
		netErr := &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257},
			Err:  fmt.Errorf("connection refused"),
		}
		err := errors.WithStack(netErr)
		err = errors.WithMessage(err, "database connection failed")
		err = errors.Wrap(err, "failed to establish TCP connection")
		err = errors.WithStack(err)
		err = errors.WithMessage(err, "unable to reach CockroachDB server")
		err = errors.Wrap(err, "connection attempt failed")
		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})

	b.Run("DeeplyNested100Levels", func(b *testing.B) {
		baseErr := errors.New("base error")
		err := baseErr

		// Create a 100-level deep error chain
		for i := 0; i < 100; i++ {
			switch i % 3 {
			case 0:
				err = errors.Wrap(err, fmt.Sprintf("wrap level %d", i))
			case 1:
				err = errors.WithMessage(err, fmt.Sprintf("message level %d", i))
			case 2:
				err = errors.WithStack(err)
			}
		}

		for range b.N {
			errors.Is(err, context.Canceled)
		}
	})
}
