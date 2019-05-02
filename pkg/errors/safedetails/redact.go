// Copyright 2019 The Cockroach Authors.
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

package safedetails

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/errors/withstack"
)

// Redact returns a redacted version of the supplied item that is safe to use in
// anonymized reporting.
func Redact(r interface{}) string {
	var err error
	switch t := r.(type) {
	case SafeMessager:
		return t.SafeMessage()
	case error:
		err = t
		// continues below
		break
	default:
		return typAnd(r, "")
	}

	// Not a sentinel. Take the common path.
	return redactErr(err)
}

func redactErr(err error) string {
	errMsg := ""
	if c := errbase.UnwrapOnce(err); c != nil {
		// Print the inner error before the outer error.
		errMsg = redactErr(c)
	} else {
		errMsg := redactLeafErr(err)
		if file, line, _, ok := withstack.GetOneLineSource(err); ok {
			errMsg = fmt.Sprintf("%s:%d: %s", file, line, errMsg)
		}
	}

	// Add any additional safe strings from the wrapper, if present.
	if payload := errbase.GetSafeDetails(err); len(payload.SafeDetails) > 0 {
		var buf bytes.Buffer
		fmt.Fprintln(&buf, errMsg)
		fmt.Fprintln(&buf, "(more details:)")
		for _, sd := range payload.SafeDetails {
			fmt.Fprintln(&buf, sd)
		}
		errMsg = buf.String()
	}

	return errMsg
}

func redactLeafErr(err error) string {
	// Now that we're looking at an error, see if it's one we can
	// deconstruct for maximum (safe) clarity. Separating this from the
	// block above ensures that the types below actually implement `error`.
	switch t := err.(type) {
	case runtime.Error:
		return typAnd(t, t.Error())
	case syscall.Errno:
		return typAnd(t, t.Error())
	case *os.SyscallError:
		s := Redact(t.Err)
		return typAnd(t, fmt.Sprintf("%s: %s", t.Syscall, s))
	case *os.PathError:
		// It hardly matters, but avoid mutating the original.
		cpy := *t
		t = &cpy
		t.Path = "<redacted>"
		return typAnd(t, t.Error())
	case *os.LinkError:
		// It hardly matters, but avoid mutating the original.
		cpy := *t
		t = &cpy

		t.Old, t.New = "<redacted>", "<redacted>"
		return typAnd(t, t.Error())
	case *net.OpError:
		// It hardly matters, but avoid mutating the original.
		cpy := *t
		t = &cpy
		t.Source = &unresolvedAddr{net: "tcp", addr: "redacted"}
		t.Addr = &unresolvedAddr{net: "tcp", addr: "redacted"}
		t.Err = errors.New(Redact(t.Err))
		return typAnd(t, t.Error())
	default:
	}

	// Is it a sentinel error? These are safe.
	if markers.IsAny(err,
		context.DeadlineExceeded,
		context.Canceled,
		os.ErrInvalid,
		os.ErrPermission,
		os.ErrExist,
		os.ErrNotExist,
		os.ErrClosed,
		os.ErrNoDeadline,
	) {
		return typAnd(err, err.Error())
	}

	// No further information about this error, simply report its type.
	return typAnd(err, "")
}

type unresolvedAddr struct {
	net  string
	addr string
}

// Network implements the net.Addr interface.
func (a *unresolvedAddr) Network() string {
	return a.net
}

// String implements the net.Addr interface.
func (a *unresolvedAddr) String() string {
	return a.addr
}

func typAnd(r interface{}, msg string) string {
	typ := fmt.Sprintf("%T", r)
	if msg == "" {
		return typ
	}
	return typ + ": " + msg
}
