// Copyright 2016 The Cockroach Authors.
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

package cli

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// MaybeDecorateGRPCError catches grpc errors and provides a more helpful error
// message to the user.
func MaybeDecorateGRPCError(
	wrapped func(*cobra.Command, []string) error,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := wrapped(cmd, args)

		if err == nil {
			return nil
		}

		connDropped := func() error {
			const format = `unable to connect or connection lost.

Please check the address and credentials such as certificates (if attempting to
communicate with a secure cluster).

%s`
			return errors.Errorf(format, err)
		}
		opTimeout := func() error {
			const format = `operation timed out.

%s`
			return errors.Errorf(format, err)
		}

		// Is this an "unable to connect" type of error?
		unwrappedErr := errors.Cause(err)
		switch unwrappedErr.(type) {
		case *roachpb.SendError:
			return connDropped()
		case *net.OpError:
			return connDropped()
		}

		// No, it's not. Is it a plain context cancellation (i.e. timeout)?
		switch unwrappedErr {
		case context.DeadlineExceeded:
			return opTimeout()
		case context.Canceled:
			return opTimeout()
		}

		// Is it a GRPC-observed context cancellation (i.e. timeout), a GRPC
		// connection error, or a known indication of a too-old server?
		if code := status.Code(unwrappedErr); code == codes.DeadlineExceeded {
			return opTimeout()
		} else if code == codes.Unimplemented &&
			strings.Contains(unwrappedErr.Error(), "unknown method Decommission") ||
			strings.Contains(unwrappedErr.Error(), "unknown service cockroach.server.serverpb.Init") {
			return fmt.Errorf(
				"incompatible client and server versions (likely server version: v1.0, required: >=v1.1)")
		} else if grpcutil.IsClosedConnection(unwrappedErr) {
			return connDropped()
		}

		// Nothing we can special case, just return what we have.
		return err
	}
}

// MaybeShoutError calls log.Shout on errors, better surfacing problems to the user.
func MaybeShoutError(
	wrapped func(*cobra.Command, []string) error,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := wrapped(cmd, args)
		if err != nil {
			severity := log.Severity_ERROR
			cause := err
			if ec, ok := errors.Cause(err).(*cliError); ok {
				severity = ec.severity
				cause = ec.cause
			}
			log.Shout(context.Background(), severity, cause)
		}
		return err
	}
}
