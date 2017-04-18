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
	"net"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// MaybeDecorateGRPCError catches grpc errors and provides a more helpful error
// message to the user.
func MaybeDecorateGRPCError(
	wrapped func(*cobra.Command, []string) error,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := wrapped(cmd, args)

		{
			unwrappedErr := errors.Cause(err)
			if unwrappedErr == nil {
				return err
			}
			_, isSendError := unwrappedErr.(*roachpb.SendError)
			isGRPCError := grpcutil.IsClosedConnection(unwrappedErr)
			_, isNetError := unwrappedErr.(*net.OpError)
			if !(isSendError || isGRPCError || isNetError) {
				return err // intentionally return original to keep wrapping
			}
		}

		format := `unable to connect or connection lost.

Please check the address and credentials such as certificates (if attempting to
communicate with a secure cluster).

%s`

		return errors.Errorf(format, err)
	}
}

// MaybeShoutError calls log.Shout on errors, better surfacing problems to the user.
func MaybeShoutError(
	wrapped func(*cobra.Command, []string) error,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := wrapped(cmd, args)
		if err != nil {
			log.Shout(context.Background(), log.Severity_ERROR, err)
		}
		return err
	}
}

func usageAndError(cmd *cobra.Command) error {
	if err := cmd.Usage(); err != nil {
		return err
	}
	return errors.New("invalid arguments")
}
