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
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type cmdFn func(*cobra.Command, []string) error

func maybeDecorateGRPCError(wrapped cmdFn) cmdFn {
	return func(cmd *cobra.Command, args []string) error {
		err := errors.Cause(wrapped(cmd, args))
		if err == nil || !grpcutil.IsClosedConnection(err) {
			return err
		}
		errCode := grpc.Code(err)
		errMsg := grpc.ErrorDesc(err)

		format := `unable to connect or connection lost.

Please check the address and credentials such as certificates (if attempting to
communicate with a secure cluster).

(error code %d: %s)`

		return errors.Errorf(format, errCode, errMsg)
	}
}
