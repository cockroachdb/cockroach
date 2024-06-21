// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package helpers

import (
	"os"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/spf13/cobra"
)

// Wrap provide `cobra.Command` functions with a standard return code handler.
// Exit codes come from rperrors.Error.ExitCode().
//
// If the wrapped error tree of an error does not contain an instance of
// rperrors.Error, the error will automatically be wrapped with
// rperrors.Unclassified.
func Wrap(f func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		var err error
		err = f(cmd, args)
		if err != nil {
			drtprodError, ok := rperrors.AsError(err)
			if !ok {
				drtprodError = rperrors.Unclassified{Err: err}
				err = drtprodError
			}

			cmd.Printf("Error: %+v\n", err)

			os.Exit(drtprodError.ExitCode())
		}
	}
}
