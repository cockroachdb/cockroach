// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clierrorplus

import (
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/cobra"
)

// MaybeShoutError calls log.Shout on errors, better surfacing problems to the user.
func MaybeShoutError(
	wrapped func(*cobra.Command, []string) error,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := wrapped(cmd, args)
		return CheckAndMaybeShout(err)
	}
}

// CheckAndMaybeShout shouts the error, if non-nil to the OPS logging
// channel.
func CheckAndMaybeShout(err error) error {
	return clierror.CheckAndMaybeLog(err, log.Ops.Shoutf)
}
