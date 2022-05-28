// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clienturl

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// FlagSetForCmd is a replacement for cmd.Flag() that properly merges
// persistent and local flags, until the upstream bug
// https://github.com/spf13/cobra/issues/961 has been fixed.
func FlagSetForCmd(cmd *cobra.Command) *pflag.FlagSet {
	_ = cmd.LocalFlags() // force merge persistent+local flags
	return cmd.Flags()
}
