// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cliflagcfg

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
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

// StringFlag creates a string flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See cli/context.go to initialize defaults.
func StringFlag(f *pflag.FlagSet, valPtr *string, flagInfo cliflags.FlagInfo) {
	f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	RegisterEnvVarDefault(f, flagInfo)
}

// IntFlag creates an int flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See cli/context.go to initialize defaults.
func IntFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo) {
	f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	RegisterEnvVarDefault(f, flagInfo)
}

// BoolFlag creates a bool flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See cli/context.go to initialize defaults.
func BoolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo) {
	f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	RegisterEnvVarDefault(f, flagInfo)
}

// DurationFlag creates a duration flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See cli/context.go to initialize defaults.
func DurationFlag(f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo) {
	f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	RegisterEnvVarDefault(f, flagInfo)
}

// VarFlag creates a custom-variable flag and registers it with the FlagSet.
// The default value is taken from the value's current value.
// See cli/context.go to initialize defaults.
func VarFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	f.VarP(value, flagInfo.Name, flagInfo.Shorthand, flagInfo.Usage())
	RegisterEnvVarDefault(f, flagInfo)
}

// StringSliceFlag creates a string slice flag and registers it with the FlagSet.
// The default value is taken from the value's current value.
// See cli/context.go to initialize defaults.
func StringSliceFlag(f *pflag.FlagSet, valPtr *[]string, flagInfo cliflags.FlagInfo) {
	f.StringSliceVar(valPtr, flagInfo.Name, *valPtr, flagInfo.Usage())
	RegisterEnvVarDefault(f, flagInfo)
}
