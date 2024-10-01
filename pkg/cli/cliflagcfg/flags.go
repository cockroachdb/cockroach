// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	StringFlagDepth(1, f, valPtr, flagInfo)
}

// StringFlagDepth is like StringFlag but the caller can control the
// call level at which the env var usage assertion is done.
func StringFlagDepth(depth int, f *pflag.FlagSet, valPtr *string, flagInfo cliflags.FlagInfo) {
	f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo, depth+1)
}

// IntFlag creates an int flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See cli/context.go to initialize defaults.
func IntFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo) {
	IntFlagDepth(1, f, valPtr, flagInfo)
}

// IntFlagDepth is like IntFlag but the caller can control the
// call level at which the env var usage assertion is done.
func IntFlagDepth(depth int, f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo) {
	f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo, depth+1)
}

// BoolFlag creates a bool flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See cli/context.go to initialize defaults.
func BoolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo) {
	BoolFlagDepth(1, f, valPtr, flagInfo)
}

// BoolFlagDepth is like BoolFlag but the caller can control the
// call level at which the env var usage assertion is done.
func BoolFlagDepth(depth int, f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo) {
	f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo, depth+1)
}

// DurationFlag creates a duration flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See cli/context.go to initialize defaults.
func DurationFlag(f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo) {
	DurationFlagDepth(1, f, valPtr, flagInfo)
}

// DurationFlagDepth is like DurationFlag but the caller can control the
// call level at which the env var usage assertion is done.
func DurationFlagDepth(
	depth int, f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo,
) {
	f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo, depth+1)
}

// VarFlag creates a custom-variable flag and registers it with the FlagSet.
// The default value is taken from the value's current value.
// See cli/context.go to initialize defaults.
func VarFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	VarFlagDepth(1, f, value, flagInfo)
}

// VarFlagDepth is like VarFlag but the caller can control the
// call level at which the env var usage assertion is done.
func VarFlagDepth(depth int, f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	f.VarP(value, flagInfo.Name, flagInfo.Shorthand, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo, depth+1)
}

// StringSliceFlag creates a string slice flag and registers it with the FlagSet.
// The default value is taken from the value's current value.
// See cli/context.go to initialize defaults.
func StringSliceFlag(f *pflag.FlagSet, valPtr *[]string, flagInfo cliflags.FlagInfo) {
	StringSliceFlagDepth(1, f, valPtr, flagInfo)
}

// StringSliceFlagDepth is like StringSliceFlag but the caller can control the
// call level at which the env var usage assertion is done.
func StringSliceFlagDepth(
	depth int, f *pflag.FlagSet, valPtr *[]string, flagInfo cliflags.FlagInfo,
) {
	f.StringSliceVar(valPtr, flagInfo.Name, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo, depth+1)
}
