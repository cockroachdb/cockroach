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
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	// envValueAnnotationKey is the map key used in pflag.Flag instances
	// to associate flags with a possible default value set by an
	// env var.
	envValueAnnotationKey = "envvalue"
)

// RegisterEnvVarDefault registers a deferred initialization of a flag
// from an environment variable.
// The caller is responsible for ensuring that the flagInfo has been
// defined in the FlagSet already.
func RegisterEnvVarDefault(f *pflag.FlagSet, flagInfo cliflags.FlagInfo) {
	if flagInfo.EnvVar == "" {
		return
	}

	value, set := envutil.EnvString(flagInfo.EnvVar, 2)
	if !set {
		// Env var is not set. Nothing to do.
		return
	}

	if err := f.SetAnnotation(flagInfo.Name, envValueAnnotationKey, []string{flagInfo.EnvVar, value}); err != nil {
		// This should never happen: an error is only returned if the flag
		// name was not defined yet.
		panic(err)
	}
}

// ProcessEnvVarDefaults injects the current value of flag-related
// environment variables into the initial value of the settings linked
// to the flags, during initialization and before the command line is
// actually parsed. For example, it will inject the value of
// $COCKROACH_URL into the urlParser object linked to the --url flag.
func ProcessEnvVarDefaults(cmd *cobra.Command) error {
	fl := FlagSetForCmd(cmd)

	var retErr error
	fl.VisitAll(func(f *pflag.Flag) {
		envv, ok := f.Annotations[envValueAnnotationKey]
		if !ok || len(envv) < 2 {
			// No env var associated. Nothing to do.
			return
		}
		varName, value := envv[0], envv[1]
		if err := fl.Set(f.Name, value); err != nil {
			retErr = errors.CombineErrors(retErr,
				errors.Wrapf(err, "setting --%s from %s", f.Name, varName))
		}
	})
	return retErr
}
