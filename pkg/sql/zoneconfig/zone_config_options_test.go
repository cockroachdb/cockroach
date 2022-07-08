// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zoneconfig

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestZoneConfigOptions iterates over all zone config options and attempts to
// set and get random values for these options as a means of sanity check. For
// YAML values fixed strings are used.
func TestZoneConfigOptions(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for _, optionName := range GetOptionNames() {
		emptyConfig := zonepb.ZoneConfig{}
		// Validate all options exist.
		option := GetOption(tree.Name(optionName))
		require.NotNil(t, option)
		// Generate a random value and try to set the option.
		val := randgen.RandDatum(rng, option.requiredType, false)
		// Some options are YAML so use fixed strings for them.
		if optionName == "constraints" ||
			optionName == "voter_constraints" {
			val = tree.NewDString("- +region=us-east1'\n")
		} else if optionName == "lease_preferences" {
			val = tree.NewDString("- - +zone=us-east2-b\n")
		}
		// Sanity check the check routines.
		featureCheckInvoked := false
		featureCheckFailError := errors.AssertionFailedf("feature not supported")
		err := option.CheckAllowed(ctx, func(featureName string) error {
			featureCheckInvoked = true
			return featureCheckFailError
		}, val)
		if option.checkAllowed == nil ||
			!featureCheckInvoked {
			require.NoError(t, err)
		} else {
			require.Error(t, err, featureCheckFailError)
		}
		// Set the option on the empty config.
		option.Set(&emptyConfig, val)
		// Get the option value back.
		newVal, err := option.Get(&emptyConfig)
		require.NoError(t, err)
		require.Equalf(t, val.String(), newVal.String(), "failed for option %v", optionName)
	}
}
