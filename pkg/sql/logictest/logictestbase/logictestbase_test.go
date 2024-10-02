// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logictestbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
)

func TestLogicTestMixedVersionConfigs(t *testing.T) {
	// Verify there is a mixed-version config for each supported release in the
	// default set.
	for _, v := range clusterversion.SupportedPreviousReleases() {
		t.Run(v.String(), func(t *testing.T) {
			for _, cIdx := range DefaultConfig {
				c := LogicTestConfigs[cIdx]
				if c.DisableUpgrade && c.BootstrapVersion == v {
					return
				}
			}
			t.Errorf("no mixed-version config for %v", v)
		})
	}
}
