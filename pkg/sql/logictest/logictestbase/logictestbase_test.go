// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logictestbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
)

func TestLogicTestMixedVersionConfigs(t *testing.T) {
	// Verify there is a mixed-version config for each supported release.
	for _, v := range clusterversion.SupportedPreviousReleases() {
		t.Run(v.String(), func(t *testing.T) {
			for _, c := range LogicTestConfigs {
				if c.DisableUpgrade && c.BootstrapVersion == v {
					return
				}
			}
			t.Errorf("no mixed-version config for %v", v)
		})
	}
}
