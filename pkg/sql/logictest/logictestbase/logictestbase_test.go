// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logictestbase

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
)

func TestLogicTestMixedVersionConfigs(t *testing.T) {
	// Verify there is a mixed-version config for each supported release in the
	// default set.
	for _, v := range clusterversion.SupportedPreviousReleases() {
		t.Run(v.String(), func(t *testing.T) {
			for _, cIdx := range DefaultConfigSets[DefaultConfigSet] {
				c := LogicTestConfigs[cIdx]
				if c.DisableUpgrade && c.BootstrapVersion == v {
					return
				}
			}
			t.Errorf("no mixed-version config for %v", v)
		})
	}
}

func TestLogicTestCockroachGoTestserverConfigs(t *testing.T) {
	var expected []string
	for _, cfg := range LogicTestConfigs {
		if cfg.UseCockroachGoTestserver {
			expected = append(expected, cfg.Name)
		}
	}
	slices.Sort(expected)
	var cfgs []string
	for _, cfg := range DefaultConfigSets["cockroach-go-testserver-configs"] {
		cfgs = append(cfgs, cfg.Name())
	}
	slices.Sort(cfgs)
	if !reflect.DeepEqual(cfgs, expected) {
		t.Fatalf(
			"cockroach-go-testserver-configs should contain %s but contains %s",
			strings.Join(expected, ","), strings.Join(cfgs, ","),
		)
	}
}
