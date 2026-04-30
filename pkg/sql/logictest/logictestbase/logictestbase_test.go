// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logictestbase

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestLogicTestMixedVersionConfigs(t *testing.T) {
	// Verify there is a mixed-version config for each supported release in the
	// default set, either as a static config or within a metamorphic config's
	// MixedVersionOptions.
	for _, v := range clusterversion.SupportedPreviousReleases() {
		t.Run(v.String(), func(t *testing.T) {
			for _, cIdx := range DefaultConfigSets[DefaultConfigSet] {
				c := LogicTestConfigs[cIdx]
				if c.DisableUpgrade && c.BootstrapVersion == v {
					return
				}
				if c.MetamorphicKnobs != nil {
					for _, opt := range c.MetamorphicKnobs.MixedVersionOptions {
						if opt == v {
							return
						}
					}
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

func TestResolveMetamorphic(t *testing.T) {
	allOff := MetamorphicKnobs{}
	noBlocklist := map[string]struct{}(nil)

	tests := []struct {
		name                         string
		knobs                        MetamorphicKnobs
		blocklist                    map[string]struct{}
		expectedEquivConfigs         []string
		expectedDisableDeclarativeSC bool
		expectedDisableSchemaLocked  bool
		expectedOverrideVectorize    string
		expectedPrepareQueries       bool
		expectedSQLExecUseDisk       bool
		expectedIsolationLevel       tree.IsolationLevel
		expectedUseSecondaryTenant   TenantMode
		expectedUseFakeSpanResolver  bool
		expectedBootstrapVersion     clusterversion.Key
		expectedDisableUpgrade       bool
		expectedSkip                 bool
	}{
		{
			name:                       "all knobs off",
			knobs:                      allOff,
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "all knobs on",
			knobs: MetamorphicKnobs{
				LegacySchemaChangerProbability: 1,
				VecOffProbability:              1,
				PrepareQueriesProbability:      1,
				DiskSpillProbability:           1,
				IsolationLevelWeights: map[tree.IsolationLevel]float64{
					tree.ReadCommittedIsolation: 1,
				},
				TenantProbability:   1,
				FakedistProbability: 1,
			},
			expectedEquivConfigs: []string{
				"local-legacy-schema-changer",
				"local-vec-off",
				"local-prepared",
				"local-read-committed",
				"3node-tenant",
				"fakedist",
				"fakedist-vec-off",
				"fakedist-disk",
			},
			expectedDisableDeclarativeSC: true,
			expectedDisableSchemaLocked:  true,
			expectedOverrideVectorize:    "off",
			expectedPrepareQueries:       true,
			expectedSQLExecUseDisk:       false,
			expectedIsolationLevel:       tree.ReadCommittedIsolation,
			expectedUseSecondaryTenant:   Always,
			expectedUseFakeSpanResolver:  true,
		},
		{
			name: "mixed version with no other knobs",
			knobs: MetamorphicKnobs{
				MixedVersionOptions: []clusterversion.Key{
					clusterversion.V25_4,
				},
			},
			expectedEquivConfigs:       []string{"local-mixed-25.4"},
			expectedBootstrapVersion:   clusterversion.V25_4,
			expectedDisableUpgrade:     true,
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "mixed version can have other knobs too",
			knobs: MetamorphicKnobs{
				LegacySchemaChangerProbability: 1,
				VecOffProbability:              1,
				MixedVersionOptions: []clusterversion.Key{
					clusterversion.V25_4,
				},
			},
			expectedEquivConfigs: []string{
				"local-mixed-25.4",
				"local-legacy-schema-changer",
				"local-vec-off",
			},
			expectedBootstrapVersion:     clusterversion.V25_4,
			expectedDisableUpgrade:       true,
			expectedDisableDeclarativeSC: true,
			expectedDisableSchemaLocked:  true,
			expectedOverrideVectorize:    "off",
			expectedUseSecondaryTenant:   Never,
		},
		{
			name: "legacy schema changer only",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.LegacySchemaChangerProbability = 1
				return k
			}(),
			expectedEquivConfigs:         []string{"local-legacy-schema-changer"},
			expectedDisableDeclarativeSC: true,
			expectedDisableSchemaLocked:  true,
			expectedUseSecondaryTenant:   Never,
		},
		{
			name: "vec-off only",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.VecOffProbability = 1
				return k
			}(),
			expectedEquivConfigs:       []string{"local-vec-off"},
			expectedOverrideVectorize:  "off",
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "prepared queries only",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.PrepareQueriesProbability = 1
				return k
			}(),
			expectedEquivConfigs:       []string{"local-prepared"},
			expectedPrepareQueries:     true,
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "disk spill only",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.DiskSpillProbability = 1
				return k
			}(),
			expectedEquivConfigs:       []string{"disk"},
			expectedSQLExecUseDisk:     true,
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "tenant only",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.TenantProbability = 1
				return k
			}(),
			expectedEquivConfigs:       []string{"3node-tenant"},
			expectedUseSecondaryTenant: Always,
		},
		{
			name: "fakedist only",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.FakedistProbability = 1
				return k
			}(),
			expectedEquivConfigs:        []string{"fakedist"},
			expectedUseFakeSpanResolver: true,
			expectedUseSecondaryTenant:  Never,
		},
		{
			name: "isolation level read committed",
			knobs: MetamorphicKnobs{
				IsolationLevelWeights: map[tree.IsolationLevel]float64{
					tree.ReadCommittedIsolation: 1,
				},
			},
			expectedEquivConfigs:       []string{"local-read-committed"},
			expectedIsolationLevel:     tree.ReadCommittedIsolation,
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "isolation level repeatable read",
			knobs: MetamorphicKnobs{
				IsolationLevelWeights: map[tree.IsolationLevel]float64{
					tree.RepeatableReadIsolation: 1,
				},
			},
			expectedEquivConfigs:       []string{"local-repeatable-read"},
			expectedIsolationLevel:     tree.RepeatableReadIsolation,
			expectedUseSecondaryTenant: Never,
		},
		{
			name:                       "isolation level serializable",
			knobs:                      MetamorphicKnobs{},
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "compound fakedist-vec-off",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.VecOffProbability = 1
				k.FakedistProbability = 1
				return k
			}(),
			expectedEquivConfigs: []string{
				"local-vec-off", "fakedist", "fakedist-vec-off",
			},
			expectedOverrideVectorize:   "off",
			expectedUseFakeSpanResolver: true,
			expectedUseSecondaryTenant:  Never,
		},
		{
			name: "compound fakedist-disk",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.DiskSpillProbability = 1
				k.FakedistProbability = 1
				return k
			}(),
			expectedEquivConfigs: []string{
				"fakedist", "fakedist-disk", "disk",
			},
			expectedSQLExecUseDisk:      true,
			expectedUseFakeSpanResolver: true,
			expectedUseSecondaryTenant:  Never,
		},
		{
			name: "compound fakedist-vec-off and fakedist-disk",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.VecOffProbability = 1
				k.DiskSpillProbability = 1
				k.FakedistProbability = 1
				return k
			}(),
			expectedEquivConfigs: []string{
				"local-vec-off", "fakedist", "fakedist-vec-off", "fakedist-disk",
			},
			expectedOverrideVectorize:   "off",
			expectedSQLExecUseDisk:      false,
			expectedUseFakeSpanResolver: true,
			expectedUseSecondaryTenant:  Never,
		},
		{
			name: "vec-off without fakedist has no compound equivalent",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.VecOffProbability = 1
				return k
			}(),
			expectedEquivConfigs:       []string{"local-vec-off"},
			expectedOverrideVectorize:  "off",
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "vec-off and disk without fakedist disables disk",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.VecOffProbability = 1
				k.DiskSpillProbability = 1
				return k
			}(),
			expectedEquivConfigs:       []string{"local-vec-off"},
			expectedOverrideVectorize:  "off",
			expectedSQLExecUseDisk:     false,
			expectedUseSecondaryTenant: Never,
		},
		// Blocklist tests.
		{
			name: "blocklist suppresses legacy schema changer",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.LegacySchemaChangerProbability = 1
				return k
			}(),
			blocklist:                  map[string]struct{}{"local-legacy-schema-changer": {}},
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "blocklist suppresses fakedist",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.FakedistProbability = 1
				return k
			}(),
			blocklist:                  map[string]struct{}{"fakedist": {}},
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "blocklist suppresses compound fakedist-disk",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.DiskSpillProbability = 1
				k.FakedistProbability = 1
				return k
			}(),
			blocklist: map[string]struct{}{"fakedist-disk": {}},
			expectedEquivConfigs: []string{
				"fakedist",
			},
			expectedUseFakeSpanResolver: true,
			expectedUseSecondaryTenant:  Never,
		},
		{
			name: "blocklist disk suppresses disk even with fakedist",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.DiskSpillProbability = 1
				k.FakedistProbability = 1
				return k
			}(),
			blocklist: map[string]struct{}{"disk": {}},
			expectedEquivConfigs: []string{
				"fakedist",
			},
			expectedUseFakeSpanResolver: true,
			expectedUseSecondaryTenant:  Never,
		},
		{
			name: "blocklist suppresses compound fakedist-vec-off",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.VecOffProbability = 1
				k.FakedistProbability = 1
				return k
			}(),
			blocklist: map[string]struct{}{"fakedist-vec-off": {}},
			expectedEquivConfigs: []string{
				"fakedist",
			},
			expectedUseFakeSpanResolver: true,
			expectedUseSecondaryTenant:  Never,
		},
		{
			name: "blocklist suppresses read committed isolation",
			knobs: MetamorphicKnobs{
				IsolationLevelWeights: map[tree.IsolationLevel]float64{
					tree.ReadCommittedIsolation: 1,
				},
			},
			blocklist:                  map[string]struct{}{"local-read-committed": {}},
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "blocklist suppresses tenant",
			knobs: func() MetamorphicKnobs {
				k := allOff
				k.TenantProbability = 1
				return k
			}(),
			blocklist:                  map[string]struct{}{"3node-tenant": {}},
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "blocklist filters mixed version options",
			knobs: MetamorphicKnobs{
				MixedVersionOptions: []clusterversion.Key{
					clusterversion.V25_4,
					clusterversion.V26_1,
					clusterversion.V26_2,
				},
			},
			blocklist: map[string]struct{}{
				"local-mixed-25.4": {},
				"local-mixed-26.1": {},
			},
			expectedEquivConfigs:       []string{"local-mixed-26.2"},
			expectedBootstrapVersion:   clusterversion.V26_2,
			expectedDisableUpgrade:     true,
			expectedUseSecondaryTenant: Never,
		},
		{
			name: "blocklist blocks all mixed version options",
			knobs: MetamorphicKnobs{
				MixedVersionOptions: []clusterversion.Key{
					clusterversion.V25_4,
					clusterversion.V26_1,
				},
			},
			blocklist: map[string]struct{}{
				"local-mixed-25.4": {},
				"local-mixed-26.1": {},
			},
			expectedSkip: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := TestClusterConfig{
				Name:             "test-meta",
				IsMetamorphic:    true,
				MetamorphicKnobs: &tc.knobs,
			}
			bl := tc.blocklist
			if bl == nil {
				bl = noBlocklist
			}
			rng := rand.New(rand.NewSource(0))
			resolved, skip := cfg.ResolveMetamorphic(rng, bl)

			require.Equal(t, tc.expectedSkip, skip)
			if skip {
				return
			}
			require.False(t, resolved.IsMetamorphic)
			require.Nil(t, resolved.MetamorphicKnobs)
			if tc.expectedEquivConfigs == nil {
				require.Nil(t, resolved.EquivalentConfigs)
			} else {
				require.Equal(t, tc.expectedEquivConfigs, resolved.EquivalentConfigs)
			}
			require.Equal(
				t, tc.expectedDisableDeclarativeSC, resolved.DisableDeclarativeSchemaChanger,
			)
			require.Equal(
				t, tc.expectedDisableSchemaLocked, resolved.DisableSchemaLockedByDefault,
			)
			require.Equal(t, tc.expectedOverrideVectorize, resolved.OverrideVectorize)
			require.Equal(t, tc.expectedPrepareQueries, resolved.PrepareQueries)
			require.Equal(t, tc.expectedSQLExecUseDisk, resolved.SQLExecUseDisk)
			require.Equal(t, tc.expectedIsolationLevel, resolved.EnableDefaultIsolationLevel)
			require.Equal(t, tc.expectedUseSecondaryTenant, resolved.UseSecondaryTenant)
			require.Equal(t, tc.expectedUseFakeSpanResolver, resolved.UseFakeSpanResolver)
			require.Equal(t, tc.expectedBootstrapVersion, resolved.BootstrapVersion)
			require.Equal(t, tc.expectedDisableUpgrade, resolved.DisableUpgrade)
		})
	}

	t.Run("original config unchanged after resolve", func(t *testing.T) {
		knobs := MetamorphicKnobs{
			LegacySchemaChangerProbability: 1,
			VecOffProbability:              1,
		}
		cfg := TestClusterConfig{
			Name:             "test-meta",
			IsMetamorphic:    true,
			MetamorphicKnobs: &knobs,
		}
		rng := rand.New(rand.NewSource(0))
		_, _ = cfg.ResolveMetamorphic(rng, nil)

		require.True(t, cfg.IsMetamorphic)
		require.NotNil(t, cfg.MetamorphicKnobs)
		require.Equal(t, &knobs, cfg.MetamorphicKnobs)
	})

	t.Run("tenant knob off produces Never", func(t *testing.T) {
		cfg := TestClusterConfig{
			Name:             "test-meta",
			IsMetamorphic:    true,
			MetamorphicKnobs: &MetamorphicKnobs{},
		}
		rng := rand.New(rand.NewSource(0))
		resolved, _ := cfg.ResolveMetamorphic(rng, nil)
		require.Equal(t, Never, resolved.UseSecondaryTenant)
	})
}

func TestResolveMetamorphicBaseline(t *testing.T) {
	t.Run("non-mixed-version produces baseline with no knobs", func(t *testing.T) {
		cfg := TestClusterConfig{
			Name:          "test-meta",
			IsMetamorphic: true,
			MetamorphicKnobs: &MetamorphicKnobs{
				LegacySchemaChangerProbability: 1,
				VecOffProbability:              1,
				IsolationLevelWeights: map[tree.IsolationLevel]float64{
					tree.ReadCommittedIsolation: 1,
				},
				TenantProbability:   1,
				FakedistProbability: 1,
			},
		}
		resolved := cfg.ResolveMetamorphicBaseline()

		require.False(t, resolved.IsMetamorphic)
		require.Nil(t, resolved.MetamorphicKnobs)
		require.Nil(t, resolved.EquivalentConfigs)
		require.False(t, resolved.DisableDeclarativeSchemaChanger)
		require.Equal(t, "", resolved.OverrideVectorize)
		require.False(t, resolved.PrepareQueries)
		require.False(t, resolved.SQLExecUseDisk)
		require.Equal(t, tree.IsolationLevel(0), resolved.EnableDefaultIsolationLevel)
		require.Equal(t, Never, resolved.UseSecondaryTenant)
		require.False(t, resolved.UseFakeSpanResolver)
	})

	t.Run("mixed-version picks first option", func(t *testing.T) {
		cfg := TestClusterConfig{
			Name:          "test-meta",
			IsMetamorphic: true,
			MetamorphicKnobs: &MetamorphicKnobs{
				MixedVersionOptions: []clusterversion.Key{
					clusterversion.V25_4,
					clusterversion.V26_1,
				},
			},
		}
		resolved := cfg.ResolveMetamorphicBaseline()

		require.False(t, resolved.IsMetamorphic)
		require.Nil(t, resolved.MetamorphicKnobs)
		require.Equal(t, clusterversion.V25_4, resolved.BootstrapVersion)
		require.True(t, resolved.DisableUpgrade)
		require.Equal(t, []string{"local-mixed-25.4"}, resolved.EquivalentConfigs)
	})

	t.Run("original config unchanged", func(t *testing.T) {
		knobs := MetamorphicKnobs{
			LegacySchemaChangerProbability: 1,
		}
		cfg := TestClusterConfig{
			Name:             "test-meta",
			IsMetamorphic:    true,
			MetamorphicKnobs: &knobs,
		}
		_ = cfg.ResolveMetamorphicBaseline()

		require.True(t, cfg.IsMetamorphic)
		require.NotNil(t, cfg.MetamorphicKnobs)
	})
}

func TestIsEquivalentTo(t *testing.T) {
	t.Run("direct match", func(t *testing.T) {
		cfg := TestClusterConfig{
			EquivalentConfigs: []string{"local-vec-off", "fakedist"},
		}
		require.True(t, cfg.IsEquivalentTo("local-vec-off"))
		require.True(t, cfg.IsEquivalentTo("fakedist"))
		require.False(t, cfg.IsEquivalentTo("local"))
		require.False(t, cfg.IsEquivalentTo("local-meta"))
	})

	t.Run("config set alias match", func(t *testing.T) {
		cfg := TestClusterConfig{
			EquivalentConfigs: []string{"local-read-committed"},
		}
		require.True(t, cfg.IsEquivalentTo("weak-iso-level-configs"))
		require.True(t, cfg.IsEquivalentTo("enterprise-configs"))
	})

	t.Run("no equivalents", func(t *testing.T) {
		cfg := TestClusterConfig{}
		require.False(t, cfg.IsEquivalentTo("local"))
		require.False(t, cfg.IsEquivalentTo("fakedist"))
	})

	t.Run("non-existent config", func(t *testing.T) {
		cfg := TestClusterConfig{
			EquivalentConfigs: []string{"local-vec-off"},
		}
		require.False(t, cfg.IsEquivalentTo("does-not-exist"))
	})
}

func writeTestFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}

func TestIsNightlyOnlyConfig(t *testing.T) {
	tests := []struct {
		name       string
		configName string
		expected   bool
	}{
		{name: "in default set", configName: "local-meta", expected: false},
		{name: "in nightly set", configName: "local", expected: true},
		{name: "in neither set", configName: "5node", expected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx, ok := findLogicTestConfig(tc.configName)
			require.True(t, ok, "config %q must exist", tc.configName)
			require.Equal(t, tc.expected, IsNightlyOnlyConfig(idx))
		})
	}

	t.Run("all default configs are not nightly-only", func(t *testing.T) {
		for _, idx := range DefaultConfigSets[DefaultConfigSet] {
			require.False(t, IsNightlyOnlyConfig(idx),
				"default config %s should not be nightly-only", idx.Name())
		}
	})

	t.Run("all nightly configs are nightly-only", func(t *testing.T) {
		for _, idx := range DefaultConfigSets[NightlyDefaultConfigSet] {
			require.True(t, IsNightlyOnlyConfig(idx),
				"nightly config %s should be nightly-only", idx.Name())
		}
	})
}

func TestReadTestFileConfigs(t *testing.T) {
	defaults := makeConfigSet("local", "fakedist", "local-meta")
	defaultNames := defaults.ConfigNames()

	dir := t.TempDir()

	tests := []struct {
		name                      string
		fileContent               string
		expectedConfigNames       []string
		expectedNonMetaBatchSizes bool
		expectedBlockedNil        bool
		expectedBlockedConfigs    map[string]struct{}
		expectedUsesDefaults      bool
	}{
		{
			name:                 "no directive",
			fileContent:          "statement ok\nSELECT 1\n",
			expectedConfigNames:  defaultNames,
			expectedBlockedNil:   true,
			expectedUsesDefaults: true,
		},
		{
			name:                 "explicit default-configs alias",
			fileContent:          "# LogicTest: default-configs\n",
			expectedConfigNames:  DefaultConfigSets[DefaultConfigSet].ConfigNames(),
			expectedUsesDefaults: true,
		},
		{
			name:                "single named config",
			fileContent:         "# LogicTest: local\n",
			expectedConfigNames: []string{"local"},
		},
		{
			name:                "blocklist only applies to defaults",
			fileContent:         "# LogicTest: !3node-tenant\n",
			expectedConfigNames: defaultNames,
			expectedBlockedConfigs: map[string]struct{}{
				"3node-tenant": {},
			},
			expectedUsesDefaults: true,
		},
		{
			name:                "named config plus blocklist",
			fileContent:         "# LogicTest: local !fakedist\n",
			expectedConfigNames: []string{"local"},
			expectedBlockedConfigs: map[string]struct{}{
				"fakedist": {},
			},
		},
		{
			name:                "config set alias",
			fileContent:         "# LogicTest: 5node-default-configs\n",
			expectedConfigNames: []string{"5node", "5node-disk"},
		},
		{
			name:                      "metamorphic-batch-sizes blocklist",
			fileContent:               "# LogicTest: !metamorphic-batch-sizes\n",
			expectedConfigNames:       defaultNames,
			expectedNonMetaBatchSizes: true,
			expectedBlockedConfigs: map[string]struct{}{
				"metamorphic-batch-sizes": {},
			},
			expectedUsesDefaults: true,
		},
		{
			name:        "blocklist config set alias expands",
			fileContent: "# LogicTest: !3node-tenant-default-configs\n",
			expectedBlockedConfigs: map[string]struct{}{
				"3node-tenant":             {},
				"3node-tenant-multiregion": {},
			},
			expectedUsesDefaults: true,
		},
		{
			name:                 "non-comment before directive returns defaults",
			fileContent:          "statement ok\n# LogicTest: local\n",
			expectedConfigNames:  defaultNames,
			expectedBlockedNil:   true,
			expectedUsesDefaults: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTestFile(t, dir, strings.ReplaceAll(tc.name, " ", "_"), tc.fileContent)
			configs, nonMetaBatch, blocked, usesDefaults :=
				ReadTestFileConfigs(t, path, defaults)

			require.Equal(t, tc.expectedUsesDefaults, usesDefaults, "usesDefaults")
			require.Equal(t, tc.expectedNonMetaBatchSizes, nonMetaBatch, "nonMetaBatchSizes")

			if tc.expectedConfigNames != nil {
				require.Equal(t, tc.expectedConfigNames, configs.ConfigNames(), "config names")
			}

			if tc.expectedBlockedNil {
				require.Nil(t, blocked, "blockedConfigs should be nil")
			} else if tc.expectedBlockedConfigs != nil {
				require.Equal(t, tc.expectedBlockedConfigs, blocked, "blockedConfigs")
			}
		})
	}

	t.Run("nonexistent file", func(t *testing.T) {
		configs, nonMetaBatch, blocked, usesDefaults :=
			ReadTestFileConfigs(t, "/nonexistent/path/test.test", defaults)
		require.Nil(t, configs)
		require.False(t, nonMetaBatch)
		require.Nil(t, blocked)
		require.False(t, usesDefaults)
	})
}

func TestEnumerateConfigs(t *testing.T) {
	defaultConfigs := DefaultConfigSets[DefaultConfigSet]
	nightlyConfigs := DefaultConfigSets[NightlyDefaultConfigSet]
	unionSet := make(map[ConfigIdx]bool, len(defaultConfigs)+len(nightlyConfigs))
	for _, idx := range defaultConfigs {
		unionSet[idx] = true
	}
	for _, idx := range nightlyConfigs {
		unionSet[idx] = true
	}

	t.Run("default and nightly sets are disjoint", func(t *testing.T) {
		defaultSet := make(map[ConfigIdx]bool, len(defaultConfigs))
		for _, idx := range defaultConfigs {
			defaultSet[idx] = true
		}
		for _, idx := range nightlyConfigs {
			require.False(t, defaultSet[idx],
				"config %s is in both DefaultConfigSet and NightlyDefaultConfigSet", idx.Name())
		}
	})

	t.Run("no directive enumerates under all union configs", func(t *testing.T) {
		dir := t.TempDir()
		path := writeTestFile(t, dir, "test_nodirective", "statement ok\nSELECT 1\n")

		result, err := EnumerateConfigs(filepath.Join(dir, "test_*"))
		require.NoError(t, err)

		for idx := range unionSet {
			require.Contains(t, result[idx], path,
				"config %s should include the file", idx.Name())
		}
		for i, paths := range result {
			if !unionSet[ConfigIdx(i)] {
				require.NotContains(t, paths, path,
					"config %s should not include the file", ConfigIdx(i).Name())
			}
		}
	})

	t.Run("explicit config enumerates only under named config", func(t *testing.T) {
		dir := t.TempDir()
		path := writeTestFile(t, dir, "test_explicit", "# LogicTest: 5node\n")

		result, err := EnumerateConfigs(filepath.Join(dir, "test_*"))
		require.NoError(t, err)

		fiveNodeIdx, ok := findLogicTestConfig("5node")
		require.True(t, ok)
		require.Contains(t, result[fiveNodeIdx], path)

		for i, paths := range result {
			if ConfigIdx(i) != fiveNodeIdx {
				require.NotContains(t, paths, path,
					"config %s should not include the file", ConfigIdx(i).Name())
			}
		}
	})

	t.Run("blocklist removes config from union defaults", func(t *testing.T) {
		dir := t.TempDir()
		path := writeTestFile(t, dir, "test_blocklist", "# LogicTest: !3node-tenant\n")

		result, err := EnumerateConfigs(filepath.Join(dir, "test_*"))
		require.NoError(t, err)

		blockedIdx, ok := findLogicTestConfig("3node-tenant")
		require.True(t, ok)
		require.NotContains(t, result[blockedIdx], path,
			"blocked config should not include the file")

		for idx := range unionSet {
			if idx == blockedIdx {
				continue
			}
			require.Contains(t, result[idx], path,
				"config %s should include the file", idx.Name())
		}
	})
}
