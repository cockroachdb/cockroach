// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logictestbase

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var (
	printBlocklistIssues = flag.Bool(
		"print-blocklist-issues", false,
		"for any test files that contain a blocklist directive, print a link to the associated issue",
	)
)

// TestClusterConfig is a struct representing a single logictest
// configuration. LogicTestConfigs contains a list of the possible
// TestClusterConfigs.
type TestClusterConfig struct {
	// Name is the name of the config (used for subtest names).
	Name     string
	NumNodes int
	// TODO(asubiotto): The fake span resolver does not currently play well with
	// contention events and tracing (see #61438).
	UseFakeSpanResolver bool
	// if non-empty, overrides the default distsql mode.
	OverrideDistSQLMode string
	// if non-empty, overrides the default vectorize mode.
	OverrideVectorize string
	// if set, queries using distSQL processors or vectorized operators that can
	// fall back to disk do so immediately, using only their disk-based
	// implementation.
	SQLExecUseDisk bool
	// if set and the -test.short flag is passed, skip this config.
	SkipShort bool
	// If not empty, bootstrapVersion controls what version the cluster will be
	// bootstrapped at.
	BootstrapVersion clusterversion.Key
	// DisableUpgrade prevents the cluster from automatically upgrading to the
	// latest version.
	DisableUpgrade bool

	// If a config profile sets this to "Always", a SQL tenant server will
	// always be started and pointed at a node in the cluster.
	// Connections on behalf of the logic test will go to that tenant.
	// If set to "Never", the tenant server will never be started.
	// If set to "Random", the default randomization logic will be used.
	UseSecondaryTenant TenantMode

	// localities is set if nodes should be set to a particular locality.
	// Nodes are 1-indexed.
	Localities map[int]roachpb.Locality
	// BackupRestoreProbability will periodically backup the cluster and restore
	// it's state to a new cluster at random points during a logic test.
	BackupRestoreProbability float64
	// disableDeclarativeSchemaChanger will disable the declarative schema changer
	// for logictest.
	DisableDeclarativeSchemaChanger bool
	// disableLocalityOptimizedSearch disables the cluster setting
	// locality_optimized_partitioned_index_scan, which is enabled by default.
	DisableLocalityOptimizedSearch bool
	// EnableDefaultIsolationLevel uses the specified isolation level for all
	// transactions by default.
	EnableDefaultIsolationLevel tree.IsolationLevel
	// DeclarativeCorpusCollection enables support for collecting corpuses
	// for the declarative schema changer.
	DeclarativeCorpusCollection bool
	// UseCockroachGoTestserver determines if the logictest uses the
	// cockroach-go/testserver package to run the logic test.
	// This allows us to do testing on different binary versions or to
	// restart/upgrade nodes. This always bootstraps with the predecessor version
	// of the current commit, and upgrades to the current commit.
	UseCockroachGoTestserver bool
	// DisableSchemaLockedByDefault prevents tables from being created
	// with schema_locked by default.
	DisableSchemaLockedByDefault bool
	// PrepareQueries executes queries and statements with Prepare and Execute.
	PrepareQueries bool

	// IsMetamorphic indicates this config resolves knobs randomly per test.
	IsMetamorphic bool
	// MetamorphicKnobs defines the probabilities for each knob. Only used
	// when IsMetamorphic is true.
	MetamorphicKnobs *MetamorphicKnobs
	// EquivalentConfigs lists the names of static configs whose behavior the
	// resolved metamorphic config currently emulates. Populated by
	// ResolveMetamorphic; used by skipif/onlyif config matching.
	EquivalentConfigs []string
}

// MetamorphicKnobs defines the probabilities for metamorphic config resolution.
//
// TODO(msbutler): today, test authors use `skipif config <config-name>` to opt
// out of specific configs, which means we need a named config for every knob
// combination a test might want to skip (e.g. disk, fakedist-disk). This
// doesn't scale. Instead, each TestClusterConfig knob should map to its own
// skipif predicate (e.g. `skipif disk-spill`, `skipif vec-off`) so tests can
// skip based on individual knobs rather than compound config names.
type MetamorphicKnobs struct {
	LegacySchemaChangerProbability float64
	VecOffProbability              float64
	PrepareQueriesProbability      float64
	DiskSpillProbability           float64
	// IsolationLevelWeights maps isolation levels to their selection
	// probability. A single die roll partitions [0, 1) across these
	// levels in enum order. The sum of all weights must equal 1.0.
	// Serializable should be included explicitly for readability.
	IsolationLevelWeights map[tree.IsolationLevel]float64
	TenantProbability     float64
	// FakedistProbability controls fake span resolver; only for 3node-meta.
	FakedistProbability float64
	// MixedVersionOptions lists bootstrap versions to pick from; only for
	// local-mixed-meta.
	MixedVersionOptions []clusterversion.Key
}

// isoLevelConfigName returns the equivalent config name for a given isolation
// level (e.g. ReadCommittedIsolation → "local-read-committed").
func isoLevelConfigName(level tree.IsolationLevel) string {
	return "local-" + strings.ToLower(strings.ReplaceAll(level.String(), " ", "-"))
}

// mixedVersionName returns the config name for a given bootstrap version key
// (e.g. "local-mixed-25.4").
func mixedVersionName(key clusterversion.Key) string {
	v := clusterversion.RemoveDevOffset(key.Version())
	return fmt.Sprintf("local-mixed-%d.%d", v.Major, v.Minor)
}

// ResolveMetamorphic resolves a metamorphic config template into a concrete
// config with randomly chosen knobs. The returned config has IsMetamorphic set
// to false and EquivalentConfigs populated. The blocklist contains config names
// that must not be produced; corresponding knobs are forced off. If skip is
// true, all possible resolutions were blocked and the test should be skipped.
func (c TestClusterConfig) ResolveMetamorphic(
	rng *rand.Rand, blocklist map[string]struct{},
) (resolved TestClusterConfig, skip bool) {
	resolved = c
	resolved.IsMetamorphic = false
	resolved.MetamorphicKnobs = nil
	resolved.EquivalentConfigs = nil
	knobs := c.MetamorphicKnobs
	if knobs == nil {
		return resolved, false
	}

	blocked := func(name string) bool {
		_, ok := blocklist[name]
		return ok
	}

	if len(knobs.MixedVersionOptions) > 0 {
		var allowed []clusterversion.Key
		for _, key := range knobs.MixedVersionOptions {
			if !blocked(mixedVersionName(key)) {
				allowed = append(allowed, key)
			}
		}
		if len(allowed) == 0 {
			return resolved, true
		}
		chosen := allowed[rng.Intn(len(allowed))]
		resolved.BootstrapVersion = chosen
		resolved.DisableUpgrade = true
		resolved.EquivalentConfigs = append(
			resolved.EquivalentConfigs, mixedVersionName(chosen),
		)
	}

	if !blocked("local-legacy-schema-changer") &&
		rng.Float64() < knobs.LegacySchemaChangerProbability {
		resolved.DisableDeclarativeSchemaChanger = true
		resolved.DisableSchemaLockedByDefault = true
		resolved.EquivalentConfigs = append(
			resolved.EquivalentConfigs, "local-legacy-schema-changer",
		)
	}

	if !blocked("local-vec-off") && rng.Float64() < knobs.VecOffProbability {
		resolved.OverrideVectorize = "off"
		resolved.EquivalentConfigs = append(
			resolved.EquivalentConfigs, "local-vec-off",
		)
	}

	if !blocked("local-prepared") && rng.Float64() < knobs.PrepareQueriesProbability {
		resolved.PrepareQueries = true
		resolved.EquivalentConfigs = append(
			resolved.EquivalentConfigs, "local-prepared",
		)
	}

	if !blocked("disk") && rng.Float64() < knobs.DiskSpillProbability {
		resolved.SQLExecUseDisk = true
	}

	// Isolation level: single roll against cumulative weights, iterated in
	// enum order. When a level is blocked, rolls within its range fall
	// through to serializable (no config override is applied).
	isoLevels := make([]tree.IsolationLevel, 0, len(knobs.IsolationLevelWeights))
	var isoWeightSum float64
	for level, weight := range knobs.IsolationLevelWeights {
		isoLevels = append(isoLevels, level)
		isoWeightSum += weight
	}
	if len(isoLevels) > 0 && math.Abs(isoWeightSum-1.0) > 1e-9 {
		panic(fmt.Sprintf(
			"IsolationLevelWeights must sum to 1.0, got %f", isoWeightSum,
		))
	}
	slices.Sort(isoLevels)
	if len(isoLevels) > 0 {
		isoRoll := rng.Float64()
		var isoCumulative float64
		for _, level := range isoLevels {
			isoCumulative += knobs.IsolationLevelWeights[level]
			if isoRoll < isoCumulative {
				if level != tree.SerializableIsolation {
					configName := isoLevelConfigName(level)
					if !blocked(configName) {
						resolved.EnableDefaultIsolationLevel = level
						resolved.EquivalentConfigs = append(
							resolved.EquivalentConfigs, configName,
						)
					}
				}
				break
			}
		}
	}

	if !blocked("3node-tenant") && rng.Float64() < knobs.TenantProbability {
		resolved.UseSecondaryTenant = Always
		resolved.EquivalentConfigs = append(
			resolved.EquivalentConfigs, "3node-tenant",
		)
	} else {
		resolved.UseSecondaryTenant = Never
	}

	if !blocked("fakedist") && rng.Float64() < knobs.FakedistProbability {
		resolved.UseFakeSpanResolver = true
		resolved.EquivalentConfigs = append(
			resolved.EquivalentConfigs, "fakedist",
		)
	}

	// Add compound equivalents for fakedist combinations, but respect the blocklist: if a compound is
	// blocked, disable the non-fakedist knob to avoid the forbidden combination.
	//
	// TODO(msbutler): we could simplify this if we remove the UseFakeSpanResolver knob.
	if resolved.UseFakeSpanResolver {
		if resolved.OverrideVectorize == "off" {
			if blocked("fakedist-vec-off") {
				resolved.OverrideVectorize = ""
				resolved.EquivalentConfigs = slices.DeleteFunc(
					resolved.EquivalentConfigs,
					func(s string) bool { return s == "local-vec-off" },
				)
			} else {
				resolved.EquivalentConfigs = append(
					resolved.EquivalentConfigs, "fakedist-vec-off",
				)
			}
		}
		if resolved.SQLExecUseDisk {
			if blocked("fakedist-disk") {
				resolved.SQLExecUseDisk = false
			} else {
				resolved.EquivalentConfigs = append(
					resolved.EquivalentConfigs, "fakedist-disk",
				)
			}
		}
	}

	// When vectorization is off, disable disk spilling: the row-based disk
	// container doesn't support all column types (e.g. RECORD, #49975),
	// while the vectorized engine handles them via colserde.
	// TODO(msbutler): remove this guard once #49975 is resolved and the
	// row-based disk container supports all column types.
	if resolved.OverrideVectorize == "off" && resolved.SQLExecUseDisk {
		resolved.SQLExecUseDisk = false
	}

	if resolved.SQLExecUseDisk {
		resolved.EquivalentConfigs = append(resolved.EquivalentConfigs, "disk")
	}

	return resolved, false
}

// ResolveMetamorphicBaseline returns a concrete config with all knobs at
// their neutral/default positions. Used for --rewrite mode to ensure
// deterministic output.
func (c TestClusterConfig) ResolveMetamorphicBaseline() TestClusterConfig {
	resolved := c
	knobs := c.MetamorphicKnobs
	resolved.IsMetamorphic = false
	resolved.MetamorphicKnobs = nil
	resolved.EquivalentConfigs = nil
	resolved.UseSecondaryTenant = Never
	if knobs == nil {
		return resolved
	}
	// Mixed-version must resolve to a concrete bootstrap version because every
	// actual test run picks one. Choosing the first option deterministically
	// ensures --rewrite produces expectations that match real runs.
	if len(knobs.MixedVersionOptions) > 0 {
		chosen := knobs.MixedVersionOptions[0]
		resolved.BootstrapVersion = chosen
		resolved.DisableUpgrade = true
		resolved.EquivalentConfigs = append(
			resolved.EquivalentConfigs, mixedVersionName(chosen),
		)
	}
	return resolved
}

// IsEquivalentTo returns true if the given config name matches any of the
// resolved equivalent configs, either directly or via a config set alias.
func (c TestClusterConfig) IsEquivalentTo(configName string) bool {
	for _, eq := range c.EquivalentConfigs {
		if eq == configName {
			return true
		}
		if ConfigIsInDefaultList(eq, configName) {
			return true
		}
	}
	return false
}

// MetamorphicSummary returns a human-readable summary of the resolved
// metamorphic knobs for logging.
func (c TestClusterConfig) MetamorphicSummary() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("nodes=%d", c.NumNodes))
	if c.DisableDeclarativeSchemaChanger {
		parts = append(parts, "legacy-schema-changer=on")
	}
	if c.OverrideVectorize == "off" {
		parts = append(parts, "vectorize=off")
	}
	if c.PrepareQueries {
		parts = append(parts, "prepared-queries=on")
	}
	if c.SQLExecUseDisk {
		parts = append(parts, "disk-spill=on")
	}
	if c.EnableDefaultIsolationLevel != 0 {
		parts = append(parts, fmt.Sprintf("isolation=%s",
			strings.ToLower(strings.ReplaceAll(c.EnableDefaultIsolationLevel.String(), " ", "-"))))
	} else {
		parts = append(parts, "isolation=serializable")
	}
	if c.UseSecondaryTenant == Always {
		parts = append(parts, "tenant=on")
	}
	if c.UseFakeSpanResolver {
		parts = append(parts, "fake-span-resolver=on")
	}
	if c.BootstrapVersion != 0 {
		parts = append(parts, fmt.Sprintf(
			"bootstrap-version=%s", mixedVersionName(c.BootstrapVersion),
		))
	}
	if len(c.EquivalentConfigs) > 0 {
		parts = append(parts, fmt.Sprintf(
			"equivalent-to=[%s]", strings.Join(c.EquivalentConfigs, ","),
		))
	}
	return strings.Join(parts, ", ")
}

// TenantMode is the type of the UseSecondaryTenant field in TestClusterConfig.
type TenantMode int8

const (
	// Random is the default behavior.
	Random TenantMode = iota
	// Always will always start a tenant server.
	Always
	// Never will never start a tenant server.
	Never
)

const threeNodeTenantConfigName = "3node-tenant"

var multiregion9node3region3azsLocalities = map[int]roachpb.Locality{
	1: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az1"},
		},
	},
	2: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az2"},
		},
	},
	3: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az3"},
		},
	},
	4: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az1"},
		},
	},
	5: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az2"},
		},
	},
	6: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az3"},
		},
	},
	7: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az1"},
		},
	},
	8: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az2"},
		},
	},
	9: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az3"},
		},
	},
}

var multiregion15node5region3azsLocalities = map[int]roachpb.Locality{
	1: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az1"},
		},
	},
	2: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az2"},
		},
	},
	3: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ap-southeast-2"},
			{Key: "availability-zone", Value: "ap-az3"},
		},
	},
	4: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az1"},
		},
	},
	5: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az2"},
		},
	},
	6: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "ca-central-1"},
			{Key: "availability-zone", Value: "ca-az3"},
		},
	},
	7: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az1"},
		},
	},
	8: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az2"},
		},
	},
	9: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-az3"},
		},
	},
	10: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-west-1"},
			{Key: "availability-zone", Value: "usw-az1"},
		},
	},
	11: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-west-1"},
			{Key: "availability-zone", Value: "usw-az2"},
		},
	},
	12: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-west-1"},
			{Key: "availability-zone", Value: "usw-az3"},
		},
	},
	13: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central-1"},
			{Key: "availability-zone", Value: "usc-az1"},
		},
	},
	14: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central-1"},
			{Key: "availability-zone", Value: "usc-az2"},
		},
	},
	15: {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-central-1"},
			{Key: "availability-zone", Value: "usc-az3"},
		},
	},
}

// LogicTestConfigs contains all possible cluster configs. A test file can
// specify a list of configs to run in a file-level comment like:
//
//	# LogicTest: default distsql
//
// The test is run once on each configuration (in different subtests).
//
// If no configs are indicated in a test file, the default configs are used.
// See DefaultConfigNames for the list of default configs.
//
// Note: If you add a new config, you should run `./dev gen testlogic`.
//
// Note: If you add a new config, it will not automatically run in CI for any
// test files. It must either be included in the list of default configs or name
// explicitly in a file-level "LogicTest:" comment.
var LogicTestConfigs = []TestClusterConfig{
	{
		Name:                "local",
		NumNodes:            1,
		OverrideDistSQLMode: "off",
		// local is the configuration where we run all tests which have bad
		// interactions with the default test tenant.
		//
		// TODO(#156124): We should review this choice. Why can't we use "Random"
		// here? If there are specific tests that are incompatible, we can
		// flag them to run only in a separate config.
		UseSecondaryTenant:          Never,
		DeclarativeCorpusCollection: true,
	},
	{
		Name:                            "local-legacy-schema-changer",
		NumNodes:                        1,
		OverrideDistSQLMode:             "off",
		DisableDeclarativeSchemaChanger: true,
		DisableSchemaLockedByDefault:    true,
	},
	{
		Name:                "local-vec-off",
		NumNodes:            1,
		OverrideDistSQLMode: "off",
		OverrideVectorize:   "off",
	},
	{
		Name:                        "local-read-committed",
		NumNodes:                    1,
		OverrideDistSQLMode:         "off",
		EnableDefaultIsolationLevel: tree.ReadCommittedIsolation,
	},
	{
		Name:                        "local-repeatable-read",
		NumNodes:                    1,
		OverrideDistSQLMode:         "off",
		EnableDefaultIsolationLevel: tree.RepeatableReadIsolation,
	},
	{
		Name:                "local-prepared",
		NumNodes:            1,
		OverrideDistSQLMode: "off",
		PrepareQueries:      true,
	},
	{
		Name:                "fakedist",
		NumNodes:            3,
		UseFakeSpanResolver: true,
		OverrideDistSQLMode: "on",
	},
	{
		Name:                "fakedist-vec-off",
		NumNodes:            3,
		UseFakeSpanResolver: true,
		OverrideDistSQLMode: "on",
		OverrideVectorize:   "off",
	},
	{
		Name:                "fakedist-disk",
		NumNodes:            3,
		UseFakeSpanResolver: true,
		OverrideDistSQLMode: "on",
		SQLExecUseDisk:      true,
		SkipShort:           true,
	},
	{
		Name:                "disk",
		NumNodes:            3,
		OverrideDistSQLMode: "on",
		SQLExecUseDisk:      true,
	},
	{
		Name:                "5node",
		NumNodes:            5,
		OverrideDistSQLMode: "on",
		// Have to disable the default test tenant here as there are test run in
		// this mode which try to modify zone configurations and we're more
		// restrictive in the way we allow zone configs to be modified by
		// secondary tenants. See #100787 for more info.
		//
		// TODO(#156124): We should review this choice. Zone configs have
		// been supported for secondary tenants since v22.2.
		// Should this config use "Random" instead?
		UseSecondaryTenant: Never,
	},
	{
		Name:                "5node-disk",
		NumNodes:            5,
		OverrideDistSQLMode: "on",
		SQLExecUseDisk:      true,
		SkipShort:           true,
	},
	{
		// 3node-tenant is a config that runs the test as a SQL tenant. This config
		// can only be run with a CCL binary, so is a noop if run through the normal
		// logictest command.
		// To run a logic test with this config as a directive, run:
		// dev testlogic base --files 3node-tenant --subtest $SUBTEST
		Name:                        threeNodeTenantConfigName,
		NumNodes:                    3,
		UseSecondaryTenant:          Always,
		OverrideDistSQLMode:         "on",
		DeclarativeCorpusCollection: true,
	},
	{
		// 3node-tenant-multiregion is a config that runs the test as a SQL tenant
		// with SQL instances and KV nodes in multiple regions. This config can only
		// be run with a CCL binary, so is a noop if run through the normal
		// logictest command.
		// To run a logic test with this config as a directive, run:
		// dev testlogic base --files 3node-tenant-multiregion --subtests $SUBTESTS
		Name:                        "3node-tenant-multiregion",
		NumNodes:                    3,
		UseSecondaryTenant:          Always,
		OverrideDistSQLMode:         "on",
		DeclarativeCorpusCollection: true,
		Localities: map[int]roachpb.Locality{
			1: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "test"},
				},
			},
			2: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "test1"},
				},
			},
			3: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "test2"},
				},
			},
		},
	},
	// Regions and zones below are named deliberately, and contain "-"'s to be reflective
	// of the naming convention in public clouds.  "-"'s are handled differently in SQL
	// (they're double double quoted) so we explicitly test them here to ensure that
	// the multi-region code handles them correctly.

	{
		Name:     "multiregion-invalid-locality",
		NumNodes: 3,
		Localities: map[int]roachpb.Locality{
			1: {
				Tiers: []roachpb.Tier{
					{Key: "invalid-region-setup", Value: "test1"},
					{Key: "availability-zone", Value: "test1-az1"},
				},
			},
			2: {
				Tiers: []roachpb.Tier{},
			},
			3: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "test1"},
					{Key: "availability-zone", Value: "test1-az3"},
				},
			},
		},
	},
	{
		Name:                        "multiregion-3node-3superlongregions",
		NumNodes:                    3,
		DeclarativeCorpusCollection: true,
		Localities: map[int]roachpb.Locality{
			1: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "veryveryveryveryveryveryverylongregion1"},
				},
			},
			2: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "veryveryveryveryveryveryverylongregion2"},
				},
			},
			3: {
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "veryveryveryveryveryveryverylongregion3"},
				},
			},
		},
	},
	{
		Name:       "multiregion-9node-3region-3azs",
		NumNodes:   9,
		Localities: multiregion9node3region3azsLocalities,
		// Need to disable the default test tenant here until we have the
		// locality optimized search working in multi-tenant configurations.
		// Tracked with #80678.
		//
		// TODO(#156124): We've fixed that issue. Review this choice. Can
		// it be "Random" instead? Then we can merge it with the next
		// config below.
		UseSecondaryTenant:          Never,
		DeclarativeCorpusCollection: true,
	},
	{
		Name:                        "multiregion-9node-3region-3azs-tenant",
		NumNodes:                    9,
		Localities:                  multiregion9node3region3azsLocalities,
		UseSecondaryTenant:          Always,
		DeclarativeCorpusCollection: true,
	},
	{
		Name:              "multiregion-9node-3region-3azs-vec-off",
		NumNodes:          9,
		Localities:        multiregion9node3region3azsLocalities,
		OverrideVectorize: "off",
	},
	{
		Name:                           "multiregion-9node-3region-3azs-no-los",
		NumNodes:                       9,
		Localities:                     multiregion9node3region3azsLocalities,
		DisableLocalityOptimizedSearch: true,
	},
	{
		Name:       "multiregion-15node-5region-3azs",
		NumNodes:   15,
		Localities: multiregion15node5region3azsLocalities,
	},
	{
		// This config runs tests using 25.4 cluster version, simulating a node that
		// is operating in a mixed-version cluster.
		Name:                        "local-mixed-25.4",
		NumNodes:                    1,
		OverrideDistSQLMode:         "off",
		BootstrapVersion:            clusterversion.V25_4,
		DisableUpgrade:              true,
		DeclarativeCorpusCollection: true,
	},
	{
		// This config runs tests using 26.1 cluster version, simulating a node that
		// is operating in a mixed-version cluster.
		Name:                        "local-mixed-26.1",
		NumNodes:                    1,
		OverrideDistSQLMode:         "off",
		BootstrapVersion:            clusterversion.V26_1,
		DisableUpgrade:              true,
		DeclarativeCorpusCollection: true,
	},
	{
		// This config runs tests using 26.2 cluster version, simulating a node that
		// is operating in a mixed-version cluster.
		Name:                        "local-mixed-26.2",
		NumNodes:                    1,
		OverrideDistSQLMode:         "off",
		BootstrapVersion:            clusterversion.V26_2,
		DisableUpgrade:              true,
		DeclarativeCorpusCollection: true,
	},
	{
		// This config runs a cluster with 3 nodes, with a separate process per
		// node. The nodes initially start on v25.4.
		Name:                     "cockroach-go-testserver-25.4",
		UseCockroachGoTestserver: true,
		BootstrapVersion:         clusterversion.V25_4,
		NumNodes:                 3,
	},
	{
		// This config runs a cluster with 3 nodes, with a separate process per
		// node. The nodes initially start on v26.1.
		Name:                     "cockroach-go-testserver-26.1",
		UseCockroachGoTestserver: true,
		BootstrapVersion:         clusterversion.V26_1,
		NumNodes:                 3,
	},
	{
		Name:     "local-meta",
		NumNodes: 1,
		// Set OverrideDistSQLMode to "off" to stabilize test output, as some tests
		// assume no distsql.
		OverrideDistSQLMode: "off",
		IsMetamorphic:       true, DeclarativeCorpusCollection: true,
		MetamorphicKnobs: &MetamorphicKnobs{
			LegacySchemaChangerProbability: 0.5,
			VecOffProbability:              0.5,
			PrepareQueriesProbability:      0.5,
			// The old default config set never had a local
			// config with disk spilling. Enabling it here causes failures for
			// types that can't be spilled (e.g. RECORD). We could consider
			// adding this in the future once disk spilling supports all types.
			DiskSpillProbability: 0,
			IsolationLevelWeights: map[tree.IsolationLevel]float64{
				tree.SerializableIsolation:   0.50,
				tree.ReadCommittedIsolation:  0.25,
				tree.RepeatableReadIsolation: 0.25,
			},
			TenantProbability: 0.5,
		},
	},
	{
		Name:     "3node-meta",
		NumNodes: 3,
		// Set OverrideDistSQLMode to "on" to stabilize test output, as some tests
		// assume distsql.
		OverrideDistSQLMode: "on",
		IsMetamorphic:       true, DeclarativeCorpusCollection: true,
		MetamorphicKnobs: &MetamorphicKnobs{
			LegacySchemaChangerProbability: 0.5,
			VecOffProbability:              0.5,
			PrepareQueriesProbability:      0.5,
			DiskSpillProbability:           0.5,
			IsolationLevelWeights: map[tree.IsolationLevel]float64{
				tree.SerializableIsolation:   0.50,
				tree.ReadCommittedIsolation:  0.25,
				tree.RepeatableReadIsolation: 0.25,
			},
			TenantProbability: 0.5,
			// TODO(msbutler): what testing value do we get from FakeDist?
			FakedistProbability: 0.5,
		},
	},
	{
		Name: "local-mixed-meta", NumNodes: 1, OverrideDistSQLMode: "off",
		IsMetamorphic: true,
		MetamorphicKnobs: &MetamorphicKnobs{
			MixedVersionOptions: []clusterversion.Key{
				clusterversion.V25_4,
				clusterversion.V26_1,
				clusterversion.V26_2,
			},
		},
	},
}

// ConfigIdx is an index in the above slice.
type ConfigIdx int

func (idx ConfigIdx) Name() string {
	return LogicTestConfigs[idx].Name
}

// ConfigSet is a collection of configurations.
type ConfigSet []ConfigIdx

// ConfigNames returns the configuration names in the set.
func (cs ConfigSet) ConfigNames() []string {
	res := make([]string, len(cs))
	for i, idx := range cs {
		res[i] = idx.Name()
	}
	return res
}

// LineScanner handles reading from input test files.
type LineScanner struct {
	*bufio.Scanner
	Line       int
	Skip       bool
	skipReason string
}

// SetSkip sets Skip to true with the corresponding reason.
func (l *LineScanner) SetSkip(reason string) {
	l.Skip = true
	l.skipReason = reason
}

// LogAndResetSkip logs the skip reason (if one is set) and resets the skip boolean and skip reason.
func (l *LineScanner) LogAndResetSkip(t logger) {
	if l.skipReason != "" {
		t.Logf("statement/query skipped with reason: %s", l.skipReason)
	}
	l.skipReason = ""
	l.Skip = false
}

// NewLineScanner returns an appropriately configured LineScanner.
func NewLineScanner(r io.Reader) *LineScanner {
	return &LineScanner{
		Scanner: bufio.NewScanner(r),
		Line:    0,
	}
}

// Scan wraps Scan() for the interior Scanner.
func (l *LineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.Line++
	}
	return ok
}

// Text wraps Text() for the interior Scanner.
func (l *LineScanner) Text() string {
	return l.Scanner.Text()
}

// DefaultConfigSet is an alias for the set of default configs.
const DefaultConfigSet = "default-configs"

// DefaultConfigSets are sets of configs that have an alias which can be used
// instead of specific config names.
//
// Config sets allow referring to multiple configs more conveniently, and allow
// updating some of these lists without changing the test files.
var DefaultConfigSets = map[string]ConfigSet{
	// Default configs which are used when a logictest file doesn't specify any
	// specific configs.
	DefaultConfigSet: makeConfigSet(
		"local-meta",
		"3node-meta",
		"local-mixed-meta",
	),

	// Special alias for all 5 node configs.
	"5node-default-configs": makeConfigSet(
		"5node",
		"5node-disk",
	),

	// Special alias for all 3-node tenant configs.
	"3node-tenant-default-configs": makeConfigSet(
		"3node-tenant",
		"3node-tenant-multiregion",
	),

	// Special alias for all enterprise configs.
	"enterprise-configs": makeConfigSet(
		"3node-tenant",
		"3node-tenant-multiregion",
		"local-read-committed",
		"local-repeatable-read",
	),

	// Special alias for all configs which default to a weak transaction isolation
	// level.
	"weak-iso-level-configs": makeConfigSet(
		"local-read-committed",
		"local-repeatable-read",
	),

	// Special alias for all testserver configs (for mixed-version testing).
	"cockroach-go-testserver-configs": makeConfigSet(
		"cockroach-go-testserver-25.4",
		"cockroach-go-testserver-26.1",
	),

	// Special alias for configs where schema locked is disabled.
	"schema-locked-disabled": makeConfigSet(
		"local-legacy-schema-changer",
	),
}

// logger is an interface implemented by testing.TB as well as stdlogger below.
type logger interface {
	Fatalf(format string, args ...interface{})
	Logf(format string, args ...interface{})
}

type stdlogger struct{}

func (l stdlogger) Fatalf(format string, args ...interface{}) {
	output := fmt.Sprintf(format, args...)
	fmt.Println(output)
	panic(output)
}

func (l stdlogger) Logf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

// ReadBackupRestoreProbabilityOverride reads any LogicTest directive at the
// beginning of a test file. A line that starts with "#
// BackupRestoreProbability:" specifies the probability with which we should run
// a cluster backup + restore between lines of the test file.
//
// Example:
//
//	# BackupRestoreProbability: 0.8
//
// If the file doesn't contain a directive, the value of the environment
// variable COCKROACH_LOGIC_TEST_BACKUP_RESTORE_PROBABILITY is used.
func ReadBackupRestoreProbabilityOverride(
	t logger, path string,
) (hasOverride bool, probability float64) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed open file %s", path)
	}
	defer file.Close()

	s := NewLineScanner(file)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if !strings.HasPrefix(cmd, "#") {
			// Stop at the first line that's not a comment (or empty).
			break
		}
		if len(fields) > 1 && cmd == "#" && fields[1] == "BackupRestoreProbability:" {
			if len(fields) == 2 {
				t.Fatalf("%s: empty LogicTest directive", path)
			}
			probability, err := strconv.ParseFloat(fields[2], 64)
			if err != nil {
				t.Fatalf("failed to parse backup+restore probability: %+v", err)
			}
			return true, probability
		}
	}

	return false, 0
}

// ReadTestFileConfigs reads any LogicTest directive at the beginning of a
// test file. A line that starts with "# LogicTest:" specifies a list of
// configuration names. The test file is run against each of those
// configurations. It also returns the set of blocked config names
// (expanded from any config-set aliases) for use by metamorphic
// resolution.
//
// Example:
//
//	# LogicTest: default distsql
//
// If the file doesn't contain a directive, the default config is returned.
func ReadTestFileConfigs(
	t logger, path string, defaults ConfigSet,
) (_ ConfigSet, nonMetamorphicBatchSizes bool, blockedConfigs map[string]struct{}) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, nil
	}
	defer file.Close()

	s := NewLineScanner(file)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if !strings.HasPrefix(cmd, "#") {
			// Stop at the first line that's not a comment (or empty).
			break
		}
		// Directive lines are of the form:
		// # LogicTest: opt1=val1 opt2=val3 boolopt1
		if len(fields) > 1 && cmd == "#" && fields[1] == "LogicTest:" {
			if len(fields) == 2 {
				t.Fatalf("%s: empty LogicTest directive", path)
			}
			cs, nonMetamorphicBatchSizes, blockedConfigs := processConfigs(t, path, defaults, fields[2:])
			return cs, nonMetamorphicBatchSizes, blockedConfigs
		}
	}
	// No directive found, return the default config.
	return defaults, false, nil
}

// getBlocklistIssueNo takes a blocklist directive with an optional issue number
// and returns the stripped blocklist name with the corresponding issue number
// as an integer.
// e.g. an input of "3node-tenant(123456)" would return "3node-tenant", 123456
func getBlocklistIssueNo(blocklistDirective string) (string, int) {
	parts := strings.Split(blocklistDirective, "(")
	if len(parts) != 2 {
		return blocklistDirective, 0
	}

	issueNo, err := strconv.Atoi(strings.TrimRight(parts[1], ")"))
	if err != nil {
		panic(fmt.Sprintf("possibly malformed blocklist directive: %s: %v", blocklistDirective, err))
	}
	return parts[0], issueNo
}

// processConfigs, given a list of configNames, returns the list of
// corresponding logicTestConfigIdxs, a boolean indicating whether
// metamorphic settings related to batch sizes should be overridden with
// default production values, and the set of blocked config names (expanded
// from any config-set aliases).
func processConfigs(
	t logger, path string, defaults ConfigSet, configNames []string,
) (_ ConfigSet, nonMetamorphicBatchSizes bool, blockedConfigs map[string]struct{}) {
	const blocklistChar = '!'
	// blocklist is a map from a blocked config to a corresponding issue number.
	// If 0, there is no associated issue.
	blocklist := make(map[string]int)
	allConfigNamesAreBlocklistDirectives := true
	for _, configName := range configNames {
		if configName[0] != blocklistChar {
			allConfigNamesAreBlocklistDirectives = false
			continue
		}

		blockedConfig, issueNo := getBlocklistIssueNo(configName[1:])
		if *printBlocklistIssues && issueNo != 0 {
			t.Logf("will skip %s config in test %s due to issue: %s", blockedConfig, path, build.MakeIssueURL(issueNo))
		}
		// Enumerate all the blocked configs if the blocked config is a default
		// config list.
		names := getDefaultConfigListNames(blockedConfig)
		if len(names) == 0 {
			if !ConfigExists(blockedConfig) && blockedConfig != "metamorphic-batch-sizes" {
				panic(fmt.Sprintf("attempted to block logic test config that doesn't exist: %s", blockedConfig))
			}
			blocklist[blockedConfig] = issueNo
		} else {
			for _, name := range names {
				blocklist[name] = issueNo
			}
		}
	}

	blockedConfigs = make(map[string]struct{}, len(blocklist))
	for name := range blocklist {
		blockedConfigs[name] = struct{}{}
	}

	if _, ok := blocklist["metamorphic-batch-sizes"]; ok {
		nonMetamorphicBatchSizes = true
	}
	if len(blocklist) != 0 && allConfigNamesAreBlocklistDirectives {
		// No configs specified, this blocklist applies to the default configs.
		return applyBlocklistToConfigs(defaults, blocklist), nonMetamorphicBatchSizes, blockedConfigs
	}

	var configs ConfigSet
	for _, configName := range configNames {
		if configName[0] == blocklistChar {
			continue
		}
		if _, ok := blocklist[configName]; ok {
			continue
		}

		idx, ok := findLogicTestConfig(configName)
		if !ok {
			configSet, ok := DefaultConfigSets[configName]
			if !ok {
				t.Fatalf("%s: unknown config name %s", path, configName)
			}
			configs = append(configs, applyBlocklistToConfigs(configSet, blocklist)...)
		} else {
			configs = append(configs, idx)
		}
	}

	return dedupConfigs(configs), nonMetamorphicBatchSizes, blockedConfigs
}

// dedupConfigs removes duplicate config indices from a ConfigSet, preserving
// the order of first occurrence.
//
// TODO(butler): when a test file explicitly names a config that is already part
// of a default config set (e.g. `default-configs 3node-tenant`), the explicit
// mention should guarantee a CI run for that config instead of being selected
// metamorphically.
func dedupConfigs(configs ConfigSet) ConfigSet {
	seen := make(map[ConfigIdx]struct{}, len(configs))
	deduped := make(ConfigSet, 0, len(configs))
	for _, idx := range configs {
		if _, ok := seen[idx]; !ok {
			seen[idx] = struct{}{}
			deduped = append(deduped, idx)
		}
	}
	return deduped
}

// applyBlocklistToConfigs applies the given blocklist to configs, returning the
// result.
func applyBlocklistToConfigs(configs ConfigSet, blocklist map[string]int) ConfigSet {
	if len(blocklist) == 0 {
		return configs
	}
	var newConfigs ConfigSet
	for _, idx := range configs {
		if _, ok := blocklist[idx.Name()]; ok {
			continue
		}
		newConfigs = append(newConfigs, idx)
	}
	return newConfigs
}

func makeConfigSet(names ...string) ConfigSet {
	ret := make(ConfigSet, len(names))
	for i, name := range names {
		idx, ok := findLogicTestConfig(name)
		if !ok {
			panic(fmt.Errorf("unknown config %s", name))
		}
		ret[i] = idx
	}
	return ret
}

func findLogicTestConfig(name string) (ConfigIdx, bool) {
	for i, cfg := range LogicTestConfigs {
		if cfg.Name == name {
			return ConfigIdx(i), true
		}
	}
	return -1, false
}

// ConfigIsInDefaultList returns true if defaultName is one of the default
// config lists and configName is a config included in that list.
func ConfigIsInDefaultList(configName, defaultName string) bool {
	for _, name := range getDefaultConfigListNames(defaultName) {
		if name == configName {
			return true
		}
	}
	return false
}

func getDefaultConfigListNames(name string) []string {
	return DefaultConfigSets[name].ConfigNames()
}

var allConfigNames = make(map[string]struct{}, len(LogicTestConfigs))

func init() {
	for _, cfg := range LogicTestConfigs {
		allConfigNames[cfg.Name] = struct{}{}
	}
}

// ConfigExists returns whether the given name matches either a config or an
// alias.
func ConfigExists(name string) bool {
	_, config := allConfigNames[name]
	_, alias := DefaultConfigSets[name]
	return config || alias
}

// EnumerateConfigs produces the list of all configuration/file pairs from the
// input list of file globs. The return value is a list of the same length as
// LogicTestConfigs, and each sub-list is the path to a file run under that
// configuration.
func EnumerateConfigs(globs ...string) ([][]string, error) {
	var paths []string
	for _, g := range globs {
		match, err := filepath.Glob(g)
		if err != nil {
			return nil, err
		}
		paths = append(paths, match...)
	}

	logger := stdlogger{}
	configPaths := make([][]string, len(LogicTestConfigs))
	configDefaults := DefaultConfigSets[DefaultConfigSet]
	for _, path := range paths {
		configs, _, _ := ReadTestFileConfigs(logger, path, configDefaults)
		for _, idx := range configs {
			configPaths[idx] = append(configPaths[idx], path)
		}
	}
	for _, paths := range configPaths {
		sort.Strings(paths)
	}
	return configPaths, nil
}
