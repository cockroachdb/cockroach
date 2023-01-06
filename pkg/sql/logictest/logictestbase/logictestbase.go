// Copyright 2022 The Cockroach Authors.
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
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	BootstrapVersion roachpb.Version
	// If not empty, binaryVersion is used to set what the Server will consider
	// to be the binary version.
	BinaryVersion  roachpb.Version
	DisableUpgrade bool
	// If true, a sql tenant server will be started and pointed at a node in the
	// cluster. Connections on behalf of the logic test will go to that tenant.
	UseTenant bool
	// Disable the default test tenant.
	DisableDefaultTestTenant bool
	// IsCCLConfig should be true for any config that can only be run with a CCL
	// binary.
	IsCCLConfig bool
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
	// DeclarativeCorpusCollection enables support for collecting corpuses
	// for the declarative schema changer.
	DeclarativeCorpusCollection bool
	// UseCockroachGoTestserver determines if the logictest uses the
	// cockroach-go/testserver package to run the logic test.
	// This allows us to do testing on different binary versions or to
	// restart/upgrade nodes.
	UseCockroachGoTestserver bool

	// CockroachGoBootstrapVersion defines the version the cockroach-go/testserver
	// is bootstrapped on for the logictest. It is required if
	// UseCockroachGoTestserver is true.
	CockroachGoBootstrapVersion string

	// CockroachGoUpgradeVersion defines the version that the
	// cockroach-go/testserver is upgraded to during the logictest. If one is not
	// specified, it uses the local cockroach binary.
	CockroachGoUpgradeVersion string
}

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
		DisableDefaultTestTenant:    true,
		DeclarativeCorpusCollection: true,
	},
	{
		Name:                            "local-legacy-schema-changer",
		NumNodes:                        1,
		OverrideDistSQLMode:             "off",
		DisableDeclarativeSchemaChanger: true,
	},
	{
		Name:                "local-vec-off",
		NumNodes:            1,
		OverrideDistSQLMode: "off",
		OverrideVectorize:   "off",
	},
	{
		Name:                "local-v1.1-at-v1.0-noupgrade",
		NumNodes:            1,
		OverrideDistSQLMode: "off",
		BootstrapVersion:    roachpb.Version{Major: 1},
		BinaryVersion:       roachpb.Version{Major: 1, Minor: 1},
		DisableUpgrade:      true,
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
		Name:                "5node",
		NumNodes:            5,
		OverrideDistSQLMode: "on",
		// Have to disable the default test tenant here as there are test run in
		// this mode which try to modify zone configurations and we're more
		// restrictive in the way we allow zone configs to be modified by
		// secondary tenants. See #75569 for more info.
		DisableDefaultTestTenant: true,
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
		// dev testlogic ccl --files 3node-tenant --subtest $SUBTEST
		Name:                        threeNodeTenantConfigName,
		NumNodes:                    3,
		UseTenant:                   true,
		IsCCLConfig:                 true,
		OverrideDistSQLMode:         "on",
		DeclarativeCorpusCollection: true,
	},
	{
		// 3node-tenant-multiregion is a config that runs the test as a SQL tenant
		// with SQL instances and KV nodes in multiple regions. This config can only
		// be run with a CCL binary, so is a noop if run through the normal
		// logictest command.
		// To run a logic test with this config as a directive, run:
		// dev testlogic ccl --files 3node-tenant-multiregion --subtests $SUBTESTS
		Name:                        "3node-tenant-multiregion",
		NumNodes:                    3,
		UseTenant:                   true,
		IsCCLConfig:                 true,
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
		DisableDefaultTestTenant:    true,
		DeclarativeCorpusCollection: true,
	},
	{
		Name:                        "multiregion-9node-3region-3azs-tenant",
		NumNodes:                    9,
		Localities:                  multiregion9node3region3azsLocalities,
		UseTenant:                   true,
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
		Name:                        "local-mixed-22.2-23.1",
		NumNodes:                    1,
		OverrideDistSQLMode:         "off",
		BootstrapVersion:            clusterversion.ByKey(clusterversion.V22_2),
		BinaryVersion:               clusterversion.ByKey(clusterversion.V23_1),
		DisableUpgrade:              true,
		DeclarativeCorpusCollection: true,
	},
	{
		Name:                        "cockroach-go-testserver-22.2-master",
		UseCockroachGoTestserver:    true,
		NumNodes:                    3,
		CockroachGoBootstrapVersion: "v22.2.1",
	},
	{
		Name:                        "cockroach-go-testserver-22.1-22.2",
		UseCockroachGoTestserver:    true,
		NumNodes:                    3,
		CockroachGoBootstrapVersion: "v22.1.6",
		CockroachGoUpgradeVersion:   "v22.2.1",
	},
}

// ConfigIdx is an index in the above slice.
type ConfigIdx int

// ConfigSet is a collection of configurations.
type ConfigSet []ConfigIdx

// ConfigIdxToName is a map of ConfigIdx to the corresponding configuration name.
var ConfigIdxToName = make(map[ConfigIdx]string)

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

var (
	// DefaultConfigName is a special alias for the default configs.
	DefaultConfigName = "default-configs"
	// DefaultConfigNames is the list of default configs captured by the DefaultConfigName.
	DefaultConfigNames = []string{
		"local",
		"local-legacy-schema-changer",
		"local-vec-off",
		"fakedist",
		"fakedist-vec-off",
		"fakedist-disk",
	}
	// FiveNodeDefaultConfigName is a special alias for all 5 node configs.
	FiveNodeDefaultConfigName = "5node-default-configs"
	// FiveNodeDefaultConfigNames is the list of 5 node configs.
	FiveNodeDefaultConfigNames = []string{
		"5node",
		"5node-disk",
	}
	// ThreeNodeTenantDefaultConfigName is a special alias for all 3-node tenant
	// configs.
	ThreeNodeTenantDefaultConfigName = "3node-tenant-default-configs"
	// ThreeNodeTenantDefaultConfigNames is the list of 3 node tenant configs.
	ThreeNodeTenantDefaultConfigNames = []string{
		"3node-tenant",
		"3node-tenant-multiregion",
	}
	// DefaultConfig is the default test configuration.
	DefaultConfig = parseTestConfig(DefaultConfigNames)
	// FiveNodeDefaultConfig is the five-node default test configuration.
	FiveNodeDefaultConfig = parseTestConfig(FiveNodeDefaultConfigNames)
	// ThreeNodeTenantDefaultConfig is the three-node tenant default test configuration.
	ThreeNodeTenantDefaultConfig = parseTestConfig(ThreeNodeTenantDefaultConfigNames)
)

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
// configurations.
//
// Example:
//
//	# LogicTest: default distsql
//
// If the file doesn't contain a directive, the default config is returned.
func ReadTestFileConfigs(
	t logger, path string, defaults ConfigSet,
) (_ ConfigSet, nonMetamorphicBatchSizes bool) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false
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
			cs, nonMetamorphicBatchSizes := processConfigs(t, path, defaults, fields[2:])
			return cs, nonMetamorphicBatchSizes
		}
	}
	// No directive found, return the default config.
	return defaults, false
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
// corresponding logicTestConfigIdxs as well as a boolean indicating whether
// metamorphic settings related to batch sizes should be overridden with default
// production values.
func processConfigs(
	t logger, path string, defaults ConfigSet, configNames []string,
) (_ ConfigSet, nonMetamorphicBatchSizes bool) {
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
			blocklist[blockedConfig] = issueNo
		} else {
			for _, name := range names {
				blocklist[name] = issueNo
			}
		}
	}

	if _, ok := blocklist["metamorphic-batch-sizes"]; ok {
		nonMetamorphicBatchSizes = true
	}
	if len(blocklist) != 0 && allConfigNamesAreBlocklistDirectives {
		// No configs specified, this blocklist applies to the default configs.
		return applyBlocklistToConfigs(defaults, blocklist), nonMetamorphicBatchSizes
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
			switch configName {
			case DefaultConfigName:
				configs = append(configs, applyBlocklistToConfigs(defaults, blocklist)...)
			case FiveNodeDefaultConfigName:
				configs = append(configs, applyBlocklistToConfigs(FiveNodeDefaultConfig, blocklist)...)
			case ThreeNodeTenantDefaultConfigName:
				configs = append(configs, applyBlocklistToConfigs(ThreeNodeTenantDefaultConfig, blocklist)...)
			default:
				t.Fatalf("%s: unknown config name %s", path, configName)
			}
		} else {
			configs = append(configs, idx)
		}
	}

	return configs, nonMetamorphicBatchSizes
}

// applyBlocklistToConfigs applies the given blocklist to configs, returning the
// result.
func applyBlocklistToConfigs(configs ConfigSet, blocklist map[string]int) ConfigSet {
	if len(blocklist) == 0 {
		return configs
	}
	var newConfigs ConfigSet
	for _, idx := range configs {
		if _, ok := blocklist[ConfigIdxToName[idx]]; ok {
			continue
		}
		newConfigs = append(newConfigs, idx)
	}
	return newConfigs
}

func parseTestConfig(names []string) ConfigSet {
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

func init() {
	for i, cfg := range LogicTestConfigs {
		ConfigIdxToName[ConfigIdx(i)] = cfg.Name
	}
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
	switch name {
	case DefaultConfigName:
		return DefaultConfigNames
	case FiveNodeDefaultConfigName:
		return FiveNodeDefaultConfigNames
	case ThreeNodeTenantDefaultConfigName:
		return ThreeNodeTenantDefaultConfigNames
	}
	return []string{}
}

// ConfigCalculator is used to enumerate a map of configuration -> file.
type ConfigCalculator struct {
	ConfigOverrides, ConfigFilterOverrides []string
	RunCCLConfigs                          bool
}

// Enumerate produces the list of all configuration/file pairs from the input
// list of file globs. The return value is a list of the same length as
// LogicTestConfigs, and each sub-list is the path to a file run under that
// configuration.
func (c ConfigCalculator) Enumerate(globs ...string) ([][]string, error) {
	var paths []string
	for _, g := range globs {
		match, err := filepath.Glob(g)
		if err != nil {
			return nil, err
		}
		paths = append(paths, match...)
	}

	logger := stdlogger{}
	// Read the configuration directives from all the files and accumulate a list
	// of paths per config.
	configPaths := make([][]string, len(LogicTestConfigs))
	var configFilter map[string]struct{}
	configDefaults := DefaultConfig
	if len(c.ConfigOverrides) > 0 {
		// If a config override is provided, we use it to replace the default
		// config set. This ensures that the overrides are used for files where:
		// 1. no config directive is present
		// 2. a config directive containing only a blocklist is present
		// 3. a config directive containing "default-configs" is present
		//
		// We also create a filter to restrict configs to only those in the
		// override list.
		configDefaults = parseTestConfig(c.ConfigOverrides)
		configFilter = make(map[string]struct{})
		for _, name := range c.ConfigOverrides {
			configFilter[name] = struct{}{}
		}
	}
	// If a config filter override is provided, add them to the filter to
	// also run tests with them as a config directive. This is in addition to
	// any configs added via the config override.
	for _, name := range c.ConfigFilterOverrides {
		configFilter[name] = struct{}{}
	}
	for _, path := range paths {
		configs, _ := ReadTestFileConfigs(logger, path, configDefaults)
		for _, idx := range configs {
			config := LogicTestConfigs[idx]
			configName := config.Name
			if _, ok := configFilter[configName]; configFilter != nil && !ok {
				// Config filter present but not containing test.
				continue
			}
			if config.IsCCLConfig && !c.RunCCLConfigs {
				// Config is a CCL config and the caller specified that CCL configs
				// should not be run.
				continue
			}
			configPaths[idx] = append(configPaths[idx], path)
		}
	}
	for _, paths := range configPaths {
		sort.Strings(paths)
	}
	return configPaths, nil
}
