// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestflags

import (
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/spf13/pflag"
)

// This block defines all roachtest flags (for the list and run/bench commands).
var (
	Cloud string = spec.GCE
	_            = registerListFlag(&Cloud, FlagInfo{
		Name: "cloud",
		Usage: `List only tests compatible with the given cloud ("local", "gce",
		        "aws", "azure", or "all")`,
	})
	_ = registerRunFlag(&Cloud, FlagInfo{
		Name: "cloud",
		Usage: `Cloud provider to use ("local", "gce", "aws", or "azure"); by
		        default, only tests compatible with the given cloud are run`,
	})
	_ = registerRunOpsFlag(&Cloud, FlagInfo{
		Name: "cloud",
		Usage: `Cloud provider that the cluster is running on. Used by operations
		        to tailor behaviour to that cloud.`,
	})

	Suite string
	_     = registerListFlag(&Suite, FlagInfo{
		Name:  "suite",
		Usage: `List only tests from the given suite (e.g. "nightly")`,
	})
	_ = registerRunFlag(&Suite, FlagInfo{
		Name:  "suite",
		Usage: `Run only tests from the given suite (e.g. "nightly")`,
	})

	Owner string
	_     = registerListFlag(&Owner, FlagInfo{
		Name:  "owner",
		Usage: `List only tests with the given owner (e.g. "kv")`,
	})
	_ = registerRunFlag(&Owner, FlagInfo{
		Name:  "owner",
		Usage: `Run only tests with the given owner (e.g. "kv")`,
	})

	OnlyBenchmarks bool
	_              = registerListFlag(&OnlyBenchmarks, FlagInfo{
		Name:  "bench",
		Usage: `List only benchmarks`,
	})

	ForceCloudCompat bool
	_                = registerRunFlag(&ForceCloudCompat, FlagInfo{
		Name:  "force-cloud-compat",
		Usage: `Include tests that are not marked as compatible with the cloud used`,
	})

	ClusterNames string
	_            = registerRunFlag(&ClusterNames, FlagInfo{
		Name:      "cluster",
		Shorthand: "c",
		Usage: `
			Comma-separated list of names of existing cluster(s) to use for running
			tests. If fewer than --parallelism names are specified, then the
			parallelism is capped to the number of clusters specified. When a cluster
			does not exist yet, it is created according to the spec.`,
	})

	Local bool
	_     = registerRunFlag(&Local, FlagInfo{
		Name:      "local",
		Shorthand: "l",
		Usage:     `Run tests locally (equivalent to --cloud=local)`,
	})

	Username string = os.Getenv("ROACHPROD_USER")
	_               = registerRunFlag(&Username, FlagInfo{
		Name:      "user",
		Shorthand: "u",
		Usage: `
			Username to use as a cluster name prefix. If blank, the current OS user is
			detected and specified`,
	})

	CockroachPath string
	_             = registerRunFlag(&CockroachPath, FlagInfo{
		Name:  "cockroach",
		Usage: `Absolute path to cockroach binary to use`,
	})

	CockroachBinaryPath string = "cockroach"
	_                          = registerRunOpsFlag(&CockroachBinaryPath, FlagInfo{
		Name:  "cockroach-binary",
		Usage: `Relative path to cockroach binary to use, on the cluster specified in --cluster`,
	})

	CertsDir string
	_        = registerRunOpsFlag(&CertsDir, FlagInfo{
		Name:  "certs-dir",
		Usage: `Absolute path to certificates directory, if the cluster specified in --cluster is secure`,
	})

	VirtualCluster string
	_              = registerRunOpsFlag(&VirtualCluster, FlagInfo{
		Name:  "virtual-cluster",
		Usage: `Specifies virtual cluster to connect to, within the specified --cluster.`,
	})

	WaitBeforeCleanup time.Duration = 5 * time.Minute
	_                               = registerRunOpsFlag(&WaitBeforeCleanup, FlagInfo{
		Name: "wait-before-cleanup",
		Usage: `Specifies the amount of time to wait before running any cleanup work defined
            by the operation. Note that this time does not count towards the timeout.`,
	})

	SkipDependencyCheck bool = false
	_                        = registerRunOpsFlag(&SkipDependencyCheck, FlagInfo{
		Name: "skip-dependency-check",
		Usage: `Skips the pre-operation dependency check and runs the operation regardless of
		whether its dependencies are met or not. Note that running operations with this flag could
		lead to cluster unavailability or operation failures.`,
	})

	CockroachEAPath string
	_               = registerRunFlag(&CockroachEAPath, FlagInfo{
		Name: "cockroach-ea",
		Usage: `
			Absolute path to cockroach binary with enabled (runtime) assertions (i.e.
			compiled with crdb_test)`,
	})

	WorkloadPath string
	_            = registerRunFlag(&WorkloadPath, FlagInfo{
		Name:  "workload",
		Usage: `Absolute path to workload binary to use`,
	})

	EncryptionProbability float64 = defaultEncryptionProbability
	_                             = registerRunFlag(&EncryptionProbability, FlagInfo{
		Name: "metamorphic-encryption-probability",
		Usage: `
			Probability that clusters will be created with encryption-at-rest enabled
			for tests that support metamorphic encryption`,
	})

	FIPSProbability float64 = defaultFIPSProbability
	_                       = registerRunFlag(&FIPSProbability, FlagInfo{
		Name: "metamorphic-fips-probability",
		Usage: `
			Conditional probability that amd64 clusters will be created with FIPS,
			i.e., P(fips | amd64), for tests that support FIPS and whose CPU
			architecture is 'amd64' (default 0) NOTE: amd64 clusters are created with
			probability 1-P(arm64), where P(arm64) is 'metamorphic-arm64-probability';
			hence, P(fips | amd64) = P(fips) * (1 - P(arm64))`,
	})

	ARM64Probability float64 = defaultARM64Probability
	_                        = registerRunFlag(&ARM64Probability, FlagInfo{
		Name: "metamorphic-arm64-probability",
		Usage: `
			Probability that clusters will be created with 'arm64' CPU architecture
			for tests that support 'arm64' (default 0)`,
	})

	// ArtifactsDir is a path to a local dir where the test logs and artifacts
	// collected from cluster will be placed.
	ArtifactsDir  string = "artifacts"
	ArtifactsFlag        = FlagInfo{
		Name:  "artifacts",
		Usage: `Path to artifacts directory`,
	}
	_ = registerRunFlag(&ArtifactsDir, ArtifactsFlag)
	_ = registerRunOpsFlag(&ArtifactsDir, ArtifactsFlag)

	// LiteralArtifactsDir is a path to the literal on-agent directory where
	// artifacts are stored. May be different from `artifacts`. Only used for
	// messages to ##teamcity[publishArtifacts] in Teamcity mode.
	LiteralArtifactsDir string
	_                   = registerRunFlag(&LiteralArtifactsDir, FlagInfo{
		Name: "artifacts-literal",
		Usage: `
			Literal path to on-agent artifacts directory. Used for messages to
			##teamcity[publishArtifacts] in --teamcity mode. May be different from
			--artifacts; defaults to the value of --artifacts if not provided`,
	})

	ClusterID string
	_         = registerRunFlag(&ClusterID, FlagInfo{
		Name:  "cluster-id",
		Usage: `An identifier to use in the name of the test cluster(s)`,
	})

	Count int = 1
	_         = registerRunFlag(&Count, FlagInfo{
		Name:  "count",
		Usage: `the number of times to run each test`,
	})

	DebugOnFailure bool
	_              = registerRunFlag(&DebugOnFailure, FlagInfo{
		Name:      "debug",
		Shorthand: "d",
		Usage:     `Don't wipe and destroy cluster if test fails`,
	})

	DebugAlways bool
	_           = registerRunFlag(&DebugAlways, FlagInfo{
		Name:  "debug-always",
		Usage: `Never wipe and destroy the cluster`,
	})

	RunSkipped bool
	_          = registerRunFlag(&RunSkipped, FlagInfo{
		Name:  "run-skipped",
		Usage: `Run skipped tests`,
	})

	SkipInit bool
	_        = registerRunFlag(&SkipInit, FlagInfo{
		Name: "skip-init",
		Usage: `
			Skip initialization step (imports, table creation, etc.) for tests that
			support it, useful when re-using clusters with --wipe=false`,
	})

	GoCoverEnabled bool
	_              = registerRunFlag(&GoCoverEnabled, FlagInfo{
		Name: "go-cover",
		Usage: `
			Enable collection of go coverage profiles (requires instrumented cockroach
			binary)`,
	})

	Parallelism int = 10
	_               = registerRunFlag(&Parallelism, FlagInfo{
		Name:  "parallelism",
		Usage: `Number of tests to run in parallel`,
	})

	deprecatedRoachprodBinary string
	_                         = registerRunFlag(&deprecatedRoachprodBinary, FlagInfo{
		Name:       "roachprod",
		Usage:      "DEPRECATED",
		Deprecated: "roachtest now uses roachprod as a library",
	})

	ClusterWipe bool = true
	_                = registerRunFlag(&ClusterWipe, FlagInfo{
		Name:  "wipe",
		Usage: `Wipe existing cluster before starting test (for use with --cluster)`,
	})

	Zones string
	_     = registerRunFlag(&Zones, FlagInfo{
		Name: "zones",
		Usage: `
			Zones for the cluster. (non-geo tests use the first zone, geo tests use
			all zones; uses defaults if empty)`,
	})

	InstanceType string
	_            = registerRunFlag(&InstanceType, FlagInfo{
		Name: "instance-type",
		Usage: `
			The instance type to use (see https://aws.amazon.com/ec2/instance-types/,
			https://cloud.google.com/compute/docs/machine-types or
			https://docs.microsoft.com/en-us/azure/virtual-machines/windows/sizes)`,
	})

	CPUQuota         int = 300
	cpuQuotaFlagInfo     = FlagInfo{
		Name:  "cpu-quota",
		Usage: `The number of cloud CPUs roachtest is allowed to use at any one time.`,
	}
	_ = registerRunFlag(&CPUQuota, cpuQuotaFlagInfo)

	HTTPPort int = 0
	_            = registerRunFlag(&HTTPPort, FlagInfo{
		Name:  "port",
		Usage: `The port on which to serve the HTTP interface`,
	})

	DogstatsdAddr string = ""
	_                    = registerRunOpsFlag(&DogstatsdAddr, FlagInfo{
		Name:  "dogstatsd-addr",
		Usage: `The address to which to connect to dogstatsd, to send Datadog events.`,
	})

	PreferLocalSSD bool = true
	_                   = registerRunFlag(&PreferLocalSSD, FlagInfo{
		Name:  "local-ssd",
		Usage: `Use a local SSD instead of an EBS volume, if the instance supports it`,
	})

	VersionsBinaryOverride map[string]string
	_                      = registerRunFlag(&VersionsBinaryOverride, FlagInfo{
		Name: "versions-binary-override",
		Usage: `
			List of <version>=<path to cockroach binary>. If a certain version <ver>
			is present in the list, the respective binary will be used when a
			mixed-version test asks for the respective binary, instead of roachprod
			stage <ver>. Example: 20.1.4=cockroach-20.1,20.2.0=cockroach-20.2.`,
	})

	SlackToken string
	_          = registerRunFlag(&SlackToken, FlagInfo{
		Name:  "slack-token",
		Usage: `Slack bot token`,
	})

	TeamCity bool
	_        = registerRunFlag(&TeamCity, FlagInfo{
		Name:  "teamcity",
		Usage: `Include teamcity-specific markers in output`,
	})

	GitHubActions bool
	_             = registerRunFlag(&GitHubActions, FlagInfo{
		Name:  "github",
		Usage: `Add GitHub-specific markers to the output where possible, and optionally populate GITHUB_STEP_SUMMARY with a summary of all tests`,
	})

	DisableIssue bool
	_            = registerRunFlag(&DisableIssue, FlagInfo{
		Name:  "disable-issue",
		Usage: `Disable posting GitHub issue for failures`,
	})

	PromPort int = 2113
	_            = registerRunFlag(&PromPort, FlagInfo{
		Name: "prom-port",
		Usage: `
			The http port on which to expose prom metrics from the roachtest
			process`,
	})

	SelectProbability float64 = 1.0
	_                         = registerRunFlag(&SelectProbability, FlagInfo{
		Name: "select-probability",
		Usage: `
			The probability of a matched test being selected to run. Note: this will
			run at least one test per prefix.`,
	})

	UseSpotVM bool
	_         = registerRunFlag(&UseSpotVM, FlagInfo{
		Name:  "use-spot",
		Usage: `Use SpotVM to run tests, If the provider does not support spotVM, it will be ignored`,
	})

	AutoKillThreshold float64 = 1.0
	_                         = registerRunFlag(&AutoKillThreshold, FlagInfo{
		Name:  "auto-kill-threshold",
		Usage: `Percentage of failed tests before all remaining tests are automatically terminated.`,
	})

	GlobalSeed int64 = randutil.NewPseudoSeed()
	_                = registerRunFlag(&GlobalSeed, FlagInfo{
		Name:  "global-seed",
		Usage: `The global random seed used for all tests.`,
	})

	ClearClusterCache bool = true
	_                      = registerRunFlag(&ClearClusterCache, FlagInfo{
		Name: "clear-cluster-cache",
		Usage: `
						Clear the local cluster cache of missing clusters when syncing resources with
						providers. Set this to false when running many concurrent Azure tests. Azure
						can return stale VM information when many PUT calls are made in succession.`,
	})
)

// The flags below override the final cluster configuration. They have no
// default values and are only effectual when they are specified (all uses are
// gated behind Changed() calls).
var (
	Lifetime time.Duration
	_        = registerRunFlag(&Lifetime, FlagInfo{
		Name:  "lifetime",
		Usage: `Lifetime of the cluster`,
	})

	OverrideUseLocalSSD bool
	_                   = registerRunFlag(&OverrideUseLocalSSD, FlagInfo{
		Name:  "roachprod-local-ssd",
		Usage: `Override use of local SSD`,
	})

	OverrideFilesystem string
	_                  = registerRunFlag(&OverrideFilesystem, FlagInfo{
		Name:  "filesystem",
		Usage: `Override the underlying file system(ext4/zfs)`,
	})

	OverrideNoExt4Barrier bool
	_                     = registerRunFlag(&OverrideNoExt4Barrier, FlagInfo{
		Name: "local-ssd-no-ext4-barrier",
		Usage: `
			Mount the local SSD with the "-o nobarrier" flag. Ignored if not using
			local SSD`,
	})

	OverrideNumNodes int
	_                = registerRunFlag(&OverrideNumNodes, FlagInfo{
		Name:      "nodes",
		Shorthand: "n",
		Usage:     `Override the number of nodes in the cluster`,
	})

	OverrideOSVolumeSizeGB int
	_                      = registerRunFlag(&OverrideOSVolumeSizeGB, FlagInfo{
		Name:  "os-volume-size",
		Usage: `Override OS disk volume size (in GB)`,
	})

	OverrideGeoDistributed bool
	_                      = registerRunFlag(&OverrideGeoDistributed, FlagInfo{
		Name:  "geo",
		Usage: `Create geo-distributed cluster`,
	})
)

const (
	defaultEncryptionProbability = 1
	defaultFIPSProbability       = 0
	defaultARM64Probability      = 0
)

// FlagInfo contains the name and usage of a flag. Used to make the code
// defining them self-documenting.
type FlagInfo struct {
	// Name of the flag (as will be passed in the command-line).
	Name string

	// Shorthand is the one-letter abbreviated flag that can be used with a single
	// dash (optional).
	Shorthand string

	// Usage description. The string can be broken up into many lines
	// arbitrarily; it is cleaned up to a single line with extra whitespace
	// removed.
	Usage string

	// Deprecated is used only for deprecated flags; it is the message shown when
	// the flag is used.
	Deprecated string
}

// AddListFlags adds all flags registered for the list command to the given
// command flag set.
func AddListFlags(cmdFlags *pflag.FlagSet) {
	globalMan.AddFlagsToCommand(listCmdID, cmdFlags)
}

// AddRunFlags adds all flags registered for the run command to the given
// command flag set.
func AddRunFlags(cmdFlags *pflag.FlagSet) {
	globalMan.AddFlagsToCommand(runCmdID, cmdFlags)
}

// AddRunOpsFlags adds all flags registered for the run-operations command to
// the given command flag set.
func AddRunOpsFlags(cmdFlags *pflag.FlagSet) {
	globalMan.AddFlagsToCommand(runOpsCmdID, cmdFlags)
}

// Changed returns true if a flag associated with a given value was present.
//
// For example: roachtestflags.Changed(&roachtestflags.Cloud) returns true if
// the `--cloud` flag was passed (even if the given value was the same with the
// default value).
func Changed(valPtr interface{}) bool {
	return globalMan.Changed(valPtr)
}

var globalMan manager

func registerListFlag(valPtr interface{}, info FlagInfo) struct{} {
	globalMan.RegisterFlag(listCmdID, valPtr, info)
	return struct{}{}
}

func registerRunFlag(valPtr interface{}, info FlagInfo) struct{} {
	globalMan.RegisterFlag(runCmdID, valPtr, info)
	return struct{}{}
}

func registerRunOpsFlag(valPtr interface{}, info FlagInfo) struct{} {
	globalMan.RegisterFlag(runOpsCmdID, valPtr, info)
	return struct{}{}
}
