// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestflags

import (
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/spf13/pflag"
)

// This block defines all roachtest flags (for the list and run/bench commands).
var (
	Cloud spec.Cloud = spec.GCE
	_                = registerListFlag(&Cloud, FlagInfo{
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

	SelectiveTests = false
	_              = registerRunFlag(&SelectiveTests, FlagInfo{
		Name:  "selective-tests",
		Usage: `Use selective tests to run based on previous test execution. Incompatible with --select-probability`,
	})

	SuccessfulTestsSelectPct = 0.35
	_                        = registerRunFlag(&SuccessfulTestsSelectPct, FlagInfo{
		Name:  "successful-test-select-pct",
		Usage: `The percent of test that should be selected from the tests that have been running successfully as per test selection. Default is 0.35`,
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

	ConfigPath string
	_          = registerRunOpsFlag(&ConfigPath, FlagInfo{
		Name: "config",
		Usage: `Path to a YAML config file containing the state of the cluster.
						Used by operations to determine cluster settings, start options,
						and the cluster spec.`,
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

	CockroachEAProbability float64 = defaultCockroachEAProbability
	_                              = registerRunFlag(&CockroachEAProbability, FlagInfo{
		Name: "metamorphic-cockroach-ea-probability",
		Usage: `
			Probability that tests will be run with assertions enabled. A cockroach
      binary built with the --crdb_test flag must be passed to --cockroach-ea
      for assertions to be enabled.`,
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

	ForceCpuProfile bool
	_               = registerRunFlag(&ForceCpuProfile, FlagInfo{
		Name: "force-cpu-profile",
		Usage: `
                        Enable unconditional collection of CPU profiles`,
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

	ExportOpenmetrics bool = false
	_                      = registerRunFlag(&ExportOpenmetrics, FlagInfo{
		Name:  "export-openmetrics",
		Usage: `flag to denote if roachtest should export openmetrics file for performance metrics.`,
	})

	OpenmetricsLabels string = ""
	_                        = registerRunFlag(&OpenmetricsLabels, FlagInfo{
		Name:  "openmetrics-labels",
		Usage: `flag to pass custom labels to pass to openmetrics for performance metrics,`,
	})

	DatadogSite string = "us5.datadoghq.com"
	_                  = registerRunOpsFlag(&DatadogSite, FlagInfo{
		Name:  "datadog-site",
		Usage: `Datadog site to communicate with (e.g., us5.datadoghq.com).`,
	})

	DatadogAPIKey string = ""
	_                    = registerRunOpsFlag(&DatadogAPIKey, FlagInfo{
		Name:  "datadog-api-key",
		Usage: `Datadog API key to emit telemetry data to Datadog.`,
	})

	DatadogApplicationKey string = ""
	_                            = registerRunOpsFlag(&DatadogApplicationKey, FlagInfo{
		Name:  "datadog-app-key",
		Usage: `Datadog application key to read telemetry data from Datadog.`,
	})

	DatadogTags string = ""
	_                  = registerRunOpsFlag(&DatadogTags, FlagInfo{
		Name:  "datadog-tags",
		Usage: `A comma-separated list of tags to attach to telemetry data (e.g., key1:val1,key2:val2).`,
	})

	DBName string = ""
	_             = registerRunOpsFlag(&DBName, FlagInfo{
		Name: "db",
		Usage: "Specify the name of the database to run the operation against (e.g., tpcc, tpch). If the given database " +
			"does not exist, the operation fails. If not provided, a random database is selected, excluding system-created databases",
	})

	TableName string = ""
	_                = registerRunOpsFlag(&TableName, FlagInfo{
		Name: "db-table",
		Usage: "Specifies the name of the database table to run the operation against, using the database provided in the --db flag. " +
			"If the table does not exist, the operation fails. If not provided, a random table is selected.",
	})

	OperationParallelism int = 1
	_                        = registerRunOpsFlag(&OperationParallelism, FlagInfo{
		Name:  "parallelism",
		Usage: fmt.Sprintf("Number of operations to run in parallel, max value is %d", MaxOperationParallelism),
	})

	WaitBeforeNextExecution time.Duration = 15 * time.Minute
	_                                     = registerRunOpsFlag(&WaitBeforeNextExecution, FlagInfo{
		Name:  "wait-before-next-execution",
		Usage: "Interval to wait before the operation next execution after the previous run.",
	})

	RunForever bool = false
	_               = registerRunOpsFlag(&RunForever, FlagInfo{
		Name:  "run-forever",
		Usage: "Execute operations indefinitely until the command is terminated, (default false).",
	})

	WorkloadCluster string = ""
	_                      = registerRunOpsFlag(&WorkloadCluster, FlagInfo{
		Name: "workload-cluster",
		Usage: "Specify the name of the workload cluster. The workload cluster is the one running operations and " +
			"workloads, such as TPC-C, on the cluster",
	})

	SideEyeApiToken string = ""
	_                      = registerRunFlag(&SideEyeApiToken, FlagInfo{
		Name: "side-eye-token",
		Usage: `The API token to use for configuring the Side-Eye agents running on the
						created clusters. If empty, the Side-Eye agents will not be started.
						When set, app.side-eye.io can be used to monitor running clusters and also
						timing out tests will get a snapshot before their clusters are destroyed.`,
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
			stage <ver>. Example: v20.1.4=cockroach-20.1,v20.2.0=cockroach-20.2.`,
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
			The probability of a matched test being selected to run. Incompatible with --selective-tests.
			Note: this will run at least one test per prefix.`,
	})

	UseSpotVM = NeverUseSpot
	_         = registerRunFlag(&UseSpotVM, FlagInfo{
		Name: "use-spot",
		Usage: `
			[never|always|auto] Use spot VMs for the cluster. If 'auto', the framework may use SpotVM to run tests.
			Note, this is merely a _hint_. The framework decides if a SpotVM should be used.`,
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

	AlwaysCollectArtifacts bool = false
	_                           = registerRunFlag(&AlwaysCollectArtifacts, FlagInfo{
		Name: "always-collect-artifacts",
		Usage: `
						Always collect artifacts during test teardown, even if the test did not
						time out or fail.`,
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
	defaultEncryptionProbability  = 1
	defaultFIPSProbability        = 0
	defaultARM64Probability       = 0
	defaultCockroachEAProbability = 0
	NeverUseSpot                  = "never"
	AlwaysUseSpot                 = "always"
	AutoUseSpot                   = "auto"
	MaxOperationParallelism       = 10
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
	for _, provider := range vm.Providers {
		provider.ConfigureProviderFlags(cmdFlags, vm.SingleProject)
	}
}

// AddRunOpsFlags adds all flags registered for the run-operations command to
// the given command flag set.
func AddRunOpsFlags(cmdFlags *pflag.FlagSet) {
	globalMan.AddFlagsToCommand(runOpsCmdID, cmdFlags)
}

// Changed returns non-nil FlagInfo iff a flag associated with a given value was present.
//
// For example: roachtestflags.Changed(&roachtestflags.Cloud) returns non-nil FlagInfo iff
// the `--cloud` flag was passed (even if the given value was the same with the
// default value).
func Changed(valPtr interface{}) *FlagInfo {
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
