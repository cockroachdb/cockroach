// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	test2 "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterNodes(t *testing.T) {
	c := &clusterImpl{spec: spec.MakeClusterSpec(10, spec.WorkloadNode())}
	c2 := &clusterImpl{spec: spec.MakeClusterSpec(10, spec.WorkloadNodeCount(0))}
	c3 := &clusterImpl{spec: spec.MakeClusterSpec(10, spec.WorkloadNodeCount(1))}
	c4 := &clusterImpl{spec: spec.MakeClusterSpec(10, spec.WorkloadNodeCount(4))}
	opts := func(opts ...option.Option) []option.Option {
		return opts
	}
	testCases := []struct {
		opts     []option.Option
		expected string
	}{
		{opts(), ""},
		{opts(c.All()), ":1-10"},
		{opts(c.Range(1, 2)), ":1-2"},
		{opts(c.Range(2, 5)), ":2-5"},
		{opts(c.All(), c.Range(2, 5)), ":1-10"},
		{opts(c.Range(2, 5), c.Range(7, 9)), ":2-5,7-9"},
		{opts(c.Range(2, 5), c.Range(6, 8)), ":2-8"},
		{opts(c.Node(2), c.Node(4), c.Node(6)), ":2,4,6"},
		{opts(c.Node(2), c.Node(3), c.Node(4)), ":2-4"},
		{opts(c.CRDBNodes()), ":1-9"},
		{opts(c.WorkloadNode()), ":10"},
		{opts(c2.CRDBNodes()), ":1-10"},
		{opts(c2.WorkloadNode()), ""},
		{opts(c3.CRDBNodes()), ":1-9"},
		{opts(c3.WorkloadNode()), ":10"},
		{opts(c4.CRDBNodes()), ":1-6"},
		{opts(c4.WorkloadNode()), ":7-10"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			nodes := c.MakeNodes(tc.opts...)
			if tc.expected != nodes {
				t.Fatalf("expected %s, but found %s", tc.expected, nodes)
			}
		})
	}
}

type testWrapper struct {
	*testing.T
	l *logger.Logger
}

func (t testWrapper) GetRunId() string {
	return "mock-run-id"
}

func (t testWrapper) Owner() string {
	return "mock-owner"
}

func (t testWrapper) ExportOpenmetrics() bool {
	return false
}

func (t testWrapper) SnapshotPrefix() string {
	return ""
}

func (t testWrapper) BuildVersion() *version.Version {
	panic("implement me")
}

func (t testWrapper) Cockroach() string {
	return "./dummy-path/to/cockroach"
}

func (t testWrapper) StandardCockroach() string {
	return "./dummy-path/to/cockroach"
}

func (t testWrapper) RuntimeAssertionsCockroach() string {
	return "./dummy-path/to/cockroach-ea"
}

func (t testWrapper) DeprecatedWorkload() string {
	return "./dummy-path/to/workload"
}

func (t testWrapper) IsBuildVersion(s string) bool {
	panic("implement me")
}

func (t testWrapper) Spec() interface{} {
	panic("implement me")
}

func (t testWrapper) VersionsBinaryOverride() map[string]string {
	panic("implement me")
}

func (t testWrapper) SkipInit() bool {
	panic("implement me")
}

func (t testWrapper) Progress(f float64) {
	panic("implement me")
}

func (t testWrapper) WorkerStatus(args ...interface{}) {
}

func (t testWrapper) WorkerProgress(f float64) {
	panic("implement me")
}

func (t testWrapper) IsDebug() bool {
	return false
}

func (t testWrapper) GoWithCancel(_ task.Func, _ ...task.Option) context.CancelFunc {
	panic("implement me")
}

func (t testWrapper) Go(_ task.Func, _ ...task.Option) {
	panic("implement me")
}

func (t testWrapper) NewGroup(_ ...task.Option) task.Group {
	panic("implement me")
}

func (t testWrapper) NewErrorGroup(_ ...task.Option) task.ErrorGroup {
	panic("implement me")
}

var _ test2.Test = testWrapper{}

// ArtifactsDir is part of the test.Test interface.
func (t testWrapper) ArtifactsDir() string {
	return ""
}

// PerfArtifactsDir is part of the test.Test interface.
func (t testWrapper) PerfArtifactsDir() string {
	return ""
}

// GoCoverArtifactsDir is part of the test.Test interface.
func (t testWrapper) GoCoverArtifactsDir() string {
	return ""
}

// logger is part of the testI interface.
func (t testWrapper) L() *logger.Logger {
	return t.l
}

// Status is part of the testI interface.
func (t testWrapper) Status(args ...interface{}) {}

func (t testWrapper) AddParam(label, value string) {}

func TestClusterMachineType(t *testing.T) {
	type machineTypeTestCase struct {
		machineType      string
		expectedCPUCount int
	}
	testCases := []machineTypeTestCase{
		// AWS machine types
		{"m6i.large", 2},
		{"m6i.xlarge", 4},
		{"m6i.2xlarge", 8},
		{"m6i.4xlarge", 16},
		{"m6id.8xlarge", 32},
		{"m6id.12xlarge", 48},
		{"m6id.16xlarge", 64},
		{"m6i.24xlarge", 96},
		{"m6id.large", 2},
		{"m6id.xlarge", 4},
		{"m6id.2xlarge", 8},
		{"m6id.4xlarge", 16},
		{"m6id.8xlarge", 32},
		{"m6id.12xlarge", 48},
		{"m6id.16xlarge", 64},
		{"m6id.24xlarge", 96},
		{"c6id.large", 2},
		{"c6id.xlarge", 4},
		{"c6id.2xlarge", 8},
		{"c6id.4xlarge", 16},
		{"c6id.8xlarge", 32},
		{"c6id.12xlarge", 48},
		{"c6id.16xlarge", 64},
		{"c6id.24xlarge", 96},
		// GCE machine types
		{"n2-standard-2", 2},
		{"n2-standard-4", 4},
		{"n2-standard-8", 8},
		{"n2-standard-16", 16},
		{"n2-standard-32", 32},
		{"n2-standard-64", 64},
		{"n2-standard-96", 96},
		{"n2-highmem-8", 8},
		{"n2-highcpu-16-2048", 16},
		{"n2-custom-32-65536", 32},
		{"t2a-standard-2", 2},
		{"t2a-standard-4", 4},
		{"t2a-standard-8", 8},
		{"t2a-standard-16", 16},
		{"t2a-standard-32", 32},
		{"t2a-standard-48", 48},
	}
	// Azure machine types
	for i := 2; i <= 96; i *= 2 {
		testCases = append(testCases, machineTypeTestCase{fmt.Sprintf("Standard_D%dds_v5", i), i})
		testCases = append(testCases, machineTypeTestCase{fmt.Sprintf("Standard_D%dpds_v5", i), i})
		testCases = append(testCases, machineTypeTestCase{fmt.Sprintf("Standard_D%dlds_v5", i), i})
		testCases = append(testCases, machineTypeTestCase{fmt.Sprintf("Standard_D%dplds_v5", i), i})
		testCases = append(testCases, machineTypeTestCase{fmt.Sprintf("Standard_E%dds_v5", i), i})
		testCases = append(testCases, machineTypeTestCase{fmt.Sprintf("Standard_E%dpds_v5", i), i})
	}

	for _, tc := range testCases {
		t.Run(tc.machineType, func(t *testing.T) {
			cpuCount := MachineTypeToCPUs(tc.machineType)
			if tc.expectedCPUCount != cpuCount {
				t.Fatalf("expected %d CPUs, but found %d", tc.expectedCPUCount, cpuCount)
			}
		})
	}
}

type machineTypeTestCase struct {
	cpus                int
	mem                 spec.MemPerCPU
	localSSD            bool
	arch                vm.CPUArch
	expectedMachineType string
	expectedArch        vm.CPUArch
}

func TestAWSMachineType(t *testing.T) {
	testCases := []machineTypeTestCase{}

	xlarge := func(cpus int) string {
		var size string
		switch {
		case cpus <= 2:
			size = "large"
		case cpus <= 4:
			size = "xlarge"
		case cpus <= 8:
			size = "2xlarge"
		case cpus <= 16:
			size = "4xlarge"
		case cpus <= 32:
			size = "8xlarge"
		case cpus <= 48:
			size = "12xlarge"
		case cpus <= 64:
			size = "16xlarge"
		case cpus <= 96:
			size = "24xlarge"
		default:
			size = "24xlarge"
		}
		return size
	}

	addAMD := func(mem spec.MemPerCPU) {
		family := func() string {
			switch mem {
			case spec.Auto:
				return "m6i"
			case spec.Standard:
				return "m6i"
			case spec.High:
				return "r6i"
			}
			return ""
		}

		for _, arch := range []vm.CPUArch{vm.ArchAMD64, vm.ArchFIPS} {
			family := family()

			testCases = append(testCases, machineTypeTestCase{1, mem, false, arch,
				fmt.Sprintf("%s.%s", family, xlarge(1)), arch})
			testCases = append(testCases, machineTypeTestCase{1, mem, true, arch,
				fmt.Sprintf("%sd.%s", family, xlarge(1)), arch})
			for _, i := range []int{2, 4, 8, 16, 32, 64, 96, 128} {
				if i > 16 && mem == spec.Auto {
					if i > 80 {
						// N.B. to keep parity with GCE, we use AMD Milan instead of Intel Ice Lake, keeping same 2GB RAM per CPU ratio.
						family = "c6a"
					} else {
						family = "c6i"
					}
				}
				testCases = append(testCases, machineTypeTestCase{i, mem, false, arch,
					fmt.Sprintf("%s.%s", family, xlarge(i)), arch})
				expectedMachineTypeWithLocalSSD := fmt.Sprintf("%sd.%s", family, xlarge(i))
				if family == "c6a" {
					// N.B. c6a doesn't support local SSD.
					expectedMachineTypeWithLocalSSD = fmt.Sprintf("%s.%s", family, xlarge(i))
				}
				testCases = append(testCases, machineTypeTestCase{i, mem, true, arch,
					expectedMachineTypeWithLocalSSD, arch})
			}
		}
	}
	addARM := func(mem spec.MemPerCPU) {
		fallback := false
		var family string

		switch mem {
		case spec.Auto:
			family = "m7g"
		case spec.Standard:
			family = "m7g"
		case spec.High:
			family = "r6i"
			fallback = true
		}

		if fallback {
			testCases = append(testCases, machineTypeTestCase{1, mem, false, vm.ArchARM64,
				fmt.Sprintf("%s.%s", family, xlarge(1)), vm.ArchAMD64})
			testCases = append(testCases, machineTypeTestCase{1, mem, true, vm.ArchARM64,
				fmt.Sprintf("%sd.%s", family, xlarge(1)), vm.ArchAMD64})
		} else {
			testCases = append(testCases, machineTypeTestCase{1, mem, false, vm.ArchARM64,
				fmt.Sprintf("%s.%s", family, xlarge(1)), vm.ArchARM64})
			testCases = append(testCases, machineTypeTestCase{1, mem, true, vm.ArchARM64,
				fmt.Sprintf("%sd.%s", family, xlarge(1)), vm.ArchARM64})
		}
		for _, i := range []int{2, 4, 8, 16, 32, 64, 96, 128} {
			if i > 16 && mem == spec.Auto {
				family = "c7g"
			}
			fallback = fallback || i > 64

			if fallback {
				if mem == spec.Auto {
					if i > 80 {
						// N.B. to keep parity with GCE, we use AMD Milan instead of Intel Ice Lake, keeping same 2GB RAM per CPU ratio.
						family = "c6a"
					} else {
						family = "c6i"
					}
				} else if mem == spec.Standard {
					family = "m6i"
				}
				// Expect fallback to AMD64.
				testCases = append(testCases, machineTypeTestCase{i, mem, false, vm.ArchARM64,
					fmt.Sprintf("%s.%s", family, xlarge(i)), vm.ArchAMD64})
				expectedMachineTypeWithLocalSSD := fmt.Sprintf("%sd.%s", family, xlarge(i))
				if family == "c6a" {
					// N.B. c6a doesn't support local SSD.
					expectedMachineTypeWithLocalSSD = fmt.Sprintf("%s.%s", family, xlarge(i))
				}
				testCases = append(testCases, machineTypeTestCase{i, mem, true, vm.ArchARM64,
					expectedMachineTypeWithLocalSSD, vm.ArchAMD64})
			} else {
				testCases = append(testCases, machineTypeTestCase{i, mem, false, vm.ArchARM64,
					fmt.Sprintf("%s.%s", family, xlarge(i)), vm.ArchARM64})
				testCases = append(testCases, machineTypeTestCase{i, mem, true, vm.ArchARM64,
					fmt.Sprintf("%sd.%s", family, xlarge(i)), vm.ArchARM64})
			}
		}
	}
	for _, mem := range []spec.MemPerCPU{spec.Auto, spec.Standard, spec.High} {
		addAMD(mem)
		addARM(mem)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d/%s/%t/%s", tc.cpus, tc.mem, tc.localSSD, tc.arch), func(t *testing.T) {
			machineType, selectedArch, err := spec.SelectAWSMachineType(tc.cpus, tc.mem, tc.localSSD, tc.arch)

			require.Equal(t, tc.expectedMachineType, machineType)
			require.Equal(t, tc.expectedArch, selectedArch)
			require.NoError(t, err)
		})
	}
	// spec.Low is not supported.
	_, _, err := spec.SelectAWSMachineType(4, spec.Low, false, vm.ArchAMD64)
	require.Error(t, err)

	_, _, err2 := spec.SelectAWSMachineType(16, spec.Low, false, vm.ArchAMD64)
	require.Error(t, err2)
}

func TestGCEMachineType(t *testing.T) {
	testCases := []machineTypeTestCase{}

	addAMD := func(mem spec.MemPerCPU) {
		series := func() string {
			switch mem {
			case spec.Auto:
				return "standard"
			case spec.Standard:
				return "standard"
			case spec.High:
				return "highmem"
			case spec.Low:
				return "highcpu"
			}
			return ""
		}

		for _, arch := range []vm.CPUArch{vm.ArchAMD64, vm.ArchFIPS} {
			series := series()

			testCases = append(testCases, machineTypeTestCase{1, mem, false, arch,
				fmt.Sprintf("n2-%s-%d", series, 2), arch})
			for _, i := range []int{2, 4, 8, 16, 32, 64, 96, 128} {
				if i > 16 && mem == spec.Auto {
					var expectedMachineType string
					if i > 80 {
						// N.B. n2 doesn't support custom instances with > 80 vCPUs. So, the best we can do is to go with n2d.
						expectedMachineType = fmt.Sprintf("n2d-custom-%d-%d", i, i*2048)
					} else {
						expectedMachineType = fmt.Sprintf("n2-custom-%d-%d", i, i*2048)
					}
					testCases = append(testCases, machineTypeTestCase{i, mem, false, arch,
						expectedMachineType, arch})
				} else {
					testCases = append(testCases, machineTypeTestCase{i, mem, false, arch,
						fmt.Sprintf("n2-%s-%d", series, i), arch})
				}
			}
		}
	}
	addARM := func(mem spec.MemPerCPU) {
		fallback := false
		var series string

		switch mem {
		case spec.Auto:
			series = "standard"
		case spec.Standard:
			series = "standard"
		case spec.High:
			fallback = true
			series = "highmem"
		case spec.Low:
			fallback = true
			series = "highcpu"
		}

		if fallback {
			testCases = append(testCases, machineTypeTestCase{1, mem, false, vm.ArchARM64,
				fmt.Sprintf("n2-%s-%d", series, 2), vm.ArchAMD64})
		} else {
			testCases = append(testCases, machineTypeTestCase{1, mem, false, vm.ArchARM64,
				fmt.Sprintf("t2a-%s-%d", series, 1), vm.ArchARM64})
		}
		for _, i := range []int{2, 4, 8, 16, 32, 64, 96, 128} {
			fallback = fallback || i > 48 || (i > 16 && mem == spec.Auto)

			if fallback {
				expectedMachineType := fmt.Sprintf("n2-%s-%d", series, i)
				if i > 16 && mem == spec.Auto {
					if i > 80 {
						// N.B. n2 doesn't support custom instances with > 80 vCPUs. So, the best we can do is to go with n2d.
						expectedMachineType = fmt.Sprintf("n2d-custom-%d-%d", i, i*2048)
					} else {
						expectedMachineType = fmt.Sprintf("n2-custom-%d-%d", i, i*2048)
					}
				}
				// Expect fallback to AMD64.
				testCases = append(testCases, machineTypeTestCase{i, mem, false, vm.ArchARM64,
					expectedMachineType, vm.ArchAMD64})
			} else {
				testCases = append(testCases, machineTypeTestCase{i, mem, false, vm.ArchARM64,
					fmt.Sprintf("t2a-%s-%d", series, i), vm.ArchARM64})
			}
		}
	}
	for _, mem := range []spec.MemPerCPU{spec.Auto, spec.Standard, spec.High, spec.Low} {
		addAMD(mem)
		addARM(mem)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d/%s/%s", tc.cpus, tc.mem, tc.arch), func(t *testing.T) {
			machineType, selectedArch := spec.SelectGCEMachineType(tc.cpus, tc.mem, tc.arch)

			require.Equal(t, tc.expectedMachineType, machineType)
			require.Equal(t, tc.expectedArch, selectedArch)
		})
	}
}

func TestAzureMachineType(t *testing.T) {
	testCases := []machineTypeTestCase{}

	addAMD := func(mem spec.MemPerCPU) {
		var series string
		switch mem {
		case spec.Auto:
			series = "D?ds_v5"
		case spec.Standard:
			series = "D?ds_v5"
		case spec.High:
			series = "E?ds_v5"
		}

		for _, arch := range []vm.CPUArch{vm.ArchAMD64, vm.ArchFIPS} {
			testCases = append(testCases, machineTypeTestCase{1, mem, false, arch,
				fmt.Sprintf("Standard_%s", strings.Replace(series, "?", strconv.Itoa(2), 1)), arch})
			for _, i := range []int{2, 4, 8, 16, 32, 64, 96, 128} {
				if i > 16 && mem == spec.Auto {
					// Dlds_v5 with 2GB per CPU.
					testCases = append(testCases, machineTypeTestCase{i, mem, false, arch,
						fmt.Sprintf("Standard_D%dlds_v5", i), arch})
				} else {
					testCases = append(testCases, machineTypeTestCase{i, mem, false, arch,
						fmt.Sprintf("Standard_%s", strings.Replace(series, "?", strconv.Itoa(i), 1)), arch})
				}
			}
		}
	}
	addARM := func(mem spec.MemPerCPU) {
		var series string
		switch mem {
		case spec.Auto:
			series = "D?pds_v5"
		case spec.Standard:
			series = "D?pds_v5"
		case spec.High:
			series = "E?pds_v5"
		}

		testCases = append(testCases, machineTypeTestCase{1, mem, false, vm.ArchARM64,
			fmt.Sprintf("Standard_%s", strings.Replace(series, "?", strconv.Itoa(2), 1)), vm.ArchARM64})

		for i := 2; i <= 96; i *= 2 {
			fallback := (series == "D?pds_v5" && i > 64) || (series == "D?plds_v5" && i > 64) || (series == "E?pds_v5" && i > 32)

			if fallback {
				var expectedMachineType string
				if series == "D?pds_v5" {
					expectedMachineType = fmt.Sprintf("Standard_D%dds_v5", i)
				} else if series == "D?plds_v5" {
					expectedMachineType = fmt.Sprintf("Standard_D%dlds_v5", i)
				} else if series == "E?pds_v5" {
					expectedMachineType = fmt.Sprintf("Standard_E%dds_v5", i)
				}
				// Expect fallback to AMD64.
				testCases = append(testCases, machineTypeTestCase{i, mem, false, vm.ArchARM64,
					expectedMachineType, vm.ArchAMD64})
			} else {
				if i > 16 && mem == spec.Auto {
					// Dplds_v5 with 2GB per CPU.
					testCases = append(testCases, machineTypeTestCase{i, mem, false, vm.ArchARM64,
						fmt.Sprintf("Standard_D%dplds_v5", i), vm.ArchARM64})
				} else {
					testCases = append(testCases, machineTypeTestCase{i, mem, false, vm.ArchARM64,
						fmt.Sprintf("Standard_%s", strings.Replace(series, "?", strconv.Itoa(i), 1)), vm.ArchARM64})
				}
			}
		}
	}
	for _, mem := range []spec.MemPerCPU{spec.Auto, spec.Standard, spec.High} {
		addAMD(mem)
		addARM(mem)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d/%s/%s", tc.cpus, tc.mem, tc.arch), func(t *testing.T) {
			machineType, selectedArch, err := spec.SelectAzureMachineType(tc.cpus, tc.mem, tc.arch)

			require.Equal(t, tc.expectedMachineType, machineType)
			require.Equal(t, tc.expectedArch, selectedArch)
			require.NoError(t, err)

			if tc.expectedArch != vm.ArchFIPS {
				// Check that we can derive the right cpu architecture from the machine type.
				require.Equal(t, tc.expectedArch, azure.CpuArchFromAzureMachineType(machineType))
			}
		})
	}
	// spec.Low is not supported.
	_, _, err := spec.SelectAzureMachineType(4, spec.Low, vm.ArchAMD64)
	require.Error(t, err)

	_, _, err2 := spec.SelectAzureMachineType(16, spec.Low, vm.ArchAMD64)
	require.Error(t, err2)
}

func TestMachineTypes(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "cluster_test"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd != "select-machine-type" {
				t.Fatalf("unknown directive: %q", d.Cmd)
			}
			var mem spec.MemPerCPU
			var cpuArch vm.CPUArch

			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "mem-per-cpu":
					mem = spec.ParseMemCPU(arg.Vals[0])
					if mem.String() == "unknown" {
						t.Fatalf("illegal value for 'mem-per-cpu': %s", arg.Vals[0])
					}
				case "cpu-arch":
					cpuArch = vm.ParseArch(arg.Vals[0])
					if cpuArch == vm.ArchUnknown {
						t.Fatalf("illegal value for 'cpu-arch': %s", arg.Vals[0])
					}
				default:
					t.Fatalf("unknown machine-type spec: %q", arg.Key)
				}
			}
			if cpuArch == "" {
				t.Fatalf("missing 'cpu-arch' spec")
			}
			var out strings.Builder
			var err error
			var gceMachineType, awsMachineType, azureMachineType []string

			cpu := []int{1, 2, 4, 8, 16, 32, 64, 96, 128}
			out.WriteString("cpus | 1,2,4,8,16 | 32,64 | 96,128\n")
			out.WriteString("----\n")

			for _, i := range cpu {
				machineType, selectedArch := spec.SelectGCEMachineType(i, mem, cpuArch)
				if selectedArch != cpuArch {
					machineType += fmt.Sprintf(" (%s)", selectedArch)
				}
				gceMachineType = append(gceMachineType, machineType)

				machineType, selectedArch, err = spec.SelectAWSMachineType(i, mem, false, cpuArch)
				if err != nil {
					machineType = "unsupported"
				} else if selectedArch != cpuArch {
					machineType += fmt.Sprintf(" (%s)", selectedArch)
				}
				awsMachineType = append(awsMachineType, machineType)

				machineType, selectedArch, err = spec.SelectAzureMachineType(i, mem, cpuArch)
				if err != nil {
					machineType = "unsupported"
				} else if selectedArch != cpuArch {
					machineType += fmt.Sprintf(" (%s)", selectedArch)
				}
				azureMachineType = append(azureMachineType, machineType)
			}
			out.WriteString("GCE | ")
			out.WriteString(fmt.Sprintf("%s | %s | %s\n", strings.Join(gceMachineType[:5], ", "), strings.Join(gceMachineType[5:7], ", "), strings.Join(gceMachineType[7:], ", ")))
			out.WriteString("AWS | ")
			out.WriteString(fmt.Sprintf("%s | %s | %s\n", strings.Join(awsMachineType[:5], ", "), strings.Join(awsMachineType[5:7], ", "), strings.Join(awsMachineType[7:], ", ")))
			out.WriteString("Azure | ")
			out.WriteString(fmt.Sprintf("%s | %s | %s\n", strings.Join(azureMachineType[:5], ", "), strings.Join(azureMachineType[5:7], ","), strings.Join(azureMachineType[7:], ", ")))

			return out.String()
		})
	})
}

func TestCmdLogFileName(t *testing.T) {
	ts := time.Date(2000, 1, 1, 15, 4, 12, 0, time.Local)

	const exp = `run_150412.000000000_n1,3-4,9_cockroach-bla-foo-ba`
	nodes := option.NodeListOption{1, 3, 4, 9}
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach", "bla", "--foo", "bar"),
	)
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach bla --foo bar"),
	)
}

func TestVerifyLibraries(t *testing.T) {
	originalLibraryPaths := libraryFilePaths
	defer func() { libraryFilePaths = originalLibraryPaths }()
	testCases := []struct {
		name             string
		verifyLibs       []string
		libraryFilePaths []string
		expectedError    error
	}{
		{
			name:             "valid nil input",
			verifyLibs:       nil,
			libraryFilePaths: []string{"/some/path/lib.so"},
			expectedError:    nil,
		},
		{
			name:             "no match",
			verifyLibs:       []string{"required_c"},
			libraryFilePaths: []string{"/some/path/lib.so"},
			expectedError: errors.Wrap(errors.Errorf("missing required library %s (arch=\"amd64\")",
				"required_c"), "cluster.VerifyLibraries"),
		},
		{
			name:             "no match on nil libs",
			verifyLibs:       []string{"required_b"},
			libraryFilePaths: nil,
			expectedError: errors.Wrap(errors.Errorf("missing required library %s (arch=\"amd64\")",
				"required_b"), "cluster.VerifyLibraries"),
		},
		{
			name:             "single match",
			verifyLibs:       []string{"geos"},
			libraryFilePaths: []string{"/lib/geos.so"},
			expectedError:    nil,
		},
		{
			name:             "single match, multiple extensions",
			verifyLibs:       []string{"geos"},
			libraryFilePaths: []string{"/lib/geos.linux-amd.so"},
			expectedError:    nil,
		},
		{
			name:             "multiple matches",
			verifyLibs:       []string{"lib", "ltwo", "geos"},
			libraryFilePaths: []string{"ltwo.so", "a/geos.so", "/some/path/to/lib.so"},
			expectedError:    nil,
		},
		{
			name:             "multiple matches, multiple extensions",
			verifyLibs:       []string{"lib", "ltwo", "geos"},
			libraryFilePaths: []string{"ltwo.linux-arm64.so", "a/geos.linux-amd64.fips.so", "/some/path/to/lib.darwin-arm64.so"},
			expectedError:    nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			libraryFilePaths = map[vm.CPUArch][]string{vm.ArchAMD64: tc.libraryFilePaths}
			actualError := VerifyLibraries(tc.verifyLibs, vm.ArchAMD64)
			if tc.expectedError == nil {
				require.NoError(t, actualError)
			} else {
				require.NotNil(t, actualError)
				require.EqualError(t, actualError, tc.expectedError.Error())
			}
		})
	}
}
