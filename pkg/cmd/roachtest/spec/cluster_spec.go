// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/ibm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type fileSystemType int

const (
	// Since ext4 is the default of 0, it isn't being
	// used anywhere in the code. Therefore, it isn't
	// added as a const here since it is unused, and
	// leads to a lint error.

	// Zfs file system.
	Zfs fileSystemType = 1

	// Extra labels added by roachtest
	RoachtestBranch = "roachtest-branch"
)

type MemPerCPU int

const (
	Auto MemPerCPU = iota
	Standard
	High
	Low
)

func (m MemPerCPU) String() string {
	switch m {
	case Auto:
		return "auto"
	case Standard:
		return "standard"
	case High:
		return "high"
	case Low:
		return "low"
	}
	return "unknown"
}

// ParseMemCPU parses a string into a MemPerCPU value. Returns -1 if no match is found.
func ParseMemCPU(s string) MemPerCPU {
	s = strings.ToLower(s)
	switch s {
	case "auto":
		return Auto
	case "standard":
		return Standard
	case "high":
		return High
	case "low":
		return Low
	}
	return -1
}

// LocalSSDSetting controls whether test cluster nodes use an instance-local SSD
// as storage.
type LocalSSDSetting int

const (
	// LocalSSDDefault is the default mode, when the test does not have any
	// preference. A local SSD may or may not be used, depending on --local-ssd
	// flag and machine type.
	LocalSSDDefault LocalSSDSetting = iota

	// LocalSSDDisable means that we will never use a local SSD.
	LocalSSDDisable

	// LocalSSDPreferOn means that we prefer to use a local SSD. It is not a
	// guarantee (depending on other constraints on machine type).
	LocalSSDPreferOn
)

// ClusterSpec represents a test's description of what its cluster needs to
// look like. It becomes part of a clusterConfig when the cluster is created.
type ClusterSpec struct {
	Arch      vm.CPUArch // CPU architecture; auto-chosen if left empty
	NodeCount int
	// WorkloadNode indicates if we are using workload nodes.
	// WorkloadNodeCount indicates count of the last few node of the cluster
	// treated as workload node. Defaults to a VM with 4 CPUs if not specified
	// by WorkloadNodeCPUs.
	// TODO(GouravKumar): remove use of WorkloadNode, use WorkloadNodeCount instead
	WorkloadNode      bool
	WorkloadNodeCount int
	WorkloadNodeCPUs  int
	// CPUs is the number of CPUs per node.
	CPUs                 int
	Mem                  MemPerCPU
	SSDs                 int
	RAID0                bool
	VolumeSize           int
	LocalSSD             LocalSSDSetting
	Geo                  bool
	Lifetime             time.Duration
	ReusePolicy          clusterReusePolicy
	TerminateOnMigration bool
	// Use a spot instance or equivalent of a cloud provider.
	UseSpotVMs bool
	// FileSystem determines the underlying FileSystem
	// to be used. The default is ext4.
	FileSystem fileSystemType

	RandomlyUseZfs bool

	GatherCores bool

	// GCE-specific arguments. These values apply only on clusters instantiated on GCE.
	GCE struct {
		MachineType    string
		MinCPUPlatform string
		VolumeType     string
		VolumeCount    int // volume count is only supported for GCE. This can be moved up if we start supporting other clouds
		Zones          string
	} `cloud:"gce"`

	// AWS-specific arguments. These values apply only on clusters instantiated on AWS.
	AWS struct {
		MachineType string
		// VolumeThroughput is the min provisioned EBS volume throughput.
		VolumeThroughput int
		// VolumeIOPS is the provisioned EBS volume IOPS.
		VolumeIOPS int
		Zones      string
	} `cloud:"aws"`

	// Azure-specific arguments. These values apply only on clusters instantiated on Azure.
	Azure struct {
		Zones string
	} `cloud:"azure"`
	// IBM-specific arguments. These values apply only on clusters instantiated on IBM.
	IBM struct {
		MachineType string
		VolumeType  string
		VolumeIOPS  int
		VolumeCount int
		Zones       string
	} `cloud:"ibm"`
}

// MakeClusterSpec makes a ClusterSpec.
func MakeClusterSpec(nodeCount int, opts ...Option) ClusterSpec {
	spec := ClusterSpec{NodeCount: nodeCount}
	defaultOpts := []Option{CPU(4), WorkloadNodeCPU(4), nodeLifetime(12 * time.Hour), ReuseAny()}
	for _, o := range append(defaultOpts, opts...) {
		o(&spec)
	}
	return spec
}

// ClustersCompatible returns true if the clusters are compatible, i.e. the test
// asking for s2 can reuse s1.
func ClustersCompatible(s1, s2 ClusterSpec, cloud Cloud) bool {
	// only consider the specification of the cloud that we are running in
	clearClusterSpecFields(&s1, cloud)
	clearClusterSpecFields(&s2, cloud)
	return s1 == s2
}

// clearClusterSpecFields clears the cloud specific specification from the cluster spec
// if the cloud specification does not match the target cloud. This is done to ensure that
// the specification for other clouds are not considered while comparing the cluster specifications.
func clearClusterSpecFields(cs *ClusterSpec, targetCloud Cloud) {
	cs.Lifetime = 0
	structType := reflect.TypeOf(*cs)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if tag, ok := field.Tag.Lookup("cloud"); ok {
			// Zero out struct if it is not the target cloud.
			if !strings.EqualFold(tag, targetCloud.String()) {
				fieldValue := reflect.ValueOf(cs).Elem().FieldByName(field.Name)
				fieldValue.Set(reflect.Zero(fieldValue.Type()))
			}
		}
	}
}

// String implements fmt.Stringer.
func (s ClusterSpec) String() string {
	str := fmt.Sprintf("n%dcpu%d", s.NodeCount, s.CPUs)
	switch s.Mem {
	case Standard:
		str += "sm"
	case High:
		str += "hm"
	case Low:
		str += "lm"
	}
	if s.Geo {
		str += "-Geo"
	}
	return str
}

// checks if an AWS machine supports SSD volumes
func awsMachineSupportsSSD(machineType string) bool {
	typeAndSize := strings.Split(machineType, ".")
	if len(typeAndSize) == 2 {
		// All SSD machine types that we use end in 'd or begins with i3 (e.g. i3, i3en).
		return strings.HasPrefix(typeAndSize[0], "i3") || strings.HasSuffix(typeAndSize[0], "d")
	}
	return false
}

func getAWSOpts(
	machineType string, volumeSize, ebsThroughput int, ebsIOPS int, localSSD bool, useSpotVMs bool,
) vm.ProviderOpts {
	opts := aws.DefaultProviderOpts()
	if volumeSize != 0 {
		opts.DefaultEBSVolume.Disk.VolumeSize = volumeSize
	}
	if ebsIOPS != 0 {
		opts.DefaultEBSVolume.Disk.IOPs = ebsIOPS
	}
	if ebsThroughput != 0 {
		opts.DefaultEBSVolume.Disk.Throughput = ebsThroughput
	}
	if localSSD {
		opts.SSDMachineType = machineType
	} else {
		opts.MachineType = machineType
	}
	opts.UseSpot = useSpotVMs
	return opts
}

func getAWSWorkloadOpts(machineType string, useSpotVMs bool) vm.ProviderOpts {
	opts := aws.DefaultProviderOpts()
	opts.MachineType = machineType
	opts.DefaultEBSVolume.Disk.VolumeType = ""
	opts.UseSpot = useSpotVMs
	opts.BootDiskOnly = true
	return opts
}

func getGCEOpts(
	machineType string,
	volumeSize, localSSDCount int,
	localSSD bool,
	RAID0 bool,
	terminateOnMigration bool,
	minCPUPlatform string,
	arch vm.CPUArch,
	volumeType string,
	volumeCount int,
	useSpot bool,
) vm.ProviderOpts {
	opts := gce.DefaultProviderOpts()
	opts.MachineType = machineType
	if arch == vm.ArchARM64 {
		// ARM64 machines don't support minCPUPlatform.
		opts.MinCPUPlatform = ""
	} else if minCPUPlatform != "" {
		opts.MinCPUPlatform = minCPUPlatform
	}
	if volumeSize != 0 {
		opts.PDVolumeSize = volumeSize
	}
	if volumeCount != 0 {
		opts.PDVolumeCount = volumeCount
	}
	opts.SSDCount = localSSDCount
	if (localSSD && localSSDCount > 0) || (!localSSD && volumeCount > 1) {
		// NB: As the default behavior for _roachprod_ (at least in AWS/GCP) is
		// to mount multiple disks as a single store using a RAID 0 array, we
		// must explicitly ask for multiple stores to be enabled, _unless_ the
		// test has explicitly asked for RAID0.
		opts.UseMultipleDisks = !RAID0
	}
	opts.TerminateOnMigration = terminateOnMigration
	opts.UseSpot = useSpot
	if volumeType != "" {
		opts.PDVolumeType = volumeType
	}
	return opts
}

func getGCEWorkloadOpts(
	machineType string,
	terminateOnMigration bool,
	minCPUPlatform string,
	arch vm.CPUArch,
	useSpot bool,
) vm.ProviderOpts {
	opts := gce.DefaultProviderOpts()
	opts.MachineType = machineType
	if arch == vm.ArchARM64 {
		// ARM64 machines don't support minCPUPlatform.
		opts.MinCPUPlatform = ""
	} else if minCPUPlatform != "" {
		opts.MinCPUPlatform = minCPUPlatform
	}
	opts.SSDCount = 0
	opts.PDVolumeCount = 0
	opts.BootDiskOnly = true
	opts.TerminateOnMigration = terminateOnMigration
	opts.UseSpot = useSpot
	return opts
}

func getAzureOpts(machineType string, volumeSize int) vm.ProviderOpts {
	opts := azure.DefaultProviderOpts()
	opts.MachineType = machineType
	if volumeSize != 0 {
		opts.NetworkDiskSize = int32(volumeSize)
	}
	return opts
}

func getAzureWorkloadOpts(machineType string) vm.ProviderOpts {
	opts := azure.DefaultProviderOpts()
	opts.MachineType = machineType
	opts.BootDiskOnly = true
	return opts
}

func getIBMOpts(
	machineType string,
	terminateOnMigration bool,
	volumeSize int,
	volumeType string,
	volumeIOPS int,
	extraVolumeCount int,
	RAID0 bool,
) vm.ProviderOpts {
	opts := ibm.DefaultProviderOpts()
	opts.MachineType = machineType
	opts.TerminateOnMigration = terminateOnMigration

	if volumeType != "" {
		opts.DefaultVolume.VolumeType = volumeType
	}
	if volumeSize != 0 {
		opts.DefaultVolume.VolumeSize = volumeSize
	}
	if volumeIOPS != 0 {
		opts.DefaultVolume.IOPS = volumeIOPS
	}

	// We reuse the parameters of the default data volume for extra volumes.
	opts.AttachedVolumes = make(ibm.IbmVolumeList, 0)
	if extraVolumeCount > 0 {
		for i := 0; i < extraVolumeCount; i++ {
			opts.AttachedVolumes = append(opts.AttachedVolumes, &ibm.IbmVolume{
				VolumeType: opts.DefaultVolume.VolumeType,
				VolumeSize: opts.DefaultVolume.VolumeSize,
				IOPS:       opts.DefaultVolume.IOPS,
			})
		}
		opts.UseMultipleDisks = !RAID0
	}

	return opts
}

func getIBMWorkloadOpts(machineType string, terminateOnMigration bool) vm.ProviderOpts {
	opts := ibm.DefaultProviderOpts()
	opts.MachineType = machineType
	opts.TerminateOnMigration = terminateOnMigration
	opts.BootDiskOnly = true
	return opts
}

// RoachprodClusterConfig contains general roachprod cluster configuration that
// does not depend on the test. It is used in conjunction with ClusterSpec to
// determine the final configuration.
type RoachprodClusterConfig struct {
	Cloud Cloud

	// UseIOBarrierOnLocalSSD is set if we don't want to mount local SSDs with the
	// `-o nobarrier` flag.
	UseIOBarrierOnLocalSSD bool

	// PreferredArch is the preferred CPU architecture; it is not guaranteed
	// (depending on cloud and on other requirements on machine type).
	PreferredArch vm.CPUArch

	// Defaults contains configuration values that are used when the ClusterSpec
	// does not specify the corresponding option.
	Defaults struct {
		// MachineType, if set, is the default machine type (used unless the
		// ClusterSpec specifies a machine type for the current cloud).
		//
		// If it is not set (and the ClusterSpec doesn't specify one either), a
		// machine type is determined automatically.
		MachineType string

		// Zones, if set, is the default zone configuration (unless the test
		// specifies a zone configuration for the current cloud).
		Zones string

		// PreferLocalSSD is the default local SSD mode (unless the test specifies a
		// preference). If true, we try to use a local SSD if allowed by the machine
		// type. If false, we never use a local SSD.
		PreferLocalSSD bool
	}
}

// RoachprodOpts returns the opts to use when calling `roachprod.Create()`
// in order to create the cluster described in the spec.
func (s *ClusterSpec) RoachprodOpts(
	params RoachprodClusterConfig,
) (vm.CreateOpts, vm.ProviderOpts, vm.ProviderOpts, vm.CPUArch, error) {
	useIOBarrier := params.UseIOBarrierOnLocalSSD
	requestedArch := params.PreferredArch

	preferLocalSSD := params.Defaults.PreferLocalSSD
	switch s.LocalSSD {
	case LocalSSDDisable:
		preferLocalSSD = false
	case LocalSSDPreferOn:
		preferLocalSSD = true
	}

	createVMOpts := vm.DefaultCreateOpts()
	// N.B. We set "usage=roachtest" as the default, custom label for billing tracking.
	createVMOpts.CustomLabels = map[string]string{vm.TagUsage: "roachtest"}

	branch := os.Getenv("TC_BUILD_BRANCH")
	if branch != "" {
		// If the branch is set, we add it as a custom label.
		createVMOpts.CustomLabels[RoachtestBranch] = vm.SanitizeLabel(branch)
	}

	createVMOpts.ClusterName = "" // Will be set later.
	if s.Lifetime != 0 {
		createVMOpts.Lifetime = s.Lifetime
	}
	cloud := params.Cloud
	switch cloud {
	case Local:
		createVMOpts.VMProviders = []string{cloud.String()}
		// remaining opts are not applicable to local clusters
		return createVMOpts, nil, nil, requestedArch, nil
	case AWS, GCE, Azure, IBM:
		createVMOpts.VMProviders = []string{cloud.String()}
	default:
		return vm.CreateOpts{}, nil, nil, "", errors.Errorf("unsupported cloud %v", cloud)
	}
	if cloud != GCE {
		// TODO(DarrylWong): support specifying SSD count on other providers, see: #123777.
		// Once done, revisit all tests that set SSD count to see if they can run on non GCE.
		if s.SSDs != 0 {
			return vm.CreateOpts{}, nil, nil, "", errors.Errorf("specifying SSD count is not yet supported on %s", cloud)
		}
	}

	createVMOpts.GeoDistributed = s.Geo
	createVMOpts.Arch = string(requestedArch)
	ssdCount := s.SSDs

	machineType := params.Defaults.MachineType
	switch cloud {
	case AWS:
		if s.AWS.MachineType != "" {
			machineType = s.AWS.MachineType
		}
	case GCE:
		if s.GCE.MachineType != "" {
			machineType = s.GCE.MachineType
		}
	case IBM:
		if s.IBM.MachineType != "" {
			machineType = s.IBM.MachineType
		}
	}
	// Assume selected machine type has the same arch as requested unless SelectXXXMachineType says otherwise.
	selectedArch := requestedArch

	if s.CPUs != 0 {
		// Default to the user-supplied machine type, if any.
		// Otherwise, pick based on requested CPU count.

		if machineType == "" {
			// If no machine type was specified, choose one
			// based on the cloud and CPU count.
			var err error
			switch cloud {
			case AWS:
				machineType, selectedArch, err = SelectAWSMachineType(s.CPUs, s.Mem, preferLocalSSD && s.VolumeSize == 0, requestedArch)
			case GCE:
				machineType, selectedArch = SelectGCEMachineType(s.CPUs, s.Mem, requestedArch)
			case Azure:
				machineType, selectedArch, err = SelectAzureMachineType(s.CPUs, s.Mem, requestedArch)
			case IBM:
				machineType, selectedArch, err = SelectIBMMachineType(s.CPUs, s.Mem, requestedArch)
			}

			if err != nil {
				return vm.CreateOpts{}, nil, nil, "", err
			}
			if requestedArch != "" && selectedArch != requestedArch {
				// TODO(srosenberg): we need a better way to monitor the rate of this mismatch, i.e.,
				// other than grepping cluster creation logs.
				fmt.Printf("WARN: requested arch %s for machineType %s, but selected %s\n", requestedArch, machineType, selectedArch)
				createVMOpts.Arch = string(selectedArch)
			}
		}

		// Local SSD can only be requested
		// - if configured to prefer doing so,
		// - if no particular volume size is requested, and,
		// - on AWS, if the machine type supports it.
		// - on GCE, if the machine type is not ARM64.
		if preferLocalSSD && s.VolumeSize == 0 && (cloud != AWS || awsMachineSupportsSSD(machineType)) &&
			(cloud != GCE || selectedArch != vm.ArchARM64) {
			// Ensure SSD count is at least 1 if UseLocalSSD is true.
			if ssdCount == 0 {
				ssdCount = 1
			}
			createVMOpts.SSDOpts.UseLocalSSD = true
			createVMOpts.SSDOpts.NoExt4Barrier = !useIOBarrier
		} else {
			createVMOpts.SSDOpts.UseLocalSSD = false
		}
	}

	if s.FileSystem == Zfs {
		if cloud != GCE && cloud != IBM {
			return vm.CreateOpts{}, nil, nil, "", errors.Errorf(
				"node creation with zfs file system not yet supported on %s", cloud,
			)
		}
		createVMOpts.SSDOpts.FileSystem = vm.Zfs
	} else if s.RandomlyUseZfs && (cloud == GCE || cloud == IBM) {
		rng, _ := randutil.NewPseudoRand()
		if rng.Float64() <= 0.2 {
			createVMOpts.SSDOpts.FileSystem = vm.Zfs
		}
	}

	var workloadMachineType string
	var err error
	switch cloud {
	case AWS:
		workloadMachineType, _, err = SelectAWSMachineType(s.WorkloadNodeCPUs, s.Mem, false, selectedArch)
	case GCE:
		workloadMachineType, _ = SelectGCEMachineType(s.WorkloadNodeCPUs, s.Mem, selectedArch)
	case Azure:
		workloadMachineType, _, err = SelectAzureMachineType(s.WorkloadNodeCPUs, s.Mem, selectedArch)
	case IBM:
		workloadMachineType, _, err = SelectIBMMachineType(s.WorkloadNodeCPUs, s.Mem, selectedArch)
	}
	if err != nil {
		return vm.CreateOpts{}, nil, nil, "", err
	}

	if createVMOpts.Arch == string(vm.ArchFIPS) && !(cloud == GCE || cloud == AWS) {
		return vm.CreateOpts{}, nil, nil, "", errors.Errorf(
			"FIPS not yet supported on %s", cloud,
		)
	}
	var providerOpts vm.ProviderOpts
	var workloadProviderOpts vm.ProviderOpts
	switch cloud {
	case AWS:
		providerOpts = getAWSOpts(machineType, s.VolumeSize, s.AWS.VolumeThroughput, s.AWS.VolumeIOPS,
			createVMOpts.SSDOpts.UseLocalSSD, s.UseSpotVMs)
		workloadProviderOpts = getAWSWorkloadOpts(workloadMachineType, s.UseSpotVMs)
	case GCE:
		providerOpts = getGCEOpts(machineType, s.VolumeSize, ssdCount,
			createVMOpts.SSDOpts.UseLocalSSD, s.RAID0, s.TerminateOnMigration,
			s.GCE.MinCPUPlatform, vm.ParseArch(createVMOpts.Arch), s.GCE.VolumeType, s.GCE.VolumeCount, s.UseSpotVMs,
		)
		workloadProviderOpts = getGCEWorkloadOpts(workloadMachineType, s.TerminateOnMigration,
			s.GCE.MinCPUPlatform, vm.ParseArch(createVMOpts.Arch), s.UseSpotVMs,
		)
	case Azure:
		providerOpts = getAzureOpts(machineType, s.VolumeSize)
		workloadProviderOpts = getAzureWorkloadOpts(machineType)
	case IBM:
		providerOpts = getIBMOpts(machineType, s.TerminateOnMigration, s.VolumeSize,
			s.IBM.VolumeType, s.IBM.VolumeIOPS, s.IBM.VolumeCount, s.RAID0,
		)
		workloadProviderOpts = getIBMWorkloadOpts(workloadMachineType, s.TerminateOnMigration)
	}

	return createVMOpts, providerOpts, workloadProviderOpts, selectedArch, nil
}

// SetRoachprodOptsZones updates the providerOpts with the VM zones as specified in the params/spec.
// We separate this logic from RoachprodOpts as we may need to call this multiple times in order to
// randomize the default GCE zone.
func (s *ClusterSpec) SetRoachprodOptsZones(
	providerOpts, workloadProviderOpts vm.ProviderOpts, params RoachprodClusterConfig, arch string,
) (vm.ProviderOpts, vm.ProviderOpts) {
	zonesStr := params.Defaults.Zones
	cloud := params.Cloud
	switch cloud {
	case AWS:
		if s.AWS.Zones != "" {
			zonesStr = s.AWS.Zones
		}
	case GCE:
		if s.GCE.Zones != "" {
			zonesStr = s.GCE.Zones
		}
	case Azure:
		if s.Azure.Zones != "" {
			zonesStr = s.Azure.Zones
		}
	case IBM:
		if s.IBM.Zones != "" {
			zonesStr = s.IBM.Zones
		}
	}
	var zones []string
	if zonesStr != "" {
		zones = strings.Split(zonesStr, ",")
		if !s.Geo {
			zones = zones[:1]
		}
	}

	switch cloud {
	case AWS:
		if len(zones) == 0 {
			zones = aws.DefaultZones(s.Geo)
		}
		providerOpts.(*aws.ProviderOpts).CreateZones = zones
		workloadProviderOpts.(*aws.ProviderOpts).CreateZones = zones
	case GCE:
		// We randomize the list of default zones for GCE for quota reasons, so decide the zone
		// early to ensure that the workload node and CRDB cluster have the same default zone.
		if len(zones) == 0 {
			zones = gce.DefaultZones(arch, s.Geo)
		}
		providerOpts.(*gce.ProviderOpts).Zones = zones
		workloadProviderOpts.(*gce.ProviderOpts).Zones = zones
	case Azure:
		if len(zones) == 0 {
			zones = azure.DefaultZones(s.Geo)
		}
		providerOpts.(*azure.ProviderOpts).Zones = zones
		workloadProviderOpts.(*azure.ProviderOpts).Zones = zones
	case IBM:
		if len(zones) == 0 {
			zones = ibm.DefaultZones(s.Geo)
		}
		providerOpts.(*ibm.ProviderOpts).CreateZones = zones
		workloadProviderOpts.(*ibm.ProviderOpts).CreateZones = zones
	}
	return providerOpts, workloadProviderOpts
}

// Expiration is the lifetime of the cluster. It may be destroyed after
// the expiration has passed.
func (s *ClusterSpec) Expiration() time.Time {
	l := s.Lifetime
	if l == 0 {
		l = 12 * time.Hour
	}
	return timeutil.Now().Add(l)
}

// TotalCPUs is the total amount of CPUs allocated for a cluster.
func (s *ClusterSpec) TotalCPUs() int {
	if !s.WorkloadNode {
		return s.NodeCount * s.CPUs
	}
	return (s.NodeCount-s.WorkloadNodeCount)*s.CPUs + (s.WorkloadNodeCPUs * s.WorkloadNodeCount)
}
