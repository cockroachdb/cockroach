// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce/gcedb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/ibm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type fileSystemType int

const (
	Ext4 fileSystemType = iota
	Zfs
	Xfs
	F2fs
	Btrfs

	// Extra labels added by roachtest
	RoachtestBranch = "roachtest-branch"

	MetamorphicVolumeType = "MetamorphicVolumeType"
	MetamorphicFilesystem = "MetamorphicFilesystem"
)

// ArchSet represents a set of CPU architectures using bitmasking.
//
// N.B. We call this a set to mirror how we represent other sets (e.g. clouds, suites),
// but we cannot use a map directly since we compare cluster specs for reusability.
type ArchSet uint8

const (
	ArchAMD64 ArchSet = 1 << iota
	ArchARM64
	ArchFIPS
)

// AllArchs contains all supported architectures.
//
// We omit s390x here as it is only supported iff the cloud is IBM.
var AllArchs = Archs(ArchAMD64, ArchARM64, ArchFIPS)

// OnlyAMD64 contains only the AMD64 architecture.
var OnlyAMD64 = Archs(ArchAMD64)

var OnlyARM64 = Archs(ArchARM64)

// OnlyFIPS contains only the FIPS architecture.
var OnlyFIPS = Archs(ArchFIPS)

var AllExceptFIPS = AllArchs.remove(ArchFIPS)

// Archs creates an ArchSet for the given architectures.
func Archs(archs ...ArchSet) ArchSet {
	var as ArchSet
	for _, arch := range archs {
		as |= arch
	}
	return as
}

// NoAMD64 removes the AMD64 architecture and returns the new set.
func (as ArchSet) NoAMD64() ArchSet {
	return as.remove(ArchAMD64)
}

// NoARM64 removes the ARM64 architecture and returns the new set.
func (as ArchSet) NoARM64() ArchSet {
	return as.remove(ArchARM64)
}

// NoFIPS removes the FIPS architecture and returns the new set.
func (as ArchSet) NoFIPS() ArchSet {
	return as.remove(ArchFIPS)
}

// remove returns a new ArchSet with the specified architecture removed.
func (as ArchSet) remove(arch ArchSet) ArchSet {
	return as &^ arch
}

// Contains returns true if the set contains the given architecture.
func (as ArchSet) Contains(arch ArchSet) bool {
	return as&arch != 0
}

func (as ArchSet) List() []vm.CPUArch {
	var archs []vm.CPUArch
	if as.Contains(ArchAMD64) {
		archs = append(archs, vm.ArchAMD64)
	}
	if as.Contains(ArchARM64) {
		archs = append(archs, vm.ArchARM64)
	}
	if as.Contains(ArchFIPS) {
		archs = append(archs, vm.ArchFIPS)
	}
	return archs
}

func (as ArchSet) String() string {
	var elems []string
	for _, arch := range as.List() {
		elems = append(elems, string(arch))
	}
	if len(elems) == 0 {
		return "<none>"
	}
	return strings.Join(elems, ",")
}

// IsEmpty returns true if the set contains no architectures.
func (as ArchSet) IsEmpty() bool {
	return as == 0
}

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
	CompatibleArchs ArchSet // The set of all valid architectures to choose from.
	NodeCount       int
	// WorkloadNode indicates if we are using workload nodes.
	// WorkloadNodeCount indicates count of the last few node of the cluster
	// treated as workload node. Defaults to a VM with 4 CPUs if not specified
	// by WorkloadNodeCPUs.
	// TODO(GouravKumar): remove use of WorkloadNode, use WorkloadNodeCount instead
	WorkloadNode         bool
	WorkloadNodeCount    int
	WorkloadNodeCPUs     int
	WorkloadRequiresDisk bool
	// CPUs is the number of CPUs per node.
	CPUs                 int
	Mem                  MemPerCPU
	DiskCount            int
	RAID0                bool
	VolumeSize           int
	VolumeType           string
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

	RandomlyUseZfs      bool
	RandomlyUseXfs      bool
	RandomizeVolumeType bool

	GatherCores bool

	// GCE-specific arguments. These values apply only on clusters instantiated on GCE.
	GCE struct {
		MachineType    string
		MinCPUPlatform string
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
		// VolumeIOPS is the provisioned IOPS for ultra-disks.
		VolumeIOPS int
	} `cloud:"azure"`
	// IBM-specific arguments. These values apply only on clusters instantiated on IBM.
	IBM struct {
		MachineType string
		VolumeIOPS  int
		Zones       string
	} `cloud:"ibm"`

	// ExposedMetamorphicInfo tracks metamorphic choices made during cluster creation
	// (e.g., randomly selected storage type, filesystem). This field is excluded
	// from cluster compatibility comparisons via the `compareIgnore` tag.
	ExposedMetamorphicInfo map[string]string `compareIgnore:"true"`
}

// MakeClusterSpec makes a ClusterSpec.
func MakeClusterSpec(nodeCount int, opts ...Option) ClusterSpec {
	spec := ClusterSpec{
		NodeCount:              nodeCount,
		ExposedMetamorphicInfo: make(map[string]string),
	}
	defaultOpts := []Option{CPU(4), WorkloadNodeCPU(4), nodeLifetime(12 * time.Hour), ReuseAny()}
	for _, o := range append(defaultOpts, opts...) {
		o(&spec)
	}
	return spec
}

// ClustersCompatible returns true if the clusters are compatible, i.e. the test
// asking for s2 can reuse s1.
func ClustersCompatible(s1, s2 ClusterSpec, cloud Cloud) bool {
	// Clear cloud-specific and comparison-irrelevant fields.
	clearClusterSpecFields(&s1, cloud)
	clearClusterSpecFields(&s2, cloud)

	// We use `reflect.DeepEqual` instead of simple direct `==` comparison
	// because ClusterSpec contains map fields which are not comparable
	// with direct comparison.
	return reflect.DeepEqual(s1, s2)
}

// clearClusterSpecFields clears the cloud specific specification from the cluster spec
// if the cloud specification does not match the target cloud. This is done to ensure that
// the specification for other clouds are not considered while comparing the cluster specifications.
// It also clears fields marked with `compareIgnore:"true"` tag, which should be excluded from
// cluster compatibility comparisons (e.g., ExposedMetamorphicInfo for tracking metamorphic choices).
func clearClusterSpecFields(cs *ClusterSpec, targetCloud Cloud) {
	cs.Lifetime = 0
	structType := reflect.TypeOf(*cs)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldValue := reflect.ValueOf(cs).Elem().FieldByName(field.Name)

		// Clear fields marked with compareIgnore tag - these should not affect
		// cluster compatibility (e.g., metamorphic info is just metadata)
		if _, ok := field.Tag.Lookup("compareIgnore"); ok {
			fieldValue.Set(reflect.Zero(fieldValue.Type()))
			continue
		}

		// Clear cloud-specific fields if they don't match the target cloud
		if tag, ok := field.Tag.Lookup("cloud"); ok {
			if !strings.EqualFold(tag, targetCloud.String()) {
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

func (s ClusterSpec) GetMetamorphicInfo() map[string]string {
	return s.ExposedMetamorphicInfo
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
	machineType string,
	volumeSize, diskCount, ebsThroughput int,
	volumeType string,
	ebsIOPS int,
	localSSD bool,
	RAID0 bool,
	useSpotVMs bool,
	bootDiskOnly bool,
) vm.ProviderOpts {
	opts := aws.DefaultProviderOpts()
	if volumeSize != 0 {
		opts.DefaultEBSVolume.Disk.VolumeSize = volumeSize
	}
	if volumeType != "" {
		opts.DefaultEBSVolume.Disk.VolumeType = volumeType
	}
	if diskCount != 0 {
		opts.EBSVolumeCount = diskCount
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
	opts.UseMultipleDisks = !RAID0
	opts.UseSpot = useSpotVMs
	opts.BootDiskOnly = bootDiskOnly
	return opts
}

func getGCEOpts(
	machineType string,
	volumeSize, diskCount int,
	localSSD bool,
	RAID0 bool,
	terminateOnMigration bool,
	minCPUPlatform string,
	arch vm.CPUArch,
	volumeType string,
	useSpot bool,
	bootDiskOnly bool,
) vm.ProviderOpts {
	opts := gce.DefaultProviderOpts()
	opts.MachineType = machineType
	// Default to "best" so that minCPUPlatform() resolves to the latest
	// platform available for the machine type, ensuring run-to-run
	// consistency regardless of machine family. Tests that explicitly
	// set GCE.MinCPUPlatform can still override this.
	if minCPUPlatform == "" {
		opts.MinCPUPlatform = "best"
	} else {
		opts.MinCPUPlatform = minCPUPlatform
	}
	if volumeSize != 0 {
		opts.PDVolumeSize = volumeSize
	}

	// Route diskCount to either local SSDs or persistent disks based on localSSD flag.
	if localSSD {
		opts.SSDCount = diskCount
		if opts.SSDCount == 0 {
			opts.SSDCount = 1 // Default to 1 local SSD
		}
	} else {
		if diskCount != 0 {
			opts.PDVolumeCount = diskCount
		}
	}

	opts.UseMultipleDisks = !RAID0
	opts.TerminateOnMigration = terminateOnMigration
	opts.UseSpot = useSpot
	if volumeType != "" {
		opts.PDVolumeType = volumeType
	}
	opts.BootDiskOnly = bootDiskOnly
	return opts
}

func defaultGCERemoteVolumeType(machineType string) string {
	if info, err := gcedb.GetMachineInfo(machineType); err == nil {
		if slices.Contains(info.StorageTypes, "pd-ssd") {
			return "pd-ssd"
		}
		if slices.Contains(info.StorageTypes, "hyperdisk-balanced") {
			return "hyperdisk-balanced"
		}
		if len(info.StorageTypes) > 0 {
			return info.StorageTypes[0]
		}
	}
	return ""
}

func gceMachineTypeSupportsLocalSSD(machineType string, requestedCount int) bool {
	info, err := gcedb.GetMachineInfo(machineType)
	if err != nil {
		return false
	}
	return slices.ContainsFunc(info.AllowedLocalSSDCount, func(count int) bool {
		return count > 0 && (requestedCount == 0 || count == requestedCount)
	})
}

// GCEMachineTypeWithLocalSSD returns a machine type that can use local SSDs
// while preserving any explicit disk count. This distinction matters for C4A:
// its local-SSD-capable shapes are encoded as -lssd machine types with fixed
// local SSD counts. A C4A shape with 4 fixed local SSDs must not satisfy a
// benchmark that explicitly asked for 16 stores, because that would preserve
// ARM64 while silently changing the benchmark's storage shape.
//
// requestedCount == 0 means the spec did not ask for a particular count; in
// that case any positive local SSD count is acceptable.
func GCEMachineTypeWithLocalSSD(machineType string, requestedCount int) (string, bool) {
	if gceMachineTypeSupportsLocalSSD(machineType, requestedCount) {
		return machineType, true
	}
	lssdMachineType := machineType + "-lssd"
	if gceMachineTypeSupportsLocalSSD(lssdMachineType, requestedCount) {
		return lssdMachineType, true
	}
	return machineType, false
}

func getAzureOpts(
	machineType string,
	volumeSize int,
	volumeType string,
	diskCount int,
	volumeIOPS int,
	RAID0 bool,
	bootDiskOnly bool,
) vm.ProviderOpts {
	opts := azure.DefaultProviderOpts()
	opts.MachineType = machineType
	if volumeSize != 0 {
		opts.NetworkDiskSize = int32(volumeSize)
	}
	opts.BootDiskOnly = bootDiskOnly
	if volumeType != "" {
		opts.NetworkDiskType = volumeType
	}
	if diskCount != 0 {
		opts.NetworkDiskCount = diskCount
	}
	if volumeIOPS != 0 {
		opts.UltraDiskIOPS = int64(volumeIOPS)
	}
	opts.UseMultipleDisks = !RAID0
	return opts
}

func getIBMOpts(
	machineType string,
	terminateOnMigration bool,
	volumeSize int,
	volumeType string,
	volumeIOPS int,
	diskCount int,
	RAID0 bool,
	bootDiskOnly bool,
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
	if diskCount != 0 {
		opts.AttachedVolumesCount = diskCount
	}
	opts.UseMultipleDisks = !RAID0
	opts.BootDiskOnly = bootDiskOnly

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

	// Benchmark indicates that the test is a performance benchmark. This is not a
	// generic machine-family selector: GCE ARM64 still defaults to C4A. GCE
	// storage resolution uses this bit only for benchmark continuity:
	//   - benchmark remote storage without an explicit volume type preserves the
	//     pre-C4A T2A + pd-ssd history;
	//   - benchmark local SSD, whether selected by PreferLocalSSD() or by the
	//     global default, preserves local SSD/store-count semantics by falling back
	//     to AMD64 when no compatible C4A -lssd shape exists.
	Benchmark bool

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
		// preference). For benchmarks, it preserves local SSD/store-count semantics
		// by falling back to AMD64 when no compatible C4A -lssd shape exists. For
		// non-benchmarks, it is a weak preference: we try to use local SSD when the
		// selected machine type supports it, but may keep the selected machine family
		// and fall back to a compatible remote disk when it does not. If false,
		// default clusters never use local SSD.
		PreferLocalSSD bool
	}
}

// RoachprodOpts returns the opts to use when calling `roachprod.Create()`
// in order to create the cluster described in the spec. It also returns the
// selected CRDB node machine type and arch after applying defaults and
// compatibility fallbacks.
func (s *ClusterSpec) RoachprodOpts(
	params RoachprodClusterConfig,
) (vm.CreateOpts, vm.ProviderOpts, vm.ProviderOpts, string, vm.CPUArch, error) {
	useIOBarrier := params.UseIOBarrierOnLocalSSD
	requestedArch := params.PreferredArch

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
		return createVMOpts, nil, nil, "", requestedArch, nil
	case AWS, GCE, Azure, IBM:
		createVMOpts.VMProviders = []string{cloud.String()}
	default:
		return vm.CreateOpts{}, nil, nil, "", "", errors.Errorf("unsupported cloud %v", cloud)
	}
	createVMOpts.GeoDistributed = s.Geo
	createVMOpts.Arch = string(requestedArch)

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
	machineTypeAutoSelected := machineType == ""

	if s.CPUs != 0 {
		// Default to the user-supplied machine type, if any.
		// Otherwise, pick based on requested CPU count.

		if machineType == "" {
			// If no machine type was specified, choose one
			// based on the cloud and CPU count.
			var err error
			switch cloud {
			case AWS:
				machineType, selectedArch, err = SelectAWSMachineType(
					s.CPUs, s.Mem, s.mayUseLocalSSD(params.Defaults.PreferLocalSSD), requestedArch,
				)
			case GCE:
				machineType, selectedArch = SelectGCEMachineType(
					s.CPUs, s.Mem, requestedArch,
				)
			case Azure:
				machineType, selectedArch, err = SelectAzureMachineType(s.CPUs, s.Mem, requestedArch)
			case IBM:
				machineType, selectedArch, err = SelectIBMMachineType(s.CPUs, s.Mem, requestedArch)
			}

			if err != nil {
				return vm.CreateOpts{}, nil, nil, "", "", err
			}
			if requestedArch != "" && selectedArch != requestedArch {
				// TODO(srosenberg): we need a better way to monitor the rate of this mismatch, i.e.,
				// other than grepping cluster creation logs.
				fmt.Printf("WARN: requested arch %s for machineType %s, but selected %s\n", requestedArch, machineType, selectedArch)
				createVMOpts.Arch = string(selectedArch)
			}
		}
	}

	selectGCEPDSSDMachineType := func(reason string) {
		if cloud != GCE || selectedArch != vm.ArchARM64 || !machineTypeAutoSelected || s.CPUs == 0 {
			return
		}
		prevMachineType := machineType
		machineType, selectedArch = SelectGCEMachineTypeForPDSSD(s.CPUs, s.Mem, vm.ArchARM64)
		createVMOpts.Arch = string(selectedArch)
		if selectedArch == vm.ArchARM64 {
			fmt.Printf(
				"INFO: using %s instead of %s because %s; C4A cannot attach pd-ssd, and T2A is the ARM64 family that preserves pd-ssd\n",
				machineType, prevMachineType, reason,
			)
		} else {
			fmt.Printf(
				"INFO: falling back from %s to %s (%s) because %s; C4A cannot attach pd-ssd, and T2A cannot represent this CPU/memory shape without changing the requested configuration\n",
				prevMachineType, machineType, selectedArch, reason,
			)
		}
	}

	switch s.FileSystem {
	case Ext4:
		// ext4 is the default, but we can randomly select zfs/xfs if requested.
		// Each alternative filesystem gets a 20% chance of being selected,
		// leaving the remainder for ext4.
		if s.RandomlyUseZfs || s.RandomlyUseXfs {
			rng, _ := randutil.NewPseudoRand()
			randFloat := rng.Float64()

			if s.RandomlyUseZfs && randFloat <= 0.2 {
				createVMOpts.SSDOpts.FileSystem = vm.Zfs
			} else if s.RandomlyUseXfs && randFloat > 0.2 && randFloat <= 0.4 {
				createVMOpts.SSDOpts.FileSystem = vm.Xfs
			}

			s.ExposedMetamorphicInfo[MetamorphicFilesystem] = string(createVMOpts.SSDOpts.FileSystem)
		}

	case Zfs:
		createVMOpts.SSDOpts.FileSystem = vm.Zfs
	case Xfs:
		createVMOpts.SSDOpts.FileSystem = vm.Xfs
	case F2fs:
		createVMOpts.SSDOpts.FileSystem = vm.F2fs
	case Btrfs:
		createVMOpts.SSDOpts.FileSystem = vm.Btrfs
	default:
		return vm.CreateOpts{}, nil, nil, "", "", errors.Errorf("unknown file system type: %v", s.FileSystem)
	}

	selectRandomizedVolumeType := func() string {
		// If the user selected RandomizeVolumeType, randomly pick a volume type
		// from the available volume types.
		availableVolumeTypes := []string{}

		// If the user did not explicitly disable local SSD and local SSD is available
		// for the selected cloud provider, machine type and architecture,
		// add it to the list of available volume types.
		if s.LocalSSD != LocalSSDDisable && s.isLocalSSDAvailable(cloud, machineType, selectedArch) == nil {
			availableVolumeTypes = append(availableVolumeTypes, "local-ssd")
		}

		switch cloud {
		case AWS:
			availableVolumeTypes = append(availableVolumeTypes, "gp3", "io2")
		case GCE:
			// Keep remote-disk randomization to one machine-compatible type for
			// now. Adding both pd-ssd and hyperdisk on families that support both
			// would split roachperf benchmark history; reevaluate once those runs
			// can compare storage types explicitly.
			if volumeType := defaultGCERemoteVolumeType(machineType); volumeType != "" {
				availableVolumeTypes = append(availableVolumeTypes, volumeType)
			}
		case Azure:
			availableVolumeTypes = append(availableVolumeTypes, "premium-ssd", "premium-ssd-v2", "ultra-disk")
		case IBM:
			availableVolumeTypes = append(availableVolumeTypes, "10iops-tier")
		}

		if len(availableVolumeTypes) > 0 {
			rng, _ := randutil.NewPseudoRand()
			selectedVolumeType := availableVolumeTypes[rng.Intn(len(availableVolumeTypes))]

			s.ExposedMetamorphicInfo[MetamorphicVolumeType] = selectedVolumeType
			return selectedVolumeType
		}
		return ""
	}

	// Determine which storage type to use. GCE has a dedicated decision helper
	// because C4A, T2A, benchmark continuity, and pd-ssd support interact in a
	// way that is hard to follow when derived inline. Other clouds retain the
	// existing simple priority order.
	selectedVolumeType := ""
	gceStorageDecision := GCEStorageDecision{}
	if cloud == GCE {
		gceStorageDecision = s.GCEStorageDecision(params.Benchmark, params.Defaults.PreferLocalSSD)
		switch gceStorageDecision.Kind {
		case GCEStorageLocalSSD, GCEStoragePDSSD, GCEStorageExplicitRemote:
			selectedVolumeType = gceStorageDecision.VolumeType
		case GCEStorageRandomized:
			selectedVolumeType = selectRandomizedVolumeType()
		case GCEStorageDefaultRemote:
			// The default remote type depends on the selected machine family and
			// is filled in below after any C4A -> T2A switch has been applied.
		}
	} else {
		switch {
		case s.VolumeType != "":
			// User explicitly set a volume type, use it directly.
			selectedVolumeType = s.VolumeType

		case s.LocalSSD == LocalSSDPreferOn:
			// User forced local SSD preference.
			selectedVolumeType = "local-ssd"

		case s.RandomizeVolumeType:
			selectedVolumeType = selectRandomizedVolumeType()

		case s.LocalSSD != LocalSSDDisable && params.Defaults.PreferLocalSSD:
			// No forced preference, no randomization, but default is to use local SSD
			// if available.
			selectedVolumeType = "local-ssd"
		}
	}

	if cloud == GCE && selectedArch == vm.ArchARM64 && gceStorageDecision.RequiresPDSSD() {
		// The storage decision has selected pd-ssd. C4A is still the default
		// ARM64 family, but C4A cannot attach pd-ssd. Use T2A when the requested
		// shape fits, and otherwise fall back to AMD64, so the run does not
		// quietly become a hyperdisk run.
		selectGCEPDSSDMachineType(gceStorageDecision.Reason)
		selectedVolumeType = "pd-ssd"
	}

	if cloud == GCE && selectedArch == vm.ArchARM64 && machineTypeAutoSelected && s.CPUs != 0 &&
		strings.HasPrefix(strings.ToLower(machineType), "c4a-") {
		zones := splitZones(s.zonesStrForCloud(cloud, params.Defaults.Zones), s.Geo)
		if len(zones) > 0 && !gce.IsSupportedC4AZone(zones) {
			prevMachineType := machineType
			machineType, selectedArch = SelectGCEMachineType(s.CPUs, s.Mem, vm.ArchAMD64)
			createVMOpts.Arch = string(selectedArch)
			fmt.Printf(
				"INFO: falling back from %s to %s (%s) because the configured zones %q are not supported by C4A\n",
				prevMachineType, machineType, selectedArch, strings.Join(zones, ","),
			)
		}
	}

	// Ensure we pick a volume type compatible with the machine type. Some
	// machine families (e.g., C4A) only support hyperdisk and cannot use
	// pd-ssd, which is the default in roachprod.
	if selectedVolumeType == "" && cloud == GCE {
		selectedVolumeType = defaultGCERemoteVolumeType(machineType)
	}

	// Local SSD will be used if selected (explicitly, by preference, or
	// randomly), and if no volume size is requested. On GCE, C4A local SSDs are
	// represented by -lssd machine types with fixed disk counts, so first try to
	// rewrite c4a-* to a compatible c4a-*-lssd shape. If none exists, preserve
	// local SSD by falling back to AMD64 for hard local SSD requirements
	// (VolumeType("local-ssd")) and for benchmark local SSD continuity, including
	// local SSD selected by the global --local-ssd default. Non-benchmark local
	// SSD preferences, including the global --local-ssd default and
	// PreferLocalSSD(), are allowed to keep C4A and fall back to the default
	// machine-compatible remote disk. Randomized storage only chooses local SSD when
	// it is already available for the selected machine. On non-GCE clouds, local
	// SSD is only used when the selected machine supports it and DiskCount <= 1
	// (only GCE supports multiple local SSDs).
	if selectedVolumeType == "local-ssd" {
		if cloud == GCE {
			if localSSDMachineType, ok := GCEMachineTypeWithLocalSSD(machineType, s.DiskCount); ok {
				machineType = localSSDMachineType
			} else if selectedArch == vm.ArchARM64 && machineTypeAutoSelected &&
				(s.VolumeType == "local-ssd" || (params.Benchmark && gceStorageDecision.UsesLocalSSD())) {
				// No compatible C4A -lssd shape exists for this machine/disk
				// count. Switch families before the generic local-SSD fallback
				// below converts hard local SSD requests and benchmark local SSD
				// continuity requirements to remote storage. Non-benchmark local
				// SSD preferences may fall back to remote storage instead.
				prevMachineType := machineType
				machineType, selectedArch = SelectGCEMachineType(s.CPUs, s.Mem, vm.ArchAMD64)
				createVMOpts.Arch = string(selectedArch)
				fmt.Printf(
					"INFO: falling back from %s to %s (%s) because this cluster selected local SSDs, but C4A cannot satisfy the requested local SSD configuration; preserving local SSD/store-count semantics is more important than preserving ARM64\n",
					prevMachineType, machineType, selectedArch,
				)
			}
		}
		if err := s.isLocalSSDAvailable(cloud, machineType, selectedArch); err != nil {
			// Local SSD was selected but is not available; fall back to default volume type.
			createVMOpts.SSDOpts.UseLocalSSD = false
			if cloud == GCE {
				selectedVolumeType = defaultGCERemoteVolumeType(machineType)
				s.VolumeType = selectedVolumeType
			}
			if selectedVolumeType != "" && selectedVolumeType != "local-ssd" {
				runKind := "benchmark"
				if !params.Benchmark {
					runKind = "non-benchmark"
				}
				fmt.Printf(
					"INFO: local SSD selected for %s but not available (%s); using %s\n",
					runKind, err.Error(), selectedVolumeType,
				)
			} else {
				fmt.Printf(
					"WARN: local SSD selected but not available (%s); falling back to default volume type\n",
					err.Error(),
				)
			}
		} else {
			createVMOpts.SSDOpts.UseLocalSSD = true

			// Disable ext4 barriers for local SSDs unless explicitly requested.
			// This is because local SSDs have very low latency and ext4 barriers
			// can significantly degrade performance.
			// This setting is only relevant if the selected filesystem is ext4.
			if !useIOBarrier && createVMOpts.SSDOpts.FileSystem == vm.Ext4 {
				createVMOpts.SSDOpts.NoExt4Barrier = true
			}
		}
	} else {
		createVMOpts.SSDOpts.UseLocalSSD = false
		if selectedVolumeType != "" {
			s.VolumeType = selectedVolumeType
		}
	}

	var workloadMachineType string
	var err error
	switch cloud {
	case AWS:
		workloadMachineType, _, err = SelectAWSMachineType(s.WorkloadNodeCPUs, s.Mem, false, selectedArch)
	case GCE:
		workloadMachineType, _ = SelectGCEMachineType(s.WorkloadNodeCPUs, s.Mem, selectedArch)
		if createVMOpts.SSDOpts.UseLocalSSD && s.WorkloadRequiresDisk {
			if localSSDMachineType, ok := GCEMachineTypeWithLocalSSD(workloadMachineType, s.DiskCount); ok {
				workloadMachineType = localSSDMachineType
			}
		}
	case Azure:
		workloadMachineType, _, err = SelectAzureMachineType(s.WorkloadNodeCPUs, s.Mem, selectedArch)
	case IBM:
		workloadMachineType, _, err = SelectIBMMachineType(s.WorkloadNodeCPUs, s.Mem, selectedArch)
	}
	if err != nil {
		return vm.CreateOpts{}, nil, nil, "", "", err
	}

	if createVMOpts.Arch == string(vm.ArchFIPS) && !(cloud == GCE || cloud == AWS) {
		return vm.CreateOpts{}, nil, nil, "", "", errors.Errorf(
			"FIPS not yet supported on %s", cloud,
		)
	}
	var providerOpts vm.ProviderOpts
	var workloadProviderOpts vm.ProviderOpts
	switch cloud {
	case AWS:
		providerOpts = getAWSOpts(machineType, s.VolumeSize, s.DiskCount, s.AWS.VolumeThroughput, s.VolumeType, s.AWS.VolumeIOPS,
			createVMOpts.SSDOpts.UseLocalSSD, s.RAID0, s.UseSpotVMs, false)
		workloadProviderOpts = getAWSOpts(workloadMachineType, s.VolumeSize, s.DiskCount, s.AWS.VolumeThroughput, s.VolumeType, s.AWS.VolumeIOPS,
			createVMOpts.SSDOpts.UseLocalSSD, s.RAID0, s.UseSpotVMs, !s.WorkloadRequiresDisk)
	case GCE:
		workloadVolumeType := s.VolumeType
		if !createVMOpts.SSDOpts.UseLocalSSD {
			// Workload nodes are auxiliary. Even when CRDB nodes require a
			// specific remote disk type, such as pd-ssd for benchmark continuity,
			// keep workload nodes on their machine-compatible default remote disk.
			// This lets ARM64 workload nodes remain on C4A while CRDB nodes use
			// T2A to preserve pd-ssd.
			if volumeType := defaultGCERemoteVolumeType(workloadMachineType); volumeType != "" {
				workloadVolumeType = volumeType
			}
		}
		providerOpts = getGCEOpts(machineType, s.VolumeSize, s.DiskCount,
			createVMOpts.SSDOpts.UseLocalSSD, s.RAID0, s.TerminateOnMigration,
			s.GCE.MinCPUPlatform, vm.ParseArch(createVMOpts.Arch), s.VolumeType,
			s.UseSpotVMs, false,
		)
		workloadProviderOpts = getGCEOpts(workloadMachineType, s.VolumeSize, s.DiskCount,
			createVMOpts.SSDOpts.UseLocalSSD, s.RAID0, s.TerminateOnMigration,
			s.GCE.MinCPUPlatform, vm.ParseArch(createVMOpts.Arch), workloadVolumeType,
			s.UseSpotVMs, !s.WorkloadRequiresDisk,
		)
	case Azure:
		providerOpts = getAzureOpts(machineType,
			s.VolumeSize, s.VolumeType, s.DiskCount, s.Azure.VolumeIOPS, s.RAID0, false,
		)
		workloadProviderOpts = getAzureOpts(workloadMachineType,
			s.VolumeSize, s.VolumeType, s.DiskCount, s.Azure.VolumeIOPS, s.RAID0, true,
		)
	case IBM:
		providerOpts = getIBMOpts(machineType, s.TerminateOnMigration,
			s.VolumeSize, s.VolumeType, s.IBM.VolumeIOPS, s.DiskCount, s.RAID0, false,
		)
		workloadProviderOpts = getIBMOpts(workloadMachineType, s.TerminateOnMigration,
			s.VolumeSize, s.VolumeType, s.IBM.VolumeIOPS, s.DiskCount, s.RAID0, true,
		)
	}

	return createVMOpts, providerOpts, workloadProviderOpts, machineType, selectedArch, nil
}

// mayUseLocalSSD returns whether this spec could end up using local SSDs.
// This is used to determine whether to select a machine type that supports
// local SSDs (e.g. on AWS, machine families with a "d" suffix). When local
// SSDs definitely won't be used, selecting a non-local-SSD machine type
// avoids unnecessary capacity constraints.
func (s *ClusterSpec) mayUseLocalSSD(defaultPreferLocalSSD bool) bool {
	if s.VolumeType != "" && s.VolumeType != "local-ssd" {
		return false
	}
	if s.LocalSSD == LocalSSDDisable {
		return false
	}
	if s.VolumeSize != 0 {
		return false
	}
	return s.VolumeType == "local-ssd" ||
		s.LocalSSD == LocalSSDPreferOn ||
		s.RandomizeVolumeType ||
		defaultPreferLocalSSD
}

func (s *ClusterSpec) zonesStrForCloud(cloud Cloud, defaultZones string) string {
	zonesStr := defaultZones
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
	return zonesStr
}

func splitZones(zonesStr string, geo bool) []string {
	if zonesStr == "" {
		return nil
	}
	zones := strings.Split(zonesStr, ",")
	if !geo {
		zones = zones[:1]
	}
	return zones
}

func (s *ClusterSpec) isLocalSSDAvailable(
	cloud Cloud, machineType string, selectedArch vm.CPUArch,
) error {
	switch cloud {
	case AWS:
		// On AWS, local SSDs are only supported on certain machine types.
		if !awsMachineSupportsSSD(machineType) {
			return fmt.Errorf("local SSDs not supported on AWS machine type %s", machineType)
		}
	case GCE:
		if _, ok := GCEMachineTypeWithLocalSSD(machineType, s.DiskCount); !ok {
			if s.DiskCount > 0 {
				return fmt.Errorf("local SSDs not supported on GCE machine type %s with disk count %d", machineType, s.DiskCount)
			}
			return fmt.Errorf("local SSDs not supported on GCE machine type %s", machineType)
		}
	case IBM:
		// On IBM, local SSDs are not supported.
		return errors.New("local SSDs not supported on IBM Cloud")
	}

	// Local SSDs are only supported when no volume size is specified.
	if s.VolumeSize != 0 {
		return errors.New("local SSDs not supported with non-zero volume size")
	}

	// TODO(DarrylWong): support specifying SSD count on other providers, see: #123777.
	// Once done, revisit all tests that set SSD count to see if they can run on non GCE.
	if cloud != GCE && s.DiskCount > 1 {
		return fmt.Errorf("local SSDs not supported on %s with disk count > 1", cloud)
	}

	// If we reach here, local SSDs are supported on the selected cloud and machine type.
	return nil
}

// RoachprodCreateZones returns the zones to use for a roachprod create attempt.
func (s *ClusterSpec) RoachprodCreateZones(
	params RoachprodClusterConfig, arch vm.CPUArch, machineType string,
) []string {
	cloud := params.Cloud
	zones := splitZones(s.zonesStrForCloud(cloud, params.Defaults.Zones), s.Geo)
	if len(zones) == 0 {
		zones = DefaultZonesForMachineType(cloud, arch, s.Geo, machineType)
	}
	return append([]string(nil), zones...)
}

// SetRoachprodOptsZones updates providerOpts with the zones for a roachprod
// create attempt.
func (s *ClusterSpec) SetRoachprodOptsZones(
	providerOpts, workloadProviderOpts vm.ProviderOpts, cloud Cloud, zones []string,
) (vm.ProviderOpts, vm.ProviderOpts) {
	switch cloud {
	case AWS:
		providerOpts.(*aws.ProviderOpts).CreateZones = zones
		workloadProviderOpts.(*aws.ProviderOpts).CreateZones = zones
	case GCE:
		providerOpts.(*gce.ProviderOpts).Zones = zones
		workloadProviderOpts.(*gce.ProviderOpts).Zones = zones
	case Azure:
		providerOpts.(*azure.ProviderOpts).Zones = zones
		workloadProviderOpts.(*azure.ProviderOpts).Zones = zones
	case IBM:
		providerOpts.(*ibm.ProviderOpts).CreateZones = zones
		workloadProviderOpts.(*ibm.ProviderOpts).CreateZones = zones
	}
	return providerOpts, workloadProviderOpts
}

// DefaultZones returns the package-level roachprod default zones for the cloud.
func DefaultZones(cloud Cloud, arch vm.CPUArch, geoDistributed bool) []string {
	return DefaultZonesForMachineType(cloud, arch, geoDistributed, "")
}

// DefaultZonesForMachineType returns package-level roachprod default zones for
// the cloud, using machine-family-specific GCE defaults where needed.
func DefaultZonesForMachineType(
	cloud Cloud, arch vm.CPUArch, geoDistributed bool, machineType string,
) []string {
	switch cloud {
	case AWS:
		return aws.DefaultZones(geoDistributed)
	case GCE:
		machineType = strings.ToLower(machineType)
		if strings.HasPrefix(machineType, "t2a-") {
			return gce.DefaultT2AZones(geoDistributed)
		}
		if strings.HasPrefix(machineType, "c4a-") {
			return gce.DefaultC4AZones(geoDistributed)
		}
		return gce.DefaultZones(string(arch), geoDistributed)
	case Azure:
		return azure.DefaultZones(geoDistributed)
	case IBM:
		return ibm.DefaultZones(geoDistributed)
	default:
		return nil
	}
}

// DefaultRetryZoneCandidates returns the provider's default non-geo zone pool.
// Roachtest uses this to choose a different zone after provider capacity
// failures.
func DefaultRetryZoneCandidates(cloud Cloud, machineType string) []string {
	switch cloud {
	case AWS:
		return aws.DefaultRetryZoneCandidates()
	case GCE:
		return gce.DefaultRetryZoneCandidates(machineType)
	case Azure:
		return azure.DefaultRetryZoneCandidates()
	default:
		return nil
	}
}

func (s *ClusterSpec) roachprodOptsZonesString(params RoachprodClusterConfig) (string, bool) {
	zonesStr := params.Defaults.Zones
	explicit := zonesStr != ""
	switch params.Cloud {
	case AWS:
		if s.AWS.Zones != "" {
			zonesStr = s.AWS.Zones
			explicit = true
		}
	case GCE:
		if s.GCE.Zones != "" {
			zonesStr = s.GCE.Zones
			explicit = true
		}
	case Azure:
		if s.Azure.Zones != "" {
			zonesStr = s.Azure.Zones
			explicit = true
		}
	case IBM:
		if s.IBM.Zones != "" {
			zonesStr = s.IBM.Zones
			explicit = true
		}
	}
	return zonesStr, explicit
}

// UsesExplicitZones returns true when the cluster has user-specified zones.
func (s *ClusterSpec) UsesExplicitZones(params RoachprodClusterConfig) bool {
	_, explicit := s.roachprodOptsZonesString(params)
	return explicit
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
