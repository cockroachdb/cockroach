// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

const (
	// TagCluster is cluster name tag const.
	TagCluster = "cluster"
	// TagCreated is created time tag const, RFC3339-formatted timestamp.
	TagCreated = "created"
	// TagLifetime is lifetime tag const.
	TagLifetime = "lifetime"
	// TagRoachprod is roachprod tag const, value is true & false.
	TagRoachprod = "roachprod"
	// TagSpotInstance is a tag added to spot instance vms with value as true.
	TagSpotInstance = "spot"
	// TagUsage indicates where a certain resource is used. "roachtest" is used
	// as the key for roachtest created resources.
	TagUsage = "usage"
	// TagArch is the CPU architecture tag const.
	TagArch = "arch"

	ArchARM64   = CPUArch("arm64")
	ArchAMD64   = CPUArch("amd64")
	ArchFIPS    = CPUArch("fips")
	ArchUnknown = CPUArch("unknown")
)

// UnimplementedError is returned when a method is not implemented by a
// provider. An error is returned instead of panicking to isolate failures to a
// single test (in the context of `roachtest`), otherwise the entire test run
// would fail.
var UnimplementedError = errors.New("unimplemented")

type CPUArch string

// ParseArch parses a string into a CPUArch using a simple, non-exhaustive heuristic.
// Supported input values were extracted from the following CLI tools/binaries: file, gcloud, aws
func ParseArch(s string) CPUArch {
	if s == "" {
		return ArchUnknown
	}
	arch := strings.ToLower(s)

	if strings.Contains(arch, "amd64") || strings.Contains(arch, "x86_64") ||
		strings.Contains(arch, "intel") {
		return ArchAMD64
	}
	if strings.Contains(arch, "arm64") || strings.Contains(arch, "aarch64") ||
		strings.Contains(arch, "ampere") || strings.Contains(arch, "graviton") {
		return ArchARM64
	}
	if strings.Contains(arch, "fips") {
		return ArchFIPS
	}
	return ArchUnknown
}

// GetDefaultLabelMap returns a label map for a common set of labels.
func GetDefaultLabelMap(opts CreateOpts) map[string]string {
	// Add architecture override tag, only if it was specified.
	if opts.Arch != "" {
		return map[string]string{
			TagCluster:   opts.ClusterName,
			TagLifetime:  opts.Lifetime.String(),
			TagRoachprod: "true",
			TagArch:      opts.Arch,
		}
	}
	return map[string]string{
		TagCluster:   opts.ClusterName,
		TagLifetime:  opts.Lifetime.String(),
		TagRoachprod: "true",
	}
}

// A VM is an abstract representation of a specific machine instance.  This type is used across
// the various cloud providers supported by roachprod.
type VM struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	// If non-empty, indicates that some or all of the data in the VM instance
	// is not present or otherwise invalid.
	Errors      []error           `json:"errors"`
	Lifetime    time.Duration     `json:"lifetime"`
	Preemptible bool              `json:"preemptible"`
	Labels      map[string]string `json:"labels"`
	// The provider-internal DNS name for the VM instance
	DNS string `json:"dns"`

	// PublicDNS is the public DNS name that can be used to connect to the VM.
	PublicDNS string `json:"public_dns"`
	// The DNS provider to use for DNS operations performed for this VM.
	DNSProvider string `json:"dns_provider"`

	// The name of the cloud provider that hosts the VM instance
	Provider string `json:"provider"`
	// The provider-specific id for the instance.  This may or may not be the same as Name, depending
	// on whether or not the cloud provider automatically assigns VM identifiers.
	ProviderID string `json:"provider_id"`
	PrivateIP  string `json:"private_ip"`
	PublicIP   string `json:"public_ip"`
	// The username that should be used to connect to the VM.
	RemoteUser string `json:"remote_user"`
	// The VPC value defines an equivalency set for VMs that can route
	// to one another via private IP addresses.  We use this later on
	// when determining whether or not cluster member should advertise
	// their public or private IP.
	VPC         string `json:"vpc"`
	MachineType string `json:"machine_type"`
	// When available, either vm.ArchAMD64 or vm.ArchARM64.
	CPUArch CPUArch `json:"cpu_architecture"`
	// When available, 'Haswell', 'Skylake', etc.
	CPUFamily string `json:"cpu_family"`
	Zone      string `json:"zone"`
	// Project represents the project to which this vm belongs, if the VM is in a
	// cloud that supports project (i.e. GCE). Empty otherwise.
	Project string `json:"project"`

	// LocalClusterName is only set for VMs in a local cluster.
	LocalClusterName string `json:"local_cluster_name,omitempty"`

	// NonBootAttachedVolumes are the non-bootable, _persistent_ volumes attached to the VM.
	NonBootAttachedVolumes []Volume `json:"non_bootable_volumes"`

	// BootVolume is the bootable, _persistent_ volume attached to the VM.
	BootVolume Volume `json:"bootable_volume"`

	// LocalDisks are the ephemeral SSD disks attached to the VM.
	LocalDisks []Volume `json:"local_disks"`

	// CostPerHour is the estimated cost per hour of this VM, in US dollars. 0 if
	//there is no estimate available.
	CostPerHour float64

	// EmptyCluster indicates that the VM does not exist. Azure allows for empty
	// clusters, but roachprod does not allow VM-less clusters except when deleting them.
	// A fake VM will be used in this scenario.
	EmptyCluster bool
}

// Name generates the name for the i'th node in a cluster.
func Name(cluster string, idx int) string {
	return fmt.Sprintf("%s-%0.4d", cluster, idx)
}

// Error values for VM.Error
var (
	ErrBadNetwork    = errors.New("could not determine network information")
	ErrBadScheduling = errors.New("could not determine scheduling information")
	ErrInvalidName   = errors.New("invalid VM name")
	ErrNoExpiration  = errors.New("could not determine expiration")
)

var regionRE = regexp.MustCompile(`(.*[^-])-?[a-z]$`)

// IsLocal returns true if the VM represents the local host.
func (vm *VM) IsLocal() bool {
	return vm.Zone == config.Local
}

// Locality returns the cloud, region, and zone for the VM.  We want to include the cloud, since
// GCE and AWS use similarly-named regions (e.g. us-east-1)
func (vm *VM) Locality() (string, error) {
	var region string
	if vm.IsLocal() {
		region = vm.Zone
	} else if match := regionRE.FindStringSubmatch(vm.Zone); len(match) == 2 {
		region = match[1]
	} else {
		return "", errors.Newf("unable to parse region from zone %q", vm.Zone)
	}
	return fmt.Sprintf("cloud=%s,region=%s,zone=%s", vm.Provider, region, vm.Zone), nil
}

// ZoneEntry returns a line representing the VMs DNS zone entry
func (vm *VM) ZoneEntry() (string, error) {
	if len(vm.Name) >= 60 {
		return "", errors.Errorf("Name too long: %s", vm.Name)
	}
	if vm.PublicIP == "" {
		return "", errors.Errorf("Missing IP address: %s", vm.Name)
	}
	// TODO(rail): We should probably skip local VMs too. They add a bunch of
	// entries for localhost.roachprod.crdb.io pointing to 127.0.0.1.
	return fmt.Sprintf("%s 60 IN A %s\n", vm.Name, vm.PublicIP), nil
}

func (vm *VM) AttachVolume(l *logger.Logger, v Volume) (deviceName string, _ error) {
	vm.NonBootAttachedVolumes = append(vm.NonBootAttachedVolumes, v)
	if err := ForProvider(vm.Provider, func(provider Provider) error {
		var err error
		deviceName, err = provider.AttachVolume(l, v, vm)
		return err
	}); err != nil {
		return "", err
	}
	return deviceName, nil
}

const vmNameFormat = "user-<clusterid>-<nodeid>"

// ClusterName returns the cluster name a VM belongs to.
func (vm *VM) ClusterName() (string, error) {
	if vm.IsLocal() {
		return vm.LocalClusterName, nil
	}
	name := vm.Name
	parts := strings.Split(name, "-")
	if len(parts) < 3 {
		return "", fmt.Errorf("expected VM name in the form %s, got %s", vmNameFormat, name)
	}
	return strings.Join(parts[:len(parts)-1], "-"), nil
}

// UserName returns the username of a VM.
func (vm *VM) UserName() (string, error) {
	if vm.IsLocal() {
		return config.Local, nil
	}
	name := vm.Name
	parts := strings.Split(name, "-")
	if len(parts) < 3 {
		return "", fmt.Errorf("expected VM name in the form %s, got %s", vmNameFormat, name)
	}
	return parts[0], nil
}

// List represents a list of VMs.
type List []VM

func (vl List) Len() int           { return len(vl) }
func (vl List) Swap(i, j int)      { vl[i], vl[j] = vl[j], vl[i] }
func (vl List) Less(i, j int) bool { return vl[i].Name < vl[j].Name }

// Names sxtracts all VM.Name entries from the List
func (vl List) Names() []string {
	ret := make([]string, len(vl))
	for i, vm := range vl {
		ret[i] = vm.Name
	}
	return ret
}

// ProviderIDs extracts all ProviderID values from the List.
func (vl List) ProviderIDs() []string {
	ret := make([]string, len(vl))
	for i, vm := range vl {
		ret[i] = vm.ProviderID
	}
	return ret
}

const (
	// Zfs refers to the zfs file system.
	Zfs = "zfs"
	// Ext4 refers to the ext4 file system.
	Ext4 = "ext4"
)

// CreateOpts is the set of options when creating VMs.
type CreateOpts struct {
	ClusterName  string
	Lifetime     time.Duration
	CustomLabels map[string]string

	GeoDistributed bool
	Arch           string
	VMProviders    []string
	SSDOpts        struct {
		UseLocalSSD bool
		// NoExt4Barrier, if set, makes the "-o nobarrier" flag be used when
		// mounting the SSD. Ignored if UseLocalSSD is not set.
		NoExt4Barrier bool
		// The file system to be used. This is set to "ext4" by default.
		FileSystem string
	}
	OsVolumeSize int
}

// DefaultCreateOpts returns a new vm.CreateOpts with default values set.
func DefaultCreateOpts() CreateOpts {
	defaultCreateOpts := CreateOpts{
		ClusterName:    "",
		Lifetime:       12 * time.Hour,
		GeoDistributed: false,
		VMProviders:    []string{},
		OsVolumeSize:   10,
		// N.B. When roachprod is used via CLI, this will be overridden by {"roachprod":"true"}.
		CustomLabels: map[string]string{"roachtest": "true"},
	}
	defaultCreateOpts.SSDOpts.UseLocalSSD = true
	defaultCreateOpts.SSDOpts.NoExt4Barrier = true
	defaultCreateOpts.SSDOpts.FileSystem = Ext4

	return defaultCreateOpts
}

// MultipleProjectsOption is used to specify whether a command accepts multiple
// values for the --gce-project flag.
type MultipleProjectsOption bool

const (
	// SingleProject means that a single project is accepted.
	SingleProject MultipleProjectsOption = false
	// AcceptMultipleProjects means that multiple projects are supported.
	AcceptMultipleProjects = true
)

// ProviderOpts is a hook point for Providers to supply additional,
// provider-specific options to various roachprod commands. In general, the flags
// should be prefixed with the provider's name to prevent collision between
// similar options.
//
// If a new command is added (perhaps `roachprod enlarge`) that needs
// additional provider- specific flags, add a similarly-named method
// `ConfigureEnlargeFlags` to mix in the additional flags.
type ProviderOpts interface {
	// ConfigureCreateFlags configures a FlagSet with any options relevant to the
	// `create` command.
	ConfigureCreateFlags(*pflag.FlagSet)
}

// VolumeSnapshot is an abstract representation of a specific volume snapshot.
// This type is used across various cloud providers supported by roachprod.
type VolumeSnapshot struct {
	ID   string
	Name string
}

type VolumeSnapshots []VolumeSnapshot

func (v VolumeSnapshots) Len() int {
	return len(v)
}

func (v VolumeSnapshots) Less(i, j int) bool {
	// This sorting-by-name looks like it happens by default in the gcloud API,
	// but it doesn't hurt to be paranoid. Since node index number is part of
	// the fingerprint, this plays nicely with applying snapshots in index
	// order. That matters -- if the workload is being run on the 10th node,
	// it's not expecting to have CRDB state. Nor should we expect the 9-node
	// CRDB cluster to have to work out that now the 10th roachprod node should
	// be running the CRDB process post snapshot application.
	return strings.Compare(v[i].Name, v[j].Name) < 0
}

func (v VolumeSnapshots) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

var _ sort.Interface = VolumeSnapshots{}

// VolumeSnapshotCreateOpts groups input callers can provide when creating
// volume snapshots. Namely, what name it has, the labels it's created with, and
// a description (visible through cloud consoles).
type VolumeSnapshotCreateOpts struct {
	Name        string
	Labels      map[string]string
	Description string
}

// VolumeSnapshotListOpts provides a way to search for specific volume
// snapshots. Callers can regex match snapshot names, search by exact labels, or
// filter only for snapshots created before some timestamp. Individual
// parameters are optional and can be combined with others.
type VolumeSnapshotListOpts struct {
	NamePrefix    string
	Labels        map[string]string
	CreatedBefore time.Time
}

// Volume is an abstract representation of a specific volume/disks. This type is
// used across various cloud providers supported by roachprod, and can typically
// be snapshotted or attached, detached, mounted from existing VMs.
type Volume struct {
	ProviderResourceID string
	ProviderVolumeType string
	Zone               string
	Encrypted          bool
	Name               string
	Labels             map[string]string
	Size               int
}

// VolumeCreateOpts groups input callers can provide when creating volumes.
type VolumeCreateOpts struct {
	Name string
	// N.B. Customer managed encryption is not supported at this time
	Encrypted        bool
	Architecture     string
	IOPS             int
	Size             int
	Type             string
	SourceSnapshotID string
	Zone             string
	Labels           map[string]string
}

type ListOptions struct {
	Username             string // if set, <username>-.* clusters are detected as 'mine'
	IncludeVolumes       bool
	IncludeEmptyClusters bool
	ComputeEstimatedCost bool
	IncludeProviders     []string
}

type PreemptedVM struct {
	Name        string
	PreemptedAt time.Time
}

// CreatePreemptedVMs returns a list of PreemptedVM created from given list of vmNames
func CreatePreemptedVMs(vmNames []string) []PreemptedVM {
	preemptedVMs := make([]PreemptedVM, len(vmNames))
	for i, name := range vmNames {
		preemptedVMs[i] = PreemptedVM{Name: name}
	}
	return preemptedVMs
}

// ServiceAddress stores the IP and port of a service.
type ServiceAddress struct {
	IP   string
	Port int
}

// A Provider is a source of virtual machines running on some hosting platform.
type Provider interface {
	// ConfigureProviderFlags is used to specify flags that apply to the provider
	// instance and should be used for all clusters managed by the provider.
	ConfigureProviderFlags(*pflag.FlagSet, MultipleProjectsOption)

	// ConfigureClusterCleanupFlags configures a FlagSet with any options
	// relevant to commands (`gc`)
	ConfigureClusterCleanupFlags(*pflag.FlagSet)

	CreateProviderOpts() ProviderOpts
	CleanSSH(l *logger.Logger) error

	// ConfigSSH takes a list of zones and configures SSH for machines in those
	// zones for the given provider.
	ConfigSSH(l *logger.Logger, zones []string) error
	Create(l *logger.Logger, names []string, opts CreateOpts, providerOpts ProviderOpts) (List, error)
	Grow(l *logger.Logger, vms List, clusterName string, names []string) (List, error)
	Shrink(l *logger.Logger, vmsToRemove List, clusterName string) error
	Reset(l *logger.Logger, vms List) error
	Delete(l *logger.Logger, vms List) error
	Extend(l *logger.Logger, vms List, lifetime time.Duration) error
	// Return the account name associated with the provider
	FindActiveAccount(l *logger.Logger) (string, error)
	List(l *logger.Logger, opts ListOptions) (List, error)
	// AddLabels adds (or updates) the given labels to the given VMs.
	// N.B. If a VM contains a label with the same key, its value will be updated.
	AddLabels(l *logger.Logger, vms List, labels map[string]string) error
	RemoveLabels(l *logger.Logger, vms List, labels []string) error
	// The name of the Provider, which will also surface in the top-level Providers map.
	Name() string

	// Active returns true if the provider is properly installed and capable of
	// operating, false if it's just a stub. This allows one to test whether a
	// particular provider is functioning properly by calling, for example,
	// Providers[gce.ProviderName].Active. Note that just looking at
	// Providers[gce.ProviderName] != nil doesn't work because
	// Providers[gce.ProviderName] can be a stub.
	Active() bool

	// ProjectActive returns true if the given project is currently active in the
	// provider.
	ProjectActive(project string) bool

	// Volume and volume snapshot related APIs.

	// CreateVolume creates a new volume using the given options.
	CreateVolume(l *logger.Logger, vco VolumeCreateOpts) (Volume, error)
	// ListVolumes lists all volumes already attached to the given VM.
	ListVolumes(l *logger.Logger, vm *VM) ([]Volume, error)
	// DeleteVolume detaches and deletes the given volume from the given VM.
	DeleteVolume(l *logger.Logger, volume Volume, vm *VM) error
	// AttachVolume attaches the given volume to the given VM.
	AttachVolume(l *logger.Logger, volume Volume, vm *VM) (string, error)
	// CreateVolumeSnapshot creates a snapshot of the given volume, using the
	// given options.
	CreateVolumeSnapshot(l *logger.Logger, volume Volume, vsco VolumeSnapshotCreateOpts) (VolumeSnapshot, error)
	// ListVolumeSnapshots lists the individual volume snapshots that satisfy
	// the search criteria.
	ListVolumeSnapshots(l *logger.Logger, vslo VolumeSnapshotListOpts) ([]VolumeSnapshot, error)
	// DeleteVolumeSnapshots permanently deletes the given snapshots.
	DeleteVolumeSnapshots(l *logger.Logger, snapshot ...VolumeSnapshot) error

	// SpotVM related APIs.

	// SupportsSpotVMs returns if the provider supports spot VMs.
	SupportsSpotVMs() bool
	// GetPreemptedSpotVMs returns a list of Spot VMs that were preempted since the time specified.
	// Returns nil, nil when SupportsSpotVMs() is false.
	GetPreemptedSpotVMs(l *logger.Logger, vms List, since time.Time) ([]PreemptedVM, error)
	// GetHostErrorVMs returns a list of VMs that had host error since the time specified.
	GetHostErrorVMs(l *logger.Logger, vms List, since time.Time) ([]string, error)
	// GetVMSpecs returns a map from VM.Name to a map of VM attributes, according to a specific cloud provider.
	GetVMSpecs(l *logger.Logger, vms List) (map[string]map[string]interface{}, error)

	// CreateLoadBalancer creates a load balancer, for a specific port, that
	// delegates to the given cluster.
	CreateLoadBalancer(l *logger.Logger, vms List, port int) error

	// DeleteLoadBalancer deletes a load balancers created for a specific port.
	DeleteLoadBalancer(l *logger.Logger, vms List, port int) error

	// ListLoadBalancers returns a list of load balancer IPs and ports that are currently
	// routing to services for the given VMs.
	ListLoadBalancers(l *logger.Logger, vms List) ([]ServiceAddress, error)
}

// DeleteCluster is an optional capability for a Provider which can
// destroy an entire cluster in a single operation.
type DeleteCluster interface {
	DeleteCluster(l *logger.Logger, name string) error
}

// Providers contains all known Provider instances. This is initialized by subpackage init() functions.
var Providers = map[string]Provider{}

// ProviderOptionsContainer is a container for a collection of provider-specific options.
type ProviderOptionsContainer map[string]ProviderOpts

// CreateProviderOptionsContainer returns a ProviderOptionsContainer which is
// populated with options for all registered providers. Only call it after
// initiliazing providers (populating vm.Providers).
func CreateProviderOptionsContainer() ProviderOptionsContainer {
	container := make(ProviderOptionsContainer)
	for providerName, providerInstance := range Providers {
		container[providerName] = providerInstance.CreateProviderOpts()
	}
	return container
}

// SetProviderOpts updates the container it operates on with the given
// provider options for the given provider.
func (container ProviderOptionsContainer) SetProviderOpts(
	providerName string, providerOpts ProviderOpts,
) {
	container[providerName] = providerOpts
}

// AllProviderNames returns the names of all known vm Providers.  This is useful with the
// ProvidersSequential or ProvidersParallel methods.
func AllProviderNames() []string {
	var ret []string
	for name := range Providers {
		ret = append(ret, name)
	}
	return ret
}

// FanOut collates a collection of VMs by their provider and invoke the callbacks in parallel.
func FanOut(list List, action func(Provider, List) error) error {
	var m = map[string]List{}
	for _, vm := range list {
		m[vm.Provider] = append(m[vm.Provider], vm)
	}

	var g errgroup.Group
	for name, vms := range m {
		g.Go(func() error {
			p, ok := Providers[name]
			if !ok {
				return errors.Errorf("unknown provider name: %s", name)
			}
			return action(p, vms)
		})
	}

	return g.Wait()
}

// Memoizes return value from FindActiveAccounts.
var cachedActiveAccounts map[string]string

// FindActiveAccounts queries the active providers for the name of the user
// account.
func FindActiveAccounts(l *logger.Logger) (map[string]string, error) {
	source := cachedActiveAccounts

	if source == nil {
		// Ask each Provider for its active account name.
		source = map[string]string{}
		err := ProvidersSequential(AllProviderNames(), func(p Provider) error {
			account, err := p.FindActiveAccount(l)
			if err != nil {
				l.Printf("WARN: provider=%q has no active account", p.Name())
				//nolint:returnerrcheck
				return nil
			}
			if len(account) > 0 {
				source[p.Name()] = account
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		cachedActiveAccounts = source
	}

	// Return a copy.
	ret := make(map[string]string, len(source))
	for k, v := range source {
		ret[k] = v
	}

	return ret, nil
}

// ForProvider resolves the Provider with the given name and executes the
// action.
func ForProvider(named string, action func(Provider) error) error {
	p, ok := Providers[named]
	if !ok {
		return errors.Errorf("unknown vm provider: %s", named)
	}
	if err := action(p); err != nil {
		return errors.Wrapf(err, "in provider: %s", named)
	}
	return nil
}

// ProvidersParallel concurrently executes actions for each named Provider.
func ProvidersParallel(named []string, action func(Provider) error) error {
	var g errgroup.Group
	for _, name := range named {
		g.Go(func() error {
			return ForProvider(name, action)
		})
	}
	return g.Wait()
}

// ProvidersSequential sequentially executes actions for each named Provider.
func ProvidersSequential(named []string, action func(Provider) error) error {
	for _, name := range named {
		if err := ForProvider(name, action); err != nil {
			return err
		}
	}
	return nil
}

// ZonePlacement allocates zones to numNodes in an equally sized groups in the
// same order as zones. If numNodes is not divisible by len(zones) the remainder
// is allocated in a round-robin fashion and placed at the end of the returned
// slice. The returned slice has a length of numNodes where each value is in
// [0, numZones).
//
// For example:
//
//	ZonePlacement(3, 8) = []int{0, 0, 1, 1, 2, 2, 0, 1}
func ZonePlacement(numZones, numNodes int) (nodeZones []int) {
	if numZones < 1 {
		panic("expected 1 or more zones")
	}
	numPerZone := numNodes / numZones
	if numPerZone < 1 {
		numPerZone = 1
	}
	extraStartIndex := numPerZone * numZones
	nodeZones = make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeZones[i] = i / numPerZone
		if i >= extraStartIndex {
			nodeZones[i] = i % numZones
		}
	}
	return nodeZones
}

// ExpandZonesFlag takes a slice of strings which may be of the format
// zone:N which implies that a given zone should be repeated N times and
// expands it. For example ["us-west1-b:2", "us-east1-a:2"] will expand to
// ["us-west1-b", "us-west1-b", "us-east1-a", "us-east1-a"].
func ExpandZonesFlag(zoneFlag []string) (zones []string, err error) {
	for _, zone := range zoneFlag {
		colonIdx := strings.Index(zone, ":")
		if colonIdx == -1 {
			zones = append(zones, zone)
			continue
		}
		n, err := strconv.Atoi(zone[colonIdx+1:])
		if err != nil {
			return zones, errors.Wrapf(err, "failed to parse %q", zone)
		}
		for i := 0; i < n; i++ {
			zones = append(zones, zone[:colonIdx])
		}
	}
	return zones, nil
}

// DNSSafeName takes a string and returns a cleaned version of the string that can be used in DNS entries.
// Unsafe characters are dropped. No length check is performed.
func DNSSafeName(name string) string {
	safe := func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return unicode.ToLower(r)
		case r >= '0' && r <= '9':
			return r
		case r == '-':
			return r
		default:
			// Negative value tells strings.Map to drop the rune.
			return -1
		}
	}
	name = strings.Map(safe, name)

	// DNS entries cannot start or end with hyphens.
	name = strings.Trim(name, "-")

	// Consecutive hyphens are allowed in DNS entries, but disallow it for readability.
	return regexp.MustCompile(`-+`).ReplaceAllString(name, "-")
}

// SanitizeLabel returns a version of the string that can be used as a (resource) label.
// This takes the lowest common denominator of the label requirements;
// GCE: "The value can only contain lowercase letters, numeric characters, underscores and dashes.
// The value can be at most 63 characters long"
func SanitizeLabel(label string) string {
	// Replace any non-alphanumeric characters with hyphens
	re := regexp.MustCompile("[^a-zA-Z0-9]+")
	label = re.ReplaceAllString(label, "-")
	label = strings.ToLower(label)

	// Truncate the label to 63 characters (the maximum allowed by GCP)
	if len(label) > 63 {
		label = label[:63]
	}
	// Remove any leading or trailing hyphens
	label = strings.Trim(label, "-")
	return label
}

// SanitizeLabelValues returns the same set of keys with sanitized values.
func SanitizeLabelValues(labels map[string]string) map[string]string {
	sanitized := map[string]string{}
	for k, v := range labels {
		sanitized[k] = SanitizeLabel(v)
	}
	return sanitized
}
