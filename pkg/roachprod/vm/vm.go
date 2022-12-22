// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vm

import (
	"fmt"
	"regexp"
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
)

// GetDefaultLabelMap returns a label map for a common set of labels.
func GetDefaultLabelMap(opts CreateOpts) map[string]string {
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
	Errors   []error           `json:"errors"`
	Lifetime time.Duration     `json:"lifetime"`
	Labels   map[string]string `json:"labels"`
	// The provider-internal DNS name for the VM instance
	DNS string `json:"dns"`
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
	Zone        string `json:"zone"`
	// Project represents the project to which this vm belongs, if the VM is in a
	// cloud that supports project (i.e. GCE). Empty otherwise.
	Project string `json:"project"`

	// SQLPort is the port on which the cockroach process is listening for SQL
	// connections.
	// Usually config.DefaultSQLPort, except for local clusters.
	SQLPort int `json:"sql_port"`

	// AdminUIPort is the port on which the cockroach process is listening for
	// HTTP traffic for the Admin UI.
	// Usually config.DefaultAdminUIPort, except for local clusters.
	AdminUIPort int `json:"adminui_port"`

	// LocalClusterName is only set for VMs in a local cluster.
	LocalClusterName string `json:"local_cluster_name,omitempty"`

	// NonBootAttachedVolumes are the non-bootable volumes attached to the VM.
	NonBootAttachedVolumes []Volume `json:"non_bootable_volumes"`
}

// Name generates the name for the i'th node in a cluster.
func Name(cluster string, idx int) string {
	return fmt.Sprintf("%s-%0.4d", cluster, idx)
}

// Error values for VM.Error
var (
	ErrBadNetwork   = errors.New("could not determine network information")
	ErrInvalidName  = errors.New("invalid VM name")
	ErrNoExpiration = errors.New("could not determine expiration")
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

func (vm *VM) AttachVolume(v Volume) (deviceName string, err error) {
	vm.NonBootAttachedVolumes = append(vm.NonBootAttachedVolumes, v)
	err = ForProvider(vm.Provider, func(provider Provider) error {
		deviceName, err = provider.AttachVolumeToVM(v, vm)
		return err
	})
	return deviceName, err
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
	// ConfigureClusterFlags configures a FlagSet with any options relevant to
	// cluster manipulation commands (`create`, `destroy`, `list`, `sync` and
	// `gc`).
	ConfigureClusterFlags(*pflag.FlagSet, MultipleProjectsOption)
}

type Volume struct {
	ProviderResourceID string
	ProviderVolumeType string
	Zone               string
	Encrypted          bool
	Name               string
	Labels             map[string]string
	Size               int
}

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
	IncludeVolumes bool
}

// A Provider is a source of virtual machines running on some hosting platform.
type Provider interface {
	CreateProviderOpts() ProviderOpts
	CleanSSH() error

	// ConfigSSH takes a list of zones and configures SSH for machines in those
	// zones for the given provider.
	ConfigSSH(zones []string) error
	Create(l *logger.Logger, names []string, opts CreateOpts, providerOpts ProviderOpts) error
	Reset(vms List) error
	Delete(vms List) error
	Extend(vms List, lifetime time.Duration) error
	// Return the account name associated with the provider
	FindActiveAccount() (string, error)
	List(l *logger.Logger, opts ListOptions) (List, error)
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

	CreateVolume(vco VolumeCreateOpts) (Volume, error)
	AttachVolumeToVM(volume Volume, vm *VM) (string, error)
	SnapshotVolume(volume Volume, name, description string, labels map[string]string) (string, error)
}

// DeleteCluster is an optional capability for a Provider which can
// destroy an entire cluster in a single operation.
type DeleteCluster interface {
	DeleteCluster(name string) error
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
		// capture loop variables
		n := name
		v := vms
		g.Go(func() error {
			p, ok := Providers[n]
			if !ok {
				return errors.Errorf("unknown provider name: %s", n)
			}
			return action(p, v)
		})
	}

	return g.Wait()
}

// Memoizes return value from FindActiveAccounts.
var cachedActiveAccounts map[string]string

// FindActiveAccounts queries the active providers for the name of the user
// account.
func FindActiveAccounts() (map[string]string, error) {
	source := cachedActiveAccounts

	if source == nil {
		// Ask each Provider for its active account name.
		source = map[string]string{}
		err := ProvidersSequential(AllProviderNames(), func(p Provider) error {
			account, err := p.FindActiveAccount()
			if err != nil {
				return err
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
		// capture loop variable
		n := name
		g.Go(func() error {
			return ForProvider(n, action)
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

// DNSSafeAccount takes a string and returns a cleaned version of the string that can be used in DNS entries.
// Unsafe characters are dropped. No length check is performed.
func DNSSafeAccount(account string) string {
	safe := func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return unicode.ToLower(r)
		default:
			// Negative value tells strings.Map to drop the rune.
			return -1
		}
	}
	return strings.Map(safe, account)
}
