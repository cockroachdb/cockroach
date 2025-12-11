// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/logging/logadmin"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	cloudbilling "google.golang.org/api/cloudbilling/v1beta"
	"google.golang.org/api/option"
)

const (
	ProviderName = "gce"
	DefaultImage = "ubuntu-2204-jammy-v20240319"
	ARM64Image   = "ubuntu-2204-jammy-arm64-v20240319"
	// TODO(DarrylWong): Upgrade FIPS to Ubuntu 22 when it is available.
	FIPSImage           = "ubuntu-pro-fips-2004-focal-v20230811"
	defaultImageProject = "ubuntu-os-cloud"
	FIPSImageProject    = "ubuntu-os-pro-cloud"
	ManagedLabel        = "managed"

	// These values limit concurrent `gcloud` CLI operations, and command
	// length, to avoid overwhelming the API when managing large clusters. The
	// limits were determined through empirical testing.
	MaxConcurrentCommands = 100
	MaxConcurrentHosts    = 100
)

var (
	armMachineTypes = []string{"a4x", "c4a-", "n4a", "t2a-"}
)

type VolumeType string

const (
	// VolumeTypeUnknown represents an unknown volume type.
	VolumeTypeUnknown VolumeType = ""
	// VolumeTypeLocalSSD represents a local solid-state drive.
	VolumeTypeLocalSSD VolumeType = "local-ssd"
	// VolumeTypePremium represents a boot disk.
	VolumeTypeBoot VolumeType = "boot"
	// VolumeTypeStandard represents an attached persistent disk.
	VolumeTypePersistent VolumeType = "persistent"

	DefaultProjectID = "cockroach-ephemeral"
)

var (
	defaultDefaultProject, defaultMetadataProject, defaultDNSProject, defaultDefaultServiceAccount string
	// projects for which a cron GC job exists.
	projectsWithGC []string
)

func initGCEProjectDefaults() {
	defaultDefaultProject = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DEFAULT_PROJECT", DefaultProjectID,
	)
	defaultMetadataProject = config.EnvOrDefaultString(
		"ROACHPROD_GCE_METADATA_PROJECT",
		defaultDefaultProject,
	)
	defaultDNSProject = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DNS_PROJECT", "cockroach-shared",
	)

	// Service account to use if the default project is in use.
	defaultDefaultServiceAccount = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT",
		"21965078311-compute@developer.gserviceaccount.com",
	)
	projectsWithGC = []string{defaultDefaultProject}
}

// DefaultProject returns the default GCE project. This is used to determine whether
// certain features, such as DNS names are enabled.
func DefaultProject() string {
	// If the provider was already initialized, read the default project from the
	// provider.
	if p, ok := vm.Providers[ProviderName].(*Provider); ok {
		return p.defaultProject
	}
	return defaultDefaultProject
}

// Denotes if this provider was successfully initialized.
var initialized = false

// Init registers the GCE provider into vm.Providers.
//
// If the gcloud tool is not available on the local path, the provider is a
// stub.
//
// Note that, when roachprod is used as a binary, the defaults for
// providerInstance properties initialized here can be overriden by flags.
func Init() error {
	initGCEProjectDefaults()
	initDNSDefault()

	providerOpts := []Option{}
	projectFromEnv := os.Getenv("GCE_PROJECT")
	if projectFromEnv != "" {
		fmt.Printf("WARN: `GCE_PROJECT` is deprecated; please, use `ROACHPROD_GCE_DEFAULT_PROJECT` instead\n")
		providerOpts = append(providerOpts, WithProject(projectFromEnv))
	}

	// Init the default provider
	providerInstance, err := NewProvider(providerOpts...)
	if err != nil {
		vm.Providers[ProviderName] = flagstub.New(
			&Provider{},
			fmt.Sprintf("unable to init gce provider: %s", err),
		)
		return err
	}

	if _, err := exec.LookPath("gcloud"); err != nil {
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, "please install the gcloud CLI utilities "+
			"(https://cloud.google.com/sdk/downloads)")
		return errors.New("gcloud not found")
	}

	initialized = true
	vm.Providers[ProviderName] = providerInstance
	vm.DNSProviders[providerInstance.dnsProvider.ProviderName()] = providerInstance.dnsProvider
	Infrastructure = providerInstance

	return nil
}

// NewProvider returns a new GCE provider with the given options applied.
func NewProvider(options ...Option) (*Provider, error) {

	// Create a new provider with the default options.
	p := &Provider{
		dnsProviderOpts: NewDNSProviderDefaultOptions(),
		Projects:        []string{},
		defaultProject:  defaultDefaultProject,
		metadataProject: defaultMetadataProject,
	}

	for _, option := range options {
		option.apply(p)
	}

	// If no projects were specified by the options, use the default project.
	if len(p.Projects) == 0 {
		p.Projects = []string{defaultDefaultProject}
	}

	// Initialize the GCE clients.
	if err := p.initSDKClients(context.Background()); err != nil {
		return nil, errors.Wrap(err, "unable to init GCE clients")
	}

	// If no DNS provider was provided as an option, we initialize the default
	// SDK DNS provider.
	if p.dnsProvider == nil {
		dnsProvider, err := NewSDKDNSProvider(
			(&SDKDNSProviderOpts{}).NewFromGCEDNSProviderOpts(p.dnsProviderOpts),
		)
		if err != nil {
			return nil, errors.Wrap(err, "unable to init dns client")
		}

		p.dnsProvider = dnsProvider
	}

	return p, nil
}

func (p *Provider) initSDKClients(ctx context.Context) error {
	creds, _, err := roachprodutil.GetGCECredentials(
		context.Background(),
		roachprodutil.IAPTokenSourceOptions{},
	)
	if err != nil {
		return errors.Wrap(err, "failed to get credentials")
	}

	instancesClient, err := compute.NewInstancesRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute instances client")
	}

	instanceTemplatesClient, err := compute.NewInstanceTemplatesRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute instance templates client")
	}

	instanceGroupManagersClient, err := compute.NewInstanceGroupManagersRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute instance group managers client")
	}

	disksClient, err := compute.NewDisksRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute disks client")
	}

	snapshotsClient, err := compute.NewSnapshotsRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute snapshots client")
	}

	loggingAdminClient, err := logadmin.NewClient(
		context.Background(),
		p.GetProject(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init logging admin client")
	}

	healthChecksClient, err := compute.NewHealthChecksRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute health checks client")
	}

	backendServicesClient, err := compute.NewBackendServicesRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute backend services client")
	}

	targetTcpProxiesClient, err := compute.NewTargetTcpProxiesRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute target TCP proxies client")
	}

	globalForwardingRulesClient, err := compute.NewGlobalForwardingRulesRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute global forwarding rules client")
	}

	instanceGroupsClient, err := compute.NewInstanceGroupsRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute instance groups client")
	}

	projectsClient, err := compute.NewProjectsRESTClient(
		context.Background(),
		option.WithCredentials(creds),
	)
	if err != nil {
		return errors.Wrap(err, "unable to init compute projects client")
	}

	p.computeInstancesClient = instancesClient
	p.computeInstanceTemplatesClient = instanceTemplatesClient
	p.computeInstanceGroupManagersClient = instanceGroupManagersClient
	p.computeDisksClient = disksClient
	p.computeSnapshotsClient = snapshotsClient
	p.computeHealthChecksClient = healthChecksClient
	p.computeBackendServicesClient = backendServicesClient
	p.computeTargetTcpProxiesClient = targetTcpProxiesClient
	p.computeGlobalForwardingRulesClient = globalForwardingRulesClient
	p.computeInstanceGroupsClient = instanceGroupsClient
	p.computeProjectsClient = projectsClient
	p.loggingAdminClient = loggingAdminClient

	return nil
}

func runJSONCommand(args []string, parsed interface{}) error {
	cmd := exec.Command("gcloud", args...)

	rawJSON, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
			stderr = exitErr.Stderr
		}
		// TODO(peter,ajwerner): Remove this hack once gcloud behaves when adding
		// new zones.
		if matched, _ := regexp.Match(`.*Unknown zone`, stderr); !matched {
			return errors.Wrapf(err, "failed to run: gcloud %s\nstdout: %s\nstderr: %s\n",
				strings.Join(args, " "), bytes.TrimSpace(rawJSON), bytes.TrimSpace(stderr))
		}
	}

	if err := json.Unmarshal(rawJSON, &parsed); err != nil {
		return errors.Wrapf(err, "failed to parse json %s: %v", rawJSON, rawJSON)
	}

	return nil
}

type jsonAuth struct {
	Account string
	Status  string
}

// DefaultProviderOpts returns a new gce.ProviderOpts with default values set.
func DefaultProviderOpts() *ProviderOpts {
	return &ProviderOpts{
		// N.B. we set minCPUPlatform to "Intel Ice Lake" by default because it's readily available in the majority of GCE
		// regions. Furthermore, it gets us closer to AWS instances like m6i which exclusively run Ice Lake.
		MachineType:          "n2-standard-4",
		MinCPUPlatform:       "Intel Ice Lake",
		Zones:                nil,
		Image:                DefaultImage,
		SSDCount:             1,
		PDVolumeType:         "pd-ssd",
		PDVolumeSize:         500,
		PDVolumeCount:        1,
		TerminateOnMigration: false,
		UseSpot:              false,
		preemptible:          false,

		defaultServiceAccount: defaultDefaultServiceAccount,
		ServiceAccount:        os.Getenv("GCE_SERVICE_ACCOUNT"),
	}
}

// CreateProviderOpts returns a new gce.ProviderOpts with default values set.
func (p *Provider) CreateProviderOpts() vm.ProviderOpts {
	return DefaultProviderOpts()
}

// ProviderOpts provides user-configurable, gce-specific create options.
type ProviderOpts struct {
	// projects represent the GCE projects to operate on. Accessed through
	// GetProject() or GetProjects() depending on whether the command accepts
	// multiple projects or a single one.
	MachineType                   string
	MinCPUPlatform                string
	BootDiskType                  string
	Zones                         []string
	Image                         string
	SSDCount                      int
	PDVolumeType                  string
	PDVolumeSize                  int
	PDVolumeCount                 int
	PDVolumeProvisionedIOPS       int
	PDVolumeProvisionedThroughput int
	UseMultipleDisks              bool
	// use spot instances (i.e., latest version of preemptibles which can run > 24 hours)
	UseSpot bool
	// Use an instance template and a managed instance group to create VMs. This
	// enables cluster resizing, load balancing, and health monitoring.
	Managed bool
	// Enable turbo mode for the instance. Only supported on C4 VM families.
	// See: https://cloud.google.com/sdk/docs/release-notes#compute_engine_23
	TurboMode string
	// The number of visible threads per physical core.
	// See: https://cloud.google.com/compute/docs/instances/configuring-simultaneous-multithreading.
	ThreadsPerCore int
	// This specifies a subset of the Zones above that will run on spot instances.
	// VMs running in Zones not in this list will be provisioned on-demand. This
	// is only used by managed instance groups.
	ManagedSpotZones []string
	// Enable the cron service. It is disabled by default.
	EnableCron bool
	// BootDiskOnly ensures that no additional disks will be attached, other than
	// the boot disk.
	BootDiskOnly bool

	// GCE allows two availability policies in case of a maintenance event (see --maintenance-policy via gcloud),
	// 'TERMINATE' or 'MIGRATE'. The default is 'MIGRATE' which we denote by 'TerminateOnMigration == false'.
	TerminateOnMigration bool
	// use preemptible instances
	preemptible bool

	ServiceAccount string

	// The service account to use if the default project is in use and no
	// ServiceAccount was specified.
	defaultServiceAccount string
}

// Provider is the GCE implementation of the vm.Provider interface.
type Provider struct {
	dnsProvider     vm.DNSProvider
	dnsProviderOpts dnsOpts

	Projects []string

	// The project to use for looking up metadata. In particular, this includes
	// user keys.
	metadataProject string

	// The project that provides the core roachprod services.
	defaultProject string

	// ComputeClients
	computeInstancesClient             *compute.InstancesClient
	computeInstanceTemplatesClient     *compute.InstanceTemplatesClient
	computeInstanceGroupManagersClient *compute.InstanceGroupManagersClient
	computeDisksClient                 *compute.DisksClient
	computeSnapshotsClient             *compute.SnapshotsClient
	computeHealthChecksClient          *compute.HealthChecksClient
	computeBackendServicesClient       *compute.BackendServicesClient
	computeTargetTcpProxiesClient      *compute.TargetTcpProxiesClient
	computeGlobalForwardingRulesClient *compute.GlobalForwardingRulesClient
	computeInstanceGroupsClient        *compute.InstanceGroupsClient
	computeProjectsClient              *compute.ProjectsClient
	loggingAdminClient                 *logadmin.Client
}

type dnsOpts struct {
	DNSProject    string
	PublicZone    string
	PublicDomain  string
	ManagedZone   string
	ManagedDomain string
}

func NewDNSProviderDefaultOptions() dnsOpts {
	return dnsOpts{
		DNSProject:    defaultDNSProject,
		PublicZone:    dnsDefaultZone,
		PublicDomain:  dnsDefaultDomain,
		ManagedZone:   dnsDefaultManagedZone,
		ManagedDomain: dnsDefaultManagedDomain,
	}
}

func (p *Provider) SupportsSpotVMs() bool {
	return true
}

// IsLocalProvider returns false because gcloud is a remote provider.
func (p *Provider) IsLocalProvider() bool {
	return false
}

// GetPreemptedSpotVMs checks the preemption status of the given VMs, by querying the GCP logging service.
func (p *Provider) GetPreemptedSpotVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {
	return p.GetPreemptedSpotVMsWithContext(context.Background(), l, vms, since)
}

// GetHostErrorVMs checks the host error status of the given VMs, by querying the GCP logging service.
func (p *Provider) GetHostErrorVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	return p.GetHostErrorVMsWithContext(context.Background(), l, vms, since)
}

func (p *Provider) GetLiveMigrationVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	return nil, nil
}

// GetVMSpecs returns a map from VM.Name to a map of VM attributes, provided by GCE
func (p *Provider) GetVMSpecs(
	l *logger.Logger, vms vm.List,
) (map[string]map[string]interface{}, error) {
	return p.GetVMSpecsWithContext(context.Background(), l, vms)
}

func (p *Provider) CreateVolumeSnapshot(
	l *logger.Logger, volume vm.Volume, vsco vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	return p.CreateVolumeSnapshotWithContext(context.Background(), l, volume, vsco)
}

func (p *Provider) ListVolumeSnapshots(
	l *logger.Logger, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	return p.ListVolumeSnapshotsWithContext(context.Background(), l, vslo)
}

func (p *Provider) DeleteVolumeSnapshots(l *logger.Logger, snapshots ...vm.VolumeSnapshot) error {
	return p.DeleteVolumeSnapshotsWithContext(context.Background(), l, snapshots...)
}

func (p *Provider) CreateVolume(
	l *logger.Logger, vco vm.VolumeCreateOpts,
) (vol vm.Volume, err error) {
	return p.CreateVolumeWithContext(context.Background(), l, vco)
}

func (p *Provider) DeleteVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) error {
	return p.DeleteVolumeWithContext(context.Background(), l, volume, vm)
}

func (p *Provider) ListVolumes(l *logger.Logger, v *vm.VM) ([]vm.Volume, error) {
	return p.ListVolumesWithContext(context.Background(), l, v)
}

func (p *Provider) AttachVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) (string, error) {
	return p.AttachVolumeWithContext(context.Background(), l, volume, vm)
}

// ProjectsVal is the implementation for the --gce-projects flag. It populates
// (Provider.Projects).
type ProjectsVal struct {
	AcceptMultipleProjects bool
	Provider               *Provider
}

// DefaultZones is the list of  zones used by default for cluster creation.
// If the geo flag is specified, nodes are distributed between zones.
// These are GCP zones available according to this page:
// https://cloud.google.com/compute/docs/regions-zones#available
//
// Note that the default zone (the first zone returned by this
// function) is always in the us-east1 region (or us-central1 for
// ARM64 builds), but we randomize the specific zone. This is to avoid
// "zone exhausted" errors in one particular zone, especially during
// nightly roachtest runs.
func DefaultZones(arch string, geoDistributed bool) []string {
	zones := []string{"us-east1-b", "us-east1-c", "us-east1-d"}
	if vm.ParseArch(arch) == vm.ArchARM64 {
		// T2A instances are only available in us-central1 in NA.
		zones = []string{"us-central1-a", "us-central1-b", "us-central1-f"}
	}
	rand.Shuffle(len(zones), func(i, j int) { zones[i], zones[j] = zones[j], zones[i] })

	if geoDistributed {
		return []string{
			zones[0],
			"us-west1-b",
			"europe-west2-b",
			zones[1],
			"us-west1-c",
			"europe-west2-c",
			zones[2],
			"us-west1-a",
			"europe-west2-a",
		}
	}

	return []string{zones[0]}
}

// Set is part of the pflag.Value interface.
func (v ProjectsVal) Set(projects string) error {
	if projects == "" {
		return fmt.Errorf("empty GCE project")
	}
	prj := strings.Split(projects, ",")
	if !v.AcceptMultipleProjects && len(prj) > 1 {
		return fmt.Errorf("multiple GCE projects not supported for command")
	}
	v.Provider.Projects = prj
	return nil
}

// Type is part of the pflag.Value interface.
func (v ProjectsVal) Type() string {
	if v.AcceptMultipleProjects {
		return "comma-separated list of GCE projects"
	}
	return "GCE project name"
}

// String is part of the pflag.Value interface.
func (v ProjectsVal) String() string {
	return strings.Join(v.Provider.Projects, ",")
}

// GetProject returns the GCE project on which we're configured to operate.
// If multiple projects were configured, this panics.
func (p *Provider) GetProject() string {
	if len(p.Projects) > 1 {
		panic(fmt.Sprintf(
			"multiple projects not supported (%d specified)", len(p.Projects)))
	}
	return p.Projects[0]
}

// GetProjects returns the list of GCE projects on which we're configured to
// operate.
func (p *Provider) GetProjects() []string {
	return p.Projects
}

// ConfigureCreateFlags implements vm.ProviderOptions.
func (o *ProviderOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.MachineType, "machine-type", "n2-standard-4", "DEPRECATED")
	_ = flags.MarkDeprecated("machine-type", "use "+ProviderName+"-machine-type instead")
	flags.StringSliceVar(&o.Zones, "zones", nil, "DEPRECATED")
	_ = flags.MarkDeprecated("zones", "use "+ProviderName+"-zones instead")
	flags.StringSliceVar(&o.ManagedSpotZones, ProviderName+"-managed-spot-zones", nil,
		"subset of zones in managed instance groups that will use spot instances")

	flags.StringVar(&o.ServiceAccount, ProviderName+"-service-account",
		o.ServiceAccount, "Service account to use")
	flags.StringVar(&o.defaultServiceAccount,
		ProviderName+"-default-service-account", defaultDefaultServiceAccount,
		"Service account to use if the default project is in use and no "+
			"--gce-service-account was specified")

	flags.StringVar(&o.MachineType, ProviderName+"-machine-type", "n2-standard-4",
		"Machine type (see https://cloud.google.com/compute/docs/machine-types)")
	flags.StringVar(&o.BootDiskType, ProviderName+"-boot-disk-type", "pd-ssd",
		"Type of the boot disk volume")
	flags.StringVar(&o.MinCPUPlatform, ProviderName+"-min-cpu-platform", "Intel Ice Lake",
		"Minimum CPU platform (see https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform)")
	flags.StringVar(&o.Image, ProviderName+"-image", DefaultImage,
		"Image to use to create the vm, "+
			"use `gcloud compute images list --filter=\"family=ubuntu-2004-lts\"` to list available images. "+
			"Note: this option is ignored if --fips is passed.")

	flags.IntVar(&o.SSDCount, ProviderName+"-local-ssd-count", 1,
		"Number of local SSDs to create, only used if local-ssd=true")
	flags.StringVar(&o.PDVolumeType, ProviderName+"-pd-volume-type", "pd-ssd",
		"Type of the persistent disk volume, only used if local-ssd=false "+
			"(pd-ssd, pd-balanced, pd-extreme, pd-standard, hyperdisk-balanced)")
	flags.IntVar(&o.PDVolumeSize, ProviderName+"-pd-volume-size", 500,
		"Size in GB of persistent disk volume, only used if local-ssd=false")
	flags.IntVar(&o.PDVolumeCount, ProviderName+"-pd-volume-count", 1,
		"Number of persistent disk volumes, only used if local-ssd=false")
	flags.IntVar(&o.PDVolumeProvisionedIOPS, ProviderName+"-pd-volume-provisioned-iops", 0,
		"Provisioned IOPS for the disk volume (required for hyperdisk-balanced, optional for pd-extreme)")
	flags.IntVar(&o.PDVolumeProvisionedThroughput, ProviderName+"-pd-volume-provisioned-throughput", 0,
		"Provisioned throughput in MiB/s for the disk volume (required for hyperdisk-balanced)")
	flags.BoolVar(&o.UseMultipleDisks, ProviderName+"-enable-multiple-stores",
		false, "Enable the use of multiple stores by creating one store directory per disk. "+
			"Default is to raid0 stripe all disks.")

	flags.StringSliceVar(&o.Zones, ProviderName+"-zones", nil,
		fmt.Sprintf("Zones for cluster. If zones are formatted as AZ:N where N is an integer, the zone\n"+
			"will be repeated N times. If > 1 zone specified, nodes will be geo-distributed\n"+
			"regardless of geo (default [%s])",
			strings.Join(DefaultZones(string(vm.ArchAMD64), true), ",")))
	flags.BoolVar(&o.preemptible, ProviderName+"-preemptible", false,
		"use preemptible GCE instances (lifetime cannot exceed 24h)")
	flags.BoolVar(&o.UseSpot, ProviderName+"-use-spot", false,
		"use spot GCE instances (like preemptible but lifetime can exceed 24h)")
	flags.BoolVar(&o.TerminateOnMigration, ProviderName+"-terminateOnMigration", false,
		"use 'TERMINATE' maintenance policy (for GCE live migrations)")
	flags.BoolVar(&o.Managed, ProviderName+"-managed", false,
		"use a managed instance group (enables resizing, load balancing, and health monitoring)")
	flags.BoolVar(&o.EnableCron, ProviderName+"-enable-cron",
		false, "Enables the cron service (it is disabled by default)")
	flags.StringVar(&o.TurboMode, ProviderName+"-turbo-mode", "",
		"enable turbo mode for the instance (only supported on C4 VM families, valid value: 'ALL_CORE_MAX')")
	flags.IntVar(&o.ThreadsPerCore, ProviderName+"-threads-per-core", 0,
		"the number of visible threads per physical core (valid values: 1 or 2), default is 0 (auto)")
	flags.BoolVar(&o.BootDiskOnly, ProviderName+"-boot-disk-only", o.BootDiskOnly,
		"Only attach the boot disk. No additional volumes will be provisioned even if specified.")
}

// ConfigureProviderFlags implements Provider
func (p *Provider) ConfigureProviderFlags(flags *pflag.FlagSet, opt vm.MultipleProjectsOption) {
	var usage string
	if opt == vm.SingleProject {
		usage = "GCE project to manage"
	} else {
		usage = "List of GCE projects to manage"
	}

	flags.Var(
		ProjectsVal{
			AcceptMultipleProjects: opt == vm.AcceptMultipleProjects,
			Provider:               p,
		},
		ProviderName+"-project", /* name */
		usage)

	// Flags about DNS override the default values in
	// dnsProvider.
	flags.StringVar(
		&p.dnsProviderOpts.DNSProject, ProviderName+"-dns-project",
		p.dnsProviderOpts.DNSProject,
		"project to use to set up DNS",
	)
	flags.StringVar(
		&p.dnsProviderOpts.PublicZone,
		ProviderName+"-dns-zone",
		p.dnsProviderOpts.PublicZone,
		"zone file in gcloud project to use to set up public DNS records",
	)
	flags.StringVar(
		&p.dnsProviderOpts.PublicDomain,
		ProviderName+"-dns-domain",
		p.dnsProviderOpts.PublicDomain,
		"zone domian in gcloud project to use to set up public DNS records",
	)
	flags.StringVar(
		&p.dnsProviderOpts.ManagedZone,
		ProviderName+"-managed-dns-zone",
		p.dnsProviderOpts.ManagedZone,
		"zone file in gcloud project to use to set up DNS SRV records",
	)
	flags.StringVar(
		&p.dnsProviderOpts.ManagedDomain,
		ProviderName+"-managed-dns-domain",
		p.dnsProviderOpts.ManagedDomain,
		"zone file in gcloud project to use to set up DNS SRV records",
	)

	// Flags about the GCE project to use override the defaults in
	// the provider.
	flags.StringVar(
		&p.metadataProject, ProviderName+"-metadata-project",
		p.metadataProject,
		"google cloud project to use to store and fetch SSH keys",
	)
	flags.StringVar(
		&p.defaultProject, ProviderName+"-default-project",
		p.defaultProject,
		"google cloud project to use to run core roachprod services",
	)
}

// newLimitedErrorGroupWithContext creates a `ctxgroup.Group` with the cloud provider's
// default limit on the number of concurrent operations.
func newLimitedErrorGroupWithContext(ctx context.Context) ctxgroup.Group {
	g := ctxgroup.WithContext(ctx)
	g.SetLimit(MaxConcurrentCommands)
	return g
}

// useArmAMI returns true if the machine type is an arm64 machine type.
func (o *ProviderOpts) useArmAMI() bool {
	for _, armType := range armMachineTypes {
		if strings.HasPrefix(strings.ToLower(o.MachineType), armType) {
			return true
		}
	}
	return false
}

func (o *ProviderOpts) machineTypeSupportsLocalSSD() bool {
	machineType := strings.ToLower(o.MachineType)

	// A2 support local SSDs.
	if strings.HasPrefix(machineType, "a2") {
		return true
	}

	// A3 support local SSDs.
	if strings.HasPrefix(machineType, "a3") {
		return true
	}

	// A4, A4X support local SSDs.
	if strings.HasPrefix(machineType, "a4") {
		return true
	}

	// C2, C2D support local SSDs.
	if strings.HasPrefix(machineType, "c2") {
		return true
	}

	// C3, C4D support local SSDs only with the -lssd suffix.
	if strings.HasPrefix(machineType, "c3") && strings.HasSuffix(machineType, "-lssd") {
		return true
	}

	// C4, C4A, C4D support local SSDs only with the -lssd suffix.
	if strings.HasPrefix(machineType, "c4") && strings.HasSuffix(machineType, "-lssd") {
		return true
	}

	// G2 support local SSDs.
	if strings.HasPrefix(machineType, "g2") {
		return true
	}

	// G4 support local SSDs.
	if strings.HasPrefix(machineType, "g4") {
		return true
	}

	// H4D support local SSDs with suffix -lssd.
	if strings.HasPrefix(machineType, "h4d-") && strings.HasSuffix(machineType, "-lssd") {
		return true
	}

	// M1 partially support local SSDs.
	if strings.HasPrefix(machineType, "m1") {
		return true
	}

	// M3 support local SSDs.
	if strings.HasPrefix(machineType, "m3") {
		return true
	}

	// N1 support local SSDs.
	if strings.HasPrefix(machineType, "n1") {
		return true
	}

	// N2, N2D support local SSDs
	if strings.HasPrefix(machineType, "n2") {
		return true
	}

	// Z3 support local SSDs with suffix -(standard|high)ssd.
	if strings.HasPrefix(machineType, "z3") {
		return true
	}

	// E2 do not support local SSDs.
	// H3 do not support local SSDs.
	// M2, M4 do not support local SSDs.
	// N4, N4A, N4D do not support local SSDs.
	// T2A, T2D don't support local SSDs.

	// We return false by default until we manually allowlist new machine types.
	return false
}

// ConfigureClusterCleanupFlags is part of ProviderOpts. This implementation is a no-op.
func (p *Provider) ConfigureClusterCleanupFlags(flags *pflag.FlagSet) {
}

// CleanSSH TODO(peter): document
func (p *Provider) CleanSSH(l *logger.Logger) error {
	for _, prj := range p.GetProjects() {
		args := []string{"compute", "config-ssh", "--project", prj, "--quiet", "--remove"}
		cmd := exec.Command("gcloud", args...)

		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
		}
	}
	return nil
}

// ConfigSSH is part of the vm.Provider interface. For this provider,
// it verifies that the test runner has a public SSH key, as that is
// required when setting up new clusters.
func (p *Provider) ConfigSSH(l *logger.Logger, zones []string) error {
	_, err := config.SSHPublicKey()
	return err
}

// AddLabels adds (or updates) the given labels to the given VMs.
// N.B. If a VM contains a label with the same key, its value will be updated.
func (p *Provider) AddLabels(l *logger.Logger, vms vm.List, labels map[string]string) error {
	return p.AddLabelsWithContext(context.Background(), l, vms, labels)
}

func (p *Provider) RemoveLabels(l *logger.Logger, vms vm.List, labels []string) error {
	return p.RemoveLabelsWithContext(context.Background(), l, vms, labels)
}

// computeZones computes the zones to be passed to the gcloud commands during
// cluster creation. It's possible that only a subset of the zones get used
// depending on how many nodes are requested.
func computeZones(opts vm.CreateOpts, providerOpts *ProviderOpts) ([]string, error) {
	zones, err := vm.ExpandZonesFlag(providerOpts.Zones)
	if err != nil {
		return nil, err
	}
	if len(zones) == 0 {
		zones = DefaultZones(opts.Arch, opts.GeoDistributed)
	}
	if providerOpts.useArmAMI() {
		if len(providerOpts.Zones) == 0 {
			zones = []string{"us-central1-a"}
		}

		if !IsSupportedT2AZone(providerOpts.Zones) {
			return nil, errors.Newf("T2A instances are not supported outside of [%s]", strings.Join(SupportedT2AZones, ","))
		}
	}
	return zones, nil
}

// instanceTemplateNamePrefix returns the prefix part of the instance template
// (without the trailing zone).
func instanceTemplateNamePrefix(clusterName string) string {
	return fmt.Sprintf("%s-template", clusterName)
}

// instanceTemplateName returns the full instance template name (with the zone).
func instanceTemplateName(clusterName string, zone string) string {
	return fmt.Sprintf("%s-%s", instanceTemplateNamePrefix(clusterName), zone)
}

func instanceGroupName(clusterName string) string {
	return fmt.Sprintf("%s-group", clusterName)
}

// Create instantiates the requested VMs on GCE. If the cluster is managed, it
// will create an instance template and instance group, otherwise it will create
// individual instances.
func (p *Provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, vmProviderOpts vm.ProviderOpts,
) (vm.List, error) {
	return p.CreateWithContext(context.Background(), l, names, opts, vmProviderOpts)
}

// Shrink shrinks the cluster by deleting the given VMs. This is only supported
// for managed instance groups. Currently, nodes should only be deleted from the
// tail of the cluster, due to complexities thar arise when the node names are
// not contiguous.
func (p *Provider) Shrink(l *logger.Logger, vmsToDelete vm.List, clusterName string) error {
	return p.ShrinkWithContext(context.Background(), l, vmsToDelete, clusterName)
}

func (p *Provider) Grow(
	l *logger.Logger, vms vm.List, clusterName string, names []string,
) (vm.List, error) {
	return p.GrowWithContext(context.Background(), l, vms, clusterName, names)
}

// DeleteLoadBalancer implements the vm.Provider interface.
func (p *Provider) DeleteLoadBalancer(l *logger.Logger, vms vm.List, port int) error {
	return p.DeleteLoadBalancerWithContext(context.Background(), l, vms, port)
}

// loadBalancerNameParts returns the cluster name, resource type, and port of a
// load balancer resource name. The resource type is the type of resource, e.g.
// "health-check", "load-balancer", "proxy".
func loadBalancerNameParts(name string) (cluster string, resourceType string, port int, ok bool) {
	regex := regexp.MustCompile(`^([a-z0-9\-]+)-(\d+)-([a-z0-9\-]+)-roachprod$`)
	match := regex.FindStringSubmatch(name)
	if match != nil {
		cluster = match[1]
		port, _ = strconv.Atoi(match[2])
		resourceType = match[3]
		return cluster, resourceType, port, true
	}
	return "", "", 0, false
}

// loadBalancerResourceName returns the name of a load balancer resource. The
// port is used instead of a service name in order to be able to identify
// different parts of the name, since we have limited delimiter options.
func loadBalancerResourceName(clusterName string, port int, resourceType string) string {
	return fmt.Sprintf("%s-%d-%s-roachprod", clusterName, port, resourceType)
}

// CreateLoadBalancer creates a load balancer for the given cluster, derived
// from the VM list, and port. The cluster has to be part of a managed instance
// group. Additionally, a health check is created for the given port. A proxy is
// used to support global load balancing. The different parts of the load
// balancer are created sequentially, as they depend on each other.
func (p *Provider) CreateLoadBalancer(l *logger.Logger, vms vm.List, port int) error {
	return p.CreateLoadBalancerWithContext(context.Background(), l, vms, port)
}

// ListLoadBalancers returns the list of load balancers associated with the
// given VMs. The VMs have to be part of a managed instance group. The load
// balancers are returned as a list of service addresses.
func (p *Provider) ListLoadBalancers(l *logger.Logger, vms vm.List) ([]vm.ServiceAddress, error) {
	return p.ListLoadBalancersWithContext(context.Background(), l, vms)
}

// parseMachineType parses a GCE machine type string into its components.
// Returns family, specialization, number of CPUs, memory in MB, SSD option, error.
// For example:
// - Input: "n2-standard-4" -> Output: ("n2", "standard", 4, 0, "", nil)
// - Input: "n2-custom-8-16384" -> Output: ("n2", "custom", 8, 16384, "", nil)
// - Input: "c4d-standard-384-metal" -> Output: ("c4d", "standard", 384, 0, "", nil)
// - Input: "c4a-standard-8-lssd" -> Output: ("c4a", "standard", 8, 0, "lssd", nil)
// - Input: "c4-standard-288-lssd-metal" -> Output: ("c4", "standard", 288, 0, "lssd", nil)
// The SSD option corresponds to the newest nomenclature for machine types
// with Titanium SSDs support.
func parseMachineType(
	machineType string,
) (family string, specialization string, numCPUs int, memMb int, ssdOption string, err error) {
	// E.g., n2-standard-4, n2-custom-8-16384, c4d-standard-384-metal, c4a-standard-8-lssd, c4-standard-288-lssd-metal.
	machineTypes := regexp.MustCompile(`^([a-z]+\d+[a-z]*)-(standard|highcpu|highmem|highgpu|ultragpu|edgegpu|custom)-(\d+)(?:-(\d+))?(.*)$`)
	matches := machineTypes.FindStringSubmatch(machineType)

	// Regexp captures the following informations:
	// - matches[1]: machine family (n1, n2, h4d, ...)
	// - matches[2]: machine type (standard|highcpu|highmem|custom)
	// - matches[3]: number of CPUs
	// - matches[4]: memory size in MB (only for custom types)
	// - matches[5]: optional suffixes (e.g., "-lssd-metal", "-metal", "-lssd")

	if len(matches) >= 3 {
		family = matches[1]
		specialization = matches[2]
		numCPUs, err = strconv.Atoi(matches[3])
		if err != nil {
			return "", "", 0, 0, "", err
		}
		if specialization == "custom" {
			if len(matches) >= 4 && matches[4] != "" {
				memMb, err = strconv.Atoi(matches[4])
				if err != nil {
					return "", "", 0, 0, "", err
				}
			} else {
				return "", "", 0, 0, "", fmt.Errorf("custom machine type %q missing memory size", machineType)
			}
		}
		if len(matches) >= 6 && matches[5] != "" {
			// Parse the suffixes (e.g., "-lssd-metal" -> ["lssd", "metal"])
			suffixes := matches[5]
			// Split by dashes and check each part
			for _, suffix := range strings.Split(suffixes, "-") {
				if suffix == "" {
					continue
				}
				// Only return SSD options, ignore other suffixes like "metal"
				switch suffix {
				case "highssd", "standardlssd", "lssd":
					ssdOption = suffix
				}
			}
		}
	}
	return family, specialization, numCPUs, memMb, ssdOption, nil
}

// Given a machine type, return the allowed number (> 0) of local SSDs, sorted in ascending order.
// N.B. Only n1, n2, n2d and c2 instances are supported since we don't typically use other instance types.
// Consult https://cloud.google.com/compute/docs/disks/#local_ssd_machine_type_restrictions for other types of instances.
func AllowedLocalSSDCount(machineType string) ([]int, error) {

	family, _, numCPU, _, ssdOption, err := parseMachineType(machineType)
	if err != nil {
		return nil, err
	}

	switch family {
	case "c3":
		if ssdOption != "lssd" {
			return nil, fmt.Errorf("unsupported local SSD option %q for machine type %q", ssdOption, machineType)
		}
		switch numCPU {
		case 4:
			return []int{1}, nil
		case 8:
			return []int{2}, nil
		case 22:
			return []int{4}, nil
		case 44:
			return []int{8}, nil
		case 88:
			return []int{16}, nil
		case 176:
			return []int{32}, nil
		}

	case "c3d":
		if ssdOption != "lssd" {
			return nil, fmt.Errorf("unsupported local SSD option %q for machine type %q", ssdOption, machineType)
		}
		switch numCPU {
		case 8, 16:
			return []int{1}, nil
		case 30:
			return []int{2}, nil
		case 60:
			return []int{4}, nil
		case 90:
			return []int{8}, nil
		case 180:
			return []int{16}, nil
		case 360:
			return []int{32}, nil
		}

	case "c4":
		if ssdOption != "lssd" {
			return nil, fmt.Errorf("unsupported local SSD option %q for machine type %q", ssdOption, machineType)
		}
		switch numCPU {
		case 4, 8:
			return []int{1}, nil
		case 16:
			return []int{2}, nil
		case 24:
			return []int{4}, nil
		case 32:
			return []int{5}, nil
		case 48:
			return []int{8}, nil
		case 96:
			return []int{16}, nil
		case 144:
			return []int{24}, nil
		case 192:
			return []int{32}, nil
		case 288:
			return []int{48}, nil
		}

	case "c4a":
		if ssdOption != "lssd" {
			return nil, fmt.Errorf("unsupported local SSD option %q for machine type %q", ssdOption, machineType)
		}
		switch numCPU {
		case 4:
			return []int{1}, nil
		case 8:
			return []int{2}, nil
		case 16:
			return []int{4}, nil
		case 32:
			return []int{6}, nil
		case 48:
			return []int{10}, nil
		case 64:
			return []int{14}, nil
		case 72:
			return []int{16}, nil
		}

	case "c4d":
		if ssdOption != "lssd" {
			return nil, fmt.Errorf("unsupported local SSD option %q for machine type %q", ssdOption, machineType)
		}
		switch numCPU {
		case 8, 16:
			return []int{1}, nil
		case 32:
			return []int{2}, nil
		case 48:
			return []int{4}, nil
		case 64:
			return []int{6}, nil
		case 96:
			return []int{8}, nil
		case 192:
			return []int{16}, nil
		case 384:
			return []int{32}, nil
		}

	case "h4d":
		return []int{10}, nil

	case "z3":
		switch numCPU {
		case 8, 14:
			return []int{1}, nil
		case 16:
			return []int{2}, nil
		case 22:
			switch ssdOption {
			case "standardlssd":
				return []int{2}, nil
			case "highlssd":
				return []int{3}, nil
			}
		case 32:
			return []int{4}, nil
		case 44:
			switch ssdOption {
			case "standardlssd":
				return []int{3}, nil
			case "highlssd":
				return []int{6}, nil
			}
		case 88:
			switch ssdOption {
			case "standardlssd":
				return []int{6}, nil
			case "highlssd":
				return []int{12}, nil
			}
		case 176, 192:
			return []int{12}, nil

		}

	case "n1":
		return []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, nil

	case "n2":
		if numCPU <= 10 {
			return []int{1, 2, 4, 8, 16, 24}, nil
		}
		if numCPU <= 20 {
			return []int{2, 4, 8, 16, 24}, nil
		}
		if numCPU <= 40 {
			return []int{4, 8, 16, 24}, nil
		}
		if numCPU <= 80 {
			return []int{8, 16, 24}, nil
		}
		if numCPU <= 128 {
			return []int{16, 24}, nil
		}

	case "n2d":
		if numCPU <= 16 {
			return []int{1, 2, 4, 8, 16, 24}, nil
		}
		if numCPU <= 48 {
			return []int{2, 4, 8, 16, 24}, nil
		}
		if numCPU <= 80 {
			return []int{4, 8, 16, 24}, nil
		}
		if numCPU <= 224 {
			return []int{8, 16, 24}, nil
		}

	case "c2":
		if numCPU <= 8 {
			return []int{1, 2, 4, 8}, nil
		}
		if numCPU <= 16 {
			return []int{2, 4, 8}, nil
		}
		if numCPU <= 30 {
			return []int{4, 8}, nil
		}
		if numCPU <= 60 {
			return []int{8}, nil
		}

	case "c2d":
		if numCPU <= 16 {
			return []int{1, 2, 4, 8}, nil
		}
		if numCPU <= 32 {
			return []int{2, 4, 8}, nil
		}
		if numCPU <= 56 {
			return []int{4, 8}, nil
		}
		if numCPU <= 112 {
			return []int{8}, nil
		}

	case "g2":
		switch numCPU {
		case 4, 8, 12, 16, 32:
			return []int{1}, nil
		case 24:
			return []int{2}, nil
		case 48:
			return []int{4}, nil
		case 96:
			return []int{8}, nil
		}

	case "g4":
		switch numCPU {
		case 48:
			return []int{4}, nil
		case 96:
			return []int{8}, nil
		case 192:
			return []int{16}, nil
		case 384:
			return []int{32}, nil
		}

	case "m1":
		switch numCPU {
		case 40:
			return []int{1, 2, 3, 4, 5}, nil
		case 80:
			return []int{1, 2, 3, 4, 5, 6, 7, 8}, nil
		}

	case "m3":
		switch numCPU {
		case 32, 64:
			return []int{4, 8}, nil
		case 128:
			return []int{8}, nil
		}
	}

	return nil, fmt.Errorf("unsupported machine type: %q", machineType)
}

// isManaged returns true if the cluster is part of a managed instance group.
// This function makes the assumption that a cluster is either completely
// managed or not at all.
func isManaged(vms vm.List) bool {
	return vms[0].Labels[ManagedLabel] == "true"
}

// Delete is part of the vm.Provider interface.
func (p *Provider) Delete(l *logger.Logger, vms vm.List) error {
	return p.DeleteWithContext(context.Background(), l, vms)
}

// Reset implements the vm.Provider interface.
func (p *Provider) Reset(l *logger.Logger, vms vm.List) error {
	return p.ResetWithContext(context.Background(), l, vms)
}

// Extend TODO(peter): document
func (p *Provider) Extend(l *logger.Logger, vms vm.List, lifetime time.Duration) error {
	return p.AddLabels(l, vms, map[string]string{
		vm.TagLifetime: lifetime.String(),
	})
}

// FindActiveAccount TODO(peter): document
func (p *Provider) FindActiveAccount(l *logger.Logger) (string, error) {
	args := []string{"auth", "list", "--format", "json", "--filter", "status~ACTIVE"}

	accounts := make([]jsonAuth, 0)
	if err := runJSONCommand(args, &accounts); err != nil {
		return "", err
	}

	if len(accounts) != 1 {
		return "", fmt.Errorf("no active accounts found, please configure gcloud")
	}

	if !strings.HasSuffix(accounts[0].Account, config.EmailDomain) {
		return "", fmt.Errorf("active account %q does not belong to domain %s",
			accounts[0].Account, config.EmailDomain)
	}
	_ = accounts[0].Status // silence unused warning

	username := strings.Split(accounts[0].Account, "@")[0]
	return username, nil
}

// List queries gcloud to produce a list of VM info objects.
func (p *Provider) List(
	ctx context.Context, l *logger.Logger, opts vm.ListOptions,
) (vm.List, error) {
	return p.list(ctx, l, opts)
}

func (p *Provider) String() string {
	return fmt.Sprintf("%s-%s", ProviderName, strings.Join(p.Projects, "_"))
}

// populateCostPerHour adds an approximate cost per hour to each VM in the list,
// using a basic estimation method.
//  1. Compute and attached disks are estimated at the list prices, ignoring
//     all discounts, but including any automatically applied credits.
//  2. Network egress costs are completely ignored.
//  3. Blob storage costs are completely ignored.
func populateCostPerHour(ctx context.Context, l *logger.Logger, vms vm.List) error {
	// Construct cost estimation service
	service, err := cloudbilling.NewService(ctx)
	if err != nil {
		return err
	}
	beta := cloudbilling.NewV1betaService(service)
	scenario := cloudbilling.EstimateCostScenarioWithListPriceRequest{
		CostScenario: &cloudbilling.CostScenario{
			ScenarioConfig: &cloudbilling.ScenarioConfig{
				EstimateDuration: "3600s",
			},
			Workloads: []*cloudbilling.Workload{},
		},
	}
	// Workload estimation service can handle 100 workloads at a time, so page
	// 100 VMs at a time.
	for len(vms) > 0 {
		scenario.CostScenario.Workloads = scenario.CostScenario.Workloads[:0]
		var page vm.List
		if len(vms) <= 100 {
			page, vms = vms, nil
		} else {
			page, vms = vms[:100], vms[100:]
		}
		for _, vm := range page {
			machineType := vm.MachineType
			zone := vm.Zone

			workload := cloudbilling.Workload{
				Name: vm.Name,
				ComputeVmWorkload: &cloudbilling.ComputeVmWorkload{
					InstancesRunning: &cloudbilling.Usage{
						UsageRateTimeline: &cloudbilling.UsageRateTimeline{
							UsageRateTimelineEntries: []*cloudbilling.UsageRateTimelineEntry{
								{
									// We're estimating the cost of 1 vm at a time.
									UsageRate: 1,
								},
							},
						},
					},
					Preemptible:     vm.Preemptible,
					PersistentDisks: []*cloudbilling.PersistentDisk{},
					Region:          zone[:len(zone)-2],
				},
			}
			if !strings.Contains(machineType, "custom") {
				workload.ComputeVmWorkload.MachineType = &cloudbilling.MachineType{
					PredefinedMachineType: &cloudbilling.PredefinedMachineType{
						MachineType: machineType,
					},
				}
			} else {
				decodeCustomType := func() (string, int64, int64, error) {
					parts := strings.Split(machineType, "-")
					decodeErr := errors.Newf("invalid custom machineType %s", machineType)
					if len(parts) != 4 {
						return "", 0, 0, decodeErr
					}
					series, cpus, memory := parts[0], parts[2], parts[3]
					cpusInt, parseErr := strconv.Atoi(cpus)
					if parseErr != nil {
						return "", 0, 0, decodeErr
					}
					memoryInt, parseErr := strconv.Atoi(memory)
					if parseErr != nil {
						return "", 0, 0, decodeErr
					}
					return series, int64(cpusInt), int64(memoryInt), nil
				}
				series, cpus, memory, err := decodeCustomType()
				if err != nil {
					l.Errorf("Error estimating VM costs, "+
						"continuing without (consider ROACHPROD_NO_COST_ESTIMATES=true): %v", err)
					continue
				}
				workload.ComputeVmWorkload.MachineType = &cloudbilling.MachineType{
					CustomMachineType: &cloudbilling.CustomMachineType{
						MachineSeries:   series,
						VirtualCpuCount: cpus,
						MemorySizeGb:    float64(memory / 1024),
					},
				}
			}
			volumes := append(vm.NonBootAttachedVolumes, vm.BootVolume)
			for _, v := range volumes {
				workload.ComputeVmWorkload.PersistentDisks = append(workload.ComputeVmWorkload.PersistentDisks, &cloudbilling.PersistentDisk{
					DiskSize: &cloudbilling.Usage{
						UsageRateTimeline: &cloudbilling.UsageRateTimeline{
							Unit: "GiBy",
							UsageRateTimelineEntries: []*cloudbilling.UsageRateTimelineEntry{
								{
									UsageRate: float64(v.Size),
								},
							},
						},
					},
					DiskType: v.ProviderVolumeType,
					Scope:    "SCOPE_ZONAL",
				})
			}
			scenario.CostScenario.Workloads = append(scenario.CostScenario.Workloads, &workload)
		}
		estimate, err := beta.EstimateCostScenario(&scenario).Do()
		if err != nil {
			l.Errorf("Error estimating VM costs (will continue without): %v", err)
			continue
		}
		workloadEstimates := estimate.CostEstimationResult.SegmentCostEstimates[0].WorkloadCostEstimates
		for i := range workloadEstimates {
			workloadEstimate := workloadEstimates[i].WorkloadTotalCostEstimate.NetCostEstimate
			page[i].CostPerHour = float64(workloadEstimate.Units) + float64(workloadEstimate.Nanos)/1e9
			// Add the estimated cost of local disks since the billing API only supports persistent disks.
			for _, v := range page[i].LocalDisks {
				// "For example, in the Iowa, Oregon, Taiwan, and Belgium regions, local SSDs cost $0.080 per GB per month."
				// Normalized to per hour billing, 0.08 / 730 = 0.0001095890410958904
				// https://cloud.google.com/compute/disks-image-pricing#disk
				page[i].CostPerHour += float64(v.Size) * 0.00011
			}
		}
	}
	return nil
}

func serializeLabel(s string) string {
	var output = make([]rune, len(s))
	for idx, c := range s {
		if c != '_' && c != '-' && !unicode.IsDigit(c) && !unicode.IsLetter(c) {
			output[idx] = '_'
		} else {
			output[idx] = unicode.ToLower(c)
		}
	}
	return string(output)
}

// Name TODO(peter): document
func (p *Provider) Name() string {
	return ProviderName
}

// Active is part of the vm.Provider interface.
func (p *Provider) Active() bool {
	return initialized
}

// ProjectActive is part of the vm.Provider interface.
func (p *Provider) ProjectActive(project string) bool {
	for _, p := range p.GetProjects() {
		if p == project {
			return true
		}
	}
	return false
}

// lastComponent splits a url path and returns only the last part. This is
// used because some fields in GCE APIs are defined using URLs like:
//
//	"https://www.googleapis.com/compute/v1/projects/cockroach-shared/zones/us-east1-b/machineTypes/n2-standard-16"
//
// We want to strip this down to "n2-standard-16", so we only want the last
// component.
func lastComponent(url string) string {
	s := strings.Split(url, "/")
	return s[len(s)-1]
}

// zoneFromSelfLink splits a GCE self link and returns the zone. This is used
// because some fields in GCE APIs are defined using URLs like:
//
//	"https://www.googleapis.com/compute/v1/projects/cockroach-shared/zones/us-east1-b/machineTypes/n2-standard-16"
//
// We want to extract the "us-east1-b" part, which is the zone.
func zoneFromSelfLink(selfLink string) string {
	substr := "zones/"
	idx := strings.Index(selfLink, substr)
	if idx == -1 {
		return ""
	}
	selfLink = selfLink[idx+len(substr):]
	s := strings.Split(selfLink, "/")
	return s[0]
}
