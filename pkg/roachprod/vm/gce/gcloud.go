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
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Masterminds/semver/v3"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	cloudbilling "google.golang.org/api/cloudbilling/v1beta"
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
	MaxConcurrentVMOps  = 16
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
)

var (
	defaultDefaultProject, defaultMetadataProject, defaultDNSProject, defaultDefaultServiceAccount string
	// projects for which a cron GC job exists.
	projectsWithGC []string
)

func initGCEProjectDefaults() {
	defaultDefaultProject = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DEFAULT_PROJECT", "cockroach-ephemeral",
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

	providerInstance := &Provider{}
	providerInstance.Projects = []string{defaultDefaultProject}
	projectFromEnv := os.Getenv("GCE_PROJECT")
	if projectFromEnv != "" {
		fmt.Printf("WARNING: `GCE_PROJECT` is deprecated; please, use `ROACHPROD_GCE_DEFAULT_PROJECT` instead")
		providerInstance.Projects = []string{projectFromEnv}
	}
	if _, err := exec.LookPath("gcloud"); err != nil {
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, "please install the gcloud CLI utilities "+
			"(https://cloud.google.com/sdk/downloads)")
		return errors.New("gcloud not found")
	}
	providerInstance.dnsProvider = NewDNSProvider()

	providerInstance.defaultProject = defaultDefaultProject
	providerInstance.metadataProject = defaultMetadataProject

	initialized = true
	vm.Providers[ProviderName] = providerInstance
	Infrastructure = providerInstance

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

// Used to parse the gcloud responses
type jsonVM struct {
	Name              string
	Labels            map[string]string
	CreationTimestamp time.Time
	NetworkInterfaces []struct {
		Network       string
		NetworkIP     string
		AccessConfigs []struct {
			Name  string
			NatIP string
		}
	}
	Scheduling struct {
		AutomaticRestart          bool
		Preemptible               bool
		OnHostMaintenance         string
		InstanceTerminationAction string
		ProvisioningModel         string
	}
	Metadata struct {
		Items []struct {
			Key   string
			Value string
		}
	}
	MachineType string
	// CPU platform corresponding to machine type; see https://cloud.google.com/compute/docs/cpu-platforms
	CPUPlatform string
	SelfLink    string
	Zone        string
	instanceDisksResponse
}

// Convert the JSON VM data into our common VM type.
func (jsonVM *jsonVM) toVM(project string, dnsDomain string) (ret *vm.VM) {
	var vmErrors []error
	var err error

	// Check "lifetime" label.
	var lifetime time.Duration
	if lifetimeStr, ok := jsonVM.Labels[vm.TagLifetime]; ok {
		if lifetime, err = time.ParseDuration(lifetimeStr); err != nil {
			vmErrors = append(vmErrors, vm.ErrNoExpiration)
		}
	} else {
		vmErrors = append(vmErrors, vm.ErrNoExpiration)
	}

	// Extract network information
	var publicIP, privateIP, vpc string
	if len(jsonVM.NetworkInterfaces) == 0 {
		vmErrors = append(vmErrors, vm.ErrBadNetwork)
	} else {
		privateIP = jsonVM.NetworkInterfaces[0].NetworkIP
		if len(jsonVM.NetworkInterfaces[0].AccessConfigs) == 0 {
			vmErrors = append(vmErrors, vm.ErrBadNetwork)
		} else {
			_ = jsonVM.NetworkInterfaces[0].AccessConfigs[0].Name // silence unused warning
			publicIP = jsonVM.NetworkInterfaces[0].AccessConfigs[0].NatIP
			vpc = lastComponent(jsonVM.NetworkInterfaces[0].Network)
		}
	}
	if jsonVM.Scheduling.OnHostMaintenance == "" {
		// N.B. 'onHostMaintenance' is always non-empty, hence its absense implies a parsing error
		vmErrors = append(vmErrors, vm.ErrBadScheduling)
	}

	machineType := lastComponent(jsonVM.MachineType)
	cpuPlatform := jsonVM.CPUPlatform
	zone := lastComponent(jsonVM.Zone)
	remoteUser := config.SharedUser
	if !config.UseSharedUser {
		// N.B. gcloud uses the local username to log into instances rather
		// than the username on the authenticated Google account but we set
		// up the shared user at cluster creation time. Allow use of the
		// local username if requested.
		remoteUser = config.OSUser.Username
	}

	var volumes []vm.Volume
	var localDisks []vm.Volume
	var bootVolume vm.Volume

	for _, jsonVMDisk := range jsonVM.Disks {

		vol, volType, err := jsonVMDisk.toVolume()
		if err != nil {
			vmErrors = append(vmErrors, err)
			continue
		}

		switch volType {
		case VolumeTypeBoot:
			bootVolume = *vol
		case VolumeTypeLocalSSD:
			localDisks = append(localDisks, *vol)
		default:
			volumes = append(volumes, *vol)
		}

	}

	return &vm.VM{
		Name:                   jsonVM.Name,
		CreatedAt:              jsonVM.CreationTimestamp,
		Errors:                 vmErrors,
		DNS:                    fmt.Sprintf("%s.%s.%s", jsonVM.Name, zone, project),
		Lifetime:               lifetime,
		Preemptible:            jsonVM.Scheduling.Preemptible,
		Labels:                 jsonVM.Labels,
		PrivateIP:              privateIP,
		Provider:               ProviderName,
		DNSProvider:            ProviderName,
		ProviderID:             jsonVM.Name,
		PublicIP:               publicIP,
		PublicDNS:              fmt.Sprintf("%s.%s", jsonVM.Name, dnsDomain),
		RemoteUser:             remoteUser,
		VPC:                    vpc,
		MachineType:            machineType,
		CPUArch:                vm.ParseArch(cpuPlatform),
		CPUFamily:              strings.Replace(strings.ToLower(cpuPlatform), "intel ", "", 1),
		Zone:                   zone,
		Project:                project,
		NonBootAttachedVolumes: volumes,
		BootVolume:             bootVolume,
		LocalDisks:             localDisks,
	}
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
	MachineType      string
	MinCPUPlatform   string
	Zones            []string
	Image            string
	SSDCount         int
	PDVolumeType     string
	PDVolumeSize     int
	PDVolumeCount    int
	UseMultipleDisks bool
	// use spot instances (i.e., latest version of preemptibles which can run > 24 hours)
	UseSpot bool
	// Use an instance template and a managed instance group to create VMs. This
	// enables cluster resizing, load balancing, and health monitoring.
	Managed bool
	// This specifies a subset of the Zones above that will run on spot instances.
	// VMs running in Zones not in this list will be provisioned on-demand. This
	// is only used by managed instance groups.
	ManagedSpotZones []string
	// Enable the cron service. It is disabled by default.
	EnableCron bool

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
	*dnsProvider
	Projects []string

	// The project to use for looking up metadata. In particular, this includes
	// user keys.
	metadataProject string

	// The project that provides the core roachprod services.
	defaultProject string
}

// LogEntry represents a single log entry from the gcloud logging(stack driver)
type LogEntry struct {
	LogName  string `json:"logName"`
	Resource struct {
		Labels struct {
			InstanceID string `json:"instance_id"`
		} `json:"labels"`
	} `json:"resource"`
	Timestamp    string `json:"timestamp"`
	ProtoPayload struct {
		ResourceName string `json:"resourceName"`
	} `json:"protoPayload"`
}

func (p *Provider) SupportsSpotVMs() bool {
	return true
}

// GetPreemptedSpotVMs checks the preemption status of the given VMs, by querying the GCP logging service.
func (p *Provider) GetPreemptedSpotVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {
	args, err := buildFilterPreemptionCliArgs(vms, p.GetProject(), since)
	if err != nil {
		l.Printf("Error building gcloud cli command: %v\n", err)
		return nil, err
	}
	var logEntries []LogEntry
	if err := runJSONCommand(args, &logEntries); err != nil {
		l.Printf("Error running gcloud cli command: %v\n", err)
		return nil, err
	}
	// Extract the VM name and the time of preemption from logs.
	var preemptedVMs []vm.PreemptedVM
	for _, logEntry := range logEntries {
		timestamp, err := time.Parse(time.RFC3339, logEntry.Timestamp)
		if err != nil {
			l.Printf("Error parsing gcp log timestamp, Preemption time not available: %v", err)
			preemptedVMs = append(preemptedVMs, vm.PreemptedVM{Name: logEntry.ProtoPayload.ResourceName})
			continue
		}
		preemptedVMs = append(preemptedVMs, vm.PreemptedVM{Name: logEntry.ProtoPayload.ResourceName, PreemptedAt: timestamp})
	}
	return preemptedVMs, nil
}

// GetHostErrorVMs checks the host error status of the given VMs, by querying the GCP logging service.
func (p *Provider) GetHostErrorVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	args, err := buildFilterHostErrorCliArgs(vms, since, p.GetProject())
	if err != nil {
		l.Printf("Error building gcloud cli command: %v\n", err)
		return nil, err
	}
	var logEntries []LogEntry
	if err := runJSONCommand(args, &logEntries); err != nil {
		l.Printf("Error running gcloud cli command: %v\n", err)
		return nil, err
	}
	// Extract the name of the VM with host error from logs.
	var hostErrorVMs []string
	for _, logEntry := range logEntries {
		hostErrorVMs = append(hostErrorVMs, logEntry.ProtoPayload.ResourceName)
	}
	return hostErrorVMs, nil
}

// GetVMSpecs returns a map from VM.Name to a map of VM attributes, provided by GCE
func (p *Provider) GetVMSpecs(
	l *logger.Logger, vms vm.List,
) (map[string]map[string]interface{}, error) {
	if p.GetProject() == "" {
		return nil, errors.New("project name cannot be empty")
	}
	if vms == nil {
		return nil, errors.New("vms cannot be nil")
	}
	// Extract the spec of all VMs and create a map from VM name to spec.
	vmSpecs := make(map[string]map[string]interface{})
	for _, vmInstance := range vms {
		var vmSpec map[string]interface{}
		vmFullResourceName := "projects/" + p.GetProject() + "/zones/" + vmInstance.Zone + "/instances/" + vmInstance.Name
		args := []string{"compute", "instances", "describe", vmFullResourceName, "--format=json"}

		if err := runJSONCommand(args, &vmSpec); err != nil {
			return nil, errors.Wrapf(err, "error describing instance %s in zone %s", vmInstance.Name, vmInstance.Zone)
		}
		name, ok := vmSpec["name"].(string)
		if !ok {
			l.Errorf("failed to create spec files for VM\n%v", vmSpec)
			continue
		}
		vmSpecs[name] = vmSpec
	}
	return vmSpecs, nil
}

func buildFilterCliArgs(
	vms vm.List, projectName string, since time.Time, filter string,
) ([]string, error) {
	if projectName == "" {
		return nil, errors.New("project name cannot be empty")
	}
	if since.After(timeutil.Now()) {
		return nil, errors.New("since cannot be in the future")
	}
	if vms == nil {
		return nil, errors.New("vms cannot be nil")
	}
	// construct full resource names
	vmFullResourceNames := make([]string, len(vms))
	for i, vmNode := range vms {
		// example format : projects/cockroach-ephemeral/zones/us-east1-b/instances/test-name
		vmFullResourceNames[i] = "projects/" + projectName + "/zones/" + vmNode.Zone + "/instances/" + vmNode.Name
	}
	// Prepend vmFullResourceNames with "protoPayload.resourceName=" to help with filter construction.
	vmIDFilter := make([]string, len(vms))
	for i, vmID := range vmFullResourceNames {
		vmIDFilter[i] = fmt.Sprintf("protoPayload.resourceName=%s", vmID)
	}
	filter += fmt.Sprintf(` AND (%s)`, strings.Join(vmIDFilter, " OR "))
	args := []string{
		"logging",
		"read",
		"--project=" + projectName,
		"--format=json",
		fmt.Sprintf("--freshness=%dh", int(timeutil.Since(since).Hours()+1)), // only look at logs from the "since" specified
		filter,
	}
	return args, nil
}

// buildFilterPreemptionCliArgs returns the arguments to be passed to gcloud cli to query the logs for preemption events.
func buildFilterPreemptionCliArgs(
	vms vm.List, projectName string, since time.Time,
) ([]string, error) {
	// Create a filter to match preemption events
	filter := `resource.type=gce_instance AND (protoPayload.methodName=compute.instances.preempted)`
	return buildFilterCliArgs(vms, projectName, since, filter)
}

// buildFilterHostErrorCliArgs returns the arguments to be passed to gcloud cli to query the logs for host error events.
func buildFilterHostErrorCliArgs(
	vms vm.List, since time.Time, projectName string,
) ([]string, error) {
	// Create a filter to match hostError events for the specified projectName
	filter := fmt.Sprintf(`resource.type=gce_instance AND protoPayload.methodName=compute.instances.hostError
		AND logName=projects/%s/logs/cloudaudit.googleapis.com%%2Fsystem_event`, projectName)
	return buildFilterCliArgs(vms, projectName, since, filter)
}

type snapshotJson struct {
	CreationTimestamp  time.Time `json:"creationTimestamp"`
	Description        string    `json:"description"`
	DiskSizeGb         string    `json:"diskSizeGb"`
	DownloadBytes      string    `json:"downloadBytes"`
	ID                 string    `json:"id"`
	Kind               string    `json:"kind"`
	LabelFingerprint   string    `json:"labelFingerprint"`
	Name               string    `json:"name"`
	SelfLink           string    `json:"selfLink"`
	SourceDisk         string    `json:"sourceDisk"`
	SourceDiskId       string    `json:"sourceDiskId"`
	Status             string    `json:"status"`
	StorageBytes       string    `json:"storageBytes"`
	StorageBytesStatus string    `json:"storageBytesStatus"`
	StorageLocations   []string  `json:"storageLocations"`
}

func (p *Provider) CreateVolumeSnapshot(
	l *logger.Logger, volume vm.Volume, vsco vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	args := []string{
		"compute",
		"--project", p.GetProject(),
		"snapshots",
		"create", vsco.Name,
		"--source-disk", volume.ProviderResourceID,
		"--source-disk-zone", volume.Zone,
		"--description", vsco.Description,
		"--format", "json",
	}

	var createJsonResponse snapshotJson
	if err := runJSONCommand(args, &createJsonResponse); err != nil {
		return vm.VolumeSnapshot{}, err
	}

	sb := strings.Builder{}
	for k, v := range vsco.Labels {
		fmt.Fprintf(&sb, "%s=%s,", serializeLabel(k), serializeLabel(v))
	}
	s := sb.String()

	args = []string{
		"compute",
		"--project", p.GetProject(),
		"snapshots",
		"add-labels", vsco.Name,
		"--labels", s[:len(s)-1],
	}
	cmd := exec.Command("gcloud", args...)
	if _, err := cmd.CombinedOutput(); err != nil {
		return vm.VolumeSnapshot{}, err
	}
	return vm.VolumeSnapshot{
		ID:   createJsonResponse.ID,
		Name: createJsonResponse.Name,
	}, nil
}

func (p *Provider) ListVolumeSnapshots(
	l *logger.Logger, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	args := []string{
		"compute",
		"--project", p.GetProject(),
		"snapshots",
		"list",
		"--format", "json(name,id)",
	}
	var filters []string
	if vslo.NamePrefix != "" {
		filters = append(filters, fmt.Sprintf("name:%s", vslo.NamePrefix))
	}
	if !vslo.CreatedBefore.IsZero() {
		filters = append(filters, fmt.Sprintf("creationTimestamp<'%s'", vslo.CreatedBefore.Format("2006-01-02")))
	}
	for k, v := range vslo.Labels {
		filters = append(filters, fmt.Sprintf("labels.%s=%s", k, v))
	}
	if len(filters) > 0 {
		args = append(args, "--filter", strings.Join(filters, " AND "))
	}

	var snapshotsJSONResponse []snapshotJson
	if err := runJSONCommand(args, &snapshotsJSONResponse); err != nil {
		return nil, err
	}

	var snapshots []vm.VolumeSnapshot
	for _, snapshotJson := range snapshotsJSONResponse {
		if !strings.HasPrefix(snapshotJson.Name, vslo.NamePrefix) {
			continue
		}
		snapshots = append(snapshots, vm.VolumeSnapshot{
			ID:   snapshotJson.ID,
			Name: snapshotJson.Name,
		})
	}
	sort.Sort(vm.VolumeSnapshots(snapshots))
	return snapshots, nil
}

func (p *Provider) DeleteVolumeSnapshots(l *logger.Logger, snapshots ...vm.VolumeSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	args := []string{
		"compute",
		"--project", p.GetProject(),
		"snapshots",
		"delete",
	}
	for _, snapshot := range snapshots {
		args = append(args, snapshot.Name)
	}

	cmd := exec.Command("gcloud", args...)
	if _, err := cmd.CombinedOutput(); err != nil {
		return err
	}
	return nil
}

type describeVolumeCommandResponse struct {
	CreationTimestamp      time.Time         `json:"creationTimestamp"`
	ID                     string            `json:"id"`
	Kind                   string            `json:"kind"`
	LabelFingerprint       string            `json:"labelFingerprint"`
	Name                   string            `json:"name"`
	PhysicalBlockSizeBytes string            `json:"physicalBlockSizeBytes"`
	SelfLink               string            `json:"selfLink"`
	SizeGB                 string            `json:"sizeGb"`
	Status                 string            `json:"status"`
	Type                   string            `json:"type"`
	Zone                   string            `json:"zone"`
	Labels                 map[string]string `json:"labels"`
	Users                  []string          `json:"users"`
}

func (p *Provider) CreateVolume(
	l *logger.Logger, vco vm.VolumeCreateOpts,
) (vol vm.Volume, err error) {
	// TODO(leon): IOPS is not handled.
	if vco.IOPS != 0 {
		err = errors.New("Creating a volume with IOPS is not supported at this time.")
		return vol, err
	}
	args := []string{
		"compute",
		"--project", p.GetProject(),
		"disks",
		"create", vco.Name,
		"--size", strconv.Itoa(vco.Size),
		"--zone", vco.Zone,
		"--format", "json",
	}
	if vco.SourceSnapshotID != "" {
		args = append(args, "--source-snapshot", vco.SourceSnapshotID)
	}

	if vco.Size == 0 {
		return vol, errors.New("Cannot create a volume of size 0")
	}

	if vco.Encrypted {
		return vol, errors.New("Volume encryption is not implemented for GCP")
	}

	if vco.Architecture != "" {
		if vco.Architecture == "ARM64" || vco.Architecture == "X86_64" {
			args = append(args, "--architecture", vco.Architecture)
		} else {
			return vol, errors.Newf("Expected architecture to be one of ARM64, X86_64 got %s\n", vco.Architecture)
		}
	}

	switch vco.Type {
	case "local-ssd", "pd-balanced", "pd-extreme", "pd-ssd", "pd-standard":
		args = append(args, "--type", vco.Type)
	case "":
	// use the default
	default:
		return vol, errors.Newf("Expected type to be one of local-ssd, pd-balanced, pd-extreme, pd-ssd, pd-standard got %s\n", vco.Type)
	}

	var commandResponse []describeVolumeCommandResponse
	err = runJSONCommand(args, &commandResponse)
	if err != nil {
		return vm.Volume{}, err
	}
	if len(commandResponse) != 1 {
		return vol, errors.Newf("Expected to create 1 volume created %d", len(commandResponse))
	}

	createdVolume := commandResponse[0]

	size, err := strconv.Atoi(createdVolume.SizeGB)
	if err != nil {
		return vol, err
	}
	if len(vco.Labels) > 0 {
		sb := strings.Builder{}
		for k, v := range vco.Labels {
			fmt.Fprintf(&sb, "%s=%s,", serializeLabel(k), serializeLabel(v))
		}
		s := sb.String()
		args = []string{
			"compute",
			"--project", p.GetProject(),
			"disks",
			"add-labels", vco.Name,
			"--labels", s[:len(s)-1],
			"--zone", vco.Zone,
		}
		cmd := exec.Command("gcloud", args...)
		if _, err := cmd.CombinedOutput(); err != nil {
			return vm.Volume{}, err
		}
	}

	return vm.Volume{
		ProviderResourceID: createdVolume.Name,
		ProviderVolumeType: lastComponent(createdVolume.Type),
		Zone:               lastComponent(createdVolume.Zone),
		Encrypted:          false, // only used for aws
		Name:               createdVolume.Name,
		Labels:             createdVolume.Labels,
		Size:               size,
	}, nil
}

func (p *Provider) DeleteVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) error {
	{ // Detach disks.
		args := []string{
			"compute",
			"--project", p.GetProject(),
			"instances",
			"detach-disk", vm.Name,
			"--disk", volume.ProviderResourceID,
			"--zone", volume.Zone,
		}
		cmd := exec.Command("gcloud", args...)
		if _, err := cmd.CombinedOutput(); err != nil {
			return err
		}
	}
	{ // Delete disks.
		args := []string{
			"compute",
			"--project", p.GetProject(),
			"disks",
			"delete",
			volume.ProviderResourceID,
			"--zone", volume.Zone,
			"--quiet",
		}
		cmd := exec.Command("gcloud", args...)
		if _, err := cmd.CombinedOutput(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) ListVolumes(l *logger.Logger, v *vm.VM) ([]vm.Volume, error) {
	var attachedDisks []attachDiskCmdDisk
	var describedVolumes []describeVolumeCommandResponse

	{
		// We're running the equivalent of:
		//  	gcloud compute instances describe irfansharif-snapshot-0001 \
		//  		--project cockroach-ephemeral --zone us-east1-b \
		// 			--format json(disks)
		//
		// We'll use this data to filter out boot disks.
		var commandResponse instanceDisksResponse
		args := []string{
			"compute",
			"instances",
			"describe",
			v.Name,
			"--project", p.GetProject(),
			"--zone", v.Zone,
			"--format", "json(disks)",
		}
		if err := runJSONCommand(args, &commandResponse); err != nil {
			return nil, err
		}
		attachedDisks = commandResponse.Disks
	}

	{
		// We're running the equivalent of
		// 		gcloud compute disks list --project cockroach-ephemeral \
		//			--filter "users:(irfansharif-snapshot-0001)" --format json
		//
		// This contains more per-disk metadata than the command above, but
		// annoyingly does not contain whether the disk is a boot volume.
		args := []string{
			"compute",
			"disks",
			"list",
			"--project", p.GetProject(),
			"--filter", fmt.Sprintf("users:(%s)", v.Name),
			"--format", "json",
		}
		if err := runJSONCommand(args, &describedVolumes); err != nil {
			return nil, err
		}
	}

	var volumes []vm.Volume
	for idx := range attachedDisks {
		attachedDisk := attachedDisks[idx]
		if attachedDisk.Boot {
			continue
		}
		describedVolume := describedVolumes[idx]
		size, err := strconv.Atoi(describedVolume.SizeGB)
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, vm.Volume{
			ProviderResourceID: describedVolume.Name,
			ProviderVolumeType: lastComponent(describedVolume.Type),
			Zone:               lastComponent(describedVolume.Zone),
			Encrypted:          false, // only used for aws
			Name:               describedVolume.Name,
			Labels:             describedVolume.Labels,
			Size:               size,
		})
	}

	// TODO(irfansharif): Update v.NonBootAttachedVolumes? It's awkward to have
	// that field at all.
	return volumes, nil
}

type instanceDisksResponse struct {
	// Disks that are attached to the instance.
	// N.B. Unattached disks can be enumerated via,
	//	gcloud compute --project $project disks list --filter="-users:*"
	Disks []attachDiskCmdDisk `json:"disks"`
}
type attachDiskCmdDisk struct {
	AutoDelete bool   `json:"autoDelete"`
	Boot       bool   `json:"boot"`
	DeviceName string `json:"deviceName"`
	DiskSizeGB string `json:"diskSizeGb"`
	Index      int    `json:"index"`
	Interface  string `json:"interface"`
	Kind       string `json:"kind"`
	Mode       string `json:"mode"`
	Source     string `json:"source"`
	Type       string `json:"type"`
}

// toVolume converts an attachedDisk struct (disks returned by `instances list`)
// to a vm.Volume struct.
func (d *attachDiskCmdDisk) toVolume() (*vm.Volume, VolumeType, error) {
	diskSize, err := strconv.Atoi(d.DiskSizeGB)
	if err != nil {
		return nil, VolumeTypeUnknown, err
	}

	// This is a scratch disk.
	if d.Source == "" && d.Type == "SCRATCH" {
		return &vm.Volume{
			Size:               diskSize,
			ProviderVolumeType: "local-ssd",
		}, VolumeTypeLocalSSD, nil
	}

	volType := VolumeTypePersistent
	if d.Boot {
		volType = VolumeTypeBoot
	}

	vol := &vm.Volume{
		Name:               lastComponent(d.Source),
		Zone:               zoneFromSelfLink(d.Source),
		Size:               diskSize,
		ProviderResourceID: lastComponent(d.Source),
		// Unfortunately, the attachedDisk struct does not return the actual type
		// (standard or ssd) of the persistent disk. We assume `pd_ssd` as those
		// are common and is a worst case scenario for cost computation.
		ProviderVolumeType: "pd-ssd",
	}

	return vol, volType, nil
}

func (p *Provider) AttachVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) (string, error) {
	// Volume attach.
	args := []string{
		"compute",
		"--project", p.GetProject(),
		"instances",
		"attach-disk",
		vm.ProviderID,
		"--disk", volume.ProviderResourceID,
		"--device-name", volume.ProviderResourceID,
		"--zone", vm.Zone,
		"--format=json(disks)",
	}

	var commandResponse []instanceDisksResponse
	if err := runJSONCommand(args, &commandResponse); err != nil {
		return "", err
	}
	found := false
	if len(commandResponse) != 1 {
		return "", errors.Newf("Expected to get back json with just a single item got %d", len(commandResponse))
	}
	cmdRespDisks := commandResponse[0].Disks
	for _, response := range cmdRespDisks {
		found = found || strings.Contains(response.Source, volume.ProviderResourceID)
	}
	if !found {
		return "", errors.Newf("Could not find created disk '%s' in list of disks for %s",
			volume.ProviderResourceID, vm.ProviderID)
	}

	// Volume auto delete.
	args = []string{
		"compute",
		"--project", p.GetProject(),
		"instances",
		"set-disk-auto-delete", vm.ProviderID,
		"--auto-delete",
		"--device-name", volume.ProviderResourceID,
		"--zone", vm.Zone,
		"--format=json(disks)",
	}

	if err := runJSONCommand(args, &commandResponse); err != nil {
		return "", err
	}

	if len(commandResponse) != 1 {
		return "", errors.Newf("Expected to get back json with just a single item got %d", len(commandResponse))
	}
	cmdRespDisks = commandResponse[0].Disks
	for _, response := range cmdRespDisks {
		if response.DeviceName == volume.ProviderResourceID && !response.AutoDelete {
			return "", errors.Newf("Could not set disk '%s' to auto-delete on instance termination",
				volume.ProviderResourceID)
		}
	}

	return "/dev/disk/by-id/google-" + volume.ProviderResourceID, nil
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
func DefaultZones(arch string) []string {
	zones := []string{"us-east1-b", "us-east1-c", "us-east1-d"}
	if vm.ParseArch(arch) == vm.ArchARM64 {
		// T2A instances are only available in us-central1 in NA.
		zones = []string{"us-central1-a", "us-central1-b", "us-central1-f"}
	}
	rand.Shuffle(len(zones), func(i, j int) { zones[i], zones[j] = zones[j], zones[i] })

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
	flags.StringVar(&o.MinCPUPlatform, ProviderName+"-min-cpu-platform", "Intel Ice Lake",
		"Minimum CPU platform (see https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform)")
	flags.StringVar(&o.Image, ProviderName+"-image", DefaultImage,
		"Image to use to create the vm, "+
			"use `gcloud compute images list --filter=\"family=ubuntu-2004-lts\"` to list available images. "+
			"Note: this option is ignored if --fips is passed.")

	flags.IntVar(&o.SSDCount, ProviderName+"-local-ssd-count", 1,
		"Number of local SSDs to create, only used if local-ssd=true")
	flags.StringVar(&o.PDVolumeType, ProviderName+"-pd-volume-type", "pd-ssd",
		"Type of the persistent disk volume, only used if local-ssd=false")
	flags.IntVar(&o.PDVolumeSize, ProviderName+"-pd-volume-size", 500,
		"Size in GB of persistent disk volume, only used if local-ssd=false")
	flags.IntVar(&o.PDVolumeCount, ProviderName+"-pd-volume-count", 1,
		"Number of persistent disk volumes, only used if local-ssd=false")
	flags.BoolVar(&o.UseMultipleDisks, ProviderName+"-enable-multiple-stores",
		false, "Enable the use of multiple stores by creating one store directory per disk. "+
			"Default is to raid0 stripe all disks.")

	flags.StringSliceVar(&o.Zones, ProviderName+"-zones", nil,
		fmt.Sprintf("Zones for cluster. If zones are formatted as AZ:N where N is an integer, the zone\n"+
			"will be repeated N times. If > 1 zone specified, nodes will be geo-distributed\n"+
			"regardless of geo (default [%s])",
			strings.Join(DefaultZones(string(vm.ArchAMD64)), ",")))
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
	dnsProviderInstance := p.dnsProvider
	flags.StringVar(
		&dnsProviderInstance.dnsProject, ProviderName+"-dns-project",
		dnsProviderInstance.dnsProject,
		"project to use to set up DNS",
	)
	flags.StringVar(
		&dnsProviderInstance.publicZone,
		ProviderName+"-dns-zone",
		dnsProviderInstance.publicZone,
		"zone file in gcloud project to use to set up public DNS records",
	)
	flags.StringVar(
		&dnsProviderInstance.publicDomain,
		ProviderName+"-dns-domain",
		dnsProviderInstance.publicDomain,
		"zone domian in gcloud project to use to set up public DNS records",
	)
	flags.StringVar(
		&dnsProviderInstance.managedZone,
		ProviderName+"managed-dns-zone",
		dnsProviderInstance.managedZone,
		"zone file in gcloud project to use to set up DNS SRV records",
	)
	flags.StringVar(
		&dnsProviderInstance.managedDomain,
		ProviderName+"managed-dns-domain",
		dnsProviderInstance.managedDomain,
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

// useArmAMI returns true if the machine type is an arm64 machine type.
func (o *ProviderOpts) useArmAMI() bool {
	return strings.HasPrefix(strings.ToLower(o.MachineType), "t2a-")
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

func (p *Provider) editLabels(
	l *logger.Logger, vms vm.List, labels map[string]string, remove bool,
) error {
	cmdArgs := []string{"compute", "instances"}
	if remove {
		cmdArgs = append(cmdArgs, "remove-labels")
	} else {
		cmdArgs = append(cmdArgs, "add-labels")
	}

	tagArgs := make([]string, 0, len(labels))
	for key, value := range labels {
		if remove {
			tagArgs = append(tagArgs, key)
		} else {
			tagArgs = append(tagArgs, fmt.Sprintf("%s=%s", key, value))
		}
	}
	tagArgsString := strings.Join(tagArgs, ",")
	commonArgs := []string{"--project", p.GetProject(), fmt.Sprintf("--labels=%s", tagArgsString)}

	var g errgroup.Group
	g.SetLimit(MaxConcurrentVMOps)
	for _, v := range vms {
		vmArgs := make([]string, len(cmdArgs))
		copy(vmArgs, cmdArgs)

		vmArgs = append(vmArgs, v.Name, "--zone", v.Zone)
		vmArgs = append(vmArgs, commonArgs...)

		g.Go(func() error {
			cmd := exec.Command("gcloud", vmArgs...)
			if b, err := cmd.CombinedOutput(); err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", vmArgs, string(b))
			}
			return nil
		})
	}
	return g.Wait()
}

// AddLabels adds (or updates) the given labels to the given VMs.
// N.B. If a VM contains a label with the same key, its value will be updated.
func (p *Provider) AddLabels(l *logger.Logger, vms vm.List, labels map[string]string) error {
	return p.editLabels(l, vms, labels, false /* remove */)
}

func (p *Provider) RemoveLabels(l *logger.Logger, vms vm.List, labels []string) error {
	labelsMap := make(map[string]string, len(labels))
	for _, label := range labels {
		labelsMap[label] = ""
	}
	return p.editLabels(l, vms, labelsMap, true /* remove */)
}

// computeLabelsArg computes the labels arg to be passed to the gcloud command
// during cluster creation.
func computeLabelsArg(opts vm.CreateOpts, providerOpts *ProviderOpts) (string, error) {
	m := vm.GetDefaultLabelMap(opts)
	// Format according to gce label naming convention requirement.
	time := timeutil.Now().Format(time.RFC3339)
	time = strings.ToLower(strings.ReplaceAll(time, ":", "_"))
	m[vm.TagCreated] = time

	var labelPairs []string
	addLabel := func(key, value string) {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", key, value))
	}

	if providerOpts.Managed {
		addLabel(ManagedLabel, "true")
	}

	if providerOpts.UseSpot {
		addLabel(vm.TagSpotInstance, "true")
	}

	for key, value := range opts.CustomLabels {
		_, ok := m[strings.ToLower(key)]
		if ok {
			return "", fmt.Errorf("duplicate label name defined: %s", key)
		}
		addLabel(key, value)
	}

	for key, value := range m {
		addLabel(key, value)
	}

	return strings.Join(labelPairs, ","), nil
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
		if opts.GeoDistributed {
			zones = DefaultZones(opts.Arch)
		} else {
			zones = []string{DefaultZones(opts.Arch)[0]}
		}
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

// computeInstanceArgs computes the arguments to be passed to the gcloud command
// to create a VM or create an instance template for a VM. This function must
// ensure that it returns arguments compatible with both the `gcloud compute
// instances create` and `gcloud compute instance-templates create` commands.
func (p *Provider) computeInstanceArgs(
	l *logger.Logger, opts vm.CreateOpts, providerOpts *ProviderOpts,
) (args []string, cleanUpFn func(), err error) {
	cleanUpFn = func() {}
	project := p.GetProject()

	// Fixed args.
	image := providerOpts.Image
	imageProject := defaultImageProject

	if opts.Arch == string(vm.ArchARM64) && !providerOpts.useArmAMI() {
		return nil, cleanUpFn, errors.Errorf("Requested arch is arm64, but machine type is %s. Do specify a t2a VM", providerOpts.MachineType)
	}

	if providerOpts.useArmAMI() && (opts.Arch != "" && opts.Arch != string(vm.ArchARM64)) {
		return nil, cleanUpFn, errors.Errorf("machine type %s is arm64, but requested arch is %s", providerOpts.MachineType, opts.Arch)
	}
	if providerOpts.useArmAMI() && opts.SSDOpts.UseLocalSSD {
		return nil, cleanUpFn, errors.New("local SSDs are not supported with T2A instances, use --local-ssd=false")
	}
	if providerOpts.useArmAMI() {
		if providerOpts.MinCPUPlatform != "" {
			l.Printf("WARNING: --gce-min-cpu-platform is ignored for T2A instances")
			providerOpts.MinCPUPlatform = ""
		}
		// TODO(srosenberg): remove this once we have a better way to detect ARM64 machines
		image = ARM64Image
		l.Printf("Using ARM64 AMI: %s for machine type: %s", image, providerOpts.MachineType)
	}
	if opts.Arch == string(vm.ArchFIPS) {
		// NB: if FIPS is enabled, it overrides the image passed via CLI (--gce-image)
		image = FIPSImage
		imageProject = FIPSImageProject
		l.Printf("Using FIPS-enabled AMI: %s for machine type: %s", image, providerOpts.MachineType)
	}

	args = []string{
		"--scopes", "cloud-platform",
		"--image", image,
		"--image-project", imageProject,
		"--boot-disk-type", "pd-ssd",
	}

	if project == p.defaultProject && providerOpts.ServiceAccount == "" {
		providerOpts.ServiceAccount = providerOpts.defaultServiceAccount
	}
	if providerOpts.ServiceAccount != "" {
		args = append(args, "--service-account", providerOpts.ServiceAccount)
	}

	if providerOpts.preemptible {
		// Make sure the lifetime is no longer than 24h
		if opts.Lifetime > time.Hour*24 {
			return nil, cleanUpFn, errors.New("lifetime cannot be longer than 24 hours for preemptible instances")
		}
		if !providerOpts.TerminateOnMigration {
			return nil, cleanUpFn, errors.New("preemptible instances require 'TERMINATE' maintenance policy; use --gce-terminateOnMigration")
		}
		args = append(args, "--preemptible")
		// Preemptible instances require the following arguments set explicitly
		args = append(args, "--maintenance-policy", "TERMINATE")
		args = append(args, "--no-restart-on-failure")
	} else if providerOpts.UseSpot {
		args = append(args, "--provisioning-model", "SPOT")
	} else {
		if providerOpts.TerminateOnMigration {
			args = append(args, "--maintenance-policy", "TERMINATE")
		} else {
			args = append(args, "--maintenance-policy", "MIGRATE")
		}
	}

	extraMountOpts := ""
	// Dynamic args.
	if opts.SSDOpts.UseLocalSSD {
		if counts, err := AllowedLocalSSDCount(providerOpts.MachineType); err != nil {
			return nil, cleanUpFn, err
		} else {
			// Make sure the minimum number of local SSDs is met.
			minCount := counts[0]
			if providerOpts.SSDCount < minCount {
				l.Printf("WARNING: SSD count must be at least %d for %q. Setting --gce-local-ssd-count to %d", minCount, providerOpts.MachineType, minCount)
				providerOpts.SSDCount = minCount
			}
		}
		for i := 0; i < providerOpts.SSDCount; i++ {
			args = append(args, "--local-ssd", "interface=NVME")
		}
		// Add `discard` for Local SSDs on NVMe, as is advised in:
		// https://cloud.google.com/compute/docs/disks/add-local-ssd
		extraMountOpts = "discard"
		if opts.SSDOpts.NoExt4Barrier {
			extraMountOpts = fmt.Sprintf("%s,nobarrier", extraMountOpts)
		}
	} else {
		// create the "PDVolumeCount" number of persistent disks with the same configuration
		for i := 0; i < providerOpts.PDVolumeCount; i++ {
			pdProps := []string{
				fmt.Sprintf("type=%s", providerOpts.PDVolumeType),
				fmt.Sprintf("size=%dGB", providerOpts.PDVolumeSize),
				"auto-delete=yes",
			}
			// TODO(pavelkalinnikov): support disk types with "provisioned-throughput"
			// option, such as Hyperdisk Throughput:
			// https://cloud.google.com/compute/docs/disks/add-hyperdisk#hyperdisk-throughput.
			args = append(args, "--create-disk", strings.Join(pdProps, ","))
		}
		// Enable DISCARD commands for persistent disks, as is advised in:
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#formatting_parameters.
		extraMountOpts = "discard"
	}

	// Create GCE startup script file.
	filename, err := writeStartupScript(
		extraMountOpts, opts.SSDOpts.FileSystem,
		providerOpts.UseMultipleDisks, opts.Arch == string(vm.ArchFIPS),
		providerOpts.EnableCron,
	)
	if err != nil {
		return nil, cleanUpFn, errors.Wrapf(err, "could not write GCE startup script to temp file")
	}
	cleanUpFn = func() {
		_ = os.Remove(filename)
	}

	args = append(args, "--machine-type", providerOpts.MachineType)
	if providerOpts.MinCPUPlatform != "" {
		if strings.HasPrefix(providerOpts.MachineType, "n2d-") && strings.HasPrefix(providerOpts.MinCPUPlatform, "Intel") {
			l.Printf("WARNING: MinCPUPlatform=%q is not supported for MachineType=%q, falling back to AMD Milan", providerOpts.MinCPUPlatform, providerOpts.MachineType)
			providerOpts.MinCPUPlatform = "AMD Milan"
		}
		args = append(args, "--min-cpu-platform", providerOpts.MinCPUPlatform)
	}

	args = append(args, "--metadata-from-file", fmt.Sprintf("startup-script=%s", filename))
	args = append(args, "--project", project)
	args = append(args, fmt.Sprintf("--boot-disk-size=%dGB", opts.OsVolumeSize))
	return args, cleanUpFn, nil
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

// createInstanceTemplates creates instance templates for a cluster for each
// zone with the specified instance args for each template. This is currently
// only used for managed instance group clusters.
func createInstanceTemplates(
	l *logger.Logger, clusterName string, zoneToInstanceArgs map[string][]string, labelsArg string,
) (map[string]jsonInstanceTemplate, error) {
	zonesInstanceTemplates := make(map[string]jsonInstanceTemplate)
	var instanceTemplatesMu syncutil.Mutex
	g := ui.NewDefaultSpinnerGroup(l, "creating instance templates", len(zoneToInstanceArgs))
	for zone, args := range zoneToInstanceArgs {
		templateName := instanceTemplateName(clusterName, zone)
		createTemplateArgs := []string{"compute", "instance-templates", "create"}
		createTemplateArgs = append(createTemplateArgs, args...)
		createTemplateArgs = append(createTemplateArgs, "--labels", labelsArg)
		createTemplateArgs = append(createTemplateArgs, templateName)
		createTemplateArgs = append(createTemplateArgs, "--format", "json")
		g.Go(func() error {
			var j []jsonInstanceTemplate
			err := runJSONCommand(createTemplateArgs, &j)
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\n", createTemplateArgs)
			}
			// We expect only one template to be created, gcloud returns a list.
			if len(j) > 0 {
				instanceTemplatesMu.Lock()
				defer instanceTemplatesMu.Unlock()
				zonesInstanceTemplates[zone] = j[0]
			}
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}
	return zonesInstanceTemplates, nil
}

// createInstanceGroups creates an instance group in each zone, for the cluster
func createInstanceGroups(
	l *logger.Logger, project, clusterName string, zones []string, opts vm.CreateOpts,
) error {
	groupName := instanceGroupName(clusterName)
	// Note that we set the IP addresses to be stateful, so that they remain the
	// same when instances are auto-healed, updated, or recreated.
	createGroupArgs := []string{"compute", "instance-groups", "managed", "create",
		"--size", "0",
		"--stateful-external-ip", "enabled,auto-delete=on-permanent-instance-deletion",
		"--stateful-internal-ip", "enabled,auto-delete=on-permanent-instance-deletion",
		"--project", project,
		groupName}

	// Determine the number of stateful disks the instance group should retain. If
	// we don't use a local SSD, we use 2 stateful disks, a boot disk and a
	// persistent disk. If we use a local SSD, we use 1 stateful disk, the boot
	// disk.
	numStatefulDisks := 1
	if !opts.SSDOpts.UseLocalSSD {
		numStatefulDisks = 2
	}
	statefulDiskArgs := make([]string, 0)
	for i := 0; i < numStatefulDisks; i++ {
		statefulDiskArgs = append(
			statefulDiskArgs,
			"--stateful-disk",
			fmt.Sprintf("device-name=persistent-disk-%d,auto-delete=on-permanent-instance-deletion", i),
		)
	}
	createGroupArgs = append(createGroupArgs, statefulDiskArgs...)

	g := ui.NewDefaultSpinnerGroup(l, "creating instance groups", len(zones))
	for _, zone := range zones {
		templateName := instanceTemplateName(clusterName, zone)
		argsWithZone := make([]string, len(createGroupArgs))
		copy(argsWithZone, createGroupArgs)
		argsWithZone = append(argsWithZone, "--zone", zone)
		argsWithZone = append(argsWithZone, "--template", templateName)
		g.Go(func() error {
			cmd := exec.Command("gcloud", argsWithZone...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", argsWithZone, output)
			}
			return nil
		})
	}
	return g.Wait()
}

// waitForGroupStability waits for the instance groups, in the given zones, to become stable.
func waitForGroupStability(l *logger.Logger, project, groupName string, zones []string) error {
	// Wait for group to become stable // zone TBD
	g := ui.NewDefaultSpinnerGroup(l, "waiting for instance groups to become stable", len(zones))
	for _, zone := range zones {
		groupStableArgs := []string{"compute", "instance-groups", "managed", "wait-until", "--stable",
			"--zone", zone,
			"--project", project,
			groupName}
		g.Go(func() error {
			cmd := exec.Command("gcloud", groupStableArgs...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", groupStableArgs, output)
			}
			return nil
		})
	}
	return g.Wait()
}

// Create instantiates the requested VMs on GCE. If the cluster is managed, it
// will create an instance template and instance group, otherwise it will create
// individual instances.
func (p *Provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, vmProviderOpts vm.ProviderOpts,
) (vm.List, error) {
	providerOpts := vmProviderOpts.(*ProviderOpts)
	project := p.GetProject()
	var gcJob bool
	for _, prj := range projectsWithGC {
		if prj == p.GetProject() {
			gcJob = true
			break
		}
	}
	if !gcJob {
		l.Printf("WARNING: --lifetime functionality requires "+
			"`roachprod gc --gce-project=%s` cronjob", project)
	}
	if providerOpts.Managed {
		if err := checkSDKVersion("450.0.0" /* minVersion */, "required by managed instance groups"); err != nil {
			return nil, err
		}
	}

	instanceArgs, cleanUpFn, err := p.computeInstanceArgs(l, opts, providerOpts)
	if cleanUpFn != nil {
		defer cleanUpFn()
	}
	if err != nil {
		return nil, err
	}
	zones, err := computeZones(opts, providerOpts)
	if err != nil {
		return nil, err
	}
	labels, err := computeLabelsArg(opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Work out in which zones VMs should be created.
	nodeZones := vm.ZonePlacement(len(zones), len(names))
	// N.B. when len(zones) > len(names), we don't need to map unused zones
	zoneToHostNames := make(map[string][]string, min(len(zones), len(names)))
	for i, name := range names {
		zone := zones[nodeZones[i]]
		zoneToHostNames[zone] = append(zoneToHostNames[zone], name)
	}
	usedZones := maps.Keys(zoneToHostNames)

	var vmList vm.List
	var vmListMutex syncutil.Mutex
	switch {
	case providerOpts.Managed:
		zoneToInstanceArgs := make(map[string][]string)
		for _, zone := range usedZones {
			zoneToInstanceArgs[zone] = instanceArgs
		}
		// If spot instance are requested for specific zones, set the instance args
		// for those zones to use spot instances.
		if len(providerOpts.ManagedSpotZones) > 0 {
			if providerOpts.UseSpot {
				return nil, errors.Newf("Use either --%[1]s-use-spot or --%[1]s-managed-spot-zones, not both", ProviderName)
			}
			spotProviderOpts := *providerOpts
			spotProviderOpts.UseSpot = true
			spotInstanceArgs, spotCleanUpFn, err := p.computeInstanceArgs(l, opts, &spotProviderOpts)
			if spotCleanUpFn != nil {
				defer spotCleanUpFn()
			}
			if err != nil {
				return nil, err
			}
			for _, zone := range providerOpts.ManagedSpotZones {
				if _, ok := zoneToInstanceArgs[zone]; !ok {
					return nil, errors.Newf("the managed spot zone %q is not in the list of zones for the cluster", zone)
				}
				zoneToInstanceArgs[zone] = spotInstanceArgs
			}
		}

		zonesInstanceTemplates, err := createInstanceTemplates(l, opts.ClusterName, zoneToInstanceArgs, labels)
		if err != nil {
			return nil, err
		}
		err = createInstanceGroups(l, project, opts.ClusterName, usedZones, opts)
		if err != nil {
			return nil, err
		}

		groupName := instanceGroupName(opts.ClusterName)
		createArgs := []string{"compute", "instance-groups", "managed", "create-instance",
			"--project", project,
			groupName}

		g := ui.NewDefaultSpinnerGroup(l, fmt.Sprintf("creating %d managed instances, distributed across [%s]",
			len(names), strings.Join(usedZones, ", ")), len(names))
		for zone, zoneHosts := range zoneToHostNames {
			argsWithZone := append(createArgs[:len(createArgs):len(createArgs)], "--zone", zone)
			for _, host := range zoneHosts {
				argsWithHost := append(argsWithZone[:len(argsWithZone):len(argsWithZone)], []string{"--instance", host}...)
				g.Go(func() error {
					cmd := exec.Command("gcloud", argsWithHost...)
					output, err := cmd.CombinedOutput()
					if err != nil {
						return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", argsWithHost, output)
					}
					return nil
				})
			}
		}
		err = g.Wait()
		if err != nil {
			return nil, err
		}
		err = waitForGroupStability(l, project, groupName, usedZones)
		if err != nil {
			return nil, err
		}

		// Now that the instance-group is stable,
		// fetch the list of instances in the managed instance group.
		vmList, err = getManagedInstanceGroupVMs(
			l, project, groupName, zonesInstanceTemplates, p.publicDomain,
		)
		if err != nil {
			return nil, err
		}

	default:
		var g errgroup.Group
		createArgs := []string{"compute", "instances", "create", "--subnet", "default", "--format", "json"}
		createArgs = append(createArgs, "--labels", labels)
		createArgs = append(createArgs, instanceArgs...)

		l.Printf("Creating %d instances, distributed across [%s]", len(names), strings.Join(usedZones, ", "))
		for zone, zoneHosts := range zoneToHostNames {
			argsWithZone := append(createArgs, "--zone", zone)
			argsWithZone = append(argsWithZone, zoneHosts...)
			g.Go(func() error {
				var instances []jsonVM
				err := runJSONCommand(argsWithZone, &instances)
				if err != nil {
					return errors.Wrapf(err, "Command: gcloud %s", argsWithZone)
				}
				vmListMutex.Lock()
				defer vmListMutex.Unlock()
				for _, i := range instances {
					v := i.toVM(project, p.publicDomain)
					vmList = append(vmList, *v)
				}
				return nil
			})

		}
		err = g.Wait()
		if err != nil {
			return nil, err
		}
	}
	return vmList, propagateDiskLabels(l, project, labels, zoneToHostNames, opts.SSDOpts.UseLocalSSD, providerOpts.PDVolumeCount)
}

// computeGrowDistribution computes the distribution of new nodes across the
// existing instance groups. Groups must be sorted by size from smallest to
// largest before passing to this function. The distribution is computed
// naively, for simplicity, by growing the instance group with the smallest
// size, and then the next smallest, and so on.
func computeGrowDistribution(groups []jsonManagedInstanceGroup, newNodeCount int) []int {
	addCount := make([]int, len(groups))
	curIndex := 0
	for i := 0; i < newNodeCount; i++ {
		nextIndex := (curIndex + 1) % len(groups)
		if groups[curIndex].Size+addCount[curIndex] >
			groups[nextIndex].Size+addCount[nextIndex] {
			curIndex = nextIndex
		} else {
			curIndex = 0
		}
		addCount[curIndex]++
	}
	return addCount
}

// Shrink shrinks the cluster by deleting the given VMs. This is only supported
// for managed instance groups. Currently, nodes should only be deleted from the
// tail of the cluster, due to complexities thar arise when the node names are
// not contiguous.
func (p *Provider) Shrink(l *logger.Logger, vmsToDelete vm.List, clusterName string) error {
	if !isManaged(vmsToDelete) {
		return errors.New("shrinking is only supported for managed instance groups")
	}

	project := vmsToDelete[0].Project
	groupName := instanceGroupName(clusterName)
	vmZones := make(map[string]vm.List)
	for _, cVM := range vmsToDelete {
		vmZones[cVM.Zone] = append(vmZones[cVM.Zone], cVM)
	}

	g := errgroup.Group{}
	for zone, vms := range vmZones {
		instances := vms.Names()
		args := []string{"compute", "instance-groups", "managed", "delete-instances",
			groupName, "--project", project, "--zone", zone, "--instances", strings.Join(instances, ",")}
		g.Go(func() error {
			cmd := exec.Command("gcloud", args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return waitForGroupStability(l, project, groupName, maps.Keys(vmZones))
}

func (p *Provider) Grow(
	l *logger.Logger, vms vm.List, clusterName string, names []string,
) (vm.List, error) {
	if !isManaged(vms) {
		return nil, errors.New("growing is only supported for managed instance groups")
	}

	project := vms[0].Project
	groupName := instanceGroupName(clusterName)
	groups, err := listManagedInstanceGroups(project, groupName)
	if err != nil {
		return nil, err
	}

	newNodeCount := len(names)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Size < groups[j].Size
	})
	addCounts := computeGrowDistribution(groups, newNodeCount)

	zoneToHostNames := make(map[string][]string)
	addedVms := make(map[string]bool)
	var g errgroup.Group
	for idx, group := range groups {
		addCount := addCounts[idx]
		if addCount == 0 {
			continue
		}
		createArgs := []string{"compute", "instance-groups", "managed", "create-instance", "--zone", group.Zone, groupName,
			"--project", project}
		for i := 0; i < addCount; i++ {
			addedVms[names[i]] = true
			argsWithName := append(createArgs[:len(createArgs):len(createArgs)], []string{"--instance", names[i]}...)
			zoneToHostNames[group.Zone] = append(zoneToHostNames[group.Zone], names[i])
			g.Go(func() error {
				cmd := exec.Command("gcloud", argsWithName...)
				output, err := cmd.CombinedOutput()
				if err != nil {
					return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", argsWithName, output)
				}
				return nil
			})
		}
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	err = waitForGroupStability(l, project, groupName, maps.Keys(zoneToHostNames))
	if err != nil {
		return nil, err
	}

	// Fetch instance templates for the cluster to get required information to build VMs list.
	zoneToInstanceTemplates := make(map[string]jsonInstanceTemplate)
	templates, err := listInstanceTemplates(project, clusterName)
	if err != nil {
		return nil, err
	}
	for _, t := range templates {
		zoneToInstanceTemplates[t.getZone()] = t
	}

	// Fetch the list of instances in the managed instance group.
	vmList, err := getManagedInstanceGroupVMs(l, project, groupName, zoneToInstanceTemplates, p.publicDomain)
	if err != nil {
		return nil, err
	}

	// We only want to return the new VMs.
	var addedVmList vm.List
	for _, v := range vmList {
		if _, ok := addedVms[v.Name]; ok {
			addedVmList = append(addedVmList, v)
		}
	}

	var labelsJoined string
	for key, value := range vms[0].Labels {
		if labelsJoined != "" {
			labelsJoined += ","
		}
		labelsJoined += fmt.Sprintf("%s=%s", key, value)
	}
	err = propagateDiskLabels(l, project, labelsJoined, zoneToHostNames, len(vms[0].LocalDisks) != 0,
		len(vms[0].NonBootAttachedVolumes))
	if err != nil {
		return nil, err
	}

	return addedVmList, nil
}

type jsonBackendService struct {
	Name     string `json:"name"`
	Backends []struct {
		Group string `json:"group"`
	} `json:"backends"`
	HealthChecks []string `json:"healthChecks"`
	SelfLink     string   `json:"selfLink"`
}

func listBackendServices(project string) ([]jsonBackendService, error) {
	args := []string{"compute", "backend-services", "list", "--project", project, "--format", "json"}
	var backends []jsonBackendService
	if err := runJSONCommand(args, &backends); err != nil {
		return nil, err
	}
	return backends, nil
}

type jsonForwardingRule struct {
	Name      string `json:"name"`
	IPAddress string `json:"IPAddress"`
	SelfLink  string `json:"selfLink"`
	Target    string `json:"target"`
}

func listForwardingRules(project string) ([]jsonForwardingRule, error) {
	args := []string{"compute", "forwarding-rules", "list", "--project", project, "--format", "json"}
	var rules []jsonForwardingRule
	if err := runJSONCommand(args, &rules); err != nil {
		return nil, err
	}
	return rules, nil
}

type jsonTargetTCPProxy struct {
	Name     string `json:"name"`
	SelfLink string `json:"selfLink"`
	Service  string `json:"service"`
}

func listTargetTCPProxies(project string) ([]jsonTargetTCPProxy, error) {
	args := []string{"compute", "target-tcp-proxies", "list", "--project", project, "--format", "json"}
	var proxies []jsonTargetTCPProxy
	if err := runJSONCommand(args, &proxies); err != nil {
		return nil, err
	}
	return proxies, nil
}

type jsonHealthCheck struct {
	Name     string `json:"name"`
	SelfLink string `json:"selfLink"`
}

func listHealthChecks(project string) ([]jsonHealthCheck, error) {
	args := []string{"compute", "health-checks", "list", "--project", project, "--format", "json"}
	var checks []jsonHealthCheck
	if err := runJSONCommand(args, &checks); err != nil {
		return nil, err
	}
	return checks, nil
}

// deleteLoadBalancerResources deletes all load balancer resources associated
// with a given cluster and project. If a portFilter is specified only the load
// balancer resources associated with the specified port will be deleted. This
// function does not return an error if the resources do not exist. Multiple
// load balancers can be associated with a single cluster, so we need to delete
// all of them. Health checks associated with the cluster are also deleted.
func deleteLoadBalancerResources(project, clusterName, portFilter string) error {
	// List all the components of the load balancer resources tied to the project.
	var g errgroup.Group
	var services []jsonBackendService
	var proxies []jsonTargetTCPProxy
	var rules []jsonForwardingRule
	var healthChecks []jsonHealthCheck
	g.Go(func() (err error) {
		services, err = listBackendServices(project)
		return
	})
	g.Go(func() (err error) {
		proxies, err = listTargetTCPProxies(project)
		return
	})
	g.Go(func() (err error) {
		rules, err = listForwardingRules(project)
		return
	})
	g.Go(func() (err error) {
		healthChecks, err = listHealthChecks(project)
		return
	})
	if err := g.Wait(); err != nil {
		return err
	}

	// Determine if a load balancer resource should be excluded from deletion. The
	// gcloud commands support a filter flag, but since it does client side only
	// filtering it makes more sense for us to filter the resources ourselves.
	shouldExclude := func(name string, expectedResourceType string) bool {
		cluster, resourceType, port, ok := loadBalancerNameParts(name)
		if !ok || cluster != clusterName || resourceType != expectedResourceType {
			return true
		}
		if portFilter != "" && strconv.Itoa(port) != portFilter {
			return true
		}
		return false
	}
	filteredServices := make([]jsonBackendService, 0)
	for _, service := range services {
		if shouldExclude(service.Name, "load-balancer") {
			continue
		}
		filteredServices = append(filteredServices, service)
	}
	filteredProxies := make([]jsonTargetTCPProxy, 0)
	for _, proxy := range proxies {
		if shouldExclude(proxy.Name, "proxy") {
			continue
		}
		filteredProxies = append(filteredProxies, proxy)
	}
	filteredForwardingRules := make([]jsonForwardingRule, 0)
	for _, rule := range rules {
		if shouldExclude(rule.Name, "forwarding-rule") {
			continue
		}
		filteredForwardingRules = append(filteredForwardingRules, rule)
	}
	filteredHealthChecks := make([]jsonHealthCheck, 0)
	for _, healthCheck := range healthChecks {
		if shouldExclude(healthCheck.Name, "health-check") {
			continue
		}
		filteredHealthChecks = append(filteredHealthChecks, healthCheck)
	}

	// Delete all the components of the load balancer. Resources must be deleted
	// in the correct order to avoid dependency errors.
	g = errgroup.Group{}
	for _, rule := range filteredForwardingRules {
		args := []string{"compute", "forwarding-rules", "delete",
			rule.Name,
			"--global",
			"--quiet",
			"--project", project,
		}
		g.Go(func() error {
			cmd := exec.Command("gcloud", args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	g = errgroup.Group{}
	for _, proxy := range filteredProxies {
		args := []string{"compute", "target-tcp-proxies", "delete",
			proxy.Name,
			"--quiet",
			"--project", project,
		}
		g.Go(func() error {
			cmd := exec.Command("gcloud", args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	g = errgroup.Group{}
	for _, service := range filteredServices {
		args := []string{"compute", "backend-services", "delete",
			service.Name,
			"--global",
			"--quiet",
			"--project", project,
		}
		g.Go(func() error {
			cmd := exec.Command("gcloud", args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	g = errgroup.Group{}
	for _, healthCheck := range filteredHealthChecks {
		args := []string{"compute", "health-checks", "delete",
			healthCheck.Name,
			"--quiet",
			"--project", project,
		}
		g.Go(func() error {
			cmd := exec.Command("gcloud", args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})
	}
	return g.Wait()
}

// DeleteLoadBalancer implements the vm.Provider interface.
func (p *Provider) DeleteLoadBalancer(_ *logger.Logger, vms vm.List, port int) error {
	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return err
	}
	return deleteLoadBalancerResources(vms[0].Project, clusterName, strconv.Itoa(port))
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
	if err := checkSDKVersion("450.0.0" /* minVersion */, "required by load balancers"); err != nil {
		return err
	}
	if !isManaged(vms) {
		return errors.New("load balancer creation is only supported for managed instance groups")
	}
	project := vms[0].Project
	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return err
	}
	groups, err := listManagedInstanceGroups(project, instanceGroupName(clusterName))
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		return errors.Errorf("no managed instance groups found for cluster %s", clusterName)
	}

	var args []string
	healthCheckName := loadBalancerResourceName(clusterName, port, "health-check")
	output, err := func() ([]byte, error) {
		defer ui.NewDefaultSpinner(l, "create health check").Start()()
		args = []string{"compute", "health-checks", "create", "tcp",
			healthCheckName,
			"--project", project,
			"--port", strconv.Itoa(port),
		}
		cmd := exec.Command("gcloud", args...)
		return cmd.CombinedOutput()
	}()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	loadBalancerName := loadBalancerResourceName(clusterName, port, "load-balancer")
	args = []string{"compute", "backend-services", "create", loadBalancerName,
		"--project", project,
		"--load-balancing-scheme", "EXTERNAL_MANAGED",
		"--global-health-checks",
		"--global",
		"--protocol", "TCP",
		"--health-checks", healthCheckName,
		"--timeout", "5m",
		"--port-name", "cockroach",
	}
	output, err = func() ([]byte, error) {
		defer ui.NewDefaultSpinner(l, "creating load balancer backend").Start()()
		cmd := exec.Command("gcloud", args...)
		return cmd.CombinedOutput()
	}()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	// Add the instance group to the backend service. This has to be done
	// sequentially, and for each zone, because gcloud does not allow adding
	// multiple instance groups in parallel.
	output, err = func() ([]byte, error) {
		spinner := ui.NewDefaultCountingSpinner(l, "adding backends to load balancer", len(groups))
		defer spinner.Start()()
		for n, group := range groups {
			args = []string{"compute", "backend-services", "add-backend", loadBalancerName,
				"--project", project,
				"--global",
				"--instance-group", group.Name,
				"--instance-group-zone", group.Zone,
				"--balancing-mode", "UTILIZATION",
				"--max-utilization", "0.8",
			}
			cmd := exec.Command("gcloud", args...)
			output, err = cmd.CombinedOutput()
			if err != nil {
				return output, err
			}
			spinner.CountStatus(n + 1)
		}
		return nil, nil
	}()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	proxyName := loadBalancerResourceName(clusterName, port, "proxy")
	output, err = func() ([]byte, error) {
		defer ui.NewDefaultSpinner(l, "creating load balancer proxy").Start()()
		args = []string{"compute", "target-tcp-proxies", "create", proxyName,
			"--project", project,
			"--backend-service", loadBalancerName,
			"--proxy-header", "NONE",
		}
		cmd := exec.Command("gcloud", args...)
		return cmd.CombinedOutput()
	}()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	output, err = func() ([]byte, error) {
		defer ui.NewDefaultSpinner(l, "creating load balancer forwarding rule").Start()()
		args = []string{"compute", "forwarding-rules", "create",
			loadBalancerResourceName(clusterName, port, "forwarding-rule"),
			"--project", project,
			"--global",
			"--target-tcp-proxy", proxyName,
			"--ports", strconv.Itoa(port),
		}
		cmd := exec.Command("gcloud", args...)
		return cmd.CombinedOutput()
	}()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	// Named ports can be set in parallel for all instance groups.
	g := ui.NewDefaultSpinnerGroup(l, "setting named ports on instance groups", len(groups))
	for _, group := range groups {
		groupArgs := []string{"compute", "instance-groups", "set-named-ports", group.Name,
			"--project", project,
			"--zone", group.Zone,
			"--named-ports", "cockroach:" + strconv.Itoa(port),
		}
		g.Go(func() error {
			cmd := exec.Command("gcloud", groupArgs...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", groupArgs, output)
			}
			return nil
		})
	}
	return g.Wait()
}

// ListLoadBalancers returns the list of load balancers associated with the
// given VMs. The VMs have to be part of a managed instance group. The load
// balancers are returned as a list of service addresses.
func (p *Provider) ListLoadBalancers(_ *logger.Logger, vms vm.List) ([]vm.ServiceAddress, error) {
	// Only managed instance groups support load balancers.
	if !isManaged(vms) {
		return nil, nil
	}
	project := vms[0].Project
	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return nil, err
	}
	rules, err := listForwardingRules(project)
	if err != nil {
		return nil, err
	}

	addresses := make([]vm.ServiceAddress, 0)
	for _, rule := range rules {
		cluster, resourceType, port, ok := loadBalancerNameParts(rule.Name)
		if !ok {
			continue
		}
		if cluster == clusterName && resourceType == "forwarding-rule" {
			addresses = append(addresses, vm.ServiceAddress{IP: rule.IPAddress, Port: port})
		}
	}
	return addresses, nil
}

// Given a machine type, return the allowed number (> 0) of local SSDs, sorted in ascending order.
// N.B. Only n1, n2, n2d and c2 instances are supported since we don't typically use other instance types.
// Consult https://cloud.google.com/compute/docs/disks/#local_ssd_machine_type_restrictions for other types of instances.
func AllowedLocalSSDCount(machineType string) ([]int, error) {
	// E.g., n2-standard-4, n2-custom-8-16384.
	machineTypes := regexp.MustCompile(`^([cn])(\d+)(?:d)?-[a-z]+-(\d+)(?:-\d+)?$`)
	matches := machineTypes.FindStringSubmatch(machineType)

	if len(matches) >= 3 {
		family := matches[1] + matches[2]
		numCpus, err := strconv.Atoi(matches[3])
		if err != nil {
			return nil, err
		}
		if family == "n1" {
			return []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, nil
		}
		switch family {
		case "n2":
			if numCpus <= 10 {
				return []int{1, 2, 4, 8, 16, 24}, nil
			}
			if numCpus <= 20 {
				return []int{2, 4, 8, 16, 24}, nil
			}
			if numCpus <= 40 {
				return []int{4, 8, 16, 24}, nil
			}
			if numCpus <= 80 {
				return []int{8, 16, 24}, nil
			}
			if numCpus <= 128 {
				return []int{16, 24}, nil
			}
		case "c2":
			if numCpus <= 8 {
				return []int{1, 2, 4, 8}, nil
			}
			if numCpus <= 16 {
				return []int{2, 4, 8}, nil
			}
			if numCpus <= 30 {
				return []int{4, 8}, nil
			}
			if numCpus <= 60 {
				return []int{8}, nil
			}
		}
	}
	return nil, fmt.Errorf("unsupported machine type: %q, matches: %v", machineType, matches)
}

// N.B. neither boot disk nor additional persistent disks are assigned VM labels by default.
// Hence, we must propagate them. See: https://cloud.google.com/compute/docs/labeling-resources#labeling_boot_disks
func propagateDiskLabels(
	l *logger.Logger,
	project string,
	labels string,
	zoneToHostNames map[string][]string,
	useLocalSSD bool,
	pdVolumeCount int,
) error {
	var g errgroup.Group

	l.Printf("Propagating labels across all disks")
	argsPrefix := []string{"compute", "disks", "update"}
	argsPrefix = append(argsPrefix, "--update-labels", labels)
	argsPrefix = append(argsPrefix, "--project", project)

	for zone, zoneHosts := range zoneToHostNames {
		zoneArg := []string{"--zone", zone}

		for _, host := range zoneHosts {
			hostName := host

			g.Go(func() error {
				bootDiskArgs := append([]string(nil), argsPrefix...)
				bootDiskArgs = append(bootDiskArgs, zoneArg...)
				// N.B. boot disk has the same name as the host.
				bootDiskArgs = append(bootDiskArgs, hostName)
				cmd := exec.Command("gcloud", bootDiskArgs...)

				output, err := cmd.CombinedOutput()
				if err != nil {
					return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", bootDiskArgs, output)
				}
				return nil
			})

			if !useLocalSSD {
				// The persistent disks are already created. The disks are suffixed with an offset
				// which starts from 1. A total of "pdVolumeCount" disks are created.
				g.Go(func() error {
					// the loop is run inside the go-routine to ensure that we do not run all the gcloud commands.
					// For a 150 node with 4 disks, we have seen that the gcloud command cannot handle so many concurrent
					// commands.
					for offset := 1; offset <= pdVolumeCount; offset++ {
						persistentDiskArgs := append([]string(nil), argsPrefix...)
						persistentDiskArgs = append(persistentDiskArgs, zoneArg...)
						// N.B. additional persistent disks are suffixed with the offset, starting at 1.
						persistentDiskArgs = append(persistentDiskArgs, fmt.Sprintf("%s-%d", hostName, offset))
						cmd := exec.Command("gcloud", persistentDiskArgs...)

						output, err := cmd.CombinedOutput()
						if err != nil {
							return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", persistentDiskArgs, output)
						}
					}
					return nil
				})
			}
		}
	}
	return g.Wait()
}

type jsonInstanceTemplate struct {
	Name       string `json:"name"`
	Properties struct {
		Disks             []jsonInstanceTemplateDisk
		Labels            map[string]string `json:"labels"`
		MachineType       string            `json:"machineType"`
		NetworkInterfaces []struct {
			Name    string `json:"name"`
			Network string `json:"network"`
		} `json:"networkInterfaces"`
		Scheduling struct {
			AutomaticRestart  bool   `json:"automaticRestart"`
			OnHostMaintenance string `json:"onHostMaintenance"`
			Preemptible       bool   `json:"preemptible"`
			ProvisioningModel string `json:"provisioningModel"`
		}
	} `json:"properties"`
}

func (t *jsonInstanceTemplate) getZone() string {
	namePrefix := fmt.Sprintf("%s-", instanceTemplateNamePrefix(t.Properties.Labels[vm.TagCluster]))
	return strings.TrimPrefix(t.Name, namePrefix)
}

type jsonInstanceTemplateDisk struct {
	AutoDelete       bool   `json:"autoDelete"`
	Boot             bool   `json:"boot"`
	DeviceName       string `json:"deviceName"`
	Index            int    `json:"index"`
	InitializeParams struct {
		DiskSizeGb  string            `json:"diskSizeGb"`
		DiskType    string            `json:"diskType"`
		Labels      map[string]string `json:"labels"`
		SourceImage string            `json:"sourceImage"`
	} `json:"initializeParams"`
	Mode string `json:"mode"`
	Type string `json:"type"`
}

// toVolume converts a jsonInstanceTemplateDisk to a vm.Volume.
// This function does a job very similar to &attachDiskCmdDisk{}.toVolume(),
// but a lot of information are coming from a different place, which makes it
// hard to unify the two functions.
func (d *jsonInstanceTemplateDisk) toVolume(vmName, zone string) (*vm.Volume, VolumeType, error) {
	// We fail silently here because the disk size is not always set
	// (e.g. in case of scratch disks).
	diskSize, _ := strconv.Atoi(d.InitializeParams.DiskSizeGb)

	// This is a scratch disk.
	if d.InitializeParams.DiskType == "SCRATCH" {
		return &vm.Volume{
			Size:               diskSize,
			ProviderVolumeType: "local-ssd",
		}, VolumeTypeLocalSSD, nil
	}

	volType := VolumeTypePersistent
	if d.Boot {
		volType = VolumeTypeBoot
	}

	diskName := vmName
	if d.Index > 0 {
		diskName = fmt.Sprintf("%s-%d", vmName, d.Index)
	}

	vol := vm.Volume{
		Name: diskName,
		Zone: zone,
		Size: diskSize,
		// Unfortunately, the attachedDisk struct does not return the actual type
		// (standard or ssd) of the persistent disk. We assume `pd_ssd` as those
		// are common and is a worst case scenario for cost computation.
		ProviderVolumeType: "pd-ssd",
		ProviderResourceID: diskName,
	}

	return &vol, volType, nil
}

// listInstanceTemplates returns a list of instance templates for a given
// project.
func listInstanceTemplates(project, clusterFilter string) ([]jsonInstanceTemplate, error) {
	args := []string{"compute", "instance-templates", "list", "--project", project, "--format", "json"}
	var templates []jsonInstanceTemplate
	if err := runJSONCommand(args, &templates); err != nil {
		return nil, err
	}

	if clusterFilter == "" {
		return templates, nil
	}

	var filteredTemplates []jsonInstanceTemplate
	for _, t := range templates {
		if t.Properties.Labels[vm.TagCluster] != clusterFilter {
			continue
		}

		filteredTemplates = append(filteredTemplates, t)
	}
	return filteredTemplates, nil
}

type jsonManagedInstanceGroup struct {
	Name string `json:"name"`
	Zone string `json:"zone"`
	Size int    `json:"size"`
}

// managedInstanceGroupInstance represents the struct gcloud returns
// when listing instances of a managed instance group.
type managedInstanceGroupInstance struct {
	CurrentAction                   string
	Id                              string
	Instance                        string
	InstanceStatus                  string
	Name                            string
	PreservedStateFromConfig        PreservedState
	PreservedStateFromPolicy        PreservedState
	PropertiesFromFlexibilityPolicy struct {
		MachineType string
	}
	Version struct {
		InstanceTemplate string
		Name             string
	}
}
type PreservedState struct {
	Disks       map[string]*PreservedStatePreservedDisk
	ExternalIPs map[string]*PreservedStatePreservedNetworkIp
	InternalIPs map[string]*PreservedStatePreservedNetworkIp
	Metadata    map[string]string
}
type PreservedStatePreservedDisk struct {
	AutoDelete string
	Mode       string
	Source     string
}
type PreservedStatePreservedNetworkIp struct {
	AutoDelete string
	IpAddress  struct {
		Address string
		Literal string
	}
}

// toVM converts a managed instance group instance to a vm.VM struct
// based on data found in both the instance and the instance template.
// TODO(ludo): arch and CPU platform are not available at this time,
// so arch is derived from the VM tags. They should be inferred from
// the machine type instead.
func (j *managedInstanceGroupInstance) toVM(
	project, zone string, instanceTemplate jsonInstanceTemplate, dnsDomain string,
) *vm.VM {

	var err error
	var vmErrors []error

	remoteUser := config.SharedUser
	if !config.UseSharedUser {
		// N.B. gcloud uses the local username to log into instances rather
		// than the username on the authenticated Google account but we set
		// up the shared user at cluster creation time. Allow use of the
		// local username if requested.
		remoteUser = config.OSUser.Username
	}

	// Check "lifetime" label.
	var lifetime time.Duration
	if lifetimeStr, ok := instanceTemplate.Properties.Labels[vm.TagLifetime]; ok {
		if lifetime, err = time.ParseDuration(lifetimeStr); err != nil {
			vmErrors = append(vmErrors, vm.ErrNoExpiration)
		}
	} else {
		vmErrors = append(vmErrors, vm.ErrNoExpiration)
	}

	var arch vm.CPUArch
	var cpuPlatform string
	if instanceTemplate.Properties.Labels[vm.TagArch] != "" {
		arch = vm.CPUArch(instanceTemplate.Properties.Labels[vm.TagArch])
	}

	var volumes []vm.Volume
	var bootVolume vm.Volume
	var localDisks []vm.Volume
	for _, disk := range instanceTemplate.Properties.Disks {
		vol, volType, err := disk.toVolume(j.Name, zone)
		if err != nil {
			vmErrors = append(vmErrors, err)
			continue
		}

		switch volType {
		case VolumeTypeLocalSSD:
			localDisks = append(localDisks, *vol)
		case VolumeTypeBoot:
			bootVolume = *vol
		default:
			volumes = append(volumes, *vol)
		}
	}

	return &vm.VM{
		Name:                   j.Name,
		CreatedAt:              timeutil.Now(),
		DNS:                    fmt.Sprintf("%s.%s.%s", j.Name, zone, project),
		Lifetime:               lifetime,
		Preemptible:            instanceTemplate.Properties.Scheduling.Preemptible,
		Labels:                 instanceTemplate.Properties.Labels,
		PrivateIP:              j.PreservedStateFromPolicy.InternalIPs["nic0"].IpAddress.Literal,
		Provider:               ProviderName,
		DNSProvider:            ProviderName,
		ProviderID:             lastComponent(j.Instance),
		PublicIP:               j.PreservedStateFromPolicy.ExternalIPs["nic0"].IpAddress.Literal,
		PublicDNS:              fmt.Sprintf("%s.%s", j.Name, dnsDomain),
		RemoteUser:             remoteUser,
		VPC:                    lastComponent(instanceTemplate.Properties.NetworkInterfaces[0].Network),
		MachineType:            instanceTemplate.Properties.MachineType,
		Zone:                   zone,
		Project:                project,
		NonBootAttachedVolumes: volumes,
		BootVolume:             bootVolume,
		LocalDisks:             localDisks,
		Errors:                 vmErrors,
		CPUArch:                arch,
		CPUFamily:              cpuPlatform,
	}
}

// listManagedInstanceGroups returns a list of managed instance groups for a
// given group name. Groups may exist in multiple zones with the same name. This
// function returns a list of all groups with the given name.
func listManagedInstanceGroups(project, groupName string) ([]jsonManagedInstanceGroup, error) {
	args := []string{"compute", "instance-groups", "list", "--only-managed",
		"--project", project, "--format", "json", "--filter", fmt.Sprintf("name=%s", groupName)}
	var groups []jsonManagedInstanceGroup
	if err := runJSONCommand(args, &groups); err != nil {
		return nil, err
	}
	return groups, nil
}

// getManagedInstanceGroupVMs returns a list of VMs for a given instance group.
// The instance group may exist in multiple zones with the same name, so the
// function gathers instances across all zones. The instance templates are used
// to determine the zones to query and the extra properties of the VMs.
func getManagedInstanceGroupVMs(
	l *logger.Logger,
	project, groupName string,
	instanceTemplates map[string]jsonInstanceTemplate,
	dnsDomain string,
) (vm.List, error) {

	zones := maps.Keys(instanceTemplates)

	var vmList vm.List
	var vmListMutex syncutil.Mutex
	listArgs := []string{
		"compute", "instance-groups", "managed", "list-instances", groupName,
		"--project", project, "--format", "json",
	}
	g := ui.NewDefaultSpinnerGroup(l,
		fmt.Sprintf("gathering instances created across zones [%s]", strings.Join(zones, ", ")),
		len(zones),
	)
	for _, zone := range zones {
		g.Go(func() error {
			var zoneInstances []managedInstanceGroupInstance
			argsWithZone := append(listArgs, "--zone", zone)
			err := runJSONCommand(argsWithZone, &zoneInstances)
			if err != nil {
				return err
			}
			vmListMutex.Lock()
			defer vmListMutex.Unlock()
			for _, i := range zoneInstances {
				v := i.toVM(project, zone, instanceTemplates[zone], dnsDomain)
				vmList = append(vmList, *v)
			}
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}

	return vmList, nil
}

// deleteInstanceTemplate deletes the instance template for the cluster.
func deleteInstanceTemplate(project, templateName string) error {
	args := []string{"compute", "instance-templates", "delete", "--project", project, "--quiet", templateName}
	cmd := exec.Command("gcloud", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

// isManaged returns true if the cluster is part of a managed instance group.
// This function makes the assumption that a cluster is either completely
// managed or not at all.
func isManaged(vms vm.List) bool {
	return vms[0].Labels[ManagedLabel] == "true"
}

// Delete is part of the vm.Provider interface.
func (p *Provider) Delete(l *logger.Logger, vms vm.List) error {
	switch {
	case isManaged(vms):
		return p.deleteManaged(l, vms)
	default:
		return p.deleteUnmanaged(l, vms)
	}
}

// deleteManaged deletes the managed instance group for the given VMs. It also
// deletes any instance templates that were used to create the managed instance
// group and associated load balancers.
func (p *Provider) deleteManaged(l *logger.Logger, vms vm.List) error {
	clusterProjectMap := make(map[string]string)
	for _, v := range vms {
		clusterName, err := v.ClusterName()
		if err != nil {
			return err
		}
		clusterProjectMap[clusterName] = v.Project
	}

	var g errgroup.Group
	for cluster, project := range clusterProjectMap {
		// Delete any load balancer resources associated with the cluster. Trying to
		// delete the instance group before the load balancer resources will result
		// in an error.
		err := deleteLoadBalancerResources(project, cluster, "" /* portFilter */)
		if err != nil {
			return err
		}
		// Multiple instance groups can exist for a single cluster, one for each zone.
		projectGroups, err := listManagedInstanceGroups(project, instanceGroupName(cluster))
		if err != nil {
			return err
		}
		for _, group := range projectGroups {
			argsWithZone := []string{"compute", "instance-groups", "managed", "delete", "--quiet",
				"--project", project,
				"--zone", group.Zone,
				group.Name}
			g.Go(func() error {
				cmd := exec.Command("gcloud", argsWithZone...)
				output, err := cmd.CombinedOutput()
				if err != nil {
					return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", argsWithZone, output)
				}
				return nil
			})
		}
	}
	err := g.Wait()
	if err != nil {
		return err
	}

	// All instance groups have to be deleted before the instance templates can be
	// deleted.
	g = errgroup.Group{}
	for cluster, project := range clusterProjectMap {
		templates, err := listInstanceTemplates(project, cluster)
		if err != nil {
			return err
		}
		for _, template := range templates {
			g.Go(func() error {
				return deleteInstanceTemplate(project, template.Name)
			})
		}
	}
	return g.Wait()
}

// deleteUnmanaged deletes the given VMs that are not part of a managed instance group.
func (p *Provider) deleteUnmanaged(l *logger.Logger, vms vm.List) error {
	// Map from project to map of zone to list of machines in that project/zone.
	projectZoneMap := make(map[string]map[string][]string)
	for _, v := range vms {
		if projectZoneMap[v.Project] == nil {
			projectZoneMap[v.Project] = make(map[string][]string)
		}

		projectZoneMap[v.Project][v.Zone] = append(projectZoneMap[v.Project][v.Zone], v.Name)
	}

	var g errgroup.Group
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for project, zoneMap := range projectZoneMap {
		for zone, names := range zoneMap {
			args := []string{
				"compute", "instances", "delete",
				"--delete-disks", "all",
			}

			args = append(args, "--project", project)
			args = append(args, "--zone", zone)
			args = append(args, names...)

			g.Go(func() error {
				cmd := exec.CommandContext(ctx, "gcloud", args...)

				output, err := cmd.CombinedOutput()
				if err != nil {
					return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
				}
				return nil
			})
		}
	}

	return g.Wait()
}

// Reset implements the vm.Provider interface.
func (p *Provider) Reset(l *logger.Logger, vms vm.List) error {
	// Map from project to map of zone to list of machines in that project/zone.
	projectZoneMap := make(map[string]map[string][]string)
	for _, v := range vms {
		if v.Provider != ProviderName {
			return errors.Errorf("%s received VM instance from %s", ProviderName, v.Provider)
		}
		if projectZoneMap[v.Project] == nil {
			projectZoneMap[v.Project] = make(map[string][]string)
		}

		projectZoneMap[v.Project][v.Zone] = append(projectZoneMap[v.Project][v.Zone], v.Name)
	}

	var g errgroup.Group
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for project, zoneMap := range projectZoneMap {
		for zone, names := range zoneMap {
			args := []string{
				"compute", "instances", "reset",
			}

			args = append(args, "--project", project)
			args = append(args, "--zone", zone)
			args = append(args, names...)

			g.Go(func() error {
				cmd := exec.CommandContext(ctx, "gcloud", args...)

				output, err := cmd.CombinedOutput()
				if err != nil {
					return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
				}
				return nil
			})
		}
	}

	return g.Wait()
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
func (p *Provider) List(l *logger.Logger, opts vm.ListOptions) (vm.List, error) {

	templatesInUse := make(map[string]map[string]struct{})
	var vms vm.List
	for _, prj := range p.GetProjects() {
		args := []string{"compute", "instances", "list", "--project", prj, "--format", "json"}

		// Run the command, extracting the JSON payload
		jsonVMS := make([]jsonVM, 0)
		if err := runJSONCommand(args, &jsonVMS); err != nil {
			return nil, err
		}

		// Find all instance templates that are currently in use.
		for _, jsonVM := range jsonVMS {
			for _, entry := range jsonVM.Metadata.Items {
				if entry.Key == "instance-template" {
					if templatesInUse[prj] == nil {
						templatesInUse[prj] = make(map[string]struct{})
					}
					templateName := entry.Value[strings.LastIndex(entry.Value, "/")+1:]
					templatesInUse[prj][templateName] = struct{}{}
					break
				}
			}
		}

		// Now, convert the json payload into our common VM type
		for _, jsonVM := range jsonVMS {
			vms = append(vms, *jsonVM.toVM(prj, p.publicDomain))
		}
	}

	if opts.IncludeEmptyClusters {
		// Find any instance templates that are not in use and add an Empty
		// Cluster (VM marked as empty) for it. This allows `Delete` to clean up
		// any MIG or instance template resources when there are no VMs to
		// derive it from.
		clusterSeen := make(map[string]struct{})
		for _, prj := range p.GetProjects() {
			projTemplatesInUse := templatesInUse[prj]
			if projTemplatesInUse == nil {
				projTemplatesInUse = make(map[string]struct{})
			}
			templates, err := listInstanceTemplates(prj, "" /* clusterFilter */)
			if err != nil {
				return nil, err
			}
			for _, template := range templates {
				// Skip templates that are not marked as managed.
				if managed, ok := template.Properties.Labels[ManagedLabel]; !(ok && managed == "true") {
					continue
				}
				// There can be multiple dangling templates for the same cluster. We
				// only need to create one `EmptyCluster` VM for each cluster.
				clusterName := template.Properties.Labels[vm.TagCluster]
				if clusterName == "" {
					continue
				}
				if _, ok := clusterSeen[clusterName]; ok {
					continue
				}
				clusterSeen[clusterName] = struct{}{}
				// Create an `EmptyCluster` VM for templates that are not in use.
				if _, ok := projTemplatesInUse[template.Name]; !ok {
					vms = append(vms, vm.VM{
						Name:         vm.Name(clusterName, 0),
						Provider:     ProviderName,
						Project:      prj,
						Labels:       template.Properties.Labels,
						EmptyCluster: true,
					})
				}
			}
		}
	}

	if opts.ComputeEstimatedCost {
		if err := populateCostPerHour(l, vms); err != nil {
			// N.B. We continue despite the error since it doesn't prevent 'List' and other commands which may depend on it.

			l.Errorf("Error during cost estimation (will continue without): %v", err)
			if strings.Contains(err.Error(), "could not find default credentials") {
				l.Printf("To fix this, run `gcloud auth application-default login`")
			}
		}
	}

	return vms, nil
}

// populateCostPerHour adds an approximate cost per hour to each VM in the list,
// using a basic estimation method.
//  1. Compute and attached disks are estimated at the list prices, ignoring
//     all discounts, but including any automatically applied credits.
//  2. Network egress costs are completely ignored.
//  3. Blob storage costs are completely ignored.
func populateCostPerHour(l *logger.Logger, vms vm.List) error {
	// Construct cost estimation service
	ctx := context.Background()
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

// checkSDKVersion checks that the gcloud SDK version is at least minVersion.
// If it is not, it returns an error with the given message.
func checkSDKVersion(minVersion, message string) error {
	var jsonVersion struct {
		GoogleCloudSDK string `json:"Google Cloud SDK"`
	}
	err := runJSONCommand([]string{"version", "--format", "json"}, &jsonVersion)
	if err != nil {
		return err
	}
	v, err := semver.NewVersion(jsonVersion.GoogleCloudSDK)
	if err != nil {
		return errors.Wrapf(err, "invalid gcloud version %q", jsonVersion.GoogleCloudSDK)
	}
	minConstraint, err := semver.NewConstraint(">= " + minVersion)
	if err != nil {
		return err
	}
	if !minConstraint.Check(v) {
		return errors.Errorf("gcloud version %s is below minimum required version %s: %s", v, minVersion, message)
	}
	return nil
}
