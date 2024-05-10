// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	cloudbilling "google.golang.org/api/cloudbilling/v1beta"
)

const (
	defaultProject = "cockroach-ephemeral"
	ProviderName   = "gce"
	DefaultImage   = "ubuntu-2204-jammy-v20240319"
	ARM64Image     = "ubuntu-2204-jammy-arm64-v20240319"
	// TODO(DarrylWong): Upgrade FIPS to Ubuntu 22 when it is available.
	FIPSImage           = "ubuntu-pro-fips-2004-focal-v20230811"
	defaultImageProject = "ubuntu-os-cloud"
	FIPSImageProject    = "ubuntu-os-pro-cloud"
	ManagedLabel        = "managed"
)

// providerInstance is the instance to be registered into vm.Providers by Init.
var providerInstance = &Provider{}

// DefaultProject returns the default GCE project.
func DefaultProject() string {
	return defaultProject
}

// projects for which a cron GC job exists.
var projectsWithGC = []string{defaultProject}

// Denotes if this provider was successfully initialized.
var initialized = false

// Init registers the GCE provider into vm.Providers.
//
// If the gcloud tool is not available on the local path, the provider is a
// stub.
func Init() error {
	providerInstance.Projects = []string{defaultProject}
	projectFromEnv := os.Getenv("GCE_PROJECT")
	if projectFromEnv != "" {
		providerInstance.Projects = []string{projectFromEnv}
	}
	providerInstance.ServiceAccount = os.Getenv("GCE_SERVICE_ACCOUNT")
	if _, err := exec.LookPath("gcloud"); err != nil {
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, "please install the gcloud CLI utilities "+
			"(https://cloud.google.com/sdk/downloads)")
		return errors.New("gcloud not found")
	}
	providerInstance.DNSProvider = NewDNSProvider()
	initialized = true
	vm.Providers[ProviderName] = providerInstance
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

// Convert the JSON VM data into our common VM type
func (jsonVM *jsonVM) toVM(
	project string, disks []describeVolumeCommandResponse, opts *ProviderOpts,
) (ret *vm.VM) {
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
	if !opts.useSharedUser {
		// N.B. gcloud uses the local username to log into instances rather
		// than the username on the authenticated Google account but we set
		// up the shared user at cluster creation time. Allow use of the
		// local username if requested.
		remoteUser = config.OSUser.Username
	}

	var volumes []vm.Volume
	var localDisks []vm.Volume
	var bootVolume vm.Volume

	parseDiskSize := func(size string) int {
		if val, err := strconv.Atoi(size); err == nil {
			return val
		} else {
			vmErrors = append(vmErrors, errors.Newf("invalid disk size: %q", size))
		}
		return 0
	}

	for _, jsonVMDisk := range jsonVM.Disks {
		if jsonVMDisk.Source == "" && jsonVMDisk.Type == "SCRATCH" {
			// This is a scratch disk.
			localDisks = append(localDisks, vm.Volume{
				Size:               parseDiskSize(jsonVMDisk.DiskSizeGB),
				ProviderVolumeType: "local-ssd",
			})
			continue
		}
		// Find a persistent volume (detailedDisk) matching the attached non-boot disk.
		for _, detailedDisk := range disks {
			if detailedDisk.SelfLink == jsonVMDisk.Source {
				vol := vm.Volume{
					// NB: See TODO in toDescribeVolumeCommandResponse. We
					// should be able to "just" use detailedDisk.Name here,
					// but we're abusing that field elsewhere, and
					// incorrectly. Using SelfLink is correct.
					ProviderResourceID: lastComponent(detailedDisk.SelfLink),
					ProviderVolumeType: detailedDisk.Type,
					Zone:               lastComponent(detailedDisk.Zone),
					Name:               detailedDisk.Name,
					Labels:             detailedDisk.Labels,
					Size:               parseDiskSize(detailedDisk.SizeGB),
				}
				if !jsonVMDisk.Boot {
					volumes = append(volumes, vol)
				} else {
					bootVolume = vol
				}
			}
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
		PublicDNS:              fmt.Sprintf("%s.%s", jsonVM.Name, Subdomain),
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
		TerminateOnMigration: false,
		UseSpot:              false,
		useSharedUser:        true,
		preemptible:          false,
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
	UseMultipleDisks bool
	// use spot instances (i.e., latest version of preemptibles which can run > 24 hours)
	UseSpot bool
	// Use an instance template and a managed instance group to create VMs. This
	// enables cluster resizing, load balancing, and health monitoring.
	Managed bool
	// Enable the cron service. It is disabled by default.
	EnableCron bool

	// GCE allows two availability policies in case of a maintenance event (see --maintenance-policy via gcloud),
	// 'TERMINATE' or 'MIGRATE'. The default is 'MIGRATE' which we denote by 'TerminateOnMigration == false'.
	TerminateOnMigration bool
	// useSharedUser indicates that the shared user rather than the personal
	// user should be used to ssh into the remote machines.
	useSharedUser bool
	// use preemptible instances
	preemptible bool
}

// Provider is the GCE implementation of the vm.Provider interface.
type Provider struct {
	vm.DNSProvider
	Projects       []string
	ServiceAccount string
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
	CreationSizeBytes  string    `json:"creationSizeBytes"`
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
}

// defaultZones is the list of  zones used by default for cluster creation.
// If the geo flag is specified, nodes are distributed between zones.
// These are GCP zones available according to this page:
// https://cloud.google.com/compute/docs/regions-zones#available
//
// Note that the default zone (the first zone returned by this
// function) is always in the us-east1 region, but we randomize the
// specific zone. This is to avoid "zone exhausted" errors in one
// particular zone, especially during nightly roachtest runs.
func defaultZones() []string {
	zones := []string{"us-east1-b", "us-east1-c", "us-east1-d"}
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
	providerInstance.Projects = prj
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
	return strings.Join(providerInstance.Projects, ",")
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

	flags.StringVar(&providerInstance.ServiceAccount, ProviderName+"-service-account",
		providerInstance.ServiceAccount, "Service account to use")

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
	flags.BoolVar(&o.UseMultipleDisks, ProviderName+"-enable-multiple-stores",
		false, "Enable the use of multiple stores by creating one store directory per disk. "+
			"Default is to raid0 stripe all disks.")

	flags.StringSliceVar(&o.Zones, ProviderName+"-zones", nil,
		fmt.Sprintf("Zones for cluster. If zones are formatted as AZ:N where N is an integer, the zone\n"+
			"will be repeated N times. If > 1 zone specified, nodes will be geo-distributed\n"+
			"regardless of geo (default [%s])",
			strings.Join(defaultZones(), ",")))
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

// ConfigureClusterFlags implements vm.ProviderFlags.
func (o *ProviderOpts) ConfigureClusterFlags(flags *pflag.FlagSet, opt vm.MultipleProjectsOption) {
	var usage string
	if opt == vm.SingleProject {
		usage = "GCE project to manage"
	} else {
		usage = "List of GCE projects to manage"
	}

	flags.Var(
		ProjectsVal{
			AcceptMultipleProjects: opt == vm.AcceptMultipleProjects,
		},
		ProviderName+"-project", /* name */
		usage)

	flags.BoolVar(&o.useSharedUser,
		ProviderName+"-use-shared-user", true,
		fmt.Sprintf("use the shared user %q for ssh rather than your user %q",
			config.SharedUser, config.OSUser.Username))
}

// useArmAMI returns true if the machine type is an arm64 machine type.
func (o *ProviderOpts) useArmAMI() bool {
	return strings.HasPrefix(strings.ToLower(o.MachineType), "t2a-")
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
			tagArgs = append(tagArgs, fmt.Sprintf("%s=%s", key, vm.SanitizeLabel(value)))
		}
	}
	tagArgsString := strings.Join(tagArgs, ",")
	commonArgs := []string{"--project", p.GetProject(), fmt.Sprintf("--labels=%s", tagArgsString)}

	for _, v := range vms {
		vmArgs := make([]string, len(cmdArgs))
		copy(vmArgs, cmdArgs)

		vmArgs = append(vmArgs, v.Name, "--zone", v.Zone)
		vmArgs = append(vmArgs, commonArgs...)
		cmd := exec.Command("gcloud", vmArgs...)
		if b, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", vmArgs, string(b))
		}
	}
	return nil
}

// AddLabels adds the given labels to the given VMs.
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
			zones = defaultZones()
		} else {
			zones = []string{defaultZones()[0]}
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

	if project == defaultProject && p.ServiceAccount == "" {
		p.ServiceAccount = "21965078311-compute@developer.gserviceaccount.com"

	}
	if p.ServiceAccount != "" {
		args = append(args, "--service-account", p.ServiceAccount)
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
		pdProps := []string{
			fmt.Sprintf("type=%s", providerOpts.PDVolumeType),
			fmt.Sprintf("size=%dGB", providerOpts.PDVolumeSize),
			"auto-delete=yes",
		}
		// TODO(pavelkalinnikov): support disk types with "provisioned-throughput"
		// option, such as Hyperdisk Throughput:
		// https://cloud.google.com/compute/docs/disks/add-hyperdisk#hyperdisk-throughput.
		args = append(args, "--create-disk", strings.Join(pdProps, ","))
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

func instanceTemplateName(clusterName string) string {
	return fmt.Sprintf("%s-template", clusterName)
}

func instanceGroupName(clusterName string) string {
	return fmt.Sprintf("%s-group", clusterName)
}

// createInstanceTemplate creates an instance template for the cluster. This is
// currently only used for managed instance group clusters.
func createInstanceTemplate(clusterName string, instanceArgs []string, labelsArg string) error {
	templateName := instanceTemplateName(clusterName)
	createTemplateArgs := []string{"compute", "instance-templates", "create"}
	createTemplateArgs = append(createTemplateArgs, instanceArgs...)
	createTemplateArgs = append(createTemplateArgs, "--labels", labelsArg)
	createTemplateArgs = append(createTemplateArgs, templateName)

	cmd := exec.Command("gcloud", createTemplateArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", createTemplateArgs, output)
	}
	return nil
}

// createInstanceGroups creates an instance group in each zone, for the cluster
func createInstanceGroups(project, clusterName string, zones []string, opts vm.CreateOpts) error {
	groupName := instanceGroupName(clusterName)
	templateName := instanceTemplateName(clusterName)
	// Note that we set the IP addresses to be stateful, so that they remain the
	// same when instances are auto-healed, updated, or recreated.
	createGroupArgs := []string{"compute", "instance-groups", "managed", "create",
		"--template", templateName,
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

	var g errgroup.Group
	for _, zone := range zones {
		argsWithZone := append(createGroupArgs[:len(createGroupArgs):len(createGroupArgs)], "--zone", zone)
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
func waitForGroupStability(project, groupName string, zones []string) error {
	// Wait for group to become stable // zone TBD
	var g errgroup.Group
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
) error {
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
			return err
		}
	}

	instanceArgs, cleanUpFn, err := p.computeInstanceArgs(l, opts, providerOpts)
	if cleanUpFn != nil {
		defer cleanUpFn()
	}
	if err != nil {
		return err
	}
	zones, err := computeZones(opts, providerOpts)
	if err != nil {
		return err
	}
	labels, err := computeLabelsArg(opts, providerOpts)
	if err != nil {
		return err
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

	switch {
	case providerOpts.Managed:
		var g errgroup.Group
		err = createInstanceTemplate(opts.ClusterName, instanceArgs, labels)
		if err != nil {
			return err
		}
		err = createInstanceGroups(project, opts.ClusterName, usedZones, opts)
		if err != nil {
			return err
		}

		groupName := instanceGroupName(opts.ClusterName)
		createArgs := []string{"compute", "instance-groups", "managed", "create-instance",
			"--project", project,
			groupName}

		l.Printf("Creating %d managed instances, distributed across [%s]", len(names), strings.Join(usedZones, ", "))
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
			return err
		}
		err = waitForGroupStability(project, groupName, usedZones)
		if err != nil {
			return err
		}
	default:
		var g errgroup.Group
		createArgs := []string{"compute", "instances", "create", "--subnet", "default"}
		createArgs = append(createArgs, "--labels", labels)
		createArgs = append(createArgs, instanceArgs...)

		l.Printf("Creating %d instances, distributed across [%s]", len(names), strings.Join(usedZones, ", "))
		for zone, zoneHosts := range zoneToHostNames {
			argsWithZone := append(createArgs[:len(createArgs):len(createArgs)], "--zone", zone)
			argsWithZone = append(argsWithZone, zoneHosts...)
			g.Go(func() error {
				cmd := exec.Command("gcloud", argsWithZone...)
				output, err := cmd.CombinedOutput()
				if err != nil {
					return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", argsWithZone, output)
				}
				return nil
			})

		}
		err = g.Wait()
		if err != nil {
			return err
		}
	}
	return propagateDiskLabels(l, project, labels, zoneToHostNames, opts.SSDOpts.UseLocalSSD)
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

func (p *Provider) Grow(l *logger.Logger, vms vm.List, clusterName string, names []string) error {
	project := vms[0].Project
	groupName := instanceGroupName(clusterName)
	groups, err := listManagedInstanceGroups(project, groupName)
	if err != nil {
		return err
	}

	newNodeCount := len(names)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Size < groups[j].Size
	})
	addCounts := computeGrowDistribution(groups, newNodeCount)

	zoneToHostNames := make(map[string][]string)
	var g errgroup.Group
	for idx, group := range groups {
		addCount := addCounts[idx]
		if addCount == 0 {
			continue
		}
		createArgs := []string{"compute", "instance-groups", "managed", "create-instance", "--zone", group.Zone, groupName}
		for i := 0; i < addCount; i++ {
			name := names[0]
			names = names[1:]
			argsWithName := append(createArgs[:len(createArgs):len(createArgs)], []string{"--instance", name}...)
			zoneToHostNames[group.Zone] = append(zoneToHostNames[group.Zone], name)
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
		return err
	}

	err = waitForGroupStability(project, groupName, maps.Keys(zoneToHostNames))
	if err != nil {
		return err
	}

	var labelsJoined string
	for key, value := range vms[0].Labels {
		if labelsJoined != "" {
			labelsJoined += ","
		}
		labelsJoined += fmt.Sprintf("%s=%s", key, value)
	}
	return propagateDiskLabels(l, project, labelsJoined, zoneToHostNames, len(vms[0].LocalDisks) != 0)
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
	// Convenience function to determine if a load balancer resource should be
	// excluded from deletion.
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
	// List all the components of the load balancer resources tied to the cluster.
	services, err := listBackendServices(project)
	if err != nil {
		return err
	}
	filteredServices := make([]jsonBackendService, 0)
	// Find all backend services tied to the managed instance group.
	for _, service := range services {
		if shouldExclude(service.Name, "load-balancer") {
			continue
		}
		for _, backend := range service.Backends {
			if strings.HasSuffix(backend.Group, fmt.Sprintf("instanceGroups/%s", instanceGroupName(clusterName))) {
				filteredServices = append(filteredServices, service)
				break
			}
		}
	}
	proxies, err := listTargetTCPProxies(project)
	if err != nil {
		return err
	}
	filteredProxies := make([]jsonTargetTCPProxy, 0)
	for _, proxy := range proxies {
		if shouldExclude(proxy.Name, "proxy") {
			continue
		}
		for _, service := range filteredServices {
			if proxy.Service == service.SelfLink {
				filteredProxies = append(filteredProxies, proxy)
				break
			}
		}
	}
	rules, err := listForwardingRules(project)
	if err != nil {
		return err
	}
	filteredForwardingRules := make([]jsonForwardingRule, 0)
	for _, rule := range rules {
		for _, proxy := range filteredProxies {
			if rule.Target == proxy.SelfLink {
				filteredForwardingRules = append(filteredForwardingRules, rule)
			}
		}
	}
	healthChecks, err := listHealthChecks(project)
	if err != nil {
		return err
	}
	filteredHealthChecks := make([]jsonHealthCheck, 0)
	for _, healthCheck := range healthChecks {
		if shouldExclude(healthCheck.Name, "health-check") {
			continue
		}
		filteredHealthChecks = append(filteredHealthChecks, healthCheck)
	}

	// Delete all the components of the load balancer.
	var g errgroup.Group
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
	if err = g.Wait(); err != nil {
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
	if err = g.Wait(); err != nil {
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
	if err = g.Wait(); err != nil {
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
func (p *Provider) CreateLoadBalancer(_ *logger.Logger, vms vm.List, port int) error {
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

	healthCheckName := loadBalancerResourceName(clusterName, port, "health-check")
	args := []string{"compute", "health-checks", "create", "tcp",
		healthCheckName,
		"--project", project,
		"--port", strconv.Itoa(port),
	}
	cmd := exec.Command("gcloud", args...)
	output, err := cmd.CombinedOutput()
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
	cmd = exec.Command("gcloud", args...)
	output, err = cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	// Add the instance group to the backend service. This has to be done
	// sequentially, and for each zone, because gcloud does not allow adding
	// multiple instance groups in parallel.
	for _, group := range groups {
		args = []string{"compute", "backend-services", "add-backend", loadBalancerName,
			"--project", project,
			"--global",
			"--instance-group", group.Name,
			"--instance-group-zone", group.Zone,
			"--balancing-mode", "UTILIZATION",
			"--max-utilization", "0.8",
		}
		cmd = exec.Command("gcloud", args...)
		output, err = cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
		}
	}

	proxyName := loadBalancerResourceName(clusterName, port, "proxy")
	args = []string{"compute", "target-tcp-proxies", "create", proxyName,
		"--project", project,
		"--backend-service", loadBalancerName,
		"--proxy-header", "NONE",
	}
	cmd = exec.Command("gcloud", args...)
	output, err = cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	args = []string{"compute", "forwarding-rules", "create",
		loadBalancerResourceName(clusterName, port, "forwarding-rule"),
		"--project", project,
		"--global",
		"--target-tcp-proxy", proxyName,
		"--ports", strconv.Itoa(port),
	}
	cmd = exec.Command("gcloud", args...)
	output, err = cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}

	// Named ports can be set in parallel for all instance groups.
	var g errgroup.Group
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
				g.Go(func() error {
					persistentDiskArgs := append([]string(nil), argsPrefix...)
					persistentDiskArgs = append(persistentDiskArgs, zoneArg...)
					// N.B. additional persistent disks are suffixed with the offset, starting at 1.
					persistentDiskArgs = append(persistentDiskArgs, fmt.Sprintf("%s-1", hostName))
					cmd := exec.Command("gcloud", persistentDiskArgs...)

					output, err := cmd.CombinedOutput()
					if err != nil {
						return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", persistentDiskArgs, output)
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
		Labels map[string]string `json:"labels"`
	} `json:"properties"`
}

// listInstanceTemplates returns a list of instance templates for a given
// project.
func listInstanceTemplates(project string) ([]jsonInstanceTemplate, error) {
	args := []string{"compute", "instance-templates", "list", "--project", project, "--format", "json"}
	var templates []jsonInstanceTemplate
	if err := runJSONCommand(args, &templates); err != nil {
		return nil, err
	}
	return templates, nil
}

type jsonManagedInstanceGroup struct {
	Name string `json:"name"`
	Zone string `json:"zone"`
	Size int    `json:"size"`
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

// deleteInstanceTemplate deletes the instance template for the cluster.
func deleteInstanceTemplate(project, clusterName string) error {
	templateName := instanceTemplateName(clusterName)
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
// group.
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
		cluster, project := cluster, project
		g.Go(func() error {
			return deleteInstanceTemplate(project /* project */, cluster /* cluster */)
		})
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
	if opts.IncludeVolumes {
		l.Printf("WARN: --include-volumes is disabled; attached disks info will be partial")
	}

	templatesInUse := make(map[string]map[string]struct{})
	var vms vm.List
	for _, prj := range p.GetProjects() {
		args := []string{"compute", "instances", "list", "--project", prj, "--format", "json"}

		// Run the command, extracting the JSON payload
		jsonVMS := make([]jsonVM, 0)
		if err := runJSONCommand(args, &jsonVMS); err != nil {
			return nil, err
		}

		userVMToDetailedDisk := make(map[string][]describeVolumeCommandResponse)
		if opts.IncludeVolumes {
			var jsonVMSelfLinks []string
			for _, jsonVM := range jsonVMS {
				jsonVMSelfLinks = append(jsonVMSelfLinks, jsonVM.SelfLink)
			}

			args = []string{
				"compute",
				"disks",
				"list",
				"--project", prj,
				"--format", "json",
				"--filter", "users:(" + strings.Join(jsonVMSelfLinks, ",") + ")",
			}

			var disks []describeVolumeCommandResponse
			if err := runJSONCommand(args, &disks); err != nil {
				return nil, err
			}

			for _, d := range disks {
				for _, u := range d.Users {
					userVMToDetailedDisk[u] = append(userVMToDetailedDisk[u], d)
				}
			}
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
			defaultOpts := p.CreateProviderOpts().(*ProviderOpts)
			disks := userVMToDetailedDisk[jsonVM.SelfLink]

			if len(disks) == 0 {
				// Since `--include-volumes` is disabled, we convert attachDiskCmdDisk to describeVolumeCommandResponse.
				// The former is a subset of the latter. Some information like `Labels` will be missing.
				disks = toDescribeVolumeCommandResponse(jsonVM.Disks, jsonVM.Zone)
			}
			vms = append(vms, *jsonVM.toVM(prj, disks, defaultOpts))
		}
	}

	if opts.IncludeEmptyClusters {
		// Find any instance templates that are not in use and add an Empty
		// Cluster (VM marked as empty) for it. This allows `Delete` to clean up
		// any MIG or instance template resources when there are no VMs to
		// derive it from.
		for _, prj := range p.GetProjects() {
			projTemplatesInUse := templatesInUse[prj]
			if projTemplatesInUse == nil {
				projTemplatesInUse = make(map[string]struct{})
			}
			templates, err := listInstanceTemplates(prj)
			if err != nil {
				return nil, err
			}
			for _, template := range templates {
				// Skip templates that are not marked as managed.
				if managed, ok := template.Properties.Labels[ManagedLabel]; !(ok && managed == "true") {
					continue
				}
				// Create an `EmptyCluster` VM for templates that are not in use.
				if _, ok := projTemplatesInUse[template.Name]; !ok {
					vms = append(vms, vm.VM{
						Name:         template.Name,
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

// Convert attachDiskCmdDisk to describeVolumeCommandResponse and link via SelfLink, Source.
func toDescribeVolumeCommandResponse(
	disks []attachDiskCmdDisk, zone string,
) []describeVolumeCommandResponse {
	res := make([]describeVolumeCommandResponse, 0, len(disks))

	diskType := func(s string) string {
		if s == "PERSISTENT" {
			// Unfortunately, we don't know if it's a pd-ssd or pd-standard. We assume pd_ssd since those are common.
			return "pd-ssd"
		}
		return "unknown"
	}

	for _, d := range disks {
		if d.Source == "" && d.Type == "SCRATCH" {
			// Skip scratch disks.
			continue
		}
		res = append(res, describeVolumeCommandResponse{
			// TODO(irfansharif): Use of the device name here is wrong -- it's
			// ends up being things like "persistent-disk-1" but in other, more
			// correct uses, it's "irfansharif-snapshot-0001-1". In fact, this
			// whole transformation from attachDiskCmdDisk to
			// describeVolumeCommandResponse if funky. Use something like
			// (Provider).ListVolumes instead.
			Name:     d.DeviceName,
			SelfLink: d.Source,
			SizeGB:   d.DiskSizeGB,
			Type:     diskType(d.Type),
			Zone:     zone,
		})
	}
	return res
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
					l.Errorf("Error estimating VM costs (will continue without): %v", err)
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
