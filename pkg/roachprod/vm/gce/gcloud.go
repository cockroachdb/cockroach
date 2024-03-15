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
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

const (
	defaultProject = "cockroach-ephemeral"
	// ProviderName is gce.
	ProviderName        = "gce"
	DefaultImage        = "ubuntu-2004-focal-v20210603"
	ARM64Image          = "ubuntu-2004-focal-arm64-v20230523"
	FIPSImage           = "ubuntu-pro-fips-2004-focal-v20230302"
	defaultImageProject = "ubuntu-os-cloud"
	FIPSImageProject    = "ubuntu-os-pro-cloud"
)

// providerInstance is the instance to be registered into vm.Providers by Init.
var providerInstance = &Provider{}

// DefaultProject returns the default GCE project.
func DefaultProject() string {
	return defaultProject
}

// projects for which a cron GC job exists.
var projectsWithGC = []string{defaultProject}

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
		return errors.Wrapf(err, "failed to parse json %s", rawJSON)
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
	MachineType string
	Zone        string
}

// Convert the JSON VM data into our common VM type
func (jsonVM *jsonVM) toVM(project string, opts *ProviderOpts) (ret *vm.VM) {
	var vmErrors []error
	var err error

	// Check "lifetime" label.
	var lifetime time.Duration
	if lifetimeStr, ok := jsonVM.Labels["lifetime"]; ok {
		if lifetime, err = time.ParseDuration(lifetimeStr); err != nil {
			vmErrors = append(vmErrors, vm.ErrNoExpiration)
		}
	} else {
		vmErrors = append(vmErrors, vm.ErrNoExpiration)
	}

	// lastComponent splits a url path and returns only the last part. This is
	// used because some of the fields in jsonVM are defined using URLs like:
	//  "https://www.googleapis.com/compute/v1/projects/cockroach-shared/zones/us-east1-b/machineTypes/n1-standard-16"
	// We want to strip this down to "n1-standard-16", so we only want the last
	// component.
	lastComponent := func(url string) string {
		s := strings.Split(url, "/")
		return s[len(s)-1]
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
	zone := lastComponent(jsonVM.Zone)
	remoteUser := config.SharedUser
	if !opts.useSharedUser {
		// N.B. gcloud uses the local username to log into instances rather
		// than the username on the authenticated Google account but we set
		// up the shared user at cluster creation time. Allow use of the
		// local username if requested.
		remoteUser = config.OSUser.Username
	}
	return &vm.VM{
		Name:        jsonVM.Name,
		CreatedAt:   jsonVM.CreationTimestamp,
		Errors:      vmErrors,
		DNS:         fmt.Sprintf("%s.%s.%s", jsonVM.Name, zone, project),
		Lifetime:    lifetime,
		Preemptible: jsonVM.Scheduling.Preemptible,
		Labels:      jsonVM.Labels,
		PrivateIP:   privateIP,
		Provider:    ProviderName,
		ProviderID:  jsonVM.Name,
		PublicIP:    publicIP,
		RemoteUser:  remoteUser,
		VPC:         vpc,
		MachineType: machineType,
		Zone:        zone,
		Project:     project,
		SQLPort:     config.DefaultSQLPort,
		AdminUIPort: config.DefaultAdminUIPort,
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
		useSharedUser:        true,
		preemptible:          false,
		useSpot:              false,
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
	// GCE allows two availability policies in case of a maintenance event (see --maintenance-policy via gcloud),
	// 'TERMINATE' or 'MIGRATE'. The default is 'MIGRATE' which we denote by 'TerminateOnMigration == false'.
	TerminateOnMigration bool

	// useSharedUser indicates that the shared user rather than the personal
	// user should be used to ssh into the remote machines.
	useSharedUser bool
	// use preemptible instances
	preemptible bool
	// use spot instances (i.e., latest version of preemptibles which can run > 24 hours)
	useSpot bool
}

// Provider is the GCE implementation of the vm.Provider interface.
type Provider struct {
	Projects       []string
	ServiceAccount string
}

// ProjectsVal is the implementation for the --gce-projects flag. It populates
// (Provider.Projects).
type ProjectsVal struct {
	AcceptMultipleProjects bool
}

// defaultZones is the list of  zones used by default for cluster creation.
// If the geo flag is specified, nodes are distributed between zones.
var defaultZones = []string{
	"us-east1-b",
	"us-west1-b",
	"europe-west2-b",
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
	flags.StringVar(&o.MinCPUPlatform, ProviderName+"-min-cpu-platform", "",
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
			strings.Join(defaultZones, ",")))
	flags.BoolVar(&o.preemptible, ProviderName+"-preemptible", false,
		"use preemptible GCE instances (lifetime cannot exceed 24h)")
	flags.BoolVar(&o.useSpot, ProviderName+"-use-spot", false,
		"use spot GCE instances (like preemptible but lifetime can exceed 24h)")
	flags.BoolVar(&o.TerminateOnMigration, ProviderName+"-terminateOnMigration", false,
		"use 'TERMINATE' maintenance policy (for GCE live migrations)")
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

// CleanSSH TODO(peter): document
func (p *Provider) CleanSSH() error {
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
func (p *Provider) ConfigSSH(zones []string) error {
	_, err := config.SSHPublicKey()
	return err
}

func (p *Provider) editLabels(vms vm.List, labels map[string]string, remove bool) error {
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
func (p *Provider) AddLabels(vms vm.List, labels map[string]string) error {
	return p.editLabels(vms, labels, false /* remove */)
}

func (p *Provider) RemoveLabels(vms vm.List, labels []string) error {
	labelsMap := make(map[string]string, len(labels))
	for _, label := range labels {
		labelsMap[label] = ""
	}
	return p.editLabels(vms, labelsMap, true /* remove */)
}

// Create TODO(peter): document
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

	zones, err := vm.ExpandZonesFlag(providerOpts.Zones)
	if err != nil {
		return err
	}
	if len(zones) == 0 {
		if opts.GeoDistributed {
			zones = defaultZones
		} else {
			zones = []string{defaultZones[0]}
		}
	}

	// Fixed args.
	image := providerOpts.Image
	imageProject := defaultImageProject
	useArmAMI := strings.HasPrefix(strings.ToLower(providerOpts.MachineType), "t2a-")
	if useArmAMI && (opts.Arch != "" && opts.Arch != string(vm.ArchARM64)) {
		return errors.Errorf("machine type %s is arm64, but requested arch is %s", providerOpts.MachineType, opts.Arch)
	}
	if useArmAMI && opts.SSDOpts.UseLocalSSD {
		return errors.New("local SSDs are not supported with T2A instances, use --local-ssd=false")
	}
	if useArmAMI {
		if len(providerOpts.Zones) == 0 {
			zones = []string{"us-central1-a"}
		} else {
			for _, zone := range providerOpts.Zones {
				if !strings.HasPrefix(zone, "us-central1-") {
					return errors.New("T2A instances are not supported outside of us-central1")
				}
			}
		}
	}
	//TODO(srosenberg): remove this once we have a better way to detect ARM64 machines
	if useArmAMI {
		image = ARM64Image
		l.Printf("Using ARM64 AMI: %s for machine type: %s", image, providerOpts.MachineType)
	}
	if opts.Arch == string(vm.ArchFIPS) {
		// NB: if FIPS is enabled, it overrides the image passed via CLI (--gce-image)
		image = FIPSImage
		imageProject = FIPSImageProject
		l.Printf("Using FIPS-enabled AMI: %s for machine type: %s", image, providerOpts.MachineType)
	}
	args := []string{
		"compute", "instances", "create",
		"--subnet", "default",
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
			return errors.New("lifetime cannot be longer than 24 hours for preemptible instances")
		}
		if !providerOpts.TerminateOnMigration {
			return errors.New("preemptible instances require 'TERMINATE' maintenance policy; use --gce-terminateOnMigration")
		}
		args = append(args, "--preemptible")
		// Preemptible instances require the following arguments set explicitly
		args = append(args, "--maintenance-policy", "TERMINATE")
		args = append(args, "--no-restart-on-failure")
	} else if providerOpts.useSpot {
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
			return err
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
		if opts.SSDOpts.NoExt4Barrier {
			extraMountOpts = "nobarrier"
		}
	} else {
		pdProps := []string{
			fmt.Sprintf("type=%s", providerOpts.PDVolumeType),
			fmt.Sprintf("size=%dGB", providerOpts.PDVolumeSize),
			"auto-delete=yes",
		}
		args = append(args, "--create-disk", strings.Join(pdProps, ","))
		// Enable DISCARD commands for persistent disks, as is advised in:
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#formatting_parameters.
		extraMountOpts = "discard"
	}

	// Create GCE startup script file.
	filename, err := writeStartupScript(extraMountOpts, opts.SSDOpts.FileSystem, providerOpts.UseMultipleDisks, opts.Arch == string(vm.ArchFIPS))
	if err != nil {
		return errors.Wrapf(err, "could not write GCE startup script to temp file")
	}
	defer func() {
		_ = os.Remove(filename)
	}()

	args = append(args, "--machine-type", providerOpts.MachineType)
	if providerOpts.MinCPUPlatform != "" {
		if strings.HasPrefix(providerOpts.MachineType, "n2d-") && strings.HasPrefix(providerOpts.MinCPUPlatform, "Intel") {
			l.Printf("WARNING: MinCPUPlatform=%q is not supported for MachineType=%q, falling back to AMD Milan", providerOpts.MinCPUPlatform, providerOpts.MachineType)
			providerOpts.MinCPUPlatform = "AMD Milan"
		}
		args = append(args, "--min-cpu-platform", providerOpts.MinCPUPlatform)
	}

	m := vm.GetDefaultLabelMap(opts)
	// Format according to gce label naming convention requirement.
	time := timeutil.Now().Format(time.RFC3339)
	time = strings.ToLower(strings.ReplaceAll(time, ":", "_"))
	m[vm.TagCreated] = time

	var labelPairs []string
	addLabel := func(key, value string) {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", key, value))
	}

	for key, value := range opts.CustomLabels {
		_, ok := m[strings.ToLower(key)]
		if ok {
			return fmt.Errorf("duplicate label name defined: %s", key)
		}
		addLabel(key, value)
	}
	for key, value := range m {
		addLabel(key, value)
	}
	labels := strings.Join(labelPairs, ",")

	args = append(args, "--labels", labels)
	args = append(args, "--metadata-from-file", fmt.Sprintf("startup-script=%s", filename))
	args = append(args, "--project", project)
	args = append(args, fmt.Sprintf("--boot-disk-size=%dGB", opts.OsVolumeSize))
	var g errgroup.Group

	nodeZones := vm.ZonePlacement(len(zones), len(names))
	// N.B. when len(zones) > len(names), we don't need to map unused zones
	zoneToHostNames := make(map[string][]string, min(len(zones), len(names)))
	for i, name := range names {
		zone := zones[nodeZones[i]]
		zoneToHostNames[zone] = append(zoneToHostNames[zone], name)
	}
	l.Printf("Creating %d instances, distributed across [%s]", len(names), strings.Join(zones, ", "))

	for zone, zoneHosts := range zoneToHostNames {
		argsWithZone := append(args[:len(args):len(args)], "--zone", zone)
		argsWithZone = append(argsWithZone, zoneHosts...)
		g.Go(func() error {
			cmd := exec.Command("gcloud", argsWithZone...)

			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})

	}
	err = g.Wait()
	if err != nil {
		return err
	}

	return propagateDiskLabels(l, project, labels, zoneToHostNames, &opts)
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
	return nil, fmt.Errorf("unsupported machine type: %q", machineType)
}

// N.B. neither boot disk nor additional persistent disks are assigned VM labels by default.
// Hence, we must propagate them. See: https://cloud.google.com/compute/docs/labeling-resources#labeling_boot_disks
func propagateDiskLabels(
	l *logger.Logger,
	project string,
	labels string,
	zoneToHostNames map[string][]string,
	opts *vm.CreateOpts,
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

			if !opts.SSDOpts.UseLocalSSD {
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Delete TODO(peter): document
func (p *Provider) Delete(vms vm.List) error {
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
func (p *Provider) Reset(vms vm.List) error {
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
func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	return p.AddLabels(vms, map[string]string{
		"lifetime": lifetime.String(),
	})
}

// FindActiveAccount TODO(peter): document
func (p *Provider) FindActiveAccount() (string, error) {
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
	var vms vm.List
	for _, prj := range p.GetProjects() {
		args := []string{"compute", "instances", "list", "--project", prj, "--format", "json"}

		// Run the command, extracting the JSON payload
		jsonVMS := make([]jsonVM, 0)
		if err := runJSONCommand(args, &jsonVMS); err != nil {
			return nil, err
		}

		// Now, convert the json payload into our common VM type
		for _, jsonVM := range jsonVMS {
			defaultOpts := p.CreateProviderOpts().(*ProviderOpts)
			vms = append(vms, *jsonVM.toVM(prj, defaultOpts))
		}
	}

	return vms, nil
}

// Name TODO(peter): document
func (p *Provider) Name() string {
	return ProviderName
}

// Active is part of the vm.Provider interface.
func (p *Provider) Active() bool {
	return true
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
