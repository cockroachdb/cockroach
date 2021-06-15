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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/flagstub"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

const (
	defaultProject = "cockroach-ephemeral"
	// ProviderName is gce.
	ProviderName = "gce"
)

// DefaultProject returns the default GCE project.
func DefaultProject() string {
	return defaultProject
}

// projects for which a cron GC job exists.
var projectsWithGC = []string{defaultProject, "andrei-jepsen"}

// init will inject the GCE provider into vm.Providers, but only if the gcloud tool is available on the local path.
func init() {
	var p vm.Provider = &Provider{}
	if _, err := exec.LookPath("gcloud"); err != nil {
		p = flagstub.New(p, "please install the gcloud CLI utilities "+
			"(https://cloud.google.com/sdk/downloads)")
	} else {
		gceP := makeProvider()
		p = &gceP
	}
	vm.Providers[ProviderName] = p
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
			return errors.Errorf("failed to run: gcloud %s: %s\nstdout: %s\nstderr: %s",
				strings.Join(args, " "), err, bytes.TrimSpace(rawJSON), bytes.TrimSpace(stderr))
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
	MachineType string
	Zone        string
}

// Convert the JSON VM data into our common VM type
func (jsonVM *jsonVM) toVM(project string, opts *providerOpts) (ret *vm.VM) {
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
		PrivateIP:   privateIP,
		Provider:    ProviderName,
		ProviderID:  jsonVM.Name,
		PublicIP:    publicIP,
		RemoteUser:  remoteUser,
		VPC:         vpc,
		MachineType: machineType,
		Zone:        zone,
		Project:     project,
	}
}

type jsonAuth struct {
	Account string
	Status  string
}

// User-configurable, provider-specific options
type providerOpts struct {
	// projects represent the GCE projects to operate on. Accessed through
	// GetProject() or GetProjects() depending on whether the command accepts
	// multiple projects or a single one.
	projects       []string
	ServiceAccount string
	MachineType    string
	MinCPUPlatform string
	Zones          []string
	Image          string
	SSDCount       int
	PDVolumeType   string
	PDVolumeSize   int

	// useSharedUser indicates that the shared user rather than the personal
	// user should be used to ssh into the remote machines.
	useSharedUser bool
}

// projectsVal is the implementation for the --gce-projects flag. It populates
// opts.projects.
type projectsVal struct {
	acceptMultipleProjects bool
	opts                   *providerOpts
}

// defaultZones is the list of  zones used by default for cluster creation.
// If the geo flag is specified, nodes are distributed between zones.
var defaultZones = []string{
	"us-east1-b",
	"us-west1-b",
	"europe-west2-b",
}

// Set is part of the pflag.Value interface.
func (v projectsVal) Set(projects string) error {
	if projects == "" {
		return fmt.Errorf("empty GCE project")
	}
	prj := strings.Split(projects, ",")
	if !v.acceptMultipleProjects && len(prj) > 1 {
		return fmt.Errorf("multiple GCE projects not supported for command")
	}
	v.opts.projects = prj
	return nil
}

// Type is part of the pflag.Value interface.
func (v projectsVal) Type() string {
	if v.acceptMultipleProjects {
		return "comma-separated list of GCE projects"
	}
	return "GCE project name"
}

// String is part of the pflag.Value interface.
func (v projectsVal) String() string {
	return strings.Join(v.opts.projects, ",")
}

func makeProviderOpts() providerOpts {
	project := os.Getenv("GCE_PROJECT")
	if project == "" {
		project = defaultProject
	}
	return providerOpts{
		// projects needs space for one project, which is set by the flags for
		// commands that accept a single project.
		projects: []string{project},
	}
}

// GetProject returns the GCE project on which we're configured to operate.
// If multiple projects were configured, this panics.
func (p *Provider) GetProject() string {
	o := p.opts
	if len(o.projects) > 1 {
		panic(fmt.Sprintf(
			"multiple projects not supported (%d specified)", len(o.projects)))
	}
	return o.projects[0]
}

// GetProjects returns the list of GCE projects on which we're configured to
// operate.
func (p *Provider) GetProjects() []string {
	return p.opts.projects
}

func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.MachineType, "machine-type", "n1-standard-4", "DEPRECATED")
	_ = flags.MarkDeprecated("machine-type", "use "+ProviderName+"-machine-type instead")
	flags.StringSliceVar(&o.Zones, "zones", nil, "DEPRECATED")
	_ = flags.MarkDeprecated("zones", "use "+ProviderName+"-zones instead")

	flags.StringVar(&o.ServiceAccount, ProviderName+"-service-account",
		os.Getenv("GCE_SERVICE_ACCOUNT"), "Service account to use")

	flags.StringVar(&o.MachineType, ProviderName+"-machine-type", "n1-standard-4",
		"Machine type (see https://cloud.google.com/compute/docs/machine-types)")
	flags.StringVar(&o.MinCPUPlatform, ProviderName+"-min-cpu-platform", "",
		"Minimum CPU platform (see https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform)")
	flags.StringVar(&o.Image, ProviderName+"-image", "ubuntu-2004-focal-v20210603",
		"Image to use to create the vm, "+
			"use `gcloud compute images list --filter=\"family=ubuntu-2004-lts\"` to list available images")

	flags.IntVar(&o.SSDCount, ProviderName+"-local-ssd-count", 1,
		"Number of local SSDs to create, only used if local-ssd=true")
	flags.StringVar(&o.PDVolumeType, ProviderName+"-pd-volume-type", "pd-ssd",
		"Type of the persistent disk volume, only used if local-ssd=false")
	flags.IntVar(&o.PDVolumeSize, ProviderName+"-pd-volume-size", 500,
		"Size in GB of persistent disk volume, only used if local-ssd=false")

	flags.StringSliceVar(&o.Zones, ProviderName+"-zones", nil,
		fmt.Sprintf("Zones for cluster. If zones are formatted as AZ:N where N is an integer, the zone\n"+
			"will be repeated N times. If > 1 zone specified, nodes will be geo-distributed\n"+
			"regardless of geo (default [%s])",
			strings.Join(defaultZones, ",")))
}

func (o *providerOpts) ConfigureClusterFlags(flags *pflag.FlagSet, opt vm.MultipleProjectsOption) {
	var usage string
	if opt == vm.SingleProject {
		usage = "GCE project to manage"
	} else {
		usage = "List of GCE projects to manage"
	}

	flags.Var(
		projectsVal{
			acceptMultipleProjects: opt == vm.AcceptMultipleProjects,
			opts:                   o,
		},
		ProviderName+"-project", /* name */
		usage)

	flags.BoolVar(&o.useSharedUser,
		ProviderName+"-use-shared-user", true,
		fmt.Sprintf("use the shared user %q for ssh rather than your user %q",
			config.SharedUser, config.OSUser.Username))
}

// Provider is the GCE implementation of the vm.Provider interface.
type Provider struct {
	opts providerOpts
}

func makeProvider() Provider {
	return Provider{opts: makeProviderOpts()}
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

// ConfigSSH TODO(peter): document
func (p *Provider) ConfigSSH() error {
	for _, prj := range p.GetProjects() {
		args := []string{"compute", "config-ssh", "--project", prj, "--quiet"}
		cmd := exec.Command("gcloud", args...)

		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
		}
	}
	return nil
}

// Create TODO(peter): document
func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	project := p.GetProject()
	var gcJob bool
	for _, prj := range projectsWithGC {
		if prj == p.GetProject() {
			gcJob = true
			break
		}
	}
	if !gcJob {
		fmt.Printf("WARNING: --lifetime functionality requires "+
			"`roachprod gc --gce-project=%s` cronjob\n", project)
	}

	zones, err := vm.ExpandZonesFlag(p.opts.Zones)
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
	args := []string{
		"compute", "instances", "create",
		"--subnet", "default",
		"--maintenance-policy", "MIGRATE",
		"--scopes", "default,storage-rw",
		"--image", p.opts.Image,
		"--image-project", "ubuntu-os-cloud",
		"--boot-disk-type", "pd-ssd",
	}

	if project == defaultProject && p.opts.ServiceAccount == "" {
		p.opts.ServiceAccount = "21965078311-compute@developer.gserviceaccount.com"
	}
	if p.opts.ServiceAccount != "" {
		args = append(args, "--service-account", p.opts.ServiceAccount)
	}

	extraMountOpts := ""
	// Dynamic args.
	if opts.SSDOpts.UseLocalSSD {
		// n2-class and c2-class GCP machines cannot be requested with only 1
		// SSD; minimum number of actual SSDs is 2.
		// TODO(pbardea): This is more general for machine types that
		// come in different sizes.
		// See: https://cloud.google.com/compute/docs/disks/
		n2MachineTypes := regexp.MustCompile("^[cn]2-.+-16")
		if n2MachineTypes.MatchString(p.opts.MachineType) && p.opts.SSDCount == 1 {
			fmt.Fprint(os.Stderr, "WARNING: SSD count must be at least 2 for n2 and c2 machine types with 16vCPU. Setting --gce-local-ssd-count to 2.\n")
			p.opts.SSDCount = 2
		}
		for i := 0; i < p.opts.SSDCount; i++ {
			args = append(args, "--local-ssd", "interface=NVME")
		}
		if opts.SSDOpts.NoExt4Barrier {
			extraMountOpts = "nobarrier"
		}
	} else {
		pdProps := []string{
			fmt.Sprintf("type=%s", p.opts.PDVolumeType),
			fmt.Sprintf("size=%dGB", p.opts.PDVolumeSize),
			"auto-delete=yes",
		}
		args = append(args, "--create-disk", strings.Join(pdProps, ","))
		// Enable DISCARD commands for persistent disks, as is advised in:
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#formatting_parameters.
		extraMountOpts = "discard"
	}

	// Create GCE startup script file.
	filename, err := writeStartupScript(extraMountOpts)
	if err != nil {
		return errors.Wrapf(err, "could not write GCE startup script to temp file")
	}
	defer func() {
		_ = os.Remove(filename)
	}()

	args = append(args, "--machine-type", p.opts.MachineType)
	if p.opts.MinCPUPlatform != "" {
		args = append(args, "--min-cpu-platform", p.opts.MinCPUPlatform)
	}
	args = append(args, "--labels", fmt.Sprintf("lifetime=%s", opts.Lifetime))

	args = append(args, "--metadata-from-file", fmt.Sprintf("startup-script=%s", filename))
	args = append(args, "--project", project)
	args = append(args, fmt.Sprintf("--boot-disk-size=%dGB", opts.OsVolumeSize))
	var g errgroup.Group

	nodeZones := vm.ZonePlacement(len(zones), len(names))
	zoneHostNames := make([][]string, len(zones))
	for i, name := range names {
		zone := nodeZones[i]
		zoneHostNames[zone] = append(zoneHostNames[zone], name)
	}
	for i, zoneHosts := range zoneHostNames {
		argsWithZone := append(args[:len(args):len(args)], "--zone", zones[i])
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

	return g.Wait()
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
	// The gcloud command only takes a single instance.  Unlike Delete() above, we have to
	// perform the iteration here.
	for _, v := range vms {
		args := []string{"compute", "instances", "add-labels"}

		args = append(args, "--project", v.Project)
		args = append(args, "--zone", v.Zone)
		args = append(args, "--labels", fmt.Sprintf("lifetime=%s", lifetime))
		args = append(args, v.Name)

		cmd := exec.Command("gcloud", args...)

		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
		}
	}
	return nil
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
		return "", fmt.Errorf("active account %q does no belong to domain %s",
			accounts[0].Account, config.EmailDomain)
	}
	_ = accounts[0].Status // silence unused warning

	username := strings.Split(accounts[0].Account, "@")[0]
	return username, nil
}

// Flags TODO(peter): document
func (p *Provider) Flags() vm.ProviderFlags {
	return &p.opts
}

// List queries gcloud to produce a list of VM info objects.
func (p *Provider) List() (vm.List, error) {
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
			vms = append(vms, *jsonVM.toVM(prj, &p.opts))
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
