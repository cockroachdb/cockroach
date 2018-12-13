// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package gce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/flagstub"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

const (
	defaultProject = "cockroach-ephemeral"
	// ProviderName is gce.
	ProviderName = "gce"
)

// init will inject the GCE provider into vm.Providers, but only if the gcloud tool is available on the local path.
func init() {
	var p vm.Provider = &Provider{}
	if _, err := exec.LookPath("gcloud"); err != nil {
		p = flagstub.New(p, "please install the gcloud CLI utilities "+
			"(https://cloud.google.com/sdk/downloads)")
	}
	vm.Providers[ProviderName] = p
}

func runJSONCommand(args []string, parsed interface{}) error {
	cmd := exec.Command("gcloud", args...)

	rawJSON, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		// TODO(peter): Remove this hack once gcloud is behaving again.
		if matched, _ := regexp.Match(`europe-north.*Unknown zone`, stderr); !matched {
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
func (jsonVM *jsonVM) toVM(project string) *vm.VM {
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

	return &vm.VM{
		Name:       jsonVM.Name,
		CreatedAt:  jsonVM.CreationTimestamp,
		Errors:     vmErrors,
		DNS:        fmt.Sprintf("%s.%s.%s", jsonVM.Name, zone, project),
		Lifetime:   lifetime,
		PrivateIP:  privateIP,
		Provider:   ProviderName,
		ProviderID: jsonVM.Name,
		PublicIP:   publicIP,
		// N.B. gcloud uses the local username to log into instances rather
		// than the username on the authenticated Google account.
		RemoteUser:  config.OSUser.Username,
		VPC:         vpc,
		MachineType: machineType,
		Zone:        zone,
	}
}

type jsonAuth struct {
	Account string
	Status  string
}

// User-configurable, provider-specific options
type providerOpts struct {
	Project        string
	ServiceAccount string
	MachineType    string
	Zones          []string
}

func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.MachineType, "machine-type", "n1-standard-4", "DEPRECATED")
	_ = flags.MarkDeprecated("machine-type", "use "+ProviderName+"-machine-type instead")
	flags.StringSliceVar(&o.Zones, "zones", []string{"us-east1-b", "us-west1-b", "europe-west2-b"}, "DEPRECATED")
	_ = flags.MarkDeprecated("zones", "use "+ProviderName+"-zones instead")

	flags.StringVar(&o.ServiceAccount, ProviderName+"-service-account",
		os.Getenv("GCE_SERVICE_ACCOUNT"), "Service account to use")
	flags.StringVar(&o.MachineType, ProviderName+"-machine-type", "n1-standard-4",
		"Machine type (see https://cloud.google.com/compute/docs/machine-types)")
	flags.StringSliceVar(&o.Zones, ProviderName+"-zones",
		[]string{"us-east1-b", "us-west1-b", "europe-west2-b"}, "Zones for cluster")
}

func (o *providerOpts) ConfigureClusterFlags(flags *pflag.FlagSet) {
	project := os.Getenv("GCE_PROJECT")
	if project == "" {
		project = defaultProject
	}
	flags.StringVar(&o.Project, ProviderName+"-project", project,
		"Project to create cluster in")
}

// Provider TODO(peter): document
type Provider struct {
	opts providerOpts
}

// CleanSSH TODO(peter): document
func (p *Provider) CleanSSH() error {
	args := []string{"compute", "config-ssh", "--project", p.opts.Project, "--quiet", "--remove"}
	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

// ConfigSSH TODO(peter): document
func (p *Provider) ConfigSSH() error {
	args := []string{"compute", "config-ssh", "--project", p.opts.Project, "--quiet"}
	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

// Create TODO(peter): document
func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	if p.opts.Project != defaultProject {
		fmt.Printf("WARNING: --lifetime functionality requires "+
			"`roachprod gc --gce-project=%s` cronjob\n", p.opts.Project)
	}

	if !opts.GeoDistributed {
		p.opts.Zones = []string{p.opts.Zones[0]}
	}

	totalNodes := float64(len(names))
	totalZones := float64(len(p.opts.Zones))
	nodesPerZone := int(math.Ceil(totalNodes / totalZones))

	ct := int(0)
	i := 0

	// Fixed args.
	args := []string{
		"compute", "instances", "create",
		"--subnet", "default",
		"--maintenance-policy", "MIGRATE",
		"--scopes", "default,storage-rw",
		"--image", "ubuntu-1604-xenial-v20181204",
		"--image-project", "ubuntu-os-cloud",
		"--boot-disk-size", "10",
		"--boot-disk-type", "pd-ssd",
	}

	if p.opts.Project == defaultProject && p.opts.ServiceAccount == "" {
		p.opts.ServiceAccount = "21965078311-compute@developer.gserviceaccount.com"
	}
	if p.opts.ServiceAccount != "" {
		args = append(args, "--service-account", p.opts.ServiceAccount)
	}

	extraMountOpts := ""
	// Dynamic args.
	if opts.SSDOpts.UseLocalSSD {
		args = append(args, "--local-ssd", "interface=SCSI")
		if opts.SSDOpts.NoExt4Barrier {
			extraMountOpts = "nobarrier"
		}
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
	args = append(args, "--labels", fmt.Sprintf("lifetime=%s", opts.Lifetime))

	args = append(args, "--metadata-from-file", fmt.Sprintf("startup-script=%s", filename))
	args = append(args, "--project", p.opts.Project)

	var g errgroup.Group

	// This is calculating the number of machines to allocate per zone by taking the ceiling of the the total number
	// of machines left divided by the number of zones left. If the the number of machines isn't
	// divisible by the number of zones, then the extra machines will be allocated one per zone until there are
	// no more extra machines left.
	for i < len(names) {
		argsWithZone := append(args[:len(args):len(args)], "--zone", p.opts.Zones[ct])
		ct++
		argsWithZone = append(argsWithZone, names[i:i+nodesPerZone]...)
		i += nodesPerZone

		totalNodes -= float64(nodesPerZone)
		totalZones--
		nodesPerZone = int(math.Ceil(totalNodes / totalZones))

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
	zoneMap := make(map[string][]string)
	for _, v := range vms {
		if v.Provider != ProviderName {
			return errors.Errorf("%s received VM instance from %s", ProviderName, v.Provider)
		}
		zoneMap[v.Zone] = append(zoneMap[v.Zone], v.Name)
	}

	var g errgroup.Group

	for zone, names := range zoneMap {
		args := []string{
			"compute", "instances", "delete",
			"--delete-disks", "all",
		}

		args = append(args, "--project", p.opts.Project)
		args = append(args, "--zone", zone)
		args = append(args, names...)

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

// Extend TODO(peter): document
func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	// The gcloud command only takes a single instance.  Unlike Delete() above, we have to
	// perform the iteration here.
	for _, v := range vms {
		args := []string{"compute", "instances", "add-labels"}

		args = append(args, "--project", p.opts.Project)
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
	args := []string{"compute", "instances", "list", "--project", p.opts.Project, "--format", "json"}

	// Run the command, extracting the JSON payload
	jsonVMS := make([]jsonVM, 0)
	if err := runJSONCommand(args, &jsonVMS); err != nil {
		return nil, err
	}

	// Now, convert the json payload into our common VM type
	vms := make(vm.List, len(jsonVMS))
	for i, jsonVM := range jsonVMS {
		vms[i] = *jsonVM.toVM(p.opts.Project)
	}

	return vms, nil
}

// Name TODO(peter): document
func (p *Provider) Name() string {
	return ProviderName
}
