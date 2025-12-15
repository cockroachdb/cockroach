// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

// listWithSDK lists VMs using the GCP SDK.
func (p *Provider) listWithSDK(
	ctx context.Context, l *logger.Logger, opts vm.ListOptions,
) (vm.List, error) {

	templatesInUse := make(map[string]map[string]struct{})
	var vms vm.List
	for _, prj := range p.GetProjects() {
		req := &computepb.AggregatedListInstancesRequest{
			Project:              prj,
			ReturnPartialSuccess: proto.Bool(true),
			Filter:               proto.String("labels." + vm.TagRoachprod + " = true"),
		}

		it := p.computeInstancesClient.AggregatedList(ctx, req)

		for {
			pair, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err != nil {
				return nil, errors.Wrap(err, "error listing instances")
			}
			if pair.Value == nil || len(pair.Value.Instances) == 0 {
				continue
			}

			for _, inst := range pair.Value.Instances {

				// Convert the SDK payload into our common VM type
				vms = append(vms, *(&sdkInstance{inst}).toVM(prj, p.dnsProvider.PublicDomain()))

				// Check if the instance was created from an instance template.
				if inst.GetMetadata() != nil {
					for _, item := range inst.GetMetadata().GetItems() {
						if item.GetKey() == "instance-template" {
							if templatesInUse[prj] == nil {
								templatesInUse[prj] = make(map[string]struct{})
							}
							templateName := item.GetValue()[strings.LastIndex(item.GetValue(), "/")+1:]
							templatesInUse[prj][templateName] = struct{}{}
							break
						}
					}
				}
			}
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
			templates, err := p.listInstanceTemplatesWithSDK(ctx, l, prj, "" /* clusterFilter */)
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
				if _, ok := projTemplatesInUse[template.GetName()]; !ok {
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
		if err := populateCostPerHour(ctx, l, vms); err != nil {
			// N.B. We continue despite the error since it doesn't prevent 'List' and other commands which may depend on it.

			l.Errorf("Error during cost estimation (will continue without): %v", err)
			if strings.Contains(err.Error(), "could not find default credentials") {
				l.Printf("To fix this, run `gcloud auth application-default login`")
			}
		}
	}

	return vms, nil
}

// listInstanceTemplates returns a list of instance templates for a given
// project.
func (p *Provider) listInstanceTemplatesWithSDK(
	ctx context.Context, l *logger.Logger, project, clusterFilter string,
) ([]*computepb.InstanceTemplate, error) {

	req := &computepb.ListInstanceTemplatesRequest{Project: project}
	it := p.computeInstanceTemplatesClient.List(ctx, req)

	templates := make([]*computepb.InstanceTemplate, 0)
	for {
		template, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, err
		}

		if clusterFilter == "" {
			// No filtering.
			templates = append(templates, template)
			continue
		}

		// Skip templates that have no properties or labels.
		if template.GetProperties() == nil || template.GetProperties().GetLabels() == nil {
			continue
		}

		// Check if the cluster label matches the filter.
		if template.GetProperties().GetLabels()[vm.TagCluster] == clusterFilter {
			templates = append(templates, template)
		}
	}
	return templates, nil
}

type sdkInstance struct {
	*computepb.Instance
}

type sdkAttachedDisk struct {
	*computepb.AttachedDisk
}

func (i *sdkInstance) toVM(project, dnsDomain string) *vm.VM {

	var vmErrors []vm.VMError
	var err error

	// Check "lifetime" label.
	var lifetime time.Duration

	if lifetimeStr, ok := i.GetLabels()[vm.TagLifetime]; ok {
		if lifetime, err = time.ParseDuration(lifetimeStr); err != nil {
			vmErrors = append(vmErrors, vm.NewVMError(vm.ErrNoExpiration))
		}
	} else {
		vmErrors = append(vmErrors, vm.NewVMError(vm.ErrNoExpiration))
	}

	// Extract network information
	var publicIP, privateIP, vpc string
	if len(i.GetNetworkInterfaces()) == 0 {
		vmErrors = append(vmErrors, vm.NewVMError(vm.ErrBadNetwork))
	} else {
		privateIP = i.GetNetworkInterfaces()[0].GetNetworkIP()
		if len(i.GetNetworkInterfaces()[0].GetAccessConfigs()) == 0 {
			vmErrors = append(vmErrors, vm.NewVMError(vm.ErrBadNetwork))
		} else {
			_ = i.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetName() // silence unused warning
			publicIP = i.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP()
			vpc = lastComponent(i.GetNetworkInterfaces()[0].GetNetwork())
		}
	}
	if i.GetScheduling().GetOnHostMaintenance() == "" {
		// N.B. 'onHostMaintenance' is always non-empty, hence its absense implies a parsing error
		vmErrors = append(vmErrors, vm.NewVMError(vm.ErrBadScheduling))
	}

	machineType := lastComponent(i.GetMachineType())
	cpuPlatform := i.GetCpuPlatform()
	zone := lastComponent(i.GetZone())
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

	for _, attachedDisk := range i.GetDisks() {

		vol, volType, err := (&sdkAttachedDisk{attachedDisk}).toVolume()
		if err != nil {
			vmErrors = append(vmErrors, vm.NewVMError(err))
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
	// Parse jsonVM.SelfLink to extract the project name.
	// N.B. The self-link contains the name of the GCE project. E.g.,
	// "https://www.googleapis.com/compute/v1/projects/cockroach-workers/zones/us-central1-a/instances/..."
	projectName := ""
	if idx := strings.Index(i.GetSelfLink(), "/projects/"); idx != -1 {
		projectName = i.GetSelfLink()[idx+len("/projects/"):]
		if idx := strings.Index(projectName, "/"); idx != -1 {
			projectName = projectName[:idx]
		}
	}

	creationTimestampStr := i.GetCreationTimestamp()
	creationTimestamp, err := time.Parse(time.RFC3339, creationTimestampStr)
	if err != nil {
		creationTimestamp = timeutil.Now()
		vmErrors = append(
			vmErrors,
			vm.NewVMError(errors.Wrapf(err, "could not parse creation timestamp %q", creationTimestampStr)),
		)
	}

	return &vm.VM{
		Name:                   i.GetName(),
		CreatedAt:              creationTimestamp,
		Errors:                 vmErrors,
		DNS:                    fmt.Sprintf("%s.%s.%s", i.GetName(), zone, project),
		Lifetime:               lifetime,
		Preemptible:            i.GetScheduling().GetPreemptible(),
		Labels:                 i.GetLabels(),
		PrivateIP:              privateIP,
		Provider:               ProviderName,
		DNSProvider:            ProviderName,
		ProviderID:             i.GetName(),
		ProviderAccountID:      projectName,
		PublicIP:               publicIP,
		PublicDNS:              fmt.Sprintf("%s.%s", i.GetName(), dnsDomain),
		PublicDNSZone:          dnsDomain,
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

func (disk *sdkAttachedDisk) toVolume() (*vm.Volume, VolumeType, error) {

	diskSize := int(disk.GetDiskSizeGb())

	// This is a scratch disk.
	if disk.GetSource() == "" && disk.GetType() == "SCRATCH" {
		return &vm.Volume{
			Size:               diskSize,
			ProviderVolumeType: "local-ssd",
		}, VolumeTypeLocalSSD, nil
	}

	volType := VolumeTypePersistent
	if disk.GetBoot() {
		volType = VolumeTypeBoot
	}

	vol := &vm.Volume{
		Name:               lastComponent(disk.GetSource()),
		Zone:               zoneFromSelfLink(disk.GetSource()),
		Size:               diskSize,
		ProviderResourceID: lastComponent(disk.GetSource()),
		// Unfortunately, the attachedDisk struct does not return the actual type
		// (standard or ssd) of the persistent disk. We assume `pd_ssd` as those
		// are common and is a worst case scenario for cost computation.
		ProviderVolumeType: "pd-ssd",
	}

	return vol, volType, nil
}
