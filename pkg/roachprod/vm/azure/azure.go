// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/latest/compute/mgmt/compute"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/network/mgmt/network"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/resources/mgmt/resources"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/resources/mgmt/subscriptions"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// ProviderName is "azure".
	ProviderName = "azure"
	remoteUser   = "ubuntu"
	tagComment   = "comment"
	tagSubnet    = "subnetPrefix"
)

// providerInstance is the instance to be registered into vm.Providers by Init.
var providerInstance = &Provider{}

// Init registers the Azure provider with vm.Providers.
//
// If the Azure CLI utilities are not installed, the provider is a stub.
func Init() error {
	const cliErr = "please install the Azure CLI utilities " +
		"(https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)"
	const authErr = "unable to authenticate; please use `az login` or double check environment variables"

	providerInstance = New()
	providerInstance.OperationTimeout = 10 * time.Minute
	providerInstance.SyncDelete = false

	// If the appropriate environment variables are not set for api access,
	// then the authenticated CLI must be installed.
	if !hasEnvAuth {
		if _, err := exec.LookPath("az"); err != nil {
			vm.Providers[ProviderName] = flagstub.New(&Provider{}, cliErr)
			return err
		}
	}
	if _, err := providerInstance.getAuthToken(); err != nil {
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, authErr)
		return err
	}
	vm.Providers[ProviderName] = providerInstance
	return nil
}

// Provider implements the vm.Provider interface for the Microsoft Azure
// cloud.
type Provider struct {
	// The maximum amount of time for an Azure API operation to take.
	OperationTimeout time.Duration
	// Wait for deletions to finish before returning.
	SyncDelete bool

	mu struct {
		syncutil.Mutex

		authorizer     autorest.Authorizer
		subscriptionId string
		resourceGroups map[string]resources.Group
		subnets        map[string]network.Subnet
		securityGroups map[string]network.SecurityGroup
	}
}

func (p *Provider) SupportsSpotVMs() bool {
	return false
}

func (p *Provider) GetPreemptedSpotVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {
	return nil, nil
}

func (p *Provider) GetHostErrorVMs(l *logger.Logger, since time.Time) ([]vm.PreemptedVM, error) {
	return nil, nil
}

func (p *Provider) CreateVolumeSnapshot(
	l *logger.Logger, volume vm.Volume, vsco vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	// TODO(leon): implement
	panic("unimplemented")
}

func (p *Provider) ListVolumeSnapshots(
	l *logger.Logger, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	panic("unimplemented")
}

func (p *Provider) DeleteVolumeSnapshots(l *logger.Logger, snapshots ...vm.VolumeSnapshot) error {
	panic("unimplemented")
}

func (p *Provider) CreateVolume(*logger.Logger, vm.VolumeCreateOpts) (vm.Volume, error) {
	panic("unimplemented")
}

func (p *Provider) DeleteVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) error {
	panic("unimplemented")
}

func (p *Provider) ListVolumes(l *logger.Logger, vm *vm.VM) ([]vm.Volume, error) {
	return vm.NonBootAttachedVolumes, nil
}

func (p *Provider) AttachVolume(*logger.Logger, vm.Volume, *vm.VM) (string, error) {
	panic("unimplemented")
}

func (p *Provider) Grow(*logger.Logger, vm.List, string, []string) error {
	panic("unimplemented")
}

func (p *Provider) CreateLoadBalancer(*logger.Logger, vm.List, int) error {
	panic("unimplemented")
}

func (p *Provider) DeleteLoadBalancer(*logger.Logger, vm.List, int) error {
	panic("unimplemented")
}

func (p *Provider) ListLoadBalancers(*logger.Logger, vm.List) ([]vm.ServiceAddress, error) {
	// This Provider has no concept of load balancers yet, return an empty list.
	return nil, nil
}

// New constructs a new Provider instance.
func New() *Provider {
	p := &Provider{}
	p.mu.resourceGroups = make(map[string]resources.Group)
	p.mu.securityGroups = make(map[string]network.SecurityGroup)
	p.mu.subnets = make(map[string]network.Subnet)
	return p
}

// Active implements vm.Provider and always returns true.
func (p *Provider) Active() bool {
	return true
}

// ProjectActive is part of the vm.Provider interface.
func (p *Provider) ProjectActive(project string) bool {
	return project == ""
}

// CleanSSH implements vm.Provider, is a no-op, and returns nil.
func (p *Provider) CleanSSH(l *logger.Logger) error {
	return nil
}

// ConfigSSH is part of the vm.Provider interface and is a no-op.
func (p *Provider) ConfigSSH(l *logger.Logger, zones []string) error {
	// On Azure, the SSH public key is set as part of VM instance creation.
	return nil
}

func getAzureDefaultLabelMap(opts vm.CreateOpts) map[string]string {
	m := vm.GetDefaultLabelMap(opts)
	m[vm.TagCreated] = timeutil.Now().Format(time.RFC3339)
	return m
}

func (p *Provider) AddLabels(l *logger.Logger, vms vm.List, labels map[string]string) error {
	l.Printf("adding labels to Azure VMs not yet supported")
	return nil
}

func (p *Provider) RemoveLabels(l *logger.Logger, vms vm.List, labels []string) error {
	l.Printf("removing labels from Azure VMs not yet supported")
	return nil
}

// Create implements vm.Provider.
func (p *Provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, vmProviderOpts vm.ProviderOpts,
) error {
	providerOpts := vmProviderOpts.(*ProviderOpts)
	// Load the user's SSH public key to configure the resulting VMs.
	sshKey, err := config.SSHPublicKey()
	if err != nil {
		return err
	}

	m := getAzureDefaultLabelMap(opts)
	clusterTags := make(map[string]*string)
	for key, value := range opts.CustomLabels {
		_, ok := m[strings.ToLower(key)]
		if ok {
			return fmt.Errorf("duplicate label name defined: %s", key)
		}

		clusterTags[key] = to.StringPtr(value)
	}
	for key, value := range m {
		clusterTags[key] = to.StringPtr(value)
	}

	getClusterResourceGroupName := func(location string) string {
		return fmt.Sprintf("%s-%s", opts.ClusterName, location)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.OperationTimeout)
	defer cancel()

	if len(providerOpts.Locations) == 0 {
		if opts.GeoDistributed {
			providerOpts.Locations = defaultLocations
		} else {
			providerOpts.Locations = []string{defaultLocations[0]}
		}
	}

	if len(providerOpts.Zone) == 0 {
		providerOpts.Zone = defaultZone
	}

	if _, err := p.createVNets(l, ctx, providerOpts.Locations, *providerOpts); err != nil {
		return err
	}

	// Effectively a map of node number to location.
	nodeLocations := vm.ZonePlacement(len(providerOpts.Locations), len(names))
	// Invert it.
	nodesByLocIdx := make(map[int][]int, len(providerOpts.Locations))
	for nodeIdx, locIdx := range nodeLocations {
		nodesByLocIdx[locIdx] = append(nodesByLocIdx[locIdx], nodeIdx)
	}

	errs, _ := errgroup.WithContext(ctx)
	for locIdx, nodes := range nodesByLocIdx {
		// Shadow variables for closure.
		locIdx := locIdx
		nodes := nodes
		errs.Go(func() error {
			location := providerOpts.Locations[locIdx]

			// Create a resource group within the location.
			group, err := p.getOrCreateResourceGroup(ctx, getClusterResourceGroupName(location), location, clusterTags)
			if err != nil {
				return err
			}

			subnet, ok := func() (network.Subnet, bool) {
				p.mu.Lock()
				defer p.mu.Unlock()
				s, ok := p.mu.subnets[location]
				return s, ok
			}()
			if !ok {
				return errors.Errorf("missing subnet for location %q", location)
			}

			for _, nodeIdx := range nodes {
				name := names[nodeIdx]
				errs.Go(func() error {
					_, err := p.createVM(l, ctx, group, subnet, name, sshKey, opts, *providerOpts)
					err = errors.Wrapf(err, "creating VM %s", name)
					if err == nil {
						l.Printf("created VM %s", name)
					}
					return err
				})
			}
			return nil
		})
	}
	return errs.Wait()
}

// Delete implements the vm.Provider interface.
func (p *Provider) Delete(l *logger.Logger, vms vm.List) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.OperationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := compute.NewVirtualMachinesClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	var futures []compute.VirtualMachinesDeleteFuture
	for _, vm := range vms {
		parts, err := parseAzureID(vm.ProviderID)
		if err != nil {
			return err
		}
		future, err := client.Delete(ctx, parts.resourceGroup, parts.resourceName, nil)
		if err != nil {
			return errors.Wrapf(err, "could not delete %s", vm.ProviderID)
		}
		futures = append(futures, future)
	}

	if !p.SyncDelete {
		return nil
	}

	for _, future := range futures {
		if err := future.WaitForCompletionRef(ctx, client.Client); err != nil {
			return err
		}
		if _, err := future.Result(client); err != nil {
			return err
		}
	}
	return nil
}

// Reset implements the vm.Provider interface. It is a no-op.
func (p *Provider) Reset(l *logger.Logger, vms vm.List) error {
	return nil
}

// DeleteCluster implements the vm.DeleteCluster interface, providing
// a fast-path to tear down all resources associated with a cluster.
func (p *Provider) DeleteCluster(l *logger.Logger, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.OperationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := resources.NewGroupsClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	filter := fmt.Sprintf("tagName eq '%s' and tagValue eq '%s'", vm.TagCluster, name)
	it, err := client.ListComplete(ctx, filter, nil /* limit */)
	if err != nil {
		return err
	}

	var futures []resources.GroupsDeleteFuture
	for it.NotDone() {
		group := it.Value()
		// Don't bother waiting for the cluster to get torn down.
		future, err := client.Delete(ctx, *group.Name)
		if err != nil {
			return err
		}
		l.Printf("marked Azure resource group %s for deletion\n", *group.Name)
		futures = append(futures, future)

		if err := it.NextWithContext(ctx); err != nil {
			return err
		}
	}

	if !p.SyncDelete {
		return nil
	}

	for _, future := range futures {
		if err := future.WaitForCompletionRef(ctx, client.Client); err != nil {
			return err
		}
		if _, err := future.Result(client); err != nil {
			return err
		}
	}
	return nil
}

// Extend implements the vm.Provider interface.
func (p *Provider) Extend(l *logger.Logger, vms vm.List, lifetime time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.OperationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := compute.NewVirtualMachinesClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	futures := make([]compute.VirtualMachinesUpdateFuture, len(vms))
	for idx, m := range vms {
		vmParts, err := parseAzureID(m.ProviderID)
		if err != nil {
			return err
		}
		// N.B. VirtualMachineUpdate below overwrites _all_ VM tags. Hence, we must copy all unmodified tags.
		tags := make(map[string]*string)
		// Copy all known VM tags.
		for k, v := range m.Labels {
			tags[k] = to.StringPtr(v)
		}
		// Overwrite Lifetime tag.
		tags[vm.TagLifetime] = to.StringPtr(lifetime.String())
		update := compute.VirtualMachineUpdate{
			Tags: tags,
		}
		futures[idx], err = client.Update(ctx, vmParts.resourceGroup, vmParts.resourceName, update)
		if err != nil {
			return err
		}
	}

	for _, future := range futures {
		if err := future.WaitForCompletionRef(ctx, client.Client); err != nil {
			return err
		}
		if _, err := future.Result(client); err != nil {
			return err
		}
	}
	return nil
}

// FindActiveAccount implements vm.Provider.
func (p *Provider) FindActiveAccount(l *logger.Logger) (string, error) {
	// It's a JSON Web Token, so we'll just dissect it enough to get the
	// data that we want. There are three base64-encoded segments
	// separated by periods. The second segment is the "claims" JSON
	// object.
	token, err := p.getAuthToken()
	if err != nil {
		return "", err
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.Errorf("unexpected number of segments; expected 3, had %d", len(parts))
	}

	a := base64.NewDecoder(base64.RawStdEncoding, strings.NewReader(parts[1]))
	var data struct {
		Username string `json:"upn"`
	}
	if err := json.NewDecoder(a).Decode(&data); err != nil {
		return "", errors.Wrapf(err, "could not decode JWT claims segment")
	}

	// If this is in an email address format, we just want the username.
	username, _, _ := strings.Cut(data.Username, "@")
	return username, nil
}

// List implements the vm.Provider interface. This will query all
// Azure VMs in the subscription and select those with a roachprod tag.
func (p *Provider) List(l *logger.Logger, opts vm.ListOptions) (vm.List, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.OperationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return nil, err
	}

	// We're just going to list all VMs and filter.
	client := compute.NewVirtualMachinesClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return nil, err
	}

	it, err := client.ListAllComplete(ctx, "false")
	if err != nil {
		return nil, err
	}

	// Keep track of which clusters we find through listing VMs.
	// If later we need to list resource groups to find empty clusters,
	// we want to make sure we don't add anything twice.
	foundClusters := make(map[string]bool)

	var ret vm.List
	for it.NotDone() {
		found := it.Value()
		if _, ok := found.Tags[vm.TagRoachprod]; !ok {
			if err := it.NextWithContext(ctx); err != nil {
				return nil, err
			}
			continue
		}

		tags := make(map[string]string)
		for key, value := range found.Tags {
			tags[key] = *value
		}

		m := vm.VM{
			Name:        *found.Name,
			Labels:      tags,
			Provider:    ProviderName,
			ProviderID:  *found.ID,
			RemoteUser:  remoteUser,
			VPC:         "global",
			MachineType: string(found.HardwareProfile.VMSize),
			CPUArch:     CpuArchFromAzureMachineType(string(found.HardwareProfile.VMSize)),
			// We add a fake availability-zone suffix since other roachprod
			// code assumes particular formats. For example, "eastus2z".
			Zone: *found.Location + "z",
		}

		if createdPtr := found.Tags[vm.TagCreated]; createdPtr == nil {
			m.Errors = append(m.Errors, vm.ErrNoExpiration)
		} else if parsed, err := time.Parse(time.RFC3339, *createdPtr); err == nil {
			m.CreatedAt = parsed
		} else {
			m.Errors = append(m.Errors, vm.ErrNoExpiration)
		}

		if lifetimePtr := found.Tags[vm.TagLifetime]; lifetimePtr == nil {
			m.Errors = append(m.Errors, vm.ErrNoExpiration)
		} else if parsed, err := time.ParseDuration(*lifetimePtr); err == nil {
			m.Lifetime = parsed
		} else {
			m.Errors = append(m.Errors, vm.ErrNoExpiration)
		}

		// The network info needs a separate request.
		nicID, err := parseAzureID(*(*found.NetworkProfile.NetworkInterfaces)[0].ID)
		if err != nil {
			return nil, err
		}
		if err := p.fillNetworkDetails(ctx, &m, nicID); errors.Is(err, vm.ErrBadNetwork) {
			m.Errors = append(m.Errors, err)
		} else if err != nil {
			return nil, err
		}

		clusterName, _ := m.ClusterName()
		foundClusters[clusterName] = true
		ret = append(ret, m)

		if err := it.NextWithContext(ctx); err != nil {
			return nil, err
		}
	}

	// Azure allows for clusters to exist even if the attached VM no longer exists.
	// Such a cluster won't be found by listing all azure VMs like above.
	// Normally we don't want to access these clusters except for deleting them.
	if opts.IncludeEmptyClusters {
		groupsClient := resources.NewGroupsClient(sub)
		if groupsClient.Authorizer, err = p.getAuthorizer(); err != nil {
			return nil, err
		}

		// List all resource groups for clusters under the subscription.
		filter := fmt.Sprintf("tagName eq '%s'", vm.TagCluster)
		it, err := groupsClient.ListComplete(ctx, filter, nil /* limit */)
		if err != nil {
			return nil, err
		}

		for it.NotDone() {
			resourceGroup := it.Value()
			if _, ok := resourceGroup.Tags[vm.TagRoachprod]; !ok {
				if err := it.NextWithContext(ctx); err != nil {
					return nil, err
				}
				continue
			}

			// Resource Groups have the name format "user-<clusterid>-<region>",
			// while clusters have the name format "user-<clusterid>".
			parts := strings.Split(*resourceGroup.Name, "-")
			clusterName := strings.Join(parts[:len(parts)-1], "-")
			if foundClusters[clusterName] {
				if err := it.NextWithContext(ctx); err != nil {
					return nil, err
				}
				continue
			}

			// The cluster does not have a VM, but roachprod assumes that this is not
			// possible and implements providers on the VM level. A VM-less cluster will
			// not have access to provider info or methods. To still allow this cluster to
			// be deleted, we must create a fake VM, indicated by EmptyCluster.
			m := vm.VM{
				Name:       *resourceGroup.Name,
				Provider:   ProviderName,
				RemoteUser: remoteUser,
				VPC:        "global",
				// We add a fake availability-zone suffix since other roachprod
				// code assumes particular formats. For example, "eastus2z".
				Zone:         *resourceGroup.Location + "z",
				EmptyCluster: true,
			}

			// We ignore any parsing errors here as roachprod tries to destroy "bad VMs".
			// We don't want that since this is a fake VM, we need to destroy the resource
			// group instead. This will be done by GC when it sees that no m.CreatedAt exists.
			createdPtr := resourceGroup.Tags[vm.TagCreated]
			if createdPtr != nil {
				parsed, _ := time.Parse(time.RFC3339, *createdPtr)
				m.CreatedAt = parsed
			}

			lifetimePtr := resourceGroup.Tags[vm.TagLifetime]
			if lifetimePtr != nil {
				parsed, _ := time.ParseDuration(*lifetimePtr)
				m.Lifetime = parsed
			}

			ret = append(ret, m)

			if err := it.NextWithContext(ctx); err != nil {
				return nil, err
			}
		}
	}

	return ret, nil
}

// Name implements vm.Provider.
func (p *Provider) Name() string {
	return ProviderName
}

func (p *Provider) createVM(
	l *logger.Logger,
	ctx context.Context,
	group resources.Group,
	subnet network.Subnet,
	name, sshKey string,
	opts vm.CreateOpts,
	providerOpts ProviderOpts,
) (machine compute.VirtualMachine, err error) {
	startupArgs := azureStartupArgs{RemoteUser: remoteUser}
	if !opts.SSDOpts.UseLocalSSD {
		// We define lun42 explicitly in the data disk request below.
		lun := 42
		startupArgs.AttachedDiskLun = &lun
	}

	startupScript, err := evalStartupTemplate(startupArgs)
	if err != nil {
		return machine, err
	}
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return compute.VirtualMachine{}, err
	}

	client := compute.NewVirtualMachinesClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return
	}

	// We first need to allocate a NIC to give the VM network access
	ip, err := p.createIP(l, ctx, group, name, providerOpts)
	if err != nil {
		return compute.VirtualMachine{}, err
	}
	nic, err := p.createNIC(l, ctx, group, ip, subnet)
	if err != nil {
		return compute.VirtualMachine{}, err
	}

	tags := make(map[string]*string)
	for key, value := range opts.CustomLabels {
		tags[key] = to.StringPtr(value)
	}
	m := getAzureDefaultLabelMap(opts)
	for key, value := range m {
		tags[key] = to.StringPtr(value)
	}

	osVolumeSize := int32(opts.OsVolumeSize)
	if osVolumeSize < 32 {
		l.Printf("WARNING: increasing the OS volume size to minimally allowed 32GB")
		osVolumeSize = 32
	}
	imageSKU := func(arch string, machineType string) string {
		if arch == string(vm.ArchARM64) {
			return "22_04-lts-arm64"
		}
		version := MachineFamilyVersionFromMachineType(machineType)
		// N.B. We make a simplifying assumption that anything above v5 is gen2.
		// roachtest's SelectAzureMachineType prefers v5 machine types, some of which do not support gen1.
		// (Matrix of machine types supporting gen2: https://learn.microsoft.com/en-us/azure/virtual-machines/generation-2)
		if version >= 5 {
			return "22_04-lts-gen2"
		}
		return "22_04-lts"
	}

	// Derived from
	// https://github.com/Azure-Samples/azure-sdk-for-go-samples/blob/79e3f3af791c3873d810efe094f9d61e93a6ccaa/compute/vm.go#L41
	machine = compute.VirtualMachine{
		Location: group.Location,
		Zones:    to.StringSlicePtr([]string{providerOpts.Zone}),
		Tags:     tags,
		VirtualMachineProperties: &compute.VirtualMachineProperties{
			HardwareProfile: &compute.HardwareProfile{
				VMSize: compute.VirtualMachineSizeTypes(providerOpts.MachineType),
			},
			StorageProfile: &compute.StorageProfile{
				// From https://discourse.ubuntu.com/t/find-ubuntu-images-on-microsoft-azure/18918
				// You can find available versions by running the following command:
				// az machine image list --all --publisher Canonical
				// To get the latest 22.04 version:
				// az vm image list --all --publisher Canonical | \
				// jq '[.[] | select(.sku=="22_04-lts")] | max_by(.version)'
				ImageReference: &compute.ImageReference{
					Publisher: to.StringPtr("Canonical"),
					Offer:     to.StringPtr("0001-com-ubuntu-server-jammy"),
					Sku:       to.StringPtr(imageSKU(opts.Arch, providerOpts.MachineType)),
					Version:   to.StringPtr("22.04.202312060"),
				},
				OsDisk: &compute.OSDisk{
					CreateOption: compute.DiskCreateOptionTypesFromImage,
					ManagedDisk: &compute.ManagedDiskParameters{
						StorageAccountType: compute.StorageAccountTypesStandardSSDLRS,
					},
					DiskSizeGB: to.Int32Ptr(osVolumeSize),
				},
			},
			OsProfile: &compute.OSProfile{
				ComputerName:  to.StringPtr(name),
				AdminUsername: to.StringPtr(remoteUser),
				// Per the docs, the cloud-init script should be uploaded already
				// base64-encoded.
				CustomData: to.StringPtr(startupScript),
				LinuxConfiguration: &compute.LinuxConfiguration{
					SSH: &compute.SSHConfiguration{
						PublicKeys: &[]compute.SSHPublicKey{
							{
								Path:    to.StringPtr(fmt.Sprintf("/home/%s/.ssh/authorized_keys", remoteUser)),
								KeyData: to.StringPtr(sshKey),
							},
						},
					},
				},
			},
			NetworkProfile: &compute.NetworkProfile{
				NetworkInterfaces: &[]compute.NetworkInterfaceReference{
					{
						ID: nic.ID,
						NetworkInterfaceReferenceProperties: &compute.NetworkInterfaceReferenceProperties{
							Primary: to.BoolPtr(true),
						},
					},
				},
			},
		},
	}
	if !opts.SSDOpts.UseLocalSSD {
		caching := compute.CachingTypesNone

		switch providerOpts.DiskCaching {
		case "read-only":
			caching = compute.CachingTypesReadOnly
		case "read-write":
			caching = compute.CachingTypesReadWrite
		case "none":
			caching = compute.CachingTypesNone
		default:
			err = errors.Newf("unsupported caching behavior: %s", providerOpts.DiskCaching)
			return compute.VirtualMachine{}, err
		}
		dataDisks := []compute.DataDisk{
			{
				DiskSizeGB: to.Int32Ptr(providerOpts.NetworkDiskSize),
				Caching:    caching,
				Lun:        to.Int32Ptr(42),
			},
		}

		switch providerOpts.NetworkDiskType {
		case "ultra-disk":
			var ultraDisk compute.Disk
			ultraDisk, err = p.createUltraDisk(l, ctx, group, name+"-ultra-disk", providerOpts)
			if err != nil {
				return compute.VirtualMachine{}, err
			}
			// UltraSSD specific disk configurations.
			dataDisks[0].CreateOption = compute.DiskCreateOptionTypesAttach
			dataDisks[0].Name = ultraDisk.Name
			dataDisks[0].ManagedDisk = &compute.ManagedDiskParameters{
				StorageAccountType: compute.StorageAccountTypesUltraSSDLRS,
				ID:                 ultraDisk.ID,
			}

			// UltraSSDs must be enabled separately.
			machine.AdditionalCapabilities = &compute.AdditionalCapabilities{
				UltraSSDEnabled: to.BoolPtr(true),
			}
		case "premium-disk":
			// premium-disk specific disk configurations.
			dataDisks[0].CreateOption = compute.DiskCreateOptionTypesEmpty
			dataDisks[0].ManagedDisk = &compute.ManagedDiskParameters{
				StorageAccountType: compute.StorageAccountTypesPremiumLRS,
			}
		default:
			err = errors.Newf("unsupported network disk type: %s", providerOpts.NetworkDiskType)
			return compute.VirtualMachine{}, err
		}

		machine.StorageProfile.DataDisks = &dataDisks
	}
	future, err := client.CreateOrUpdate(ctx, *group.Name, name, machine)
	if err != nil {
		return
	}
	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return
	}
	return future.Result(client)
}

// createNIC creates a network adapter that is bound to the given public IP address.
func (p *Provider) createNIC(
	l *logger.Logger,
	ctx context.Context,
	group resources.Group,
	ip network.PublicIPAddress,
	subnet network.Subnet,
) (iface network.Interface, err error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return
	}
	client := network.NewInterfacesClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return
	}

	_, sg := p.getResourcesAndSecurityGroupByName("", p.getVnetNetworkSecurityGroupName(*group.Location))

	future, err := client.CreateOrUpdate(ctx, *group.Name, *ip.Name, network.Interface{
		Name:     ip.Name,
		Location: group.Location,
		InterfacePropertiesFormat: &network.InterfacePropertiesFormat{
			IPConfigurations: &[]network.InterfaceIPConfiguration{
				{
					Name: to.StringPtr("ipConfig"),
					InterfaceIPConfigurationPropertiesFormat: &network.InterfaceIPConfigurationPropertiesFormat{
						Subnet:                    &subnet,
						PrivateIPAllocationMethod: network.IPAllocationMethodDynamic,
						PublicIPAddress:           &ip,
					},
				},
			},
			NetworkSecurityGroup:        &sg,
			EnableAcceleratedNetworking: to.BoolPtr(true),
			Primary:                     to.BoolPtr(true),
		},
	})
	if err != nil {
		return
	}
	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return
	}
	iface, err = future.Result(client)
	if err == nil {
		l.Printf("created NIC %s %s", *iface.Name, *(*iface.IPConfigurations)[0].PrivateIPAddress)
	}
	return
}

// securityRules returns an array of TCP security rules and contains
// a list of well-known, and roachtest specific ports.
func securityRules() *[]network.SecurityRule {
	allowTCP := func(name string, priority int32, direction network.SecurityRuleDirection, destPortRange string) network.SecurityRule {
		suffix := ""
		switch direction {
		case network.SecurityRuleDirectionInbound:
			suffix = "_Inbound"
		case network.SecurityRuleDirectionOutbound:
			suffix = "_Outbound"
		default:
		}
		res := network.SecurityRule{
			Name: to.StringPtr(name + suffix),
			SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
				Priority:                 to.Int32Ptr(priority),
				Protocol:                 network.SecurityRuleProtocolTCP,
				Access:                   network.SecurityRuleAccessAllow,
				Direction:                direction,
				SourceAddressPrefix:      to.StringPtr("*"),
				SourcePortRange:          to.StringPtr("*"),
				DestinationAddressPrefix: to.StringPtr("*"),
				DestinationPortRange:     to.StringPtr(destPortRange),
			},
		}
		return res
	}

	namedInbound := map[string]string{
		"SSH":                "22",
		"HTTP":               "80",
		"HTTPS":              "43",
		"CockroachPG":        "26257",
		"CockroachAdmin":     "26258",
		"Grafana":            "3000",
		"Prometheus":         "9090",
		"Kafka":              "9092",
		"WorkloadPPROF":      "33333",
		"WorkloadPrometheus": "2112-2120",
	}

	// The names for these are generated in the form Roachtest_<index>_Inbound.
	// The mapped roachtests are not exhaustive, and at some point will be
	// cumbersome to keep adding exceptions for.
	// TODO: (miral) Consider removing all rules if this keeps tripping roachtests.
	genericInbound := []string{
		"8011",        // multitenant
		"8081",        // backup/*
		"9011",        // smoketest/secure/multitenan
		"9081-9102",   // smoketest/secure/multitenant
		"20011-20016", //multitenant/upgrade
		"27257",       //acceptance/gossip/restart-node-one
		"27259-27280", // various multitenant tenant SQL ports
		"30258",       //acceptance/multitenant
	}

	// The extra 1 is for the single allow all TCP outbound allowTCP.
	firewallRules := make([]network.SecurityRule, 1+len(namedInbound)+len(genericInbound))
	firewallRules[0] = allowTCP("TCP_All", 300, network.SecurityRuleDirectionOutbound, "*")
	r := 1
	priority := 300
	for ruleName, port := range namedInbound {
		firewallRules[r] = allowTCP(ruleName, int32(priority+r), network.SecurityRuleDirectionInbound, port)
		r++
	}

	for i, port := range genericInbound {
		firewallRules[r] = allowTCP(fmt.Sprintf("Roachtest_%d", i), int32(priority+r), network.SecurityRuleDirectionInbound, port)
		r++
	}
	return &firewallRules
}

func (p *Provider) getOrCreateNetworkSecurityGroup(
	ctx context.Context, name string, resourceGroup resources.Group,
) (network.SecurityGroup, error) {
	group, ok := func() (network.SecurityGroup, bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		g, ok := p.mu.securityGroups[name]
		return g, ok
	}()
	if ok {
		return group, nil
	}

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return network.SecurityGroup{}, err
	}
	client := network.NewSecurityGroupsClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return network.SecurityGroup{}, err
	}
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return network.SecurityGroup{}, err
	}

	cacheAndReturn := func(group network.SecurityGroup) (network.SecurityGroup, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.mu.securityGroups[name] = group
		return group, nil
	}

	future, err := client.CreateOrUpdate(ctx, *resourceGroup.Name, name, network.SecurityGroup{
		SecurityGroupPropertiesFormat: &network.SecurityGroupPropertiesFormat{
			SecurityRules: securityRules(),
		},
		Location: resourceGroup.Location,
	})
	if err != nil {
		return network.SecurityGroup{}, err
	}
	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return network.SecurityGroup{}, err
	}
	securityGroup, err := future.Result(client)
	if err != nil {
		return network.SecurityGroup{}, err
	}

	return cacheAndReturn(securityGroup)
}

func (p *Provider) getVnetNetworkSecurityGroupName(location string) string {
	return fmt.Sprintf("roachprod-vnets-nsg-%s", location)
}

// createVNets will create a VNet in each of the given locations to be
// shared across roachprod clusters. Thus, all roachprod clusters will
// be able to communicate with one another, although this is scoped by
// the value of the vnet-name flag.
func (p *Provider) createVNets(
	l *logger.Logger, ctx context.Context, locations []string, providerOpts ProviderOpts,
) (map[string]network.VirtualNetwork, error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return nil, err
	}

	groupsClient := resources.NewGroupsClient(sub)
	if groupsClient.Authorizer, err = p.getAuthorizer(); err != nil {
		return nil, err
	}

	vnetResourceGroupTags := make(map[string]*string)
	vnetResourceGroupTags[tagComment] = to.StringPtr("DO NOT DELETE: Used by all roachprod clusters")
	vnetResourceGroupTags[vm.TagRoachprod] = to.StringPtr("true")

	vnetResourceGroupName := func(location string) string {
		return fmt.Sprintf("roachprod-vnets-%s", location)
	}

	setVNetSubnetPrefix := func(group resources.Group, subnet int) (resources.Group, error) {
		return groupsClient.Update(ctx, *group.Name, resources.GroupPatchable{
			Tags: map[string]*string{
				tagSubnet: to.StringPtr(strconv.Itoa(subnet)),
			},
		})
	}

	// First, find or create a resource groups and network security groups for
	// roachprod to create the VNets in. We need one per location.
	for _, location := range locations {
		group, err := p.getOrCreateResourceGroup(ctx, vnetResourceGroupName(location), location, vnetResourceGroupTags)
		if err != nil {
			return nil, errors.Wrapf(err, "resource group for location %q", location)
		}
		_, err = p.getOrCreateNetworkSecurityGroup(ctx, p.getVnetNetworkSecurityGroupName(location), group)
		if err != nil {
			return nil, errors.Wrapf(err, "nsg for location %q", location)
		}
	}

	// In order to prevent overlapping subnets, we want to associate each
	// of the roachprod-owned RG's with a network prefix. We're going to
	// make an assumption that it's very unlikely that two users will try
	// to allocate new subnets at the same time. If this happens, it can
	// be easily fixed by deleting one of resource groups and re-running
	// roachprod to select a new network prefix.
	prefixesByLocation := make(map[string]int)
	activePrefixes := make(map[int]bool)

	nextAvailablePrefix := func() int {
		prefix := 1
		for activePrefixes[prefix] {
			prefix++
		}
		activePrefixes[prefix] = true
		return prefix
	}
	newSubnetsCreated := false

	for _, location := range providerOpts.Locations {
		group, _ := p.getResourcesAndSecurityGroupByName(vnetResourceGroupName(location), "")
		// Prefix already exists for the resource group.
		if prefixString := group.Tags[tagSubnet]; prefixString != nil {
			prefix, err := strconv.Atoi(*prefixString)
			if err != nil {
				return nil, errors.Wrapf(err, "for location %q", location)
			}
			activePrefixes[prefix] = true
			prefixesByLocation[location] = prefix
		} else {
			// The fact that the vnet didn't have a prefix means that new subnets will
			// be created.
			newSubnetsCreated = true
			prefix := nextAvailablePrefix()
			prefixesByLocation[location] = prefix
			group, _ := p.getResourcesAndSecurityGroupByName(vnetResourceGroupName(location), "")
			group, err = setVNetSubnetPrefix(group, prefix)
			if err != nil {
				return nil, errors.Wrapf(err, "for location %q", location)
			}
			// We just updated the VNet Subnet prefix on the resource group -- update
			// the cached entry to reflect that.
			func() {
				p.mu.Lock()
				defer p.mu.Unlock()
				p.mu.resourceGroups[vnetResourceGroupName(location)] = group
			}()
		}
	}

	// Now, we can ensure that the VNet exists with the requested subnet.
	// TODO(arul): Does this need to be done for all locations or just for the
	// locations that didn't have a subnet/vnet before? I'm inclined to say the
	// latter, but I'm leaving the existing behavior as is.
	ret := make(map[string]network.VirtualNetwork)
	vnets := make([]network.VirtualNetwork, len(ret))
	for location, prefix := range prefixesByLocation {
		resourceGroup, networkSecurityGroup := p.getResourcesAndSecurityGroupByName(vnetResourceGroupName(location), p.getVnetNetworkSecurityGroupName(location))
		if vnet, _, err := p.createVNet(l, ctx, resourceGroup, networkSecurityGroup, prefix, providerOpts); err == nil {
			ret[location] = vnet
			vnets = append(vnets, vnet)
		} else {
			return nil, errors.Wrapf(err, "for location %q", location)
		}
	}

	// We only need to create peerings if there are new subnets.
	if newSubnetsCreated {
		return ret, p.createVNetPeerings(l, ctx, vnets)
	}
	return ret, nil
}

// createVNet creates or retrieves a named VNet object using the 10.<offset>/16 prefix.
// A single /18 subnet will be created within the VNet.
// The results  will be memoized in the Provider.
func (p *Provider) createVNet(
	l *logger.Logger,
	ctx context.Context,
	resourceGroup resources.Group,
	securityGroup network.SecurityGroup,
	prefix int,
	providerOpts ProviderOpts,
) (vnet network.VirtualNetwork, subnet network.Subnet, err error) {
	vnetName := providerOpts.VnetName

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return
	}
	client := network.NewVirtualNetworksClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return
	}
	vnet = network.VirtualNetwork{
		Name:     to.StringPtr(vnetName),
		Location: resourceGroup.Location,
		VirtualNetworkPropertiesFormat: &network.VirtualNetworkPropertiesFormat{
			AddressSpace: &network.AddressSpace{
				AddressPrefixes: &[]string{fmt.Sprintf("10.%d.0.0/16", prefix)},
			},
			Subnets: &[]network.Subnet{
				{
					Name: resourceGroup.Name,
					SubnetPropertiesFormat: &network.SubnetPropertiesFormat{
						AddressPrefix:        to.StringPtr(fmt.Sprintf("10.%d.0.0/18", prefix)),
						NetworkSecurityGroup: &securityGroup,
					},
				},
			},
		},
	}
	future, err := client.CreateOrUpdate(ctx, *resourceGroup.Name, *resourceGroup.Name, vnet)
	if err != nil {
		err = errors.Wrapf(err, "creating Azure VNet %q in %q", vnetName, *resourceGroup.Name)
		return
	}
	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		err = errors.Wrapf(err, "creating Azure VNet %q in %q", vnetName, *resourceGroup.Name)
		return
	}
	vnet, err = future.Result(client)
	err = errors.Wrapf(err, "creating Azure VNet %q in %q", vnetName, *resourceGroup.Name)
	if err != nil {
		return
	}
	subnet = (*vnet.Subnets)[0]
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.subnets[*resourceGroup.Location] = subnet
	l.Printf("created Azure VNet %q in %q with prefix %d", vnetName, *resourceGroup.Name, prefix)
	return
}

// createVNetPeerings creates a fully-connected graph of peerings
// between the provided vnets.
func (p *Provider) createVNetPeerings(
	l *logger.Logger, ctx context.Context, vnets []network.VirtualNetwork,
) error {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := network.NewVirtualNetworkPeeringsClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	// Create cross-product of vnets.
	futures := make(map[string]network.VirtualNetworkPeeringsCreateOrUpdateFuture)
	for _, outer := range vnets {
		for _, inner := range vnets {
			if *outer.ID == *inner.ID {
				continue
			}

			linkName := fmt.Sprintf("%s-%s", *outer.Name, *inner.Name)
			peering := network.VirtualNetworkPeering{
				Name: to.StringPtr(linkName),
				VirtualNetworkPeeringPropertiesFormat: &network.VirtualNetworkPeeringPropertiesFormat{
					AllowForwardedTraffic:     to.BoolPtr(true),
					AllowVirtualNetworkAccess: to.BoolPtr(true),
					RemoteAddressSpace:        inner.AddressSpace,
					RemoteVirtualNetwork: &network.SubResource{
						ID: inner.ID,
					},
				},
			}

			outerParts, err := parseAzureID(*outer.ID)
			if err != nil {
				return err
			}

			future, err := client.CreateOrUpdate(ctx, outerParts.resourceGroup, *outer.Name, linkName, peering, network.SyncRemoteAddressSpaceTrue)
			if err != nil {
				return errors.Wrapf(err, "creating vnet peering %s", linkName)
			}
			futures[linkName] = future
		}
	}

	for name, future := range futures {
		if err := future.WaitForCompletionRef(ctx, client.Client); err != nil {
			return errors.Wrapf(err, "creating vnet peering %s", name)
		}
		peering, err := future.Result(client)
		if err != nil {
			return errors.Wrapf(err, "creating vnet peering %s", name)
		}
		l.Printf("created vnet peering %s", *peering.Name)
	}

	return nil
}

// createIP allocates an IP address that will later be bound to a NIC.
func (p *Provider) createIP(
	l *logger.Logger,
	ctx context.Context,
	group resources.Group,
	name string,
	providerOpts ProviderOpts,
) (ip network.PublicIPAddress, err error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return
	}
	ipc := network.NewPublicIPAddressesClient(sub)
	if ipc.Authorizer, err = p.getAuthorizer(); err != nil {
		return
	}
	future, err := ipc.CreateOrUpdate(ctx, *group.Name, name,
		network.PublicIPAddress{
			Name: to.StringPtr(name),
			Sku: &network.PublicIPAddressSku{
				Name: network.PublicIPAddressSkuNameStandard,
			},
			Location: group.Location,
			Zones:    to.StringSlicePtr([]string{providerOpts.Zone}),
			PublicIPAddressPropertiesFormat: &network.PublicIPAddressPropertiesFormat{
				PublicIPAddressVersion:   network.IPVersionIPv4,
				PublicIPAllocationMethod: network.IPAllocationMethodStatic,
			},
		})
	if err != nil {
		err = errors.Wrapf(err, "creating IP %s", name)
		return
	}
	err = future.WaitForCompletionRef(ctx, ipc.Client)
	if err != nil {
		err = errors.Wrapf(err, "creating IP %s", name)
		return
	}
	if ip, err = future.Result(ipc); err == nil {
		l.Printf("created Azure IP %s", *ip.Name)
	} else {
		err = errors.Wrapf(err, "creating IP %s", name)
	}

	return
}

// fillNetworkDetails makes some secondary requests to the Azure
// API in order to populate the VM details. This will return
// vm.ErrBadNetwork if the response payload is not of the expected form.
func (p *Provider) fillNetworkDetails(ctx context.Context, m *vm.VM, nicID azureID) error {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}

	nicClient := network.NewInterfacesClient(sub)
	if nicClient.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	ipClient := network.NewPublicIPAddressesClient(sub)
	ipClient.Authorizer = nicClient.Authorizer

	iface, err := nicClient.Get(ctx, nicID.resourceGroup, nicID.resourceName, "" /*expand*/)
	if err != nil {
		return err
	}
	if iface.IPConfigurations == nil {
		return vm.ErrBadNetwork
	}
	cfg := (*iface.IPConfigurations)[0]
	if cfg.PrivateIPAddress == nil {
		return vm.ErrBadNetwork
	}
	m.PrivateIP = *cfg.PrivateIPAddress
	m.DNS = m.PrivateIP
	if cfg.PublicIPAddress == nil || cfg.PublicIPAddress.ID == nil {
		return vm.ErrBadNetwork
	}
	ipID, err := parseAzureID(*cfg.PublicIPAddress.ID)
	if err != nil {
		return vm.ErrBadNetwork
	}

	ip, err := ipClient.Get(ctx, ipID.resourceGroup, ipID.resourceName, "" /*expand*/)
	if err != nil {
		return vm.ErrBadNetwork
	}
	if ip.IPAddress == nil {
		return vm.ErrBadNetwork
	}
	m.PublicIP = *ip.IPAddress

	return nil
}

// getOrCreateResourceGroup retrieves or creates a resource group with the given
// name in the specified location and with the given tags. Results are memoized
// within the Provider instance.
func (p *Provider) getOrCreateResourceGroup(
	ctx context.Context, name string, location string, tags map[string]*string,
) (resources.Group, error) {

	// First, check the local provider cache.
	group, ok := func() (resources.Group, bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		g, ok := p.mu.resourceGroups[name]
		return g, ok
	}()
	if ok {
		return group, nil
	}

	cacheAndReturn := func(group resources.Group) (resources.Group, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.mu.resourceGroups[name] = group
		return group, nil
	}

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return resources.Group{}, err
	}

	client := resources.NewGroupsClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return resources.Group{}, err
	}

	// Next, we make an API call to see if the resource already exists on Azure.
	group, err = client.Get(ctx, name)
	if err == nil {
		return cacheAndReturn(group)
	}
	var detail autorest.DetailedError
	if errors.As(err, &detail) {
		// It's okay if the resource was "not found" -- we will create it below.
		if code, ok := detail.StatusCode.(int); ok && code != 404 {
			return resources.Group{}, err
		}
	}

	group, err = client.CreateOrUpdate(ctx, name,
		resources.Group{
			Location: to.StringPtr(location),
			Tags:     tags,
		})
	if err != nil {
		return resources.Group{}, err
	}
	return cacheAndReturn(group)
}

func (p *Provider) createUltraDisk(
	l *logger.Logger,
	ctx context.Context,
	group resources.Group,
	name string,
	providerOpts ProviderOpts,
) (compute.Disk, error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return compute.Disk{}, err
	}

	client := compute.NewDisksClient(sub)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return compute.Disk{}, err
	}

	future, err := client.CreateOrUpdate(ctx, *group.Name, name,
		compute.Disk{
			Zones:    to.StringSlicePtr([]string{providerOpts.Zone}),
			Location: group.Location,
			Sku: &compute.DiskSku{
				Name: compute.DiskStorageAccountTypesUltraSSDLRS,
			},
			DiskProperties: &compute.DiskProperties{
				CreationData: &compute.CreationData{
					CreateOption: compute.DiskCreateOptionEmpty,
				},
				DiskSizeGB:        to.Int32Ptr(providerOpts.NetworkDiskSize),
				DiskIOPSReadWrite: to.Int64Ptr(providerOpts.UltraDiskIOPS),
			},
		})
	if err != nil {
		return compute.Disk{}, err
	}
	if err := future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return compute.Disk{}, err
	}
	disk, err := future.Result(client)
	if err != nil {
		return compute.Disk{}, err
	}
	l.Printf("created ultra-disk: %s\n", *disk.Name)
	return disk, err
}

// getSubscription returns env.AZURE_SUBSCRIPTION_ID if it exists
// or the first subscription when listing all available via an API call.
// The value is memoized in the Provider instance.
func (p *Provider) getSubscription(ctx context.Context) (string, error) {
	subscriptionId := func() string {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.mu.subscriptionId
	}()

	if subscriptionId != "" {
		return subscriptionId, nil
	}

	subscriptionId = os.Getenv("AZURE_SUBSCRIPTION_ID")

	// Fallback to retrieving the first subscription
	if subscriptionId == "" {
		authorizer, err := p.getAuthorizer()
		if err != nil {
			return "", err
		}
		sc := subscriptions.NewClient()
		sc.Authorizer = authorizer

		page, err := sc.List(ctx)
		if err == nil {
			if len(page.Values()) == 0 {
				err = errors.New("did not find Azure subscription")
				return "", err
			}
			s := page.Values()[0].SubscriptionID
			if s != nil {
				subscriptionId = *s
			}
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.subscriptionId = subscriptionId
	return subscriptionId, nil
}

// getResourceGroupByName receives a string name and returns
// the resources.Group associated with it.
func (p *Provider) getResourcesAndSecurityGroupByName(
	rName string, sName string,
) (resources.Group, network.SecurityGroup) {
	var rGroup resources.Group
	var sGroup network.SecurityGroup
	p.mu.Lock()
	defer p.mu.Unlock()
	if rName != "" {
		rGroup = p.mu.resourceGroups[rName]
	}
	if sName != "" {
		sGroup = p.mu.securityGroups[sName]
	}
	return rGroup, sGroup
}

var azureMachineTypes = regexp.MustCompile(`^(Standard_[DE])(\d+)([a-z]*)_v(?P<version>\d+)$`)

// CpuArchFromAzureMachineType attempts to determine the CPU architecture from the corresponding Azure
// machine type. In case the machine type is not recognized, it defaults to AMD64.
// TODO(srosenberg): remove when the Azure SDK finally exposes the CPU architecture for a given VM.
func CpuArchFromAzureMachineType(machineType string) vm.CPUArch {
	matches := azureMachineTypes.FindStringSubmatch(machineType)

	if len(matches) >= 4 {
		series := matches[1] + matches[3]
		if series == "Standard_Dps" || series == "Standard_Dpds" ||
			series == "Standard_Dplds" || series == "Standard_Dpls" ||
			series == "Standard_Eps" || series == "Standard_Epds" {
			return vm.ArchARM64
		}
	}
	return vm.ArchAMD64
}

// MachineFamilyVersionFromMachineType attempts to determine the machine family version from the machine type.
// If the version cannot be determined, it returns -1.
func MachineFamilyVersionFromMachineType(machineType string) int {
	matches := azureMachineTypes.FindStringSubmatch(machineType)
	for i, name := range azureMachineTypes.SubexpNames() {
		if i >= len(matches) {
			break
		}
		if i != 0 && name == "version" {
			res, err := strconv.Atoi(matches[i])
			if err != nil {
				return -1
			}
			return res
		}
	}
	return -1
}
