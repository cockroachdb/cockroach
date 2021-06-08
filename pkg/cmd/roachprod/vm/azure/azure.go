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
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/latest/compute/mgmt/compute"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/network/mgmt/network"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/resources/mgmt/resources"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/resources/mgmt/subscriptions"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// ProviderName is "azure".
	ProviderName = "azure"
	remoteUser   = "ubuntu"
	tagCluster   = "cluster"
	tagComment   = "comment"
	// RFC3339-formatted timestamp.
	tagCreated   = "created"
	tagLifetime  = "lifetime"
	tagRoachprod = "roachprod"
	tagSubnet    = "subnetPrefix"
)

// init registers Provider with the top-level vm package.
func init() {
	const unimplemented = "please install the Azure CLI utilities +" +
		"(https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)"

	p := New()

	if _, err := p.getAuthToken(); err == nil {
		vm.Providers[ProviderName] = p
	} else {
		vm.Providers[ProviderName] = flagstub.New(p, unimplemented)
	}
}

// Provider implements the vm.Provider interface for the Microsoft Azure
// cloud.
type Provider struct {
	opts providerOpts
	mu   struct {
		syncutil.Mutex

		authorizer     autorest.Authorizer
		subscription   subscriptions.Subscription
		resourceGroups map[string]resources.Group
		subnets        map[string]network.Subnet
		securityGroups map[string]network.SecurityGroup
	}
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

// CleanSSH implements vm.Provider, is a no-op, and returns nil.
func (p *Provider) CleanSSH() error {
	return nil
}

// ConfigSSH implements vm.Provider, is a no-op, and returns nil.
// On Azure, the SSH public key is set as part of VM instance creation.
func (p *Provider) ConfigSSH() error {
	return nil
}

// Create implements vm.Provider.
func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	// Load the user's SSH public key to configure the resulting VMs.
	var sshKey string
	sshFile := os.ExpandEnv("${HOME}/.ssh/id_rsa.pub")
	if _, err := os.Stat(sshFile); err == nil {
		if bytes, err := ioutil.ReadFile(sshFile); err == nil {
			sshKey = string(bytes)
		} else {
			return errors.Wrapf(err, "could not read SSH public key file")
		}
	} else {
		return errors.Wrapf(err, "could not find SSH public key file")
	}

	clusterTags := make(map[string]*string)
	clusterTags[tagCluster] = to.StringPtr(opts.ClusterName)
	clusterTags[tagCreated] = to.StringPtr(timeutil.Now().Format(time.RFC3339))
	clusterTags[tagLifetime] = to.StringPtr(opts.Lifetime.String())
	clusterTags[tagRoachprod] = to.StringPtr("true")

	getClusterResourceGroupName := func(location string) string {
		return fmt.Sprintf("%s-%s", opts.ClusterName, location)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.opts.operationTimeout)
	defer cancel()

	if len(p.opts.locations) == 0 {
		if opts.GeoDistributed {
			p.opts.locations = defaultLocations
		} else {
			p.opts.locations = []string{defaultLocations[0]}
		}
	}

	if len(p.opts.zone) == 0 {
		p.opts.zone = defaultZone
	}

	if _, err := p.createVNets(ctx, p.opts.locations); err != nil {
		return err
	}

	// Effectively a map of node number to location.
	nodeLocations := vm.ZonePlacement(len(p.opts.locations), len(names))
	// Invert it.
	nodesByLocIdx := make(map[int][]int, len(p.opts.locations))
	for nodeIdx, locIdx := range nodeLocations {
		nodesByLocIdx[locIdx] = append(nodesByLocIdx[locIdx], nodeIdx)
	}

	errs, _ := errgroup.WithContext(ctx)
	for locIdx, nodes := range nodesByLocIdx {
		// Shadow variables for closure.
		locIdx := locIdx
		nodes := nodes
		errs.Go(func() error {
			location := p.opts.locations[locIdx]

			// Create a resource group within the location.
			group, err := p.getOrCreateResourceGroup(
				ctx, getClusterResourceGroupName(location), location, clusterTags)
			if err != nil {
				return err
			}

			p.mu.Lock()
			subnet, ok := p.mu.subnets[location]
			p.mu.Unlock()
			if !ok {
				return errors.Errorf("missing subnet for location %q", location)
			}

			for _, nodeIdx := range nodes {
				name := names[nodeIdx]
				errs.Go(func() error {
					_, err := p.createVM(ctx, group, subnet, name, sshKey, opts)
					err = errors.Wrapf(err, "creating VM %s", name)
					if err == nil {
						log.Printf("created VM %s", name)
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
func (p *Provider) Delete(vms vm.List) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.operationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := compute.NewVirtualMachinesClient(*sub.ID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	var futures []compute.VirtualMachinesDeleteFuture
	for _, vm := range vms {
		parts, err := parseAzureID(vm.ProviderID)
		if err != nil {
			return err
		}
		future, err := client.Delete(ctx, parts.resourceGroup, parts.resourceName)
		if err != nil {
			return errors.Wrapf(err, "could not delete %s", vm.ProviderID)
		}
		futures = append(futures, future)
	}

	if !p.opts.syncDelete {
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
func (p *Provider) Reset(vms vm.List) error {
	return nil
}

// DeleteCluster implements the vm.DeleteCluster interface, providing
// a fast-path to tear down all resources associated with a cluster.
func (p *Provider) DeleteCluster(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.operationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := resources.NewGroupsClient(*sub.SubscriptionID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	filter := fmt.Sprintf("tagName eq '%s' and tagValue eq '%s'", tagCluster, name)
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
		log.Printf("marked Azure resource group %s for deletion\n", *group.Name)
		futures = append(futures, future)

		if err := it.NextWithContext(ctx); err != nil {
			return err
		}
	}

	if !p.opts.syncDelete {
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
func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.operationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := compute.NewVirtualMachinesClient(*sub.ID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	futures := make([]compute.VirtualMachinesUpdateFuture, len(vms))
	for idx, vm := range vms {
		vmParts, err := parseAzureID(vm.ProviderID)
		if err != nil {
			return err
		}
		update := compute.VirtualMachineUpdate{
			Tags: map[string]*string{
				tagLifetime: to.StringPtr(lifetime.String()),
			},
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
func (p *Provider) FindActiveAccount() (string, error) {
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

	// This is in an email address format, we just want the username.
	return data.Username[:strings.Index(data.Username, "@")], nil
}

// Flags implements the vm.Provider interface.
func (p *Provider) Flags() vm.ProviderFlags {
	return &p.opts
}

// List implements the vm.Provider interface. This will query all
// Azure VMs in the subscription and select those with a roachprod tag.
func (p *Provider) List() (vm.List, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.operationTimeout)
	defer cancel()

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return nil, err
	}

	// We're just going to list all VMs and filter.
	client := compute.NewVirtualMachinesClient(*sub.SubscriptionID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return nil, err
	}

	it, err := client.ListAllComplete(ctx)
	if err != nil {
		return nil, err
	}

	var ret vm.List
	for it.NotDone() {
		found := it.Value()

		if _, ok := found.Tags[tagRoachprod]; !ok {
			if err := it.NextWithContext(ctx); err != nil {
				return nil, err
			}
			continue
		}

		m := vm.VM{
			Name:        *found.Name,
			Provider:    ProviderName,
			ProviderID:  *found.ID,
			RemoteUser:  remoteUser,
			VPC:         "global",
			MachineType: string(found.HardwareProfile.VMSize),
			// We add a fake availability-zone suffix since other roachprod
			// code assumes particular formats. For example, "eastus2z".
			Zone: *found.Location + "z",
		}

		if createdPtr := found.Tags[tagCreated]; createdPtr == nil {
			m.Errors = append(m.Errors, vm.ErrNoExpiration)
		} else if parsed, err := time.Parse(time.RFC3339, *createdPtr); err == nil {
			m.CreatedAt = parsed
		} else {
			m.Errors = append(m.Errors, vm.ErrNoExpiration)
		}

		if lifetimePtr := found.Tags[tagLifetime]; lifetimePtr == nil {
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

		ret = append(ret, m)

		if err := it.NextWithContext(ctx); err != nil {
			return nil, err
		}

	}

	return ret, nil
}

// Name implements vm.Provider.
func (p *Provider) Name() string {
	return ProviderName
}

func (p *Provider) createVM(
	ctx context.Context,
	group resources.Group,
	subnet network.Subnet,
	name, sshKey string,
	opts vm.CreateOpts,
) (vm compute.VirtualMachine, err error) {
	startupArgs := azureStartupArgs{RemoteUser: remoteUser}
	if !opts.SSDOpts.UseLocalSSD {
		// We define lun42 explicitly in the data disk request below.
		lun := 42
		startupArgs.AttachedDiskLun = &lun
	}
	startupScript, err := evalStartupTemplate(startupArgs)
	if err != nil {
		return vm, err
	}
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return
	}

	client := compute.NewVirtualMachinesClient(*sub.SubscriptionID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return
	}

	// We first need to allocate a NIC to give the VM network access
	ip, err := p.createIP(ctx, group, name)
	if err != nil {
		return
	}
	nic, err := p.createNIC(ctx, group, ip, subnet)
	if err != nil {
		return
	}

	tags := make(map[string]*string)
	tags[tagCreated] = to.StringPtr(timeutil.Now().Format(time.RFC3339))
	tags[tagLifetime] = to.StringPtr(opts.Lifetime.String())
	tags[tagRoachprod] = to.StringPtr("true")

	osVolumeSize := int32(opts.OsVolumeSize)
	if osVolumeSize < 32 {
		log.Print("WARNING: increasing the OS volume size to minimally allowed 32GB")
		osVolumeSize = 32
	}
	// Derived from
	// https://github.com/Azure-Samples/azure-sdk-for-go-samples/blob/79e3f3af791c3873d810efe094f9d61e93a6ccaa/compute/vm.go#L41
	vm = compute.VirtualMachine{
		Location: group.Location,
		Zones:    to.StringSlicePtr([]string{p.opts.zone}),
		Tags:     tags,
		VirtualMachineProperties: &compute.VirtualMachineProperties{
			HardwareProfile: &compute.HardwareProfile{
				VMSize: compute.VirtualMachineSizeTypes(p.opts.machineType),
			},
			StorageProfile: &compute.StorageProfile{
				ImageReference: &compute.ImageReference{
					Publisher: to.StringPtr("Canonical"),
					Offer:     to.StringPtr("UbuntuServer"),
					Sku:       to.StringPtr("18.04-LTS"),
					Version:   to.StringPtr("latest"),
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

		switch p.opts.diskCaching {
		case "read-only":
			caching = compute.CachingTypesReadOnly
		case "read-write":
			caching = compute.CachingTypesReadWrite
		case "none":
			caching = compute.CachingTypesNone
		default:
			err = errors.Newf("unsupported caching behavior: %s", p.opts.diskCaching)
			return
		}
		dataDisks := []compute.DataDisk{
			{
				DiskSizeGB: to.Int32Ptr(p.opts.networkDiskSize),
				Caching:    caching,
				Lun:        to.Int32Ptr(42),
			},
		}

		switch p.opts.networkDiskType {
		case "ultra-disk":
			var ultraDisk compute.Disk
			ultraDisk, err = p.createUltraDisk(ctx, group, name+"-ultra-disk")
			if err != nil {
				return
			}
			// UltraSSD specific disk configurations.
			dataDisks[0].CreateOption = compute.DiskCreateOptionTypesAttach
			dataDisks[0].Name = ultraDisk.Name
			dataDisks[0].ManagedDisk = &compute.ManagedDiskParameters{
				StorageAccountType: compute.StorageAccountTypesUltraSSDLRS,
				ID:                 ultraDisk.ID,
			}

			// UltraSSDs must be enabled separately.
			vm.AdditionalCapabilities = &compute.AdditionalCapabilities{
				UltraSSDEnabled: to.BoolPtr(true),
			}
		case "premium-disk":
			// premium-disk specific disk configurations.
			dataDisks[0].CreateOption = compute.DiskCreateOptionTypesEmpty
			dataDisks[0].ManagedDisk = &compute.ManagedDiskParameters{
				StorageAccountType: compute.StorageAccountTypesPremiumLRS,
			}
		default:
			err = errors.Newf("unsupported network disk type: %s", p.opts.networkDiskType)
			return
		}

		vm.StorageProfile.DataDisks = &dataDisks
	}
	future, err := client.CreateOrUpdate(ctx, *group.Name, name, vm)
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
	ctx context.Context, group resources.Group, ip network.PublicIPAddress, subnet network.Subnet,
) (iface network.Interface, err error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return
	}
	client := network.NewInterfacesClient(*sub.SubscriptionID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return
	}

	p.mu.Lock()
	sg := p.mu.securityGroups[p.getVnetNetworkSecurityGroupName(*group.Location)]
	p.mu.Unlock()

	future, err := client.CreateOrUpdate(ctx, *group.Name, *ip.Name, network.Interface{
		Name:     ip.Name,
		Location: group.Location,
		InterfacePropertiesFormat: &network.InterfacePropertiesFormat{
			IPConfigurations: &[]network.InterfaceIPConfiguration{
				{
					Name: to.StringPtr("ipConfig"),
					InterfaceIPConfigurationPropertiesFormat: &network.InterfaceIPConfigurationPropertiesFormat{
						Subnet:                    &subnet,
						PrivateIPAllocationMethod: network.Dynamic,
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
		log.Printf("created NIC %s %s", *iface.Name, *(*iface.IPConfigurations)[0].PrivateIPAddress)
	}
	return
}

func (p *Provider) getOrCreateNetworkSecurityGroup(
	ctx context.Context, name string, resourceGroup resources.Group,
) (network.SecurityGroup, error) {
	p.mu.Lock()
	group, ok := p.mu.securityGroups[name]
	p.mu.Unlock()
	if ok {
		return group, nil
	}

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return network.SecurityGroup{}, err
	}
	client := network.NewSecurityGroupsClient(*sub.SubscriptionID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return network.SecurityGroup{}, err
	}
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return network.SecurityGroup{}, err
	}

	cacheAndReturn := func(group network.SecurityGroup) (network.SecurityGroup, error) {
		p.mu.Lock()
		p.mu.securityGroups[name] = group
		p.mu.Unlock()
		return group, nil
	}

	future, err := client.CreateOrUpdate(ctx, *resourceGroup.Name, name, network.SecurityGroup{
		SecurityGroupPropertiesFormat: &network.SecurityGroupPropertiesFormat{
			SecurityRules: &[]network.SecurityRule{
				{
					Name: to.StringPtr("SSH_Inbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(300),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionInbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("22"),
					},
				},
				{
					Name: to.StringPtr("SSH_Outbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(301),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionOutbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("*"),
					},
				},
				{
					Name: to.StringPtr("HTTP_Inbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(320),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionInbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("80"),
					},
				},
				{
					Name: to.StringPtr("HTTP_Outbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(321),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionOutbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("*"),
					},
				},
				{
					Name: to.StringPtr("HTTPS_Inbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(340),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionInbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("443"),
					},
				},
				{
					Name: to.StringPtr("HTTPS_Outbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(341),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionOutbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("*"),
					},
				},
				{
					Name: to.StringPtr("CockroachPG_Inbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(342),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionInbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("26257"),
					},
				},
				{
					Name: to.StringPtr("CockroachAdmin_Inbound"),
					SecurityRulePropertiesFormat: &network.SecurityRulePropertiesFormat{
						Priority:                 to.Int32Ptr(343),
						Protocol:                 network.SecurityRuleProtocolTCP,
						Access:                   network.SecurityRuleAccessAllow,
						Direction:                network.SecurityRuleDirectionInbound,
						SourceAddressPrefix:      to.StringPtr("*"),
						SourcePortRange:          to.StringPtr("*"),
						DestinationAddressPrefix: to.StringPtr("*"),
						DestinationPortRange:     to.StringPtr("26258"),
					},
				},
			},
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
	ctx context.Context, locations []string,
) (map[string]network.VirtualNetwork, error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return nil, err
	}

	groupsClient := resources.NewGroupsClient(*sub.SubscriptionID)

	vnetResourceGroupTags := make(map[string]*string)
	vnetResourceGroupTags[tagComment] = to.StringPtr("DO NOT DELETE: Used by all roachprod clusters")
	vnetResourceGroupTags[tagRoachprod] = to.StringPtr("true")

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

	for _, location := range p.opts.locations {
		p.mu.Lock()
		group := p.mu.resourceGroups[vnetResourceGroupName(location)]
		p.mu.Unlock()
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
			p.mu.Lock()
			group := p.mu.resourceGroups[vnetResourceGroupName(location)]
			p.mu.Unlock()
			group, err = setVNetSubnetPrefix(group, prefix)
			if err != nil {
				return nil, errors.Wrapf(err, "for location %q", location)
			}
			// We just updated the VNet Subnet prefix on the resource group -- update
			// the cached entry to reflect that.
			p.mu.Lock()
			p.mu.resourceGroups[vnetResourceGroupName(location)] = group
			p.mu.Unlock()
		}
	}

	// Now, we can ensure that the VNet exists with the requested subnet.
	// TODO(arul): Does this need to be done for all locations or just for the
	// locations that didn't have a subnet/vnet before? I'm inclined to say the
	// latter, but I'm leaving the existing behavior as is.
	ret := make(map[string]network.VirtualNetwork)
	vnets := make([]network.VirtualNetwork, len(ret))
	for location, prefix := range prefixesByLocation {
		p.mu.Lock()
		resourceGroup := p.mu.resourceGroups[vnetResourceGroupName(location)]
		networkSecurityGroup := p.mu.securityGroups[p.getVnetNetworkSecurityGroupName(location)]
		p.mu.Unlock()
		if vnet, _, err := p.createVNet(ctx, resourceGroup, networkSecurityGroup, prefix); err == nil {
			ret[location] = vnet
			vnets = append(vnets, vnet)
		} else {
			return nil, errors.Wrapf(err, "for location %q", location)
		}
	}

	// We only need to create peerings if there are new subnets.
	if newSubnetsCreated {
		return ret, p.createVNetPeerings(ctx, vnets)
	}
	return ret, nil
}

// createVNet creates or retrieves a named VNet object using the 10.<offset>/16 prefix.
// A single /18 subnet will be created within the VNet.
// The results  will be memoized in the Provider.
func (p *Provider) createVNet(
	ctx context.Context,
	resourceGroup resources.Group,
	securityGroup network.SecurityGroup,
	prefix int,
) (vnet network.VirtualNetwork, subnet network.Subnet, err error) {
	vnetName := p.opts.vnetName

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return
	}
	client := network.NewVirtualNetworksClient(*sub.SubscriptionID)
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
	p.mu.subnets[*resourceGroup.Location] = subnet
	p.mu.Unlock()
	log.Printf("created Azure VNet %q in %q with prefix %d", vnetName, *resourceGroup.Name, prefix)
	return
}

// createVNetPeerings creates a fully-connected graph of peerings
// between the provided vnets.
func (p *Provider) createVNetPeerings(ctx context.Context, vnets []network.VirtualNetwork) error {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return err
	}
	client := network.NewVirtualNetworkPeeringsClient(*sub.SubscriptionID)
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

			future, err := client.CreateOrUpdate(ctx, outerParts.resourceGroup, *outer.Name, linkName, peering)
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
		log.Printf("created vnet peering %s", *peering.Name)
	}

	return nil
}

// createIP allocates an IP address that will later be bound to a NIC.
func (p *Provider) createIP(
	ctx context.Context, group resources.Group, name string,
) (ip network.PublicIPAddress, err error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return
	}
	ipc := network.NewPublicIPAddressesClient(*sub.SubscriptionID)
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
			Zones:    to.StringSlicePtr([]string{p.opts.zone}),
			PublicIPAddressPropertiesFormat: &network.PublicIPAddressPropertiesFormat{
				PublicIPAddressVersion:   network.IPv4,
				PublicIPAllocationMethod: network.Static,
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
		log.Printf("created Azure IP %s", *ip.Name)
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

	nicClient := network.NewInterfacesClient(*sub.SubscriptionID)
	if nicClient.Authorizer, err = p.getAuthorizer(); err != nil {
		return err
	}

	ipClient := network.NewPublicIPAddressesClient(*sub.SubscriptionID)
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
	p.mu.Lock()
	group, ok := p.mu.resourceGroups[name]
	p.mu.Unlock()
	if ok {
		return group, nil
	}

	cacheAndReturn := func(group resources.Group) (resources.Group, error) {
		p.mu.Lock()
		p.mu.resourceGroups[name] = group
		p.mu.Unlock()
		return group, nil
	}

	sub, err := p.getSubscription(ctx)
	if err != nil {
		return resources.Group{}, err
	}

	client := resources.NewGroupsClient(*sub.SubscriptionID)
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
	ctx context.Context, group resources.Group, name string,
) (compute.Disk, error) {
	sub, err := p.getSubscription(ctx)
	if err != nil {
		return compute.Disk{}, err
	}

	client := compute.NewDisksClient(*sub.SubscriptionID)
	if client.Authorizer, err = p.getAuthorizer(); err != nil {
		return compute.Disk{}, err
	}

	future, err := client.CreateOrUpdate(ctx, *group.Name, name,
		compute.Disk{
			Zones:    to.StringSlicePtr([]string{p.opts.zone}),
			Location: group.Location,
			Sku: &compute.DiskSku{
				Name: compute.UltraSSDLRS,
			},
			DiskProperties: &compute.DiskProperties{
				CreationData: &compute.CreationData{
					CreateOption: compute.Empty,
				},
				DiskSizeGB:        to.Int32Ptr(p.opts.networkDiskSize),
				DiskIOPSReadWrite: to.Int64Ptr(p.opts.ultraDiskIOPS),
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
	log.Printf("created ultra-disk: %s\n", *disk.Name)
	return disk, err
}

// getSubscription chooses the first available subscription. The value
// is memoized in the Provider instance.
func (p *Provider) getSubscription(
	ctx context.Context,
) (sub subscriptions.Subscription, err error) {
	p.mu.Lock()
	sub = p.mu.subscription
	p.mu.Unlock()

	if sub.SubscriptionID != nil {
		return
	}

	sc := subscriptions.NewClient()
	if sc.Authorizer, err = p.getAuthorizer(); err != nil {
		return
	}

	page, err := sc.List(ctx)
	if err == nil {
		if len(page.Values()) == 0 {
			err = errors.New("did not find Azure subscription")
			return sub, err
		}
		sub = page.Values()[0]

		p.mu.Lock()
		p.mu.subscription = page.Values()[0]
		p.mu.Unlock()
	}
	return
}
