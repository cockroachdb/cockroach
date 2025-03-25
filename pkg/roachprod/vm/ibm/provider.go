// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/globaltaggingv1"
	"github.com/IBM/platform-services-go-sdk/resourcecontrollerv2"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"
	"github.com/IBM/vpc-go-sdk/vpcv1"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	ProviderName = "ibm"

	defaultAPIKeyEnvVarPrefix = "IBM"

	defaultMachineType = "cz2-4x8"
	defaultCPUArch     = vm.ArchS390x
	defaultCPUFamily   = "IBM zSystem"
	defaultRemoteUser  = "ubuntu"
	defaultImageAMI    = "ibm-ubuntu-22-04-4-minimal-s390x-3"
	defaultNTPServer   = "time.adn.networklayer.com"

	// Default values for volumes
	defaultSystemVolumeSize = 100
	defaultVolumeType       = "general-purpose" // 3 IOPS/GB
	systemVolumeSizeRange   = "[100-250]"

	InstanceNotInitialized = "not-initialized"
	InstanceInvalidStatus  = "invalid-status"
	InstanceInvalidTags    = "invalid-tags"
	InstanceNotRochprod    = "not-roachprod"
)

var (
	// DefaultZones is the list of availability zones used by default for
	// cluster creation.
	// If the geo flag is specified, nodes are distributed between zones.
	defaultZones = []string{
		"ca-tor-1",
		"ca-tor-2",
		"br-sao-1",
	}

	// Errors used in the package
	ErrKeyNotFound        = fmt.Errorf("no SSH key found")
	ErrRegionNotSupported = fmt.Errorf("region not supported by provider instance")
	ErrInvalidZoneFormat  = fmt.Errorf("invalid zone format")
	ErrImageNotFound      = fmt.Errorf("image not found")
	ErrImageObsolete      = fmt.Errorf("image is obsolete, and cannot be used")
	ErrMissingAPIKey      = fmt.Errorf("missing IBM_APIKEY environment variable")
)

// providerInstance is the global instance of the IBM provider used by the
// roachprod CLI.
var providerInstance = &Provider{}

// Init initializes the IBM provider instance for the roachprod CLI.
func Init() (err error) {

	providerInstance, err = NewProvider()
	if err != nil {
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, err.Error())
		return nil
	}

	vm.Providers[ProviderName] = providerInstance

	return nil
}

// Provider implements the vm.Provider interface for IBM.
type Provider struct {
	config *ibmCloudConfig

	regions         map[string]struct{}
	cachedIdentity  string
	cachedAccountID string
	cachedImageIDs  map[string]map[string]string
	cachedSSHKeyIDs map[string]map[string]string
	cacheMutex      syncutil.Mutex

	authenticator             core.Authenticator
	vpcServices               map[string]*vpcv1.VpcV1
	tagService                *globaltaggingv1.GlobalTaggingV1
	resourceControllerService *resourcecontrollerv2.ResourceControllerV2
	resourceManagerService    *resourcemanagerv2.ResourceManagerV2

	// GCAccounts is a list of accounts to use during garbage collection.
	// These are identifiers, expected to match API keys in the environment
	// with the format: IBM_<account>_APIKEY.
	GCAccounts []string
}

// NewProvider creates a new IBM provider.
func NewProvider(options ...Option) (p *Provider, err error) {

	p = &Provider{
		vpcServices:     make(map[string]*vpcv1.VpcV1),
		regions:         make(map[string]struct{}),
		cachedImageIDs:  make(map[string]map[string]string),
		cachedSSHKeyIDs: make(map[string]map[string]string),
	}
	for _, option := range options {
		option.apply(p)
	}

	// If an authenticator is not provided, create one from the environment.
	if p.authenticator == nil {
		p.authenticator, err = core.GetAuthenticatorFromEnvironment(defaultAPIKeyEnvVarPrefix)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"failed to create default IBM authenticator from environment variables",
			)
		}
	}

	// Build the list of supported regions if none are provided.
	if len(p.regions) == 0 {
		for _, region := range supportedRegions {
			p.regions[region] = struct{}{}
		}
	}

	// Init the regions and services.
	for _, region := range supportedRegions {

		if _, ok := p.regions[region]; !ok {
			continue
		}

		// Init the cache for images and SSH keys.
		p.cachedImageIDs[region] = make(map[string]string)
		p.cachedSSHKeyIDs[region] = make(map[string]string)

		// Create the VPC service for the region.
		p.vpcServices[region], err = vpcv1.NewVpcV1(&vpcv1.VpcV1Options{
			Authenticator: p.authenticator,
			URL:           fmt.Sprintf(defaultServiceURL, region),
		})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create VPC service for region %s", region)
		}
	}

	// Create Global Tagging service.
	p.tagService, err = globaltaggingv1.NewGlobalTaggingV1(&globaltaggingv1.GlobalTaggingV1Options{
		Authenticator: p.authenticator,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Global Tagging service")
	}

	// Resource Controller service is used to get resource details like preemption events.
	p.resourceControllerService, err = resourcecontrollerv2.NewResourceControllerV2(
		&resourcecontrollerv2.ResourceControllerV2Options{
			Authenticator: p.authenticator,
		},
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Resource Controller service")
	}

	// Resource Manager service is used to get/create the resource group
	p.resourceManagerService, err = resourcemanagerv2.NewResourceManagerV2(
		&resourcemanagerv2.ResourceManagerV2Options{
			Authenticator: p.authenticator,
		},
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Resource Manager service")
	}

	// Configure the IBM Cloud account and get the required resource IDs.
	p.config, err = p.configureCloudAccountInitial()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure IBM Cloud account")
	}

	return p, nil
}

// Name is part of the vm.Provider interface.
// It returns the name of the provider.
func (p *Provider) Name() string {
	return ProviderName
}

// Active is part of the vm.Provider interface.
func (p *Provider) Active() bool {
	return true
}

// List queries the IBM Cloud API to return all Roachprod VMs across all regions.
func (p *Provider) List(l *logger.Logger, opts vm.ListOptions) (vm.List, error) {

	var ret vm.List
	var mux syncutil.Mutex
	var g errgroup.Group

	for r := range p.vpcServices {
		g.Go(func() error {
			vms, err := p.listRegion(l, r, opts)
			if err != nil {
				// Failing to list VMs in a region is not fatal.
				l.Printf("failed to list IBM VMs in region: %s\n%v\n", r, err)
				return nil
			}

			mux.Lock()
			defer mux.Unlock()

			ret = append(ret, vms...)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return ret, nil
}

// Create is part of the vm.Provider interface.
// It creates a list of VMs in the specified zones and regions.
func (p *Provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, vmProviderOpts vm.ProviderOpts,
) (vm.List, error) {

	providerOpts := vmProviderOpts.(*ProviderOpts)

	// Check if the user provided a list of zones and match them to the vms.
	expandedZones, zones, err := p.computeZones(opts, providerOpts, len(names))
	if err != nil {
		return nil, errors.Wrap(err, "unable to compute zones during creation")
	}

	// Create requires all resources to be created in the IBM Cloud account.
	err = p.configureCloudAccountFull(p.config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure IBM Cloud account")
	}

	// Check the user provided arguments.
	err = p.checkCreateOpts(&opts, providerOpts, expandedZones)
	if err != nil {
		return nil, err
	}

	// Ensure that the SSH key(s) have been distributed to all regions.
	if err := p.ConfigSSH(l, expandedZones); err != nil {
		return nil, err
	}

	// Build the cluster tags.
	tags, err := p.computeLabels(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to compute labels during creation")
	}

	// Compute user's SSH key name.
	keyName, err := p.sshKeyName(l)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get SSH key name")
	}

	var g errgroup.Group
	var mutex syncutil.Mutex
	vms := make(vm.List, len(names))
	for i, vmName := range names {

		zone := zones[i]

		region, err := p.zoneToRegion(zone)
		if err != nil {
			return nil, errors.Wrap(err, "unable to compute zone from region")
		}

		// Get the image ID for the region.
		imageID, err := p.getImageIDForRegion(l, providerOpts.ImageAMI, region)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to get image ID for region %s", region)
		}

		// Get the SSH key ID for the region.
		sshKeyID, err := p.getSshKeyID(l, keyName, region)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to get SSH key ID for region %s", region)
		}

		g.Go(func() error {
			vm, err := p.createInstance(l, instanceOptions{
				vmName: vmName,
				region: region,
				zone:   zone,
				infraOpts: &infraOpts{
					vpcID:    p.config.regions[region].vpcID,
					subnetID: p.config.regions[region].zones[zone].subnetID,
					imageID:  imageID,
					sshKeyID: sshKeyID,
				},
				vmOpts:       opts,
				providerOpts: providerOpts,
				tags:         tags,
			})
			if err != nil {
				return errors.Wrapf(
					err,
					"failed to create instance %s in region %s", vmName, region,
				)
			}

			mutex.Lock()
			defer mutex.Unlock()
			vms[i] = *vm

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create instances")
	}

	return vms, nil
}

// Delete is part of the vm.Provider interface.
// It deletes the VMs in the list.
func (p *Provider) Delete(l *logger.Logger, vms vm.List) error {

	var g errgroup.Group
	for _, vm := range vms {

		g.Go(func() error {

			zone := vm.Zone

			// We need to query the instance to get the floating IP address
			// and the attached volumes.
			instance, err := newInstanceFromCRN(p, vm.ProviderID, true)
			if err != nil {
				return errors.Wrapf(err, "failed to get instance from CRN %s", vm.ProviderID)
			}

			vpcService, err := p.getVpcServiceFromZone(zone)
			if err != nil {
				return errors.Wrapf(err, "failed to get VPC service for zone %s", zone)
			}

			// Start by deleting the floating IP address if any
			if instance.networkInterface != nil {
				for _, fip := range instance.networkInterface.NetworkInterface.FloatingIps {
					_, err = vpcService.DeleteFloatingIP(&vpcv1.DeleteFloatingIPOptions{
						ID: fip.ID,
					})
					if err != nil {
						return errors.Wrapf(
							err, "failed to delete floating IP address %s in zone %s",
							*instance.networkInterface.ID,
							zone,
						)
					}
				}
			}

			// Then finish by deleting the instance
			_, err = vpcService.DeleteInstance(&vpcv1.DeleteInstanceOptions{
				ID: instance.instance.ID,
			})
			if err != nil {
				return errors.Wrapf(
					err, "failed to delete instance %s in zone %s",
					*instance.instance.ID,
					zone,
				)
			}

			return nil
		})

	}

	return g.Wait()
}

// Reset is part of the vm.Provider interface.
// It resets the VMs in the list.
func (p *Provider) Reset(l *logger.Logger, vms vm.List) error {

	var g errgroup.Group
	for _, vm := range vms {

		zone := vm.Zone
		instance, err := newInstanceFromCRN(p, vm.ProviderID, false /* query */)
		if err != nil {
			return errors.Wrapf(err, "failed to get instance from CRN %s", vm.ProviderID)
		}

		vpcService, err := p.getVpcServiceFromZone(zone)
		if err != nil {
			return errors.Wrapf(err, "failed to get VPC service for zone %s", zone)
		}

		g.Go(func() error {
			_, _, err = vpcService.CreateInstanceAction(&vpcv1.CreateInstanceActionOptions{
				InstanceID: instance.instance.ID,
				Type:       core.StringPtr("reboot"),
				Force:      core.BoolPtr(true),
			})
			if err != nil {
				return errors.Wrapf(
					err, "failed to reset instance %s in zone %s",
					*instance.instance.ID,
					zone,
				)
			}

			return nil
		})

	}

	return g.Wait()
}

// ConfigSSH is part of the vm.Provider interface.
// This method ensures that the user's public key is uploaded to all IBM Cloud
// regions part of the cluster so that it can be used to SSH into the instances.
func (p *Provider) ConfigSSH(l *logger.Logger, zones []string) error {
	keyName, err := p.sshKeyName(l)
	if err != nil {
		return err
	}

	// Convert the list of zones to a unique list of regions.
	regions, err := p.zonesToRegions(zones)
	if err != nil {
		return err
	}

	// Ensure that for each region we're operating in, we have
	// a <user>-<hash> keypair where <hash> is a hash of the public key.
	// We use a hash since a user probably has multiple machines they're
	// running roachprod on and these machines (ought to) have separate
	// ssh keypairs.  If the remote keypair doesn't exist, we'll upload
	// the user's ~/.ssh/id_rsa.pub file or ask them to generate one.
	var g errgroup.Group
	for _, r := range regions {
		g.Go(func() error {
			_, err := p.getSshKeyID(l, keyName, r)
			if err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					err = p.sshKeyImport(l, keyName, r)
					if err != nil {
						return errors.Wrapf(err, "unable to import SSH key in region %s", r)
					}
				} else {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// AddLabels is part of the vm.Provider interface.
// It adds the specified labels to the VMs in the list.
func (p *Provider) AddLabels(l *logger.Logger, vms vm.List, labels map[string]string) error {

	tags := []string{}
	for key, value := range labels {
		tags = append(tags, fmt.Sprintf("%s:%s", key, value))
	}

	var resources []globaltaggingv1.Resource
	for _, vm := range vms {
		resources = append(resources, globaltaggingv1.Resource{
			ResourceID: &vm.ProviderID,
		})
	}

	_, _, err := p.tagService.AttachTag(&globaltaggingv1.AttachTagOptions{
		Resources: resources,
		TagNames:  tags,
		Update:    core.BoolPtr(true),
	})
	if err != nil {
		return errors.Wrap(err, "failed to attach tags to resources")
	}

	return nil
}

// RemoveLabels is part of the vm.Provider interface.
// It removes the specified labels from the VMs in the list.
func (p *Provider) RemoveLabels(l *logger.Logger, vms vm.List, labels []string) error {

	if len(vms) == 0 || len(labels) == 0 {
		return nil
	}

	labelsMap := make(map[string]struct{})
	for _, label := range labels {
		labelsMap[label] = struct{}{}
	}

	// We assume all instances will have the same tags, and we query the current tags
	// for the first instance in the list.
	// We need to do this because labels are stored as tags under key:value format
	// and we cannot remove them by key alone.
	instance, err := newInstanceFromCRN(p, vms[0].ProviderID, false /* query */)
	if err != nil {
		return errors.Wrapf(err, "failed to get instance from CRN %s", vms[0].ProviderID)
	}

	currentLabels, err := instance.getTagsAsMap()
	if err != nil {
		return errors.Wrapf(err, "failed to get tags for instance %s", vms[0].ProviderID)
	}

	var labelsToRemove []string
	for key, value := range currentLabels {
		if _, ok := labelsMap[key]; ok {
			labelsToRemove = append(labelsToRemove, fmt.Sprintf("%s:%s", key, value))
		}
	}

	// If it turns out that the labels we want to remove are not present,
	// we can skip the API call.
	if len(labelsToRemove) == 0 {
		return nil
	}

	var resources []globaltaggingv1.Resource
	for _, vm := range vms {
		resources = append(resources, globaltaggingv1.Resource{
			ResourceID: &vm.ProviderID,
		})
	}
	_, _, err = p.tagService.DetachTag(&globaltaggingv1.DetachTagOptions{
		Resources: resources,
		TagNames:  labelsToRemove,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to detach tags from instances")
	}

	return nil

}

// Extend is part of the vm.Provider interface.
// It extends the lifetime of the VMs in the list by adding or updating a label
// with the specified lifetime.
func (p *Provider) Extend(l *logger.Logger, vms vm.List, lifetime time.Duration) error {
	return p.AddLabels(l, vms, map[string]string{
		vm.TagLifetime: lifetime.String(),
	})
}

// SupportsSpotVMs is part of the vm.Provider interface.
// There is no support for spot s390x VMs in IBM Cloud yet.
func (p *Provider) SupportsSpotVMs() bool {
	return false
}

// GetPreemptedSpotVMs is part of the vm.Provider interface.
// There is no support for spot s390x VMs in IBM Cloud yet.
func (p *Provider) GetPreemptedSpotVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {

	preemptedMap := make(map[string]string)
	for _, vm := range vms {
		preemptedMap[vm.ProviderID] = vm.Name
	}

	rcs := p.getResourceControllerService()
	reclamationsList, _, err := rcs.ListReclamations(
		rcs.NewListReclamationsOptions().
			SetAccountID(p.config.accountID).
			SetResourceGroupID(p.config.roachprodResourceGroupID),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list reclamations")
	}

	var preemptedVMs []vm.PreemptedVM
	for _, reclamation := range reclamationsList.Resources {

		if name, ok := preemptedMap[*reclamation.ResourceInstanceID]; !ok {
			// If the reclamation is not for a VM we're looking into, skip it.
			continue
		} else if !(&resourcecontrollerv2Reclamation{Reclamation: &reclamation}).wasReclaimed() {
			// If reclamation is planned but hasn't happened yet, skip it.
			continue
		} else {
			preemptedVMs = append(preemptedVMs, vm.PreemptedVM{
				Name:        name,
				PreemptedAt: time.Time(*reclamation.UpdatedAt),
			})
		}

	}

	return preemptedVMs, nil
}

// FindActiveAccount is part of the vm.Provider interface.
func (p *Provider) FindActiveAccount(l *logger.Logger) (string, error) {

	identity, _, err := p.getIdentityFromAuthenticator()
	if err != nil {
		return "", errors.Wrap(err, "failed to get identity from authenticator")
	}

	// If this is in an email address format, we just want the username.
	username, _, _ := strings.Cut(identity, "@")
	return username, nil
}

//
// Unimplemented methods, no plans to implement as of now.
//

// CleanSSH is part of the vm.Provider interface.
func (p *Provider) CleanSSH(_ *logger.Logger) error {
	return nil
}

// GetHostErrorVMs is part of the vm.Provider interface.
func (p *Provider) GetHostErrorVMs(_ *logger.Logger, _ vm.List, _ time.Time) ([]string, error) {
	return nil, nil
}

// GetVMSpecs is part of the vm.Provider interface.
func (p *Provider) GetVMSpecs(
	_ *logger.Logger, _ vm.List,
) (map[string]map[string]interface{}, error) {
	return nil, nil
}

// Grow is part of the vm.Provider interface.
func (p *Provider) Grow(_ *logger.Logger, _ vm.List, _ string, _ []string) (vm.List, error) {
	return nil, vm.UnimplementedError
}

// Shrink is part of the vm.Provider interface.
func (p *Provider) Shrink(_ *logger.Logger, _ vm.List, _ string) error {
	return vm.UnimplementedError
}

// ProjectActive is part of the vm.Provider interface.
func (p *Provider) ProjectActive(project string) bool {
	return project == ""
}

// CreateVolume is part of the vm.Provider interface.
func (p *Provider) CreateVolume(_ *logger.Logger, _ vm.VolumeCreateOpts) (vm.Volume, error) {
	return vm.Volume{}, vm.UnimplementedError
}

// ListVolumes is part of the vm.Provider interface.
func (p *Provider) ListVolumes(_ *logger.Logger, _ *vm.VM) ([]vm.Volume, error) {
	return nil, vm.UnimplementedError
}

// DeleteVolume is part of the vm.Provider interface.
func (p *Provider) DeleteVolume(_ *logger.Logger, _ vm.Volume, _ *vm.VM) error {
	return vm.UnimplementedError
}

// AttachVolume is part of the vm.Provider interface.
func (p *Provider) AttachVolume(_ *logger.Logger, _ vm.Volume, _ *vm.VM) (string, error) {
	return "", vm.UnimplementedError
}

// CreateVolumeSnapshot is part of the vm.Provider interface.
func (p *Provider) CreateVolumeSnapshot(
	_ *logger.Logger, _ vm.Volume, _ vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	return vm.VolumeSnapshot{}, vm.UnimplementedError
}

// ListVolumeSnapshots is part of the vm.Provider interface.
func (p *Provider) ListVolumeSnapshots(
	_ *logger.Logger, _ vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	return nil, vm.UnimplementedError
}

// DeleteVolumeSnapshots is part of the vm.Provider interface.
func (p *Provider) DeleteVolumeSnapshots(_ *logger.Logger, _ ...vm.VolumeSnapshot) error {
	return vm.UnimplementedError
}

// CreateLoadBalancer is part of the vm.Provider interface.
func (p *Provider) CreateLoadBalancer(_ *logger.Logger, _ vm.List, _ int) error {
	return vm.UnimplementedError
}

// DeleteLoadBalancer is part of the vm.Provider interface.
func (p *Provider) DeleteLoadBalancer(_ *logger.Logger, _ vm.List, _ int) error {
	return vm.UnimplementedError
}

// ListLoadBalancers is part of the vm.Provider interface.
func (p *Provider) ListLoadBalancers(_ *logger.Logger, _ vm.List) ([]vm.ServiceAddress, error) {
	return nil, nil
}
