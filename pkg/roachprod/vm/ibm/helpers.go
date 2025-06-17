// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/networking-go-sdk/transitgatewayapisv1"
	"github.com/IBM/platform-services-go-sdk/globalsearchv2"
	"github.com/IBM/platform-services-go-sdk/globaltaggingv1"
	"github.com/IBM/platform-services-go-sdk/resourcecontrollerv2"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"
	"github.com/IBM/vpc-go-sdk/vpcv1"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/ssh"
	"golang.org/x/sync/errgroup"
)

const (
	maxFloatingIPAttempts   = 5
	ErrUnsupportedLocalSSDs = "WARN: local SSDs are not supported by IBM Cloud; using network volumes instead"
)

// getVpcService returns the VPC service for a given region.
func (p *Provider) getVpcService(region string) (*vpcv1.VpcV1, error) {
	vpcService, ok := p.vpcServices[region]
	if !ok {
		return nil, fmt.Errorf("region %s not supported by this provider instance", region)
	}
	return vpcService, nil
}

// getVpcServiceFromZone returns the VPC service for a given zone.
func (p *Provider) getVpcServiceFromZone(zone string) (*vpcv1.VpcV1, error) {
	region, err := p.zoneToRegion(zone)
	if err != nil {
		return nil, err
	}
	return p.getVpcService(region)
}

// getTagService returns the Global Tagging service.
func (p *Provider) getTagService() *globaltaggingv1.GlobalTaggingV1 {
	return p.tagService
}

// getResourceControllerService returns the Resource Controller service.
func (p *Provider) getResourceControllerService() *resourcecontrollerv2.ResourceControllerV2 {
	return p.resourceControllerService
}

// getResourceManagerService returns the Resource Manager service.
func (p *Provider) getResourceManagerService() *resourcemanagerv2.ResourceManagerV2 {
	return p.resourceManagerService
}

// getGlobalSearchService returns the Global Search service.
func (p *Provider) getGlobalSearchService() *globalsearchv2.GlobalSearchV2 {
	return p.globalSearchService
}

// getTransitGatewayService returns the Transit Gateway service.
func (p *Provider) getTransitGatewayService() *transitgatewayapisv1.TransitGatewayApisV1 {
	return p.transitgatewayService
}

// instanceOptions holds the information needed to create an instance.
type instanceOptions struct {
	vmName       string
	region       string
	zone         string
	tags         attachedTags
	vmOpts       vm.CreateOpts
	providerOpts *ProviderOpts
	infraOpts    *infraOpts
}

// infraOpts holds the information needed to create an instance.
type infraOpts struct {
	vpcID    string
	imageID  string
	subnetID string
	sshKeyID string
}

// createInstance creates a new instance in the given region and zone.
func (p *Provider) createInstance(
	l *logger.Logger, opts instanceOptions, maxWaitForReadyState time.Duration,
) (*vm.VM, error) {

	vpcService, err := p.getVpcService(opts.region)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get VPC service for region")
	}

	// Create the startup script
	startupScript, err := p.startupScript(startupArgs{
		StartupArgs: vm.DefaultStartupArgs(
			vm.WithVMName(opts.vmName),
			vm.WithSharedUser(opts.providerOpts.RemoteUserName),
			vm.WithChronyServers([]string{defaultNTPServer}),
			vm.WithZfs(opts.vmOpts.SSDOpts.FileSystem == vm.Zfs),
		),
		UseMultipleDisks: opts.providerOpts.UseMultipleDisks,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create startup script for instance %s", opts.vmName)
	}

	// Build the boot volume information.
	bootVolume := &volumeAttachment{
		Capacity:        opts.vmOpts.OsVolumeSize,
		Name:            fmt.Sprintf("%s-boot", opts.vmName),
		Zone:            opts.zone,
		Profile:         defaultVolumeType,
		ResourceGroupID: p.config.roachprodResourceGroupID,
		UserTags:        opts.tags,
	}

	// Build the attached volumes information.
	// Let's start with the default data disk.
	attachedVolumes := volumeAttachments{
		&volumeAttachment{
			Name:                   fmt.Sprintf("%s-data-%04d", opts.vmName, 0),
			ResourceGroupID:        p.config.roachprodResourceGroupID,
			Capacity:               opts.providerOpts.DefaultVolume.VolumeSize,
			IOPS:                   opts.providerOpts.DefaultVolume.IOPS,
			Profile:                opts.providerOpts.DefaultVolume.VolumeType,
			UserTags:               opts.tags,
			DeleteOnInstanceDelete: true,
		},
	}
	// Then we add any additional attached volumes.
	for i, attachedVolume := range opts.providerOpts.AttachedVolumes {
		attachedVolumes = append(attachedVolumes, &volumeAttachment{
			Name:                   fmt.Sprintf("%s-data-%04d", opts.vmName, i+1),
			ResourceGroupID:        p.config.roachprodResourceGroupID,
			Capacity:               attachedVolume.VolumeSize,
			IOPS:                   attachedVolume.IOPS,
			Profile:                attachedVolume.VolumeType,
			UserTags:               opts.tags,
			DeleteOnInstanceDelete: true,
		})
	}

	// Determine what happen in case of a host failure or maintenance.
	hostFailureAction := "restart"
	if opts.providerOpts.TerminateOnMigration {
		hostFailureAction = "stop"
	}

	// Create the instance
	instanceResp, _, err := vpcService.CreateInstance(
		vpcService.NewCreateInstanceOptions(
			&vpcv1.InstancePrototype{
				AvailabilityPolicy: &vpcv1.InstanceAvailabilityPolicyPrototype{
					HostFailure: core.StringPtr(hostFailureAction),
				},
				BootVolumeAttachment: bootVolume.toVolumeAttachmentPrototypeInstanceByImageContext(),
				Image: &vpcv1.ImageIdentity{
					ID: &opts.infraOpts.imageID,
				},
				Keys: []vpcv1.KeyIdentityIntf{
					&vpcv1.KeyIdentity{
						ID: &opts.infraOpts.sshKeyID,
					},
				},
				MetadataService: &vpcv1.InstanceMetadataServicePrototype{
					Enabled: core.BoolPtr(true),
				},
				Name: &opts.vmName,
				PrimaryNetworkAttachment: &vpcv1.InstanceNetworkAttachmentPrototype{
					Name: core.StringPtr("eth0"),
					VirtualNetworkInterface: &vpcv1.InstanceNetworkAttachmentPrototypeVirtualNetworkInterface{
						AllowIPSpoofing:         core.BoolPtr(false),
						AutoDelete:              core.BoolPtr(true),
						EnableInfrastructureNat: core.BoolPtr(true),
						Subnet: &vpcv1.SubnetIdentity{
							ID: &opts.infraOpts.subnetID,
						},
					},
				},
				Profile: &vpcv1.InstanceProfileIdentity{
					Name: &opts.providerOpts.MachineType,
				},
				ResourceGroup: &vpcv1.ResourceGroupIdentity{
					ID: &p.config.roachprodResourceGroupID,
				},
				UserData:          &startupScript,
				VolumeAttachments: attachedVolumes.toVpcV1VolumeAttachment(),
				VPC: &vpcv1.VPCIdentity{
					ID: &opts.infraOpts.vpcID,
				},
				Zone: &vpcv1.ZoneIdentity{
					Name: &opts.zone,
				},
			},
		),
	)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to create instance %s in region %s",
			opts.vmName,
			opts.region,
		)
	}

	// Add tags to the instance before doing anything else.
	// This is to ensure that if the floating IP address creation fails,
	// the instance is still tagged and we can destroy the cluster during cleanup.
	_, _, err = p.tagService.AttachTag(opts.tags.toAttachTagOptions(*instanceResp.CRN))
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to attach tags to instance %s in region %s",
			opts.vmName,
			opts.region,
		)
	}

	// Wait for the instance to be in a valid state to get all the information.
	instanceReady := false
	stableState := struct {
		InstanceStatus  string
		NetworkAttached bool
	}{}
	waitForRunningState := retry.Start(retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     1 * time.Second,
		MaxRetries:     int(maxWaitForReadyState.Seconds()),
	})
	for waitForRunningState.Next() {
		instanceResp, _, err = vpcService.GetInstance(&vpcv1.GetInstanceOptions{
			ID: instanceResp.ID,
		})
		if err != nil {
			return nil, err
		}
		lastKnownStatus := core.StringNilMapper(instanceResp.Status)
		if lastKnownStatus == "failed" {
			return nil, fmt.Errorf(
				"instance %s in zone %s failed to start; [instance state: %s | network attached: %t]",
				opts.vmName,
				opts.zone,
				lastKnownStatus,
				stableState.NetworkAttached,
			)
		} else if lastKnownStatus != "running" {
			stableState.InstanceStatus = lastKnownStatus
			continue
		}

		if instanceResp.PrimaryNetworkAttachment == nil ||
			instanceResp.PrimaryNetworkAttachment.VirtualNetworkInterface == nil {
			continue
		}
		stableState.NetworkAttached = true

		// Fill missing boot volume information.
		if instanceResp.BootVolumeAttachment != nil && instanceResp.BootVolumeAttachment.Volume != nil {
			err := bootVolume.updateWithID(
				*instanceResp.BootVolumeAttachment.Volume.ID,
				*instanceResp.BootVolumeAttachment.Volume.CRN,
			)
			if err != nil {
				return nil, errors.Wrapf(
					err,
					"failed to update boot volume %s with ID %s",
					*instanceResp.BootVolumeAttachment.Volume.Name,
					*instanceResp.BootVolumeAttachment.Volume.ID,
				)
			}
		}

		// Fill missing attached volumes information.
		for _, attachedVolume := range instanceResp.VolumeAttachments {
			if attachedVolume.Volume == nil {
				continue
			}

			if *attachedVolume.Volume.ID == bootVolume.ID {
				continue
			}

			err := attachedVolumes.updateWithIDByName(
				*attachedVolume.Volume.Name,
				*attachedVolume.Volume.ID,
				*attachedVolume.Volume.CRN,
			)
			if err != nil {
				return nil, errors.Wrapf(
					err,
					"failed to update attached volume %s with ID %s",
					*attachedVolume.Volume.Name,
					*attachedVolume.Volume.ID,
				)
			}
		}

		instanceReady = true
		break
	}

	if !instanceReady {
		return nil, fmt.Errorf(
			`instance %s didn't reach stable state in %s; [instance state: %s|network attached: %t]`,
			opts.vmName,
			maxWaitForReadyState.String(),
			stableState.InstanceStatus,
			stableState.NetworkAttached,
		)
	}

	// Create and attach a floating IP address to the instance
	fipResp, err := p.attachOrCreateFloatingIPAddress(
		l,
		opts.zone,
		p.config.roachprodResourceGroupID,
		*instanceResp.ID,
		*instanceResp.PrimaryNetworkAttachment.VirtualNetworkInterface.ID,
		0,
	)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to create floating IP for instance %s in zone %s",
			opts.vmName,
			opts.zone,
		)
	}

	// Create an instance object with all the necessary information.
	vm := (&instance{
		provider:         p,
		instance:         instanceResp,
		bootVolume:       bootVolume,
		attachedVolumes:  attachedVolumes,
		networkInterface: (&vpcv1FloatingIP{fipResp}).toVpcv1NetworkInterface(),
		tagList:          &opts.tags,
	}).toVM()

	return &vm, nil
}

// attachOrCreateFloatingIPAddress attempts to attach an unbound floating IP
// to an instance. If no unbound floating IP is found, it creates a new one.
// If the selected floating IP address is already bound to another instance,
// it will retry the operation up to maxFloatingIPAttempts times.
//
// TODO(golgeek): this method logs a lot of information, and we should consider
// removing the logs or changing the log level to debug when it seems stable
// enough.
func (p *Provider) attachOrCreateFloatingIPAddress(
	l *logger.Logger, zone, resourceGroupID, instanceID, networkInterfaceID string, attempt int,
) (*vpcv1.FloatingIP, error) {

	if attempt >= maxFloatingIPAttempts {
		return nil, errors.Errorf(
			"failed to create floating IP address after %d attempts",
			maxFloatingIPAttempts,
		)
	}

	l.Printf(
		"Floating IP requested for instance %s in zone %s (attempt %d)",
		instanceID,
		zone,
		attempt+1,
	)

	vpcService, err := p.getVpcServiceFromZone(zone)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get VPC service for region")
	}

	// List floating IP addresses
	// TODO(golgeek): expose the list of floating IPs as a helper that can be
	// invoked directly through a roachprod CLI command for debugging purposes.
	floatingIPAddressesPager, err := vpcService.NewFloatingIpsPager(
		vpcService.NewListFloatingIpsOptions().
			SetResourceGroupID(resourceGroupID).
			SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create floating IP addresses pager")
	}

	unboundIDs := make([]string, 0)

	for floatingIPAddressesPager.HasNext() {
		floatingIPAddresses, err := floatingIPAddressesPager.GetNext()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get next floating IP addresses page")
		}

		// We're looking for unbound floating IP addresses in the same zone
		// as the instance.
		for _, floatingIPAddress := range floatingIPAddresses {
			if floatingIPAddress.Target != nil {
				continue
			}

			if floatingIPAddress.Zone != nil && *floatingIPAddress.Zone.Name == zone {
				unboundIDs = append(unboundIDs, *floatingIPAddress.ID)
			}
		}

		if len(unboundIDs) > 0 {
			break
		}
	}

	l.Printf(
		"  -> found %d potential IP addresses for instance %s in zone %s...",
		len(unboundIDs),
		instanceID,
		zone,
	)

	// If we found unbound IP addresses, we can reuse one of them.
	// We'll select a random one from the list to try and reduce the risk
	// of collisions.
	if len(unboundIDs) > 0 {

		rng, _ := randutil.NewPseudoRand()
		floatingIPAddressID := unboundIDs[rng.Intn(len(unboundIDs))]

		l.Printf(
			"    -> trying to reuse unbound IP address %s for instance %s",
			instanceID,
			networkInterfaceID,
		)

		// Wait a random amount of time before trying to reuse the IP address.
		// This is to try and cope with the fact that in case of a race condition,
		// the IP address might not return an error.
		//time.Sleep(time.Millisecond * time.Duration(rng.Intn(1000)))

		// There is a huge risk of collisions here, but we have no way to know
		// if the IP address is already being bound to another instance.
		// So we have to take the risk and hope for the best.
		_, _, err := vpcService.AddNetworkInterfaceFloatingIP(
			vpcService.NewAddNetworkInterfaceFloatingIPOptions(
				networkInterfaceID, floatingIPAddressID,
			),
		)
		if err != nil {
			l.Printf(
				"    => failed to reuse unbound IP address %s for instance %s: %s",
				floatingIPAddressID,
				instanceID,
				err,
			)
			return p.attachOrCreateFloatingIPAddress(
				l,
				zone,
				resourceGroupID,
				instanceID,
				networkInterfaceID,
				attempt+1,
			)
		}

		l.Printf(
			"    => reused unbound IP address %s for instance %s",
			floatingIPAddressID,
			instanceID,
		)

		// In case two requests fired up at the same time, the API might not have
		// returned an error, but the IP address might have been bound to another
		// instance. So we have to check if the IP address is actually bound to
		// the instance we want.

		// We'll wait a bit to give the API time to update the IP address.
		time.Sleep(time.Millisecond * time.Duration(rng.Intn(500)))

		// Get the floating IP address
		res, _, err := vpcService.GetNetworkInterfaceFloatingIP(
			vpcService.NewGetNetworkInterfaceFloatingIPOptions(
				networkInterfaceID,
				floatingIPAddressID,
			),
		)
		if err != nil {
			l.Printf("	=> failed to get floating IP address %s for instance %s: %s", floatingIPAddressID, instanceID, err)
			return p.attachOrCreateFloatingIPAddress(
				l,
				zone,
				resourceGroupID,
				instanceID,
				networkInterfaceID,
				attempt+1,
			)
		}

		return &vpcv1.FloatingIP{
			Address: res.Address,
			ID:      res.ID,
			CRN:     res.CRN,
			Name:    res.Name,
		}, nil
	}

	l.Printf(
		"    -> no unbound IP address found, creating a new one for instance %s...",
		instanceID,
	)
	// Create a new floating IP address
	res, _, err := vpcService.CreateFloatingIP(
		vpcService.NewCreateFloatingIPOptions(
			&vpcv1.FloatingIPPrototype{
				Target: &vpcv1.FloatingIPTargetPrototype{
					ID: &networkInterfaceID,
				},
				ResourceGroup: &vpcv1.ResourceGroupIdentity{
					ID: &p.config.roachprodResourceGroupID,
				},
			},
		),
	)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to create floating IP for instance %s in zone %s",
			instanceID,
			zone,
		)
	}

	return res, nil
}

// deleteFloatingIPs deletes the floating IP addresses attached to an instance.
func (p *Provider) deleteFloatingIPs(instance *instance, zone string) error {

	vpcService, err := p.getVpcServiceFromZone(zone)
	if err != nil {
		return errors.Wrap(err, "failed to get VPC service for region")
	}

	// If the instance has no network interface attached, it was not loaded.
	// We load it to get the floating IP addresses.
	if instance.networkInterface == nil {
		err := instance.load()
		if err != nil {
			return errors.Wrapf(
				err,
				"failed to load instance %s in zone %s to delete floating IP addresses",
				*instance.instance.ID,
				zone,
			)
		}
	}

	// We just double check that the instance has a network interface attached.
	if instance.networkInterface != nil {
		for _, fip := range instance.networkInterface.NetworkInterface.FloatingIps {
			_, err := vpcService.DeleteFloatingIP(&vpcv1.DeleteFloatingIPOptions{
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

	return nil
}

// getImageIDForRegion retrieves the image ID for a given image name and region.
func (p *Provider) getImageIDForRegion(l *logger.Logger, imageName, region string) (string, error) {

	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	if p.cachedImageIDs[region][imageName] != "" {
		return p.cachedImageIDs[region][imageName], nil
	}

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return "", errors.Wrap(err, "failed to get VPC service while querying image ID")
	}

	imagesPager, err := vpcService.NewImagesPager(
		vpcService.NewListImagesOptions().SetName(imageName).SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return "", errors.Wrap(err, "failed to create images pager")
	}

	for imagesPager.HasNext() {
		images, err := imagesPager.GetNext()
		if err != nil {
			return "", errors.Wrap(err, "failed to get next images page")
		}

		for _, image := range images {
			if *image.Name == imageName {

				if *image.Status == "deprecated" {
					l.Printf("WARN: image %s is deprecated, and will soon be unusable", *image.Name)
				} else if *image.Status == "obsolete" {
					return "", ErrImageObsolete
				}

				// Cache the image ID for future use.
				p.cachedImageIDs[region][imageName] = *image.ID

				return *image.ID, nil
			}
		}
	}

	return "", ErrImageNotFound
}

// getIdentityFromAuthenticator retrieves the identity from the authenticator.
// This is a workaround for the fact that the authenticator does not
// expose the identity directly. It uses the authenticator to create a
// request, and then extracts the identity from the request's
// Authorization header.
// This is a temporary solution until, maybe one day, the authenticator is
// updated to expose the identity directly.
func (p *Provider) getIdentityFromAuthenticator() (string, string, error) {

	if p.cachedIdentity != "" && p.cachedAccountID != "" {
		return p.cachedIdentity, p.cachedAccountID, nil
	}

	// Ensure the authenticator is valid.
	err := p.authenticator.Validate()
	if err != nil {
		return "", "", errors.Wrap(err, "invalid authentication")
	}

	// Create a dummy request to get the identity.
	req, _ := http.NewRequest("GET", "/dummy", nil)

	// Perform authentication on the request
	err = p.authenticator.Authenticate(req)
	if err != nil {
		return "", "", errors.Wrap(err, "unable to authenticate")
	}

	// Extract the identity from the request's Authorization header.
	token := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")

	if token == "" {
		return "", "", errors.New("no authorization token found")
	}

	// Decode the JWT token to get the claims.
	// The token is in the classic JWT format: <header>.<claims>.<signature>
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", "", errors.Errorf("invalid JWT token format")
	}

	var data struct {
		Email   string `json:"email"`
		Account struct {
			IMS string `json:"ims"`
		} `json:"account"`
	}

	a := base64.NewDecoder(base64.RawStdEncoding, strings.NewReader(parts[1]))
	if err := json.NewDecoder(a).Decode(&data); err != nil {
		return "", "", errors.Wrapf(err, "could not decode JWT claims segment")
	}

	p.cachedIdentity = data.Email
	p.cachedAccountID = data.Account.IMS

	return p.cachedIdentity, p.cachedAccountID, nil
}

// getSshKeyID checks to see if there is a an SSH key with the given name in the
// given region of IBM Cloud and returns its ID if it exists.
func (p *Provider) getSshKeyID(l *logger.Logger, keyName, region string) (string, error) {

	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	if p.cachedSSHKeyIDs[region][keyName] != "" {
		return p.cachedSSHKeyIDs[region][keyName], nil
	}

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return "", fmt.Errorf("region %s not supported by this provider instance", region)
	}

	keysPager, err := vpcService.NewKeysPager(
		vpcService.NewListKeysOptions().
			SetResourceGroupID(p.config.roachprodResourceGroupID).
			SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return "", errors.Wrap(err, "failed to create keys pager")
	}

	for keysPager.HasNext() {
		keys, err := keysPager.GetNext()
		if err != nil {
			return "", errors.Wrap(err, "failed to get next keys page")
		}

		for _, key := range keys {
			if *key.Name == keyName {

				// Cache the SSH key ID for future use.
				p.cachedSSHKeyIDs[region][keyName] = *key.ID

				return *key.ID, nil
			}
		}
	}

	return "", ErrKeyNotFound
}

// listRegion queries the IBM Cloud API to get all Roachprod VMs in a single region.
func (p *Provider) listRegion(l *logger.Logger, r string, opts vm.ListOptions) (vm.List, error) {

	// We have to force the IncludeVolumes flag to get basic volume information
	// like size and type.
	opts.IncludeVolumes = true

	var g errgroup.Group
	var volumes map[string]*vpcV1Volume
	var instances map[string]*instance

	vpcService, err := p.getVpcService(r)
	if err != nil {
		return nil, err
	}

	// Fetch instances
	g.Go(func() error {
		var err error
		instances, err = p.listRegionInstances(l, r, vpcService)
		if err != nil {
			return errors.Wrap(err, "failed to list instances")
		}
		return err
	})

	// Fetch volumes
	if opts.IncludeVolumes {
		g.Go(func() error {
			var err error
			volumes, err = p.listRegionVolumes(l, vpcService)
			if err != nil {
				return errors.Wrap(err, "failed to list volumes")
			}
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	var vms vm.List
	for _, i := range instances {

		if opts.IncludeVolumes {

			if i.instance.BootVolumeAttachment != nil {

				if i.instance.BootVolumeAttachment.Volume != nil {

					volume, ok := volumes[*i.instance.BootVolumeAttachment.Volume.ID]
					if !ok {
						continue
						// return nil, errors.Errorf(
						// 	"boot volume %s not found in prefetched volumes",
						// 	*i.instance.BootVolumeAttachment.Volume.ID,
						// )
					}
					i.bootVolume = volume.toVolumeAttachment()

				}
			}

			for _, attachedVolume := range i.instance.VolumeAttachments {

				// We don't add the boot volume to the list of attached volumes.
				if i.bootVolume != nil && i.bootVolume.ID == *attachedVolume.ID {
					continue
				}

				// Volume information is not yet available.
				if attachedVolume.Volume == nil {
					continue
				}

				volume, ok := volumes[*attachedVolume.Volume.ID]
				if !ok {
					continue
					// return nil, errors.Errorf(
					// 	"attached volume %s not found in prefetched volumes",
					// 	*attachedVolume.Volume.ID,
					// )
				}
				i.attachedVolumes = append(i.attachedVolumes, volume.toVolumeAttachment())
			}
		}

		valid, reason := i.isValidStatus()
		if !valid {
			l.Printf("WARN: discarding instance %s in region %s (%s)", *i.instance.CRN, r, reason)
			continue
		}

		vms = append(vms, i.toVM())
	}

	return vms, nil
}

// listRegionInstances queries the IBM Cloud API to get all instances
// in a region.
func (p *Provider) listRegionInstances(
	l *logger.Logger, r string, vpcService *vpcv1.VpcV1,
) (map[string]*instance, error) {

	allInstances := make(map[string]*instance)

	instancesPager, err := vpcService.NewInstancesPager(
		vpcService.NewListInstancesOptions().
			SetResourceGroupID(p.config.roachprodResourceGroupID).
			SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create instances pager")
	}

	instances, err := instancesPager.GetAll()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get all instances")
	}

	for _, i := range instances {
		allInstances[*i.CRN] = &instance{
			provider: p,
			instance: &i,
		}
	}

	return allInstances, nil
}

// listRegionVolumes queries the IBM Cloud API to get all volumes in a region.
func (p *Provider) listRegionVolumes(
	l *logger.Logger, vpcService *vpcv1.VpcV1,
) (map[string]*vpcV1Volume, error) {

	allVolumes := make(map[string]*vpcV1Volume)

	volumesPager, err := vpcService.NewVolumesPager(
		vpcService.NewListVolumesOptions().SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create volumes pager")
	}

	volumes, err := volumesPager.GetAll()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get all volumes")
	}

	for _, volume := range volumes {
		allVolumes[*volume.ID] = &vpcV1Volume{
			&volume,
		}
	}

	return allVolumes, nil
}

// sshKeyName computes the name of the SSH key that we'll store in IBM Cloud
// so that the user can SSH into the instances.
func (p *Provider) sshKeyName(l *logger.Logger) (string, error) {

	user, err := p.FindActiveAccount(l)
	if err != nil {
		return "", err
	}

	sshKey, err := config.SSHPublicKey()
	if err != nil {
		return "", err
	}

	hash := sha1.New()
	if _, err := hash.Write([]byte(sshKey)); err != nil {
		return "", err
	}
	hashBytes := hash.Sum(nil)
	hashText := base64.URLEncoding.EncodeToString(hashBytes)

	return vm.DNSSafeName(fmt.Sprintf("%s-%s", user, hashText)), nil
}

// sshKeyImport imports the user's public key into the given region of IBM Cloud
// so that it can be used to SSH into the instances.
func (p *Provider) sshKeyImport(l *logger.Logger, keyName, region string) error {

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return fmt.Errorf("region %s not supported by this provider instance", region)
	}

	sshPublicKey, err := config.SSHPublicKey()
	if err != nil {
		return err
	}

	pubKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(sshPublicKey))
	if err != nil {
		return errors.Wrap(err, "failed to parse SSH public key")
	}

	var keyType string
	switch pubKey.Type() {
	case "ssh-rsa":
		keyType = "rsa"
	case "ssh-ed25519":
		keyType = "ed25519"
	default:
		return errors.Newf("unsupported SSH key type: %s", pubKey.Type())
	}

	_, _, err = vpcService.CreateKey(
		vpcService.NewCreateKeyOptions(sshPublicKey).
			SetName(keyName).
			SetType(keyType).
			SetResourceGroup(&vpcv1.ResourceGroupIdentity{
				ID: &p.config.roachprodResourceGroupID,
			}),
	)
	return err
}

// checkCreateOpts checks the create options for validity and sets default values
// if not provided. It returns an error if any of the options are invalid.
func (p *Provider) checkCreateOpts(
	l *logger.Logger, opts *vm.CreateOpts, providerOpts *ProviderOpts, zones []string,
) error {

	// Define default values if not provided.
	if opts.Arch == "" {
		opts.Arch = string(vm.ArchS390x)
	}
	if opts.OsVolumeSize < defaultSystemVolumeSize {
		opts.OsVolumeSize = defaultSystemVolumeSize
	}

	// Check values are valid.
	if vm.ParseArch(opts.Arch) != vm.ArchS390x {
		return fmt.Errorf("IBM only supports S390x architecture, provided: %s", opts.Arch)
	}

	if opts.SSDOpts.UseLocalSSD {
		l.Printf(ErrUnsupportedLocalSSDs)
		opts.SSDOpts.UseLocalSSD = false
	}

	volSize, err := p.valInRange(opts.OsVolumeSize, systemVolumeSizeRange)
	if err != nil {
		return errors.Wrap(err, "unable to check if system volume size is in range")
	}
	if !volSize {
		return fmt.Errorf(
			"system volume size %d is out of supported range (%s)",
			opts.OsVolumeSize,
			systemVolumeSizeRange,
		)
	}

	err = p.checkAttachedVolumeTypeSizeIops(
		providerOpts.DefaultVolume.VolumeType,
		providerOpts.DefaultVolume.VolumeSize,
		providerOpts.DefaultVolume.IOPS,
	)
	if err != nil {
		return errors.Wrap(err, "default volume is invalid")
	}

	for i, attachedVolume := range providerOpts.AttachedVolumes {
		if err := p.checkAttachedVolumeTypeSizeIops(
			attachedVolume.VolumeType,
			attachedVolume.VolumeSize,
			attachedVolume.IOPS,
		); err != nil {
			return errors.Wrapf(err, "attached volume %d is invalid", i+1)
		}
	}

	// Check if the zones are valid
	for _, zone := range zones {
		r, err := p.zoneToRegion(zone)
		if err != nil {
			return errors.Wrapf(err, "invalid zone '%s'", zone)
		}

		// Check if the region is valid
		if _, ok := p.config.regions[r]; !ok {
			return fmt.Errorf("region '%s' is not supported", r)
		}

		// Check if the zone is valid
		if _, ok := p.config.regions[r].zones[zone]; !ok {
			return fmt.Errorf("zone '%s' is not supported", zone)
		}
	}

	return nil

}

// checkAttachedVolumeTypeSizeIops checks if the given volume type,
// and IOPS combination is valid based on a static map of configurations.
// It returns an error if the combination is not valid.
func (p *Provider) checkAttachedVolumeTypeSizeIops(t string, s int, i int) error {
	type profileCapacities struct {
		sizeRange           string
		iopsRange           func(i int) string
		customIopsSupported bool
	}
	availableTypes := map[string]profileCapacities{
		"general-purpose": {
			sizeRange: "[10-160000]",
			iopsRange: func(_ int) string { return "[3000-48000]" },
		},
		"5iops-tier": {
			sizeRange: "[10-9600]",
			iopsRange: func(_ int) string { return "[3000-48000]" },
		},
		"10iops-tier": {
			sizeRange: "[10-4800]",
			iopsRange: func(_ int) string { return "[3000-48000]" },
		},
		"custom": {
			sizeRange: "[10-160000]",
			iopsRange: func(s int) string {
				switch {
				case s < 40:
					return "[100-1000]"
				case s < 80:
					return "[100-2000]"
				case s < 100:
					return "[100-4000]"
				case s < 500:
					return "[100-6000]"
				case s < 1000:
					return "[100-10000]"
				case s < 2000:
					return "[100-20000]"
				case s < 4000:
					return "[200-40000]"
				case s < 8000:
					return "[300-40000]"
				case s < 10000:
					return "[500-48000]"
				default:
					return "[1000-48000]"
				}
			},
			customIopsSupported: true,
		},
	}

	if _, ok := availableTypes[t]; !ok {
		return fmt.Errorf("volume type %s is not supported", t)
	}

	// Check if the size is in range
	sizeRange, err := p.valInRange(s, availableTypes[t].sizeRange)
	if err != nil {
		return errors.Wrap(err, "unable to check if volume size is in range")
	}
	if !sizeRange {
		return fmt.Errorf("volume size %d is out of range %s", s, availableTypes[t].sizeRange)
	}

	// Check if the IOPS is in range
	if i > 0 {
		if !availableTypes[t].customIopsSupported {
			return fmt.Errorf("custom IOPS is not supported for volume type %s", t)
		}

		iopsRangeForTierSize := availableTypes[t].iopsRange(s)
		iopsRange, err := p.valInRange(i, iopsRangeForTierSize)
		if err != nil {
			return errors.Wrap(err, "unable to check if volume IOPS is in range")
		}
		if !iopsRange {
			return fmt.Errorf("volume IOPS %d is out of range %s", i, iopsRangeForTierSize)
		}
	}
	return nil
}

// valInRange checks if the given integer value is in the range specified by the
// range string. Range string should be in the format "[lower,upper)".
// Lower and upper bounds can be empty, and both inclusive and exclusive bounds
// are supported.
func (p *Provider) valInRange(value int, rangeStr string) (bool, error) {
	if len(rangeStr) < 3 {
		return false, fmt.Errorf("range too short")
	}

	if rangeStr[0] != '[' && rangeStr[0] != '(' {
		return false, fmt.Errorf("invalid range format: missing or invalid opening bracket")
	}
	if rangeStr[len(rangeStr)-1] != ']' && rangeStr[len(rangeStr)-1] != ')' {
		return false, fmt.Errorf("invalid range format: missing or invalid closing bracket")
	}

	lowerInclusive, upperInclusive := rangeStr[0] == '[', rangeStr[len(rangeStr)-1] == ']'

	parts := strings.Split(rangeStr[1:len(rangeStr)-1], "-")
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid range format: divider '-' not found")
	}

	lower, upper := math.MinInt, math.MaxInt
	if parts[0] != "" {
		if val, err := strconv.Atoi(parts[0]); err != nil {
			return false, errors.Wrap(err, "invalid lower bound")
		} else {
			lower = val
		}
	}
	if parts[1] != "" {
		if val, err := strconv.Atoi(parts[1]); err != nil {
			return false, errors.Wrap(err, "invalid upper bound")
		} else {
			upper = val
		}
	}

	if (lowerInclusive && value < lower) ||
		(!lowerInclusive && value <= lower) ||
		(upperInclusive && value > upper) ||
		(!upperInclusive && value >= upper) {
		return false, nil
	}
	return true, nil
}

// zonesToRegions returns a unique list of regions from a list of zones.
func (p *Provider) zonesToRegions(zones []string) ([]string, error) {
	regions := make(map[string]struct{})
	for _, zone := range zones {
		region, err := p.zoneToRegion(zone)
		if err != nil {
			return nil, err
		}
		regions[region] = struct{}{}
	}

	var ret []string
	for region := range regions {
		ret = append(ret, region)
	}
	return ret, nil
}

// zoneToRegion returns the region a zone is in.
func (p *Provider) zoneToRegion(zone string) (string, error) {
	parts := strings.Split(zone, "-")
	if len(parts) < 3 {
		return "", ErrInvalidZoneFormat
	}

	return strings.Join(parts[:len(parts)-1], "-"), nil
}

// computeZones computes the zones to use for the given options when creating
// an instance.
func (p *Provider) computeZones(
	opts vm.CreateOpts, providerOpts *ProviderOpts, nbVms int,
) ([]string, []string, error) {

	expandedZones, err := vm.ExpandZonesFlag(providerOpts.CreateZones)
	if err != nil {
		return nil, nil, err
	}

	if len(expandedZones) == 0 {
		expandedZones = DefaultZones(opts.GeoDistributed)
	}

	// Distribute the nodes amongst availability zones.
	nodeZones := vm.ZonePlacement(len(expandedZones), nbVms)
	zones := make([]string, len(nodeZones))
	for i, z := range nodeZones {
		zones[i] = expandedZones[z]
	}

	return expandedZones, zones, nil
}

// computeLabels computes the labels to use for the given options when creating
// an instance.
func (p *Provider) computeLabels(opts vm.CreateOpts) (attachedTags, error) {
	tags := attachedTags{
		fmt.Sprintf("%s:%s", vm.TagCreated, timeutil.Now().Format(time.RFC3339)),
	}
	defaultLabels := vm.GetDefaultLabelMap(opts)
	for key, value := range defaultLabels {
		tags = append(tags, fmt.Sprintf("%s:%s", key, value))
	}
	for key, value := range opts.CustomLabels {
		_, ok := defaultLabels[strings.ToLower(key)]
		if ok {
			return nil, fmt.Errorf("duplicated label name %s defined", key)
		}
		tags = append(tags, fmt.Sprintf("%s:%s", key, value))
	}
	return tags, nil
}

type parsedCRN struct {
	region string
	zone   string
	id     string
}

func (p *Provider) parseCRN(crn string) (*parsedCRN, error) {

	if crn == "" {
		return nil, fmt.Errorf("empty CRN")
	}

	// CRN have the following format:
	// crn:version:cname:ctype:service-name:location:scope:service-instance:resource-type:resource
	// e.g.: crn:v1:bluemix:public:is:ca-tor-1:a/ba0325a6257b4dabb3a7aa149a6578ae::instance:02q7_1a2940f7-b058-4742-8ef1-b1ee08273cf0
	// we're interested in the "resource" part, which is the 10th part
	parts := strings.Split(crn, ":")

	if len(parts) < 10 {
		return nil, nil
	}

	c := &parsedCRN{
		id: parts[9],
	}

	splitsRegion := strings.Split(parts[5], "-")
	if len(splitsRegion) < 3 {
		c.region = parts[5]
	} else {
		c.region = fmt.Sprintf("%s-%s", splitsRegion[0], splitsRegion[1])
		c.zone = parts[5]
	}

	return c, nil
}
