// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"strings"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/globaltaggingv1"
	"github.com/IBM/platform-services-go-sdk/resourcecontrollerv2"
	"github.com/IBM/vpc-go-sdk/vpcv1"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// attachedTags represents a list of tags to be attached to a resource.
type attachedTags []string

// toAttachTagOptions converts the attached tags to a format suitable for
// the IBM Cloud API.
func (t *attachedTags) toAttachTagOptions(resourceCRN string) *globaltaggingv1.AttachTagOptions {
	ato := &globaltaggingv1.AttachTagOptions{
		Resources: []globaltaggingv1.Resource{
			{
				ResourceID: &resourceCRN,
			},
		},
	}
	if t != nil {
		ato.TagNames = *t
	}
	return ato
}

// toTagMap converts the attached tags to a map of key-value pairs.
func (t *attachedTags) toTagMap() map[string]string {
	if t == nil {
		return nil
	}

	tagMap := make(map[string]string)
	for _, tag := range *t {
		tagParts := strings.Split(tag, ":")
		if len(tagParts) < 2 {
			continue
		}
		tagMap[tagParts[0]] = strings.Join(tagParts[1:], ":")
	}
	return tagMap
}

// globalTaggingV1TagList is a wrapper around the globaltaggingv1.TagList
// to provide additional functionality.
type globalTaggingV1TagList struct {
	*globaltaggingv1.TagList
}

// toAttachedTags converts the globalTaggingV1TagList to an attachedTags type.
func (t *globalTaggingV1TagList) toAttachedTags() *attachedTags {
	if t == nil {
		return nil
	}

	tagList := make(attachedTags, len(t.Items))
	for i, tag := range t.Items {
		if tag.Name == nil {
			continue
		}
		tagList[i] = *tag.Name
	}
	return &tagList
}

// vpcv1FloatingIP is a wrapper around the vpcv1.FloatingIP
// to provide additional functionality.
type vpcv1FloatingIP struct {
	*vpcv1.FloatingIP
}

// toVpcv1NetworkInterface converts our wrapper type to a vpcv1.NetworkInterface
// suitable for use with the IBM Cloud API.
func (f *vpcv1FloatingIP) toVpcv1NetworkInterface() *vpcv1NetworkInterface {
	if f == nil || f.Address == nil {
		return nil
	}

	return &vpcv1NetworkInterface{
		NetworkInterface: &vpcv1.NetworkInterface{
			FloatingIps: []vpcv1.FloatingIPReference{
				{
					Address: f.Address,
					CRN:     f.CRN,
					ID:      f.ID,
					Name:    f.Name,
				},
			},
		},
	}
}

// vpcv1NetworkInterface is a wrapper around the vpcv1.NetworkInterface.
type vpcv1NetworkInterface struct {
	*vpcv1.NetworkInterface
}

// getPublicAddress returns the public address of the network interface.
func (i *vpcv1NetworkInterface) getPublicAddress() string {
	if i == nil || i.FloatingIps == nil {
		return ""
	}
	if len(i.FloatingIps) < 1 {
		return ""
	}
	return *i.FloatingIps[0].Address
}

// volumeAttachment represents a volume attachment to an instance.
type volumeAttachment struct {
	CRN string
	ID  string

	Name            string
	Zone            string
	ResourceGroupID string

	Capacity int
	IOPS     int
	Profile  string

	UserTags attachedTags

	DeleteOnInstanceDelete bool
}

// toVolumeAttachmentPrototypeInstanceByImageContext converts the volume
// attachment to a vpcv1.VolumeAttachmentPrototypeInstanceByImageContext
// suitable for use with the IBM Cloud API.
func (vol *volumeAttachment) toVolumeAttachmentPrototypeInstanceByImageContext() *vpcv1.VolumeAttachmentPrototypeInstanceByImageContext {
	v := &vpcv1.VolumeAttachmentPrototypeInstanceByImageContext{
		DeleteVolumeOnInstanceDelete: core.BoolPtr(true),
		Volume:                       &vpcv1.VolumePrototypeInstanceByImageContext{},
	}

	if vol == nil {
		return v
	}

	if vol.ResourceGroupID != "" {
		v.Volume.ResourceGroup = &vpcv1.ResourceGroupIdentity{
			ID: &vol.ResourceGroupID,
		}
	}

	if vol.Name != "" {
		v.Volume.Name = core.StringPtr(vol.Name)
	}

	if vol.Capacity != 0 {
		v.Volume.Capacity = core.Int64Ptr(int64(vol.Capacity))
	}
	if vol.IOPS != 0 {
		v.Volume.Iops = core.Int64Ptr(int64(vol.IOPS))
		v.Volume.Profile = &vpcv1.VolumeProfileIdentity{
			Name: core.StringPtr(vpcv1.VolumeProfileFamilyCustomConst),
		}
	} else if vol.Profile != "" {
		v.Volume.Profile = &vpcv1.VolumeProfileIdentity{
			Name: core.StringPtr(vol.Profile),
		}
	}

	if vol.UserTags != nil {
		v.Volume.UserTags = vol.UserTags
	}

	return v
}

// toVolumeAttachmentPrototype converts the volume attachment to a
// vpcv1.VolumeAttachmentPrototype suitable for use with the IBM Cloud API.
func (vol *volumeAttachment) toVolumeAttachmentPrototype() vpcv1.VolumeAttachmentPrototype {

	if vol == nil {
		return vpcv1.VolumeAttachmentPrototype{}
	}

	volume := &vpcv1.VolumeAttachmentPrototypeVolumeVolumePrototypeInstanceContext{}

	if vol.ResourceGroupID != "" {
		volume.ResourceGroup = &vpcv1.ResourceGroupIdentity{
			ID: &vol.ResourceGroupID,
		}
	}

	if vol.Name != "" {
		volume.Name = core.StringPtr(vol.Name)
	}

	if vol.Capacity != 0 {
		volume.Capacity = core.Int64Ptr(int64(vol.Capacity))
	}
	if vol.IOPS != 0 {
		volume.Iops = core.Int64Ptr(int64(vol.IOPS))
		volume.Profile = &vpcv1.VolumeProfileIdentity{
			Name: core.StringPtr(vpcv1.VolumeProfileFamilyCustomConst),
		}
	} else if vol.Profile != "" {
		volume.Profile = &vpcv1.VolumeProfileIdentity{
			Name: core.StringPtr(vol.Profile),
		}
	}

	if vol.UserTags != nil {
		volume.UserTags = vol.UserTags
	}

	return vpcv1.VolumeAttachmentPrototype{
		DeleteVolumeOnInstanceDelete: core.BoolPtr(vol.DeleteOnInstanceDelete),
		Volume:                       volume,
	}
}

// toVmVolume converts the volume attachment to a vm.Volume.
func (v *volumeAttachment) toVmVolume() vm.Volume {
	if v == nil {
		return vm.Volume{}
	}
	return vm.Volume{
		Name:               v.Name,
		Size:               v.Capacity,
		Zone:               v.Zone,
		ProviderVolumeType: v.Profile,
	}
}

// updateWithID updates the volume attachment with the given ID and CRN.
func (v *volumeAttachment) updateWithID(id, crn string) error {
	if v == nil {
		v = &volumeAttachment{}
	}
	v.ID = id
	v.CRN = crn
	return nil
}

// volumeAttachments is a slice of volumeAttachment.
type volumeAttachments []*volumeAttachment

// toVpcV1VolumeAttachment converts the volume attachments to a
// vpcv1.VolumeAttachmentPrototype suitable for use with the IBM Cloud API.
func (vols volumeAttachments) toVpcV1VolumeAttachment() []vpcv1.VolumeAttachmentPrototype {
	attachments := make([]vpcv1.VolumeAttachmentPrototype, len(vols))
	for i, vol := range vols {
		attachments[i] = vol.toVolumeAttachmentPrototype()
	}
	return attachments
}

// updateWithIDByName updates the volume attachment with the given name
// with the given ID and CRN.
func (vols volumeAttachments) updateWithIDByName(name, id, crn string) error {
	for _, vol := range vols {
		if vol.Name == name {
			vol.ID = id
			vol.CRN = crn
			return nil
		}
	}
	return errors.Errorf("volume %s not found", name)
}

// vpcV1Volume is a wrapper around the vpcv1.Volume.
type vpcV1Volume struct {
	*vpcv1.Volume
}

// toVolumeAttachment converts the vpcV1Volume to a volumeAttachment type.
func (v *vpcV1Volume) toVolumeAttachment() *volumeAttachment {
	if v == nil {
		return nil
	}

	vol := &volumeAttachment{
		CRN:             core.StringNilMapper(v.CRN),
		ID:              core.StringNilMapper(v.ID),
		ResourceGroupID: core.StringNilMapper(v.ResourceGroup.ID),
		Name:            core.StringNilMapper(v.Name),
		Zone:            core.StringNilMapper(v.Zone.Name),
	}

	if v.Capacity != nil {
		vol.Capacity = int(*v.Capacity)
	}
	if v.Iops != nil {
		vol.IOPS = int(*v.Iops)
	}
	if v.Profile != nil {
		vol.Profile = core.StringNilMapper(v.Profile.Name)
	}
	if v.UserTags != nil {
		vol.UserTags = attachedTags(v.UserTags)
	}

	return vol
}

// instance represents an IBM Cloud instance.
// It wraps the vpcv1.Instance type and provides caching for related objects
// such as the network interface and attached volumes.
// It also provides methods to load the instance information and convert
// the instance to a vm.VM type.
type instance struct {
	initialized bool

	provider *Provider
	instance *vpcv1.Instance

	bootVolume       *volumeAttachment
	attachedVolumes  []*volumeAttachment
	networkInterface *vpcv1NetworkInterface
	tagList          *attachedTags
}

// newInstanceFromCRN creates a new instance from the given CRN.
// If query is true, it will load the instance information from the IBM API
// and populate the instance fields.
// If query is false, it will only set the CRN and ID fields.
func newInstanceFromCRN(provider *Provider, crn string, query bool) (*instance, error) {

	i := &instance{
		provider: provider,
		instance: &vpcv1.Instance{
			CRN: core.StringPtr(crn),
		},
	}

	parsedCrn, err := provider.parseCRN(crn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse CRN")
	}

	i.instance.ID = &parsedCrn.id
	i.instance.Zone = &vpcv1.ZoneReference{
		Name: &parsedCrn.zone,
	}

	if query {
		err := i.load()
		if err != nil {
			return nil, err
		}
	}

	return i, nil
}

// getTagsAsMap retrieves the tags for the instance and returns them as a map.
// The tags are cached in the instance struct to avoid repeated API calls.
func (i *instance) getTagsAsMap() (map[string]string, error) {

	if i == nil {
		return nil, errors.New("instance is nil")
	}

	if i.tagList != nil {
		return i.tagList.toTagMap(), nil
	}

	tagList := &globalTaggingV1TagList{
		TagList: &globaltaggingv1.TagList{
			TotalCount: core.Int64Ptr(0),
			Items:      []globaltaggingv1.Tag{},
		},
	}

	tagService := i.provider.getTagService()

	options := tagService.NewListTagsOptions().
		SetAttachedTo(*i.instance.CRN).
		SetProviders([]string{"ghost"}).
		SetLimit(defaultPaginationLimit).
		SetOffset(0)

	for {
		tagResp, _, err := tagService.ListTags(options)
		if err != nil {
			return nil, errors.Wrap(err, "failed to query instance tags")
		}

		nbItems := int64(len(tagResp.Items))
		totalItems := *tagList.TotalCount + nbItems

		tagList.TotalCount = &totalItems
		tagList.Items = append(tagList.Items, tagResp.Items...)

		if nbItems < *options.Limit {
			break
		}

		options.SetOffset(*options.Offset + *options.Limit)
	}

	i.tagList = tagList.toAttachedTags()

	return i.tagList.toTagMap(), nil
}

// getPrivateIPAddress retrieves the private IP address of the instance.
// It uses the PrimaryNetworkInterface and PrimaryIP fields of the instance
// to get the private IP address.
func (i *instance) getPrivateIPAddress() string {
	if i == nil || i.instance == nil {
		return ""
	}
	if i.instance.PrimaryNetworkInterface == nil {
		return ""
	}
	if i.instance.PrimaryNetworkInterface.PrimaryIP == nil {
		return ""
	}
	return core.StringNilMapper(i.instance.PrimaryNetworkInterface.PrimaryIP.Address)
}

// getPublicIPAddress retrieves the public IP address of the instance.
// It first checks if the network interface information is cached.
// If not, it queries the IBM Cloud API to get the primary network interface
// and then retrieves the public IP address from it.
func (i *instance) getPublicIPAddress() (string, error) {
	if i == nil || i.instance == nil {
		return "", errors.New("instance is nil")
	}

	// If detailed network interface information is available, use it.
	if i.networkInterface != nil {
		return i.networkInterface.getPublicAddress(), nil
	}

	// Cache the network interface information, and return the public IP.
	var err error
	i.networkInterface, err = i.getPrimaryNetworkInterface()
	if err != nil {
		return "", errors.Wrap(err, "failed to get primary network interface")
	}

	return i.networkInterface.getPublicAddress(), nil
}

// getPrimaryNetworkInterface retrieves the primary network interface
// for the instance. It queries the IBM Cloud API to get the network
// interface information with extra details.
func (i *instance) getPrimaryNetworkInterface() (*vpcv1NetworkInterface, error) {
	// If detailed network interface information is available, use it.
	if i.networkInterface != nil {
		return i.networkInterface, nil
	}

	vpcService, err := i.provider.getVpcServiceFromZone(*i.instance.Zone.Name)
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to get VPC service from zone to query public IP address",
		)
	}

	// Otherwise, query the network interface.
	nicResp, _, err := vpcService.GetInstanceNetworkInterface(
		&vpcv1.GetInstanceNetworkInterfaceOptions{
			InstanceID: i.instance.ID,
			ID:         i.instance.PrimaryNetworkInterface.ID,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query network interface")
	}

	// Cache the network interface information, and return the public IP.
	i.networkInterface = &vpcv1NetworkInterface{
		nicResp,
	}

	return i.networkInterface, nil
}

// isRoachprodAndValidStatus checks if a roahcprod instance in valid status.
func (i *instance) isRoachprodAndValidStatus() (bool, string) {
	if i == nil || i.instance == nil || i.instance.Status == nil {
		return false, InstanceNotInitialized
	}

	tags, err := i.getTagsAsMap()
	if err != nil {
		return false, InstanceInvalidTags
	}

	if tags[vm.TagRoachprod] != "true" {
		return false, InstanceNotRochprod
	}

	// if *i.instance.Status != "pending" && *i.instance.Status != "running" {
	// 	return false, InstanceInvalidStatus
	// }

	return true, ""
}

// load loads the instance information and the primary network interface details
// from the IBM Cloud API.
func (i *instance) load() error {

	if i == nil || i.instance == nil {
		return errors.New("instance is nil")
	}

	if i.initialized {
		return nil
	}

	if i.instance.ID == nil || i.instance.Zone == nil {
		if i.instance.CRN == nil {
			return errors.New("either instance CRN or ID and zone are required")
		} else {

			parsedCRN, err := i.provider.parseCRN(*i.instance.CRN)
			if err != nil {
				return errors.Wrap(err, "failed to parse CRN")
			}

			i.instance.ID = &parsedCRN.id
			i.instance.Zone = &vpcv1.ZoneReference{
				Name: &parsedCRN.zone,
			}
		}
	}

	vpcService, err := i.provider.getVpcServiceFromZone(*i.instance.Zone.Name)
	if err != nil {
		return errors.Wrap(err, "failed to get VPC service from zone")
	}

	instanceResp, _, err := vpcService.GetInstance(
		vpcService.NewGetInstanceOptions(*i.instance.ID),
	)
	if err != nil {
		return errors.Wrap(err, "failed to query instance")
	}

	i.instance = instanceResp
	i.networkInterface, err = i.getPrimaryNetworkInterface()
	if err != nil {
		return errors.Wrap(err, "failed to get primary network interface")
	}

	return nil
}

// toVM converts the instance to a vm.VM type.
func (i *instance) toVM() vm.VM {

	if i == nil || i.instance == nil {
		return vm.VM{
			Errors: []error{errors.New("instance is nil")},
		}
	}

	if !i.initialized {
		err := i.load()
		if err != nil {
			return vm.VM{
				Errors: []error{errors.Wrap(err, "failed to load instance")},
			}
		}
	}

	var vmErrors []error

	vpcID := ""
	region, err := i.provider.zoneToRegion(*i.instance.Zone.Name)
	if err != nil {
		vmErrors = append(vmErrors, errors.Wrap(err, "unable to get region"))
	} else {
		vpcID = i.provider.config.regions[region].vpcID
	}

	// Gather tags
	tags, err := i.getTagsAsMap()
	if err != nil {
		vmErrors = append(vmErrors, errors.Wrap(err, "unable to get tags"))
	}

	var lifetime time.Duration
	if lifeText, ok := tags[vm.TagLifetime]; ok {
		lifetime, err = time.ParseDuration(lifeText)
		if err != nil {
			vmErrors = append(vmErrors, errors.Wrap(err, "unable to compute lifetime"))
		}
	}

	privateIP := i.getPrivateIPAddress()
	publicIP, err := i.getPublicIPAddress()
	if err != nil {
		vmErrors = append(vmErrors, errors.Wrap(err, "unable to get public IP"))
	}

	nonBootAttachedVolumes := []vm.Volume{}
	for _, v := range i.attachedVolumes {
		nonBootAttachedVolumes = append(nonBootAttachedVolumes, v.toVmVolume())
	}

	v := vm.VM{
		ProviderAccountID: i.provider.config.accountID,
		Provider:          ProviderName,
		Zone:              *i.instance.Zone.Name,
		VPC:               vpcID,
		CPUArch:           defaultCPUArch,
		CPUFamily:         defaultCPUFamily,
		MachineType:       *i.instance.Profile.Name,
		RemoteUser:        defaultRemoteUser,
		Preemptible:       false,

		ProviderID: *i.instance.CRN,
		Name:       *i.instance.Name,
		CreatedAt:  time.Time(*i.instance.CreatedAt),
		PublicIP:   publicIP,
		PrivateIP:  privateIP,
		DNS:        privateIP,

		NonBootAttachedVolumes: nonBootAttachedVolumes,
		LocalDisks:             nil, // No local disks on IBM Cloud

		Lifetime: lifetime,
		Labels:   tags,
		Errors:   vmErrors,
	}

	if i.bootVolume != nil {
		v.BootVolume = i.bootVolume.toVmVolume()
	}

	return v
}

// resourcecontrollerv2Reclamation is a wrapper around the
// resourcecontrollerv2.Reclamation type to provide additional functionality.
type resourcecontrollerv2Reclamation struct {
	*resourcecontrollerv2.Reclamation
}

// wasReclaimed checks if the instance was reclaimed.
func (r *resourcecontrollerv2Reclamation) wasReclaimed() bool {

	if r == nil || r.Reclamation == nil {
		return false
	}

	if r.Reclamation.ResourceInstanceID == nil {
		return false
	}

	if r.Reclamation.State == nil {
		return false
	}

	if *r.Reclamation.State == "IN_PROGRESS" ||
		*r.Reclamation.State == "RECLAIMED_INITIATED" ||
		*r.Reclamation.State == "RECLAIMED" {
		return true
	}

	return false
}
