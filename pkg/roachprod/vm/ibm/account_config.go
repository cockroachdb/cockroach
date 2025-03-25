// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"fmt"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/vpc-go-sdk/vpcv1"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// defaultResourceGroupName is the name of the resource group used by all
	// roachprod resources.
	defaultResourceGroupName = "roachprod"

	// defaultVPCName is template for naming the VPCs.
	defaultVPCName = "vpc-roachprod-%s"

	// defaultSubnetName is the template for naming the subnets.
	defaultSubnetName = "subnet-roachprod-%s"

	// defaultPublicGatewayName is the template for naming the public gateways.
	defaultPublicGatewayName = "pgw-roachprod-%s"

	// defaultServiceURL is the template for the regional service endpoints.
	defaultServiceURL = "https://%s.iaas.cloud.ibm.com/v1"

	// defaultResourceManagerURL is number of elements returned during paginated
	// requests. This is the maximum value supported by the IBM API.
	defaultPaginationLimit = int64(100)
)

var (
	// supportedRegions is the list of regions used by roachprod resources.
	// These regions have been contractually agreed upon with IBM.
	supportedRegions = []string{
		"br-sao",
		"ca-tor",
	}
)

// ibmCloudConfig contains the configuration for the IBM Cloud provider
// for roachprod including the account ID, resource group ID, and regions info.
type ibmCloudConfig struct {
	accountID                string
	roachprodResourceGroupID string
	regions                  map[string]regionDetails

	// loaded is true if all region resources have been loaded.
	loaded bool
}

// regionDetails contains the details of a region including the VPC ID
// and zones info.
type regionDetails struct {
	vpcID string
	zones map[string]zoneDetails
}

// zoneDetails contains the details of a zone including the subnet ID.
type zoneDetails struct {
	subnetID string
}

// configureCloudAccountInitial initializes the IBM Cloud account configuration.
// It retrieves the account ID from the authenticator and creates a resource
// group for roachprod resources if it doesn't already exist.
// It returns the ibmCloudConfig struct containing the account ID and resource
// group ID.
// This function is called only once when the provider is initialized.
func (p *Provider) configureCloudAccountInitial() (*ibmCloudConfig, error) {
	// Get account ID from the authenticator.
	_, accountID, err := p.getIdentityFromAuthenticator()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get account ID")
	}

	// Get or create the roachprod resource group
	resourceGroupID, err := p.getOrCreateResourceGroup()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get or create resource group")
	}

	// Init the ibmCloudConfig struct for the provider.
	ibmCloudConfig := &ibmCloudConfig{
		accountID:                accountID,
		roachprodResourceGroupID: resourceGroupID,
		regions:                  make(map[string]regionDetails),
	}

	return ibmCloudConfig, nil
}

// configureCloudAccountFull configures the IBM Cloud account by creating
// VPCs, subnets, and public gateways for each region.
// It checks if the account is already configured and returns early if it is.
// It uses goroutines to create resources in parallel for each region.
// It also uses goroutines to create resources in parallel for each zone
// within a region.
func (p *Provider) configureCloudAccountFull(cc *ibmCloudConfig) error {

	// Check if the account is already configured.
	if cc.loaded {
		return nil
	}

	var g errgroup.Group
	var regionMutex syncutil.Mutex

	for region := range p.regions {

		g.Go(func() error {

			vpcID, aclID, sgID, err := p.getOrCreateRegionVPC(region, cc.roachprodResourceGroupID)
			if err != nil {
				return errors.Wrapf(err, "failed to get or create VPC for region %s", region)
			}

			err = p.checkOrCreateNetworkSecurityGroup(region, vpcID, sgID)
			if err != nil {
				return errors.Wrapf(
					err,
					"failed to check or create network security group for region %s",
					region,
				)
			}

			zones, err := p.getVPCZoneAddressPrefixes(region, vpcID)
			if err != nil {
				return errors.Wrapf(
					err,
					"failed to get VPC zone address prefixes for region %s",
					region,
				)
			}

			rd := regionDetails{
				vpcID: vpcID,
				zones: make(map[string]zoneDetails),
			}

			var zg errgroup.Group
			var zoneMutex syncutil.Mutex
			for zoneName, cidr := range zones {

				zg.Go(func() error {

					subnetID, err := p.getOrCreateZoneSubnet(
						region,
						cc.roachprodResourceGroupID,
						vpcID,
						aclID,
						zoneName,
						cidr,
					)
					if err != nil {
						return errors.Wrapf(
							err,
							"failed to get or create subnet for zone %s",
							zoneName,
						)
					}

					zoneMutex.Lock()
					defer zoneMutex.Unlock()
					rd.zones[zoneName] = zoneDetails{
						subnetID: subnetID,
					}

					return nil
				})

			}

			if err := zg.Wait(); err != nil {
				return errors.Wrapf(err, "failed to configure zones for region %s", region)
			}

			regionMutex.Lock()
			defer regionMutex.Unlock()
			cc.regions[region] = rd

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "failed to configure IBM Cloud account")
	}

	return nil
}

// getOrCreateResourceGroup checks if the resource group already exists and
// creates it if it doesn't. It returns the resource group ID.
func (p *Provider) getOrCreateResourceGroup() (string, error) {

	rms := p.getResourceManagerService()

	// Check if the resource group already exists.
	resourceGroups, _, err := rms.ListResourceGroups(
		rms.NewListResourceGroupsOptions().SetName(defaultResourceGroupName),
	)
	if err != nil {
		return "", errors.Wrap(err, "failed to list resource groups")
	}

	// Resource groups are unique by name, so we can just return the first one.
	for _, resourceGroup := range resourceGroups.Resources {
		return *resourceGroup.ID, nil
	}

	// If the resource group doesn't exist, create it.
	resourceGroupResp, _, err := rms.CreateResourceGroup(
		rms.NewCreateResourceGroupOptions().SetName(defaultResourceGroupName),
	)
	if err != nil {
		return "", errors.Wrap(err, "failed to create resource group")
	}

	return *resourceGroupResp.ID, nil
}

// getOrCreateRegionVPC checks if the VPC already exists for the region and
// creates it if it doesn't. It returns the VPC ID, default network ACL ID,
// and default security group ID.
func (p *Provider) getOrCreateRegionVPC(
	region, resourceGroupID string,
) (string, string, string, error) {

	expectedName := fmt.Sprintf(defaultVPCName, region)

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return "", "", "", errors.Wrapf(err, "failed to get VPC service for region %s", region)
	}

	// Get the VPC ID for the region.
	// This assumes there will be less than 100 VPCs in the region
	// for the resource group, which should be a safe assumption as we intend
	// to use a single VPC per region.
	vpcPager, err := vpcService.NewVpcsPager(
		vpcService.NewListVpcsOptions().
			SetResourceGroupID(resourceGroupID).
			SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return "", "", "", errors.Wrapf(err, "failed to create VPC pager for region %s", region)
	}

	vpcs, err := vpcPager.GetAll()
	if err != nil {
		return "", "", "", errors.Wrapf(err, "failed to get VPCs for region %s", region)
	}

	// Check if the VPC already exists.
	for _, vpc := range vpcs {
		if *vpc.Name == expectedName {
			return *vpc.ID, *vpc.DefaultNetworkACL.ID, *vpc.DefaultSecurityGroup.ID, nil
		}
	}

	// If the VPC doesn't exist, create it.
	vpcResp, _, err := vpcService.CreateVPC(
		vpcService.NewCreateVPCOptions().
			SetName(expectedName).
			SetResourceGroup(
				&vpcv1.ResourceGroupIdentity{
					ID: core.StringPtr(resourceGroupID),
				},
			),
	)
	if err != nil {
		return "", "", "", errors.Wrapf(err, "failed to create VPC for region %s", region)
	}

	return *vpcResp.ID, *vpcResp.DefaultNetworkACL.ID, *vpcResp.DefaultSecurityGroup.ID, nil
}

// checkOrCreateNetworkSecurityGroup checks if the network security group
// exists and creates it if it doesn't.
// This assumes that a rule exists with the protocol "all", direction "inbound"
// and a global reachability.
func (p *Provider) checkOrCreateNetworkSecurityGroup(region, vpcID, securityGroupID string) error {

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return errors.Wrapf(err, "failed to get VPC service for region %s", region)
	}

	// Check if the network security group already exists.
	listNSGResp, _, err := vpcService.ListSecurityGroupRules(
		vpcService.NewListSecurityGroupRulesOptions(securityGroupID),
	)
	if err != nil {
		return errors.Wrapf(err, "failed to list network security groups for region %s", region)
	}

	foundInboundGlobalRule := false

	for _, rule := range listNSGResp.Rules {

		switch r := rule.(type) {
		case *vpcv1.SecurityGroupRuleSecurityGroupRuleProtocolAll:

			if r.Direction == nil || *r.Direction != "inbound" {
				continue
			}

			if r.Protocol == nil || *r.Protocol != "all" {
				continue
			}

			rr, ok := r.Remote.(*vpcv1.SecurityGroupRuleRemote)
			if !ok {
				return errors.New("failed to cast remote to *vpcv1.SecurityGroupRuleRemote")
			}

			foundInboundGlobalRule = true

			if rr.CIDRBlock == nil || *rr.CIDRBlock != "0.0.0.0/0" {
				_, _, err = vpcService.UpdateSecurityGroupRule(
					vpcService.NewUpdateSecurityGroupRuleOptions(
						securityGroupID,
						*r.ID,
						map[string]interface{}{
							"remote": map[string]string{
								"cidr_block": "0.0.0.0/0",
							},
						},
					),
				)
				if err != nil {
					return errors.Wrapf(err, "failed to update network security group rule for region %s", region)
				}
			}
		}

		break
	}

	// If the rule doesn't exist, create it.
	if !foundInboundGlobalRule {
		_, _, err = vpcService.CreateSecurityGroupRule(
			vpcService.NewCreateSecurityGroupRuleOptions(
				securityGroupID,
				&vpcv1.SecurityGroupRulePrototypeSecurityGroupRuleProtocolTcpudp{
					Direction: core.StringPtr("inbound"),
					Local: &vpcv1.SecurityGroupRuleLocalPrototypeCIDR{
						CIDRBlock: core.StringPtr("0.0.0.0/0"),
					},
					Remote: &vpcv1.SecurityGroupRuleRemotePrototypeCIDR{
						CIDRBlock: core.StringPtr("0.0.0.0/0"),
					},
					Protocol: core.StringPtr("all"),
				},
			),
		)
		if err != nil {
			return errors.Wrapf(err, "failed to create network security group rule for region %s", region)
		}
	}

	return nil

}

// getVPCZoneAddressPrefixes retrieves the address prefixes for the VPC in the
// zone. It returns a map of zone names to CIDR blocks.
func (p *Provider) getVPCZoneAddressPrefixes(region, vpcID string) (map[string]string, error) {

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get VPC service for region %s", region)
	}

	// Get the address prefixes for the VPC in the zone.
	lapResp, _, err := vpcService.ListVPCAddressPrefixes(
		vpcService.NewListVPCAddressPrefixesOptions(vpcID),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list VPC address prefixes for region %s", region)
	}

	addressPrefixes := make(map[string]string)
	for _, prefix := range lapResp.AddressPrefixes {
		addressPrefixes[*prefix.Zone.Name] = *prefix.CIDR
	}

	return addressPrefixes, nil
}

// getOrCreateZoneSubnet checks if the subnet already exists for the zone and
// creates it if it doesn't. It returns the subnet ID.
func (p *Provider) getOrCreateZoneSubnet(
	region, resourceGroupID, vpcID, vpcACLId, zone, cidrBlock string,
) (string, error) {

	expectedName := fmt.Sprintf(defaultSubnetName, zone)

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get VPC service for region %s", region)
	}

	// Get the subnet ID for the zone.
	subnetsPager, err := vpcService.NewSubnetsPager(
		vpcService.NewListSubnetsOptions().
			SetResourceGroupID(resourceGroupID).
			SetVPCID(vpcID).
			SetZoneName(zone).
			SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create subnet pager for region %s", region)
	}

	for subnetsPager.HasNext() {
		subnets, err := subnetsPager.GetNext()
		if err != nil {
			return "", errors.Wrapf(err, "failed to get subnets for region %s", region)
		}

		for _, subnet := range subnets {
			if *subnet.Name == expectedName {
				return *subnet.ID, nil
			}
		}
	}

	// If the subnet doesn't exist, create it.

	// Start by working on the public gateway.
	publicGatewayID, err := p.getOrCreatePublicGateway(
		region, resourceGroupID, vpcID, zone,
	)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get or create public gateway for zone %s", zone)
	}

	// Then create the subnet and attach the public gateway to it.
	createSubnetResp, _, err := vpcService.CreateSubnet(
		vpcService.NewCreateSubnetOptions(
			&vpcv1.SubnetPrototype{
				VPC: &vpcv1.VPCIdentity{
					ID: core.StringPtr(vpcID),
				},
				Zone: &vpcv1.ZoneIdentity{
					Name: core.StringPtr(zone),
				},
				Name:          core.StringPtr(expectedName),
				IPVersion:     core.StringPtr("ipv4"),
				Ipv4CIDRBlock: core.StringPtr(cidrBlock),
				NetworkACL: &vpcv1.NetworkACLIdentity{
					ID: core.StringPtr(vpcACLId),
				},
				ResourceGroup: &vpcv1.ResourceGroupIdentity{
					ID: core.StringPtr(resourceGroupID),
				},
				PublicGateway: &vpcv1.PublicGatewayIdentity{
					ID: core.StringPtr(publicGatewayID),
				},
			},
		),
	)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create subnet for zone %s", zone)
	}

	return *createSubnetResp.ID, nil
}

// getOrCreatePublicGateway checks if the public gateway already exists for the
// zone and creates it if it doesn't. It returns the public gateway ID.
func (p *Provider) getOrCreatePublicGateway(
	region, resourceGroupID, vpcID, zone string,
) (string, error) {

	expectedPgwName := fmt.Sprintf(defaultPublicGatewayName, zone)

	vpcService, err := p.getVpcService(region)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get VPC service for region %s", region)
	}

	// Get the public gateway ID for the zone.
	pgPager, err := vpcService.NewPublicGatewaysPager(
		vpcService.NewListPublicGatewaysOptions().
			SetResourceGroupID(resourceGroupID).
			SetLimit(defaultPaginationLimit),
	)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create public gateway pager for region %s", region)
	}

	for pgPager.HasNext() {
		pgs, err := pgPager.GetNext()
		if err != nil {
			return "", errors.Wrapf(err, "failed to get public gateways for region %s", region)
		}

		// Check if the public gateway already exists.
		for _, pgw := range pgs {
			if *pgw.Name == expectedPgwName {
				return *pgw.ID, nil
			}
		}
	}

	// If the public gateway doesn't exist, create it.
	pgwResp, _, err := vpcService.CreatePublicGateway(
		vpcService.NewCreatePublicGatewayOptions(
			&vpcv1.VPCIdentity{ID: core.StringPtr(vpcID)},
			&vpcv1.ZoneIdentity{Name: core.StringPtr(zone)},
		).SetResourceGroup(
			&vpcv1.ResourceGroupIdentity{ID: core.StringPtr(resourceGroupID)},
		).SetName(expectedPgwName),
	)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create public gateway for region %s", region)
	}

	return *pgwResp.ID, nil
}
