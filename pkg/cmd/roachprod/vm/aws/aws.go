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

package aws

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

// ProviderName is aws.
const ProviderName = "aws"

// init will inject the AWS provider into vm.Providers, but only
// if the aws tool is available on the local path.
func init() {
	if _, err := exec.LookPath("aws"); err == nil {
		// NB: This is a bit hacky, but using something like `aws iam get-user` is
		// slow and not something we want to do at startup.
		haveCredentials := func() bool {
			const credFile = "${HOME}/.aws/credentials"
			if _, err := os.Stat(os.ExpandEnv(credFile)); err == nil {
				return true
			}
			if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
				return true
			}
			return false
		}

		if haveCredentials() {
			vm.Providers[ProviderName] = &Provider{}
		}
	} else {
		// TODO(bob): This breaks the use of `roachprod pgurl --external` to
		// power roachtest's `(*cluster).Conn` and was failing the nightlies.
		// log.Printf("please install the AWS CLI utilities " +
		// 	"(https://docs.aws.amazon.com/cli/latest/userguide/installing.html)")
		_ = 0
	}
}

// providerOpts implements the vm.ProviderFlags interface for aws.Provider.
type providerOpts struct {
	AMI            []string
	MachineType    string
	SecurityGroups []string
	SSDMachineType string
	Subnets        []string
	RemoteUserName string
}

// ConfigureCreateFlags is part of the vm.ProviderFlags interface.
// This method sets up a lot of maps between the various EC2
// regions and the ids of the things we want to use there.  This is
// somewhat complicated because different EC2 regions may as well
// be parallel universes.
func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	// You can find AMI ids here https://cloud-images.ubuntu.com/locator/ec2/
	// Ubuntu Server 16.04 LTS (HVM), SSD Volume Type
	flags.StringSliceVar(&o.AMI, ProviderName+"-ami",
		[]string{
			"us-east-2:ami-965e6bf3",
			"us-west-2:ami-79873901",
			"eu-west-2:ami-941e04f0",
		},
		"AMI images for each region")

	// m5.xlarge is a 4core, 16Gb instance, approximately equal to a GCE n1-standard-4
	flags.StringVar(&o.MachineType, ProviderName+"-machine-type", "m5.xlarge",
		"Machine type (see https://aws.amazon.com/ec2/instance-types/)")

	// The m5 devices only support EBS volumes, so we need a different instance type
	// for directly-attached SSD support. This is 4 core, 16GB ram, 150GB ssd.
	flags.StringVar(&o.SSDMachineType, ProviderName+"-machine-type-ssd", "m5d.xlarge",
		"Machine type for --local-ssd (see https://aws.amazon.com/ec2/instance-types/)")

	// The subnet actually controls placement into a particular AZ
	flags.StringSliceVar(&o.Subnets, ProviderName+"-subnet",
		[]string{
			// m5 machines not yet available in us-east-2a.
			// "us-east-2a:subnet-3ea05c57",
			"us-east-2b:subnet-49170331",
			"us-east-2c:subnet-46c7f20c",
			"us-west-2a:subnet-fc46638b",
			"us-west-2b:subnet-2910174c",
			"us-west-2c:subnet-da2a5783",
			"eu-west-2a:subnet-e98a6c92",
			"eu-west-2b:subnet-3c754e76",
			"eu-west-2c:subnet-7733c91e",
		},
		"Subnet id for zones in each region")

	// Set up a roachprod security group in each region
	flags.StringSliceVar(&o.SecurityGroups, ProviderName+"-sg",
		[]string{
			"us-east-2:sg-06a4c809644e32920",
			"us-west-2:sg-00dfe24958e988576",
			"eu-west-2:sg-057f3842f5cec0576"},
		"Security group id in each region")

	// AWS images generally use "ubuntu" or "ec2-user"
	flags.StringVar(&o.RemoteUserName, ProviderName+"-user",
		"ubuntu", "Name of the remote user to SSH as")
}

func (o *providerOpts) ConfigureClusterFlags(flags *pflag.FlagSet) {
}

// Provider implements the vm.Provider interface for AWS.
type Provider struct {
	opts providerOpts
}

// CleanSSH is part of vm.Provider.  This implementation is a no-op,
// since we depend on the user's local identity file.
func (p *Provider) CleanSSH() error {
	return nil
}

// ConfigSSH ensures that for each region we're operating in, we have
// a <user>-<hash> keypair where <hash> is a hash of the public key.
// We use a hash since a user probably has multiple machines they're
// running roachprod on and these machines (ought to) have separate
// ssh keypairs.  If the remote keypair doesn't exist, we'll upload
// the user's ~/.ssh/id_rsa.pub file or ask them to generate one.
func (p *Provider) ConfigSSH() error {
	keyName, err := p.sshKeyName()
	if err != nil {
		return err
	}

	regions, err := p.allRegions()
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, r := range regions {
		// capture loop variable
		region := r
		g.Go(func() error {
			exists, err := sshKeyExists(keyName, region)
			if err != nil {
				return err
			}
			if !exists {
				err = sshKeyImport(keyName, region)
				if err != nil {
					return err
				}
				log.Printf("imported %s as %s in region %s",
					sshPublicKeyFile, keyName, region)
			}
			return nil
		})
	}

	return g.Wait()
}

// Create is part of the vm.Provider interface.
func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	// We need to make sure that the SSH keys have been distributed to all regions
	if err := p.ConfigSSH(); err != nil {
		return err
	}

	var placements []string
	regions, err := p.allRegions()
	if err != nil {
		return err
	}

	for _, region := range regions {
		zones, err := p.allZones(region)
		if err != nil {
			return err
		}
		placements = append(placements, zones...)

		// Only use one region if we're not creating a distributed cluster
		if !opts.GeoDistributed {
			break
		}
	}

	var g errgroup.Group

	var pIdx int
	for _, name := range names {
		// capture loop variable
		capName := name
		placement := placements[pIdx]
		g.Go(func() error {
			return p.runInstance(capName, placement, opts)
		})
		pIdx = (pIdx + 1) % len(placements)
	}

	return g.Wait()
}

// Delete is part of vm.Provider.
// This will delete all instances in a single AWS command.
func (p *Provider) Delete(vms vm.List) error {
	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}
	g := errgroup.Group{}
	for region, list := range byRegion {
		args := []string{
			"ec2", "terminate-instances",
			"--region", region,
			"--instance-ids",
		}
		args = append(args, list.ProviderIDs()...)
		g.Go(func() error {
			var data struct {
				TerminatingInstances []struct {
					InstanceID string `json:"InstanceId"`
				}
			}
			_ = data.TerminatingInstances // silence unused warning
			if len(data.TerminatingInstances) > 0 {
				_ = data.TerminatingInstances[0].InstanceID // silence unused warning
			}
			return runJSONCommand(args, &data)
		})
	}
	return g.Wait()
}

// Extend is part of the vm.Provider interface.
// This will update the Lifetime tag on the instances.
func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}
	g := errgroup.Group{}
	for region, list := range byRegion {
		// Capture loop vars here
		args := []string{
			"ec2", "create-tags",
			"--region", region,
			"--tags", "Key=Lifetime,Value=" + lifetime.String(),
			"--resources",
		}
		args = append(args, list.ProviderIDs()...)

		g.Go(func() error {
			return runCommand(args)
		})
	}
	return g.Wait()
}

// cachedActiveAccount memoizes the return value from FindActiveAccount
var cachedActiveAccount string

// FindActiveAccount is part of the vm.Provider interface.
// This queries the AWS command for the current IAM user.
func (p *Provider) FindActiveAccount() (string, error) {
	if len(cachedActiveAccount) > 0 {
		return cachedActiveAccount, nil
	}
	var userInfo struct {
		User struct {
			UserName string
		}
	}
	args := []string{"iam", "get-user"}
	err := runJSONCommand(args, &userInfo)
	if err != nil {
		return "", err
	}
	cachedActiveAccount = userInfo.User.UserName
	return cachedActiveAccount, nil
}

// Flags is part of the vm.Provider interface.
func (p *Provider) Flags() vm.ProviderFlags {
	return &p.opts
}

// List is part of the vm.Provider interface.
func (p *Provider) List() (vm.List, error) {
	regions, err := p.allRegions()
	if err != nil {
		return nil, err
	}

	var ret vm.List
	var mux syncutil.Mutex
	var g errgroup.Group

	for _, r := range regions {
		// capture loop variable
		region := r
		g.Go(func() error {
			vms, err := p.listRegion(region)
			if err != nil {
				return err
			}
			mux.Lock()
			ret = append(ret, vms...)
			mux.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return ret, nil
}

// Name is part of the vm.Provider interface. This returns "aws".
func (p *Provider) Name() string {
	return ProviderName
}

// allRegions returns the regions that have been configured with
// AMI and SecurityGroup instances.
func (p *Provider) allRegions() ([]string, error) {
	// We're using an ordered list instead of a map here to guarantee
	// the same ordering between calls.
	regionList, err := orderedKeyList(p.opts.AMI)
	if err != nil {
		return nil, err
	}

	securityMap, err := splitMap(p.opts.SecurityGroups)
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, region := range regionList {
		if _, ok := securityMap[region]; ok {
			keys = append(keys, region)
		} else {
			log.Printf("ignoring region %s because it has no associated SecurityGroup", region)
		}
	}
	return keys, nil
}

// allZones returns all AWS availability zones which have been correctly
// configured within the given region.
func (p *Provider) allZones(region string) ([]string, error) {
	subnetMap, err := splitMap(p.opts.Subnets)
	if err != nil {
		return nil, err
	}

	var ret []string
	for zone := range subnetMap {
		if strings.Index(zone, region) == 0 && len(zone) == len(region)+1 {
			ret = append(ret, zone)
		}
	}

	return ret, nil
}

// listRegion extracts the roachprod-managed instances in the
// given region.
func (p *Provider) listRegion(region string) (vm.List, error) {
	var data struct {
		Reservations []struct {
			Instances []struct {
				InstanceID string `json:"InstanceId"`
				LaunchTime string
				Placement  struct {
					AvailabilityZone string
				}
				PrivateDNSName   string `json:"PrivateDnsName"`
				PrivateIPAddress string `json:"PrivateIpAddress"`
				PublicDNSName    string `json:"PublicDnsName"`
				PublicIPAddress  string `json:"PublicIpAddress"`
				State            struct {
					Code int
					Name string
				}
				Tags []struct {
					Key   string
					Value string
				}
				VpcID        string `json:"VpcId"`
				InstanceType string
			}
		}
	}
	args := []string{
		"ec2", "describe-instances",
		"--region", region,
	}
	err := runJSONCommand(args, &data)
	if err != nil {
		return nil, err
	}

	var ret vm.List
	for _, res := range data.Reservations {
	in:
		for _, in := range res.Instances {
			// Ignore any instances that are not pending or running
			if in.State.Name != "pending" && in.State.Name != "running" {
				continue in
			}
			_ = in.PublicDNSName // silence unused warning
			_ = in.State.Code    // silence unused warning

			// Convert the tag map into a more useful representation
			tagMap := make(map[string]string, len(in.Tags))
			for _, entry := range in.Tags {
				tagMap[entry.Key] = entry.Value
			}
			// Ignore any instances that we didn't create
			if tagMap["Roachprod"] != "true" {
				continue in
			}

			var errs []error
			createdAt, err := time.Parse(time.RFC3339, in.LaunchTime)
			if err != nil {
				errs = append(errs, vm.ErrNoExpiration)
			}

			var lifetime time.Duration
			if lifeText, ok := tagMap["Lifetime"]; ok {
				lifetime, err = time.ParseDuration(lifeText)
				if err != nil {
					errs = append(errs, err)
				}
			} else {
				errs = append(errs, vm.ErrNoExpiration)
			}

			m := vm.VM{
				CreatedAt:   createdAt,
				DNS:         in.PrivateDNSName,
				Name:        tagMap["Name"],
				Errors:      errs,
				Lifetime:    lifetime,
				PrivateIP:   in.PrivateIPAddress,
				Provider:    ProviderName,
				ProviderID:  in.InstanceID,
				PublicIP:    in.PublicIPAddress,
				RemoteUser:  p.opts.RemoteUserName,
				VPC:         in.VpcID,
				MachineType: in.InstanceType,
				Zone:        in.Placement.AvailabilityZone,
			}
			ret = append(ret, m)
		}
	}

	return ret, nil
}

// runInstance is responsible for allocating a single ec2 vm.
// Given that every AWS region may as well be a parallel dimension,
// we need to do a bit of work to look up all of the various ids that
// we need in order to actually allocate an instance.
func (p *Provider) runInstance(name string, zone string, opts vm.CreateOpts) error {
	region, err := zoneToRegion(zone)
	if err != nil {
		return err
	}

	amiMap, err := splitMap(p.opts.AMI)
	if err != nil {
		return err
	}
	amiID, ok := amiMap[region]
	if !ok {
		return errors.Errorf("could not find an AMI image id for region %s", region)
	}

	keyName, err := p.sshKeyName()
	if err != nil {
		return err
	}

	var machineType string
	if opts.UseLocalSSD {
		machineType = p.opts.SSDMachineType
	} else {
		machineType = p.opts.MachineType
	}

	sgMap, err := splitMap(p.opts.SecurityGroups)
	if err != nil {
		return err
	}
	sgID, ok := sgMap[region]
	if !ok {
		return errors.Errorf("could not find a security group id for region %s", region)
	}

	subnetMap, err := splitMap(p.opts.Subnets)
	if err != nil {
		return err
	}
	subnetID, ok := subnetMap[zone]
	if !ok {
		return errors.Errorf("could not find a subnet id for zone %s", zone)
	}

	// We avoid the need to make a second call to set the tags by jamming
	// all of our metadata into the TagSpec.
	tagSpecs := fmt.Sprintf(
		"ResourceType=instance,Tags=["+
			"{Key=Lifetime,Value=%s},"+
			"{Key=Name,Value=%s},"+
			"{Key=Roachprod,Value=true},"+
			"]", opts.Lifetime, name)

	var data struct {
		Instances []struct {
			InstanceID string `json:"InstanceId"`
		}
	}
	_ = data.Instances // silence unused warning
	if len(data.Instances) > 0 {
		_ = data.Instances[0].InstanceID // silence unused warning
	}

	args := []string{
		"ec2", "run-instances",
		"--associate-public-ip-address",
		"--count", "1",
		"--image-id", amiID,
		"--instance-type", machineType,
		"--key-name", keyName,
		"--region", region,
		"--security-group-ids", sgID,
		"--subnet-id", subnetID,
		"--tag-specifications", tagSpecs,
		"--user-data", awsStartupScript,
	}

	// The local NVMe devices are automatically mapped.  Otherwise, we need to map an EBS data volume.
	if !opts.UseLocalSSD {
		args = append(args,
			"--block-device-mapping",
			// Size is measured in GB.  gp2 type derives guaranteed iops from size.
			"DeviceName=/dev/sdd,Ebs={VolumeSize=500,VolumeType=gp2,DeleteOnTermination=true}",
		)
	}

	return runJSONCommand(args, &data)
}
