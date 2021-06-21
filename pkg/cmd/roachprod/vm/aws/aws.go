// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aws

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// ProviderName is aws.
const ProviderName = "aws"

// init will inject the AWS provider into vm.Providers, but only
// if the aws tool is available on the local path.
func init() {
	// aws-cli version 1 automatically base64 encodes the string passed as --public-key-material.
	// Version 2 supports file:// and fileb:// prefixes for text and binary files.
	// The latter prefix will base64-encode the file contents. See
	// https://docs.aws.amazon.//com/cli/latest/userguide/cliv2-migration.html#cliv2-migration-binaryparam
	const unsupportedAwsCliVersionPrefix = "aws-cli/1."
	const unimplemented = "please install the AWS CLI utilities version 2+ " +
		"(https://docs.aws.amazon.com/cli/latest/userguide/installing.html)"
	const noCredentials = "missing AWS credentials, expected ~/.aws/credentials file or AWS_ACCESS_KEY_ID env var"

	var p vm.Provider = &Provider{}

	haveRequiredVersion := func() bool {
		cmd := exec.Command("aws", "--version")
		output, err := cmd.Output()
		if err != nil {
			return false
		}
		if strings.HasPrefix(string(output), unsupportedAwsCliVersionPrefix) {
			return false
		}
		return true
	}
	if !haveRequiredVersion() {
		p = flagstub.New(p, unimplemented)
		vm.Providers[ProviderName] = p
		return
	}

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
	if !haveCredentials() {
		p = flagstub.New(p, noCredentials)
		vm.Providers[ProviderName] = p
		return
	}

	vm.Providers[ProviderName] = p
}

// ebsDisk represent EBS disk device.
// When marshaled to JSON format, produces JSON specification used
// by AWS sdk to configure attached volumes.
type ebsDisk struct {
	VolumeType          string `json:"VolumeType"`
	VolumeSize          int    `json:"VolumeSize"`
	IOPs                int    `json:"Iops,omitempty"`
	Throughput          int    `json:"Throughput,omitempty"`
	DeleteOnTermination bool   `json:"DeleteOnTermination"`
}

// ebsVolume represents a mounted volume: name + ebsDisk
type ebsVolume struct {
	DeviceName string  `json:"DeviceName"`
	Disk       ebsDisk `json:"Ebs"`
}

const ebsDefaultVolumeSizeGB = 500
const defaultEBSVolumeType = "gp3"

// Set implements flag Value interface.
func (d *ebsDisk) Set(s string) error {
	if err := json.Unmarshal([]byte(s), &d); err != nil {
		return err
	}

	d.DeleteOnTermination = true

	// Sanity check disk configuration.
	// This is not strictly needed since AWS sdk would return error anyway,
	// but we can return a nicer error message sooner.
	if d.VolumeSize == 0 {
		d.VolumeSize = ebsDefaultVolumeSizeGB
	}

	switch strings.ToLower(d.VolumeType) {
	case "gp2":
		// Nothing -- size checked above.
	case "gp3":
		if d.IOPs > 16000 {
			return errors.AssertionFailedf("Iops required for gp3 disk: [3000, 16000]")
		}
		if d.IOPs == 0 {
			// 30000 is a base IOPs for gp3.
			d.IOPs = 3000
		}
		if d.Throughput == 0 {
			// 125MB/s is base throughput for gp3.
			d.Throughput = 125
		}
	case "io1", "io2":
		if d.IOPs == 0 {
			return errors.AssertionFailedf("Iops required for %s disk", d.VolumeType)
		}
	default:
		return errors.Errorf("Unknown EBS volume type %s", d.VolumeType)
	}
	return nil
}

// Type implements flag Value interface.
func (d *ebsDisk) Type() string {
	return "JSON"
}

// String Implements flag Value interface.
func (d *ebsDisk) String() string {
	return "EBSDisk"
}

type ebsVolumeList []*ebsVolume

func (vl *ebsVolumeList) newVolume() *ebsVolume {
	return &ebsVolume{
		DeviceName: fmt.Sprintf("/dev/sd%c", 'd'+len(*vl)),
	}
}

// Set implements flag Value interface.
func (vl *ebsVolumeList) Set(s string) error {
	v := vl.newVolume()
	if err := v.Disk.Set(s); err != nil {
		return err
	}
	*vl = append(*vl, v)
	return nil
}

// Type implements flag Value interface.
func (vl *ebsVolumeList) Type() string {
	return "JSON"
}

// String Implements flag Value interface.
func (vl *ebsVolumeList) String() string {
	return "EBSVolumeList"
}

// providerOpts implements the vm.ProviderFlags interface for aws.Provider.
type providerOpts struct {
	Profile string
	Config  *awsConfig

	MachineType      string
	SSDMachineType   string
	CPUOptions       string
	RemoteUserName   string
	DefaultEBSVolume ebsVolume
	EBSVolumes       ebsVolumeList
	UseMultipleDisks bool

	// Use specified ImageAMI when provisioning.
	// Overrides config.json AMI.
	ImageAMI string

	// IAMProfile designates the name of the instance profile to use for created
	// EC2 instances if non-empty.
	IAMProfile string

	// CreateZones stores the list of zones for used cluster creation.
	// When > 1 zone specified, geo is automatically used, otherwise, geo depends
	// on the geo flag being set. If no zones specified, defaultCreateZones are
	// used. See defaultCreateZones.
	CreateZones []string
	// CreateRateLimit specifies the rate limit used for aws instance creation.
	// The request limit from aws' side can vary across regions, as well as the
	// size of cluster being created.
	CreateRateLimit float64
}

const (
	defaultSSDMachineType = "m5d.xlarge"
	defaultMachineType    = "m5.xlarge"
)

var defaultConfig = func() (cfg *awsConfig) {
	cfg = new(awsConfig)
	if err := json.Unmarshal(MustAsset("config.json"), cfg); err != nil {
		panic(errors.Wrap(err, "failed to embedded configuration"))
	}
	return cfg
}()

// defaultCreateZones is the list of availability zones used by default for
// cluster creation. If the geo flag is specified, nodes are distributed between
// zones.
var defaultCreateZones = []string{
	"us-east-2b",
	"us-west-2b",
	"eu-west-2b",
}

// ConfigureCreateFlags is part of the vm.ProviderFlags interface.
// This method sets up a lot of maps between the various EC2
// regions and the ids of the things we want to use there.  This is
// somewhat complicated because different EC2 regions may as well
// be parallel universes.
func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	// m5.xlarge is a 4core, 16Gb instance, approximately equal to a GCE n1-standard-4
	flags.StringVar(&o.MachineType, ProviderName+"-machine-type", defaultMachineType,
		"Machine type (see https://aws.amazon.com/ec2/instance-types/)")

	// The m5 devices only support EBS volumes, so we need a different instance type
	// for directly-attached SSD support. This is 4 core, 16GB ram, 150GB ssd.
	flags.StringVar(&o.SSDMachineType, ProviderName+"-machine-type-ssd", defaultSSDMachineType,
		"Machine type for --local-ssd (see https://aws.amazon.com/ec2/instance-types/)")

	flags.StringVar(&o.CPUOptions, ProviderName+"-cpu-options", "",
		"Options to specify number of cores and threads per core (see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-optimize-cpu.html#instance-specify-cpu-options)")

	// AWS images generally use "ubuntu" or "ec2-user"
	flags.StringVar(&o.RemoteUserName, ProviderName+"-user",
		"ubuntu", "Name of the remote user to SSH as")

	flags.StringVar(&o.DefaultEBSVolume.Disk.VolumeType, ProviderName+"-ebs-volume-type",
		"", "Type of the EBS volume, only used if local-ssd=false")
	flags.IntVar(&o.DefaultEBSVolume.Disk.VolumeSize, ProviderName+"-ebs-volume-size",
		ebsDefaultVolumeSizeGB, "Size in GB of EBS volume, only used if local-ssd=false")
	flags.IntVar(&o.DefaultEBSVolume.Disk.IOPs, ProviderName+"-ebs-iops",
		0, "Number of IOPs to provision for supported disk types (io1, io2, gp3)")
	flags.IntVar(&o.DefaultEBSVolume.Disk.Throughput, ProviderName+"-ebs-throughput",
		0, "Additional throughput to provision, in MiB/s")

	flags.VarP(&o.EBSVolumes, ProviderName+"-ebs-volume", "",
		"Additional EBS disk to attached; specified as JSON: {VolumeType=io2,VolumeSize=213,Iops=321}")

	flags.StringSliceVar(&o.CreateZones, ProviderName+"-zones", nil,
		fmt.Sprintf("aws availability zones to use for cluster creation. If zones are formatted\n"+
			"as AZ:N where N is an integer, the zone will be repeated N times. If > 1\n"+
			"zone specified, the cluster will be spread out evenly by zone regardless\n"+
			"of geo (default [%s])", strings.Join(defaultCreateZones, ",")))
	flags.StringVar(&o.ImageAMI, ProviderName+"-image-ami",
		"", "Override image AMI to use.  See https://awscli.amazonaws.com/v2/documentation/api/latest/reference/ec2/describe-images.html")
	flags.BoolVar(&o.UseMultipleDisks, ProviderName+"-enable-multiple-stores",
		false, "Enable the use of multiple stores by creating one store directory per disk.  Default is to raid0 stripe all disks.")
	flags.Float64Var(&o.CreateRateLimit, ProviderName+"-create-rate-limit", 2, "aws"+
		" rate limit (per second) for instance creation. This is used to avoid hitting the request"+
		" limits from aws, which can vary based on the region, and the size of the cluster being"+
		" created. Try lowering this limit when hitting 'Request limit exceeded' errors.")
	flags.StringVar(&o.IAMProfile, ProviderName+"-	iam-profile", "roachprod-testing",
		"the IAM instance profile to associate with created VMs if non-empty")

}

func (o *providerOpts) ConfigureClusterFlags(flags *pflag.FlagSet, _ vm.MultipleProjectsOption) {
	profile := os.Getenv("AWS_DEFAULT_PROFILE") // "" if unset
	flags.StringVar(&o.Profile, ProviderName+"-profile", profile,
		"Profile to manage cluster in")
	configFlagVal := awsConfigValue{awsConfig: *defaultConfig}
	o.Config = &configFlagVal.awsConfig
	flags.Var(&configFlagVal, ProviderName+"-config",
		"Path to json for aws configuration, defaults to predefined configuration")
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

	regions, err := p.allRegions(p.opts.Config.availabilityZoneNames())
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, r := range regions {
		// capture loop variable
		region := r
		g.Go(func() error {
			exists, err := p.sshKeyExists(keyName, region)
			if err != nil {
				return err
			}
			if !exists {
				err = p.sshKeyImport(keyName, region)
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

	expandedZones, err := vm.ExpandZonesFlag(p.opts.CreateZones)
	if err != nil {
		return err
	}

	useDefaultZones := len(expandedZones) == 0
	if useDefaultZones {
		expandedZones = defaultCreateZones
	}

	regions, err := p.allRegions(expandedZones)
	if err != nil {
		return err
	}
	if len(regions) < 1 {
		return errors.Errorf("Please specify a valid region.")
	}

	var zones []string // contains an az corresponding to each entry in names
	if !opts.GeoDistributed && (useDefaultZones || len(expandedZones) == 1) {
		// Only use one zone in the region if we're not creating a geo cluster.
		regionZones, err := p.regionZones(regions[0], expandedZones)
		if err != nil {
			return err
		}
		// Select a random AZ from the first region.
		zone := regionZones[rand.Intn(len(regionZones))]
		for range names {
			zones = append(zones, zone)
		}
	} else {
		// Distribute the nodes amongst availability zones if geo distributed.
		nodeZones := vm.ZonePlacement(len(expandedZones), len(names))
		zones = make([]string, len(nodeZones))
		for i, z := range nodeZones {
			zones[i] = expandedZones[z]
		}
	}
	var g errgroup.Group
	limiter := rate.NewLimiter(rate.Limit(p.opts.CreateRateLimit), 2 /* buckets */)
	for i := range names {
		capName := names[i]
		placement := zones[i]
		res := limiter.Reserve()
		g.Go(func() error {
			time.Sleep(res.Delay())
			return p.runInstance(capName, placement, opts)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return p.waitForIPs(names, regions)
}

// waitForIPs waits until AWS reports both internal and external IP addresses
// for all newly created VMs. If we did not wait for these IPs then attempts to
// list the new VMs after the creation might find VMs without an external IP.
// We do a bad job at higher layers detecting this lack of IP which can lead to
// commands hanging indefinitely.
func (p *Provider) waitForIPs(names []string, regions []string) error {
	waitForIPRetry := retry.Start(retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		MaxRetries:     120, // wait a bit less than 90s for IPs
	})
	makeNameSet := func() map[string]struct{} {
		m := make(map[string]struct{}, len(names))
		for _, n := range names {
			m[n] = struct{}{}
		}
		return m
	}
	for waitForIPRetry.Next() {
		vms, err := p.listRegions(regions)
		if err != nil {
			return err
		}
		nameSet := makeNameSet()
		for _, vm := range vms {
			if vm.PublicIP != "" && vm.PrivateIP != "" {
				delete(nameSet, vm.Name)
			}
		}
		if len(nameSet) == 0 {
			return nil
		}
	}
	return fmt.Errorf("failed to retrieve IPs for all vms")
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
			return p.runJSONCommand(args, &data)
		})
	}
	return g.Wait()
}

// Reset is part of vm.Provider. It is a no-op.
func (p *Provider) Reset(vms vm.List) error {
	return nil // unimplemented
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
			_, err := p.runCommand(args)
			return err
		})
	}
	return g.Wait()
}

// cachedActiveAccount memoizes the return value from FindActiveAccount
var cachedActiveAccount string

// FindActiveAccount is part of the vm.Provider interface.
// This queries the AWS command for the current IAM user or role.
func (p *Provider) FindActiveAccount() (string, error) {
	if len(cachedActiveAccount) > 0 {
		return cachedActiveAccount, nil
	}
	var account string
	var err error
	if p.opts.Profile == "" {
		account, err = p.iamGetUser()
		if err != nil {
			return "", err
		}
	} else {
		account, err = p.stsGetCallerIdentity()
		if err != nil {
			return "", err
		}
	}
	cachedActiveAccount = account
	return cachedActiveAccount, nil
}

// iamGetUser returns the identity of an IAM user.
func (p *Provider) iamGetUser() (string, error) {
	var userInfo struct {
		User struct {
			UserName string
		}
	}
	args := []string{"iam", "get-user"}
	err := p.runJSONCommand(args, &userInfo)
	if err != nil {
		return "", err
	}
	if userInfo.User.UserName == "" {
		return "", errors.Errorf("username not configured. run 'aws iam get-user'")
	}
	return userInfo.User.UserName, nil
}

// stsGetCallerIdentity returns the identity of a user assuming a role
// into the account.
func (p *Provider) stsGetCallerIdentity() (string, error) {
	var userInfo struct {
		Arn string
	}
	args := []string{"sts", "get-caller-identity"}
	err := p.runJSONCommand(args, &userInfo)
	if err != nil {
		return "", err
	}
	s := strings.Split(userInfo.Arn, "/")
	if len(s) < 2 {
		return "", errors.Errorf("Could not parse caller identity ARN '%s'", userInfo.Arn)
	}
	return s[1], nil
}

// Flags is part of the vm.Provider interface.
func (p *Provider) Flags() vm.ProviderFlags {
	return &p.opts
}

// List is part of the vm.Provider interface.
func (p *Provider) List() (vm.List, error) {
	regions, err := p.allRegions(p.opts.Config.availabilityZoneNames())
	if err != nil {
		return nil, err
	}
	return p.listRegions(regions)
}

func (p *Provider) listRegions(regions []string) (vm.List, error) {
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
func (p *Provider) allRegions(zones []string) (regions []string, err error) {
	byName := make(map[string]struct{})
	for _, z := range zones {
		az := p.opts.Config.getAvailabilityZone(z)
		if az == nil {
			return nil, fmt.Errorf("unknown availability zone %v, please provide a "+
				"correct value or update your config accordingly", z)
		}
		if _, have := byName[az.region.Name]; !have {
			byName[az.region.Name] = struct{}{}
			regions = append(regions, az.region.Name)
		}
	}
	return regions, nil
}

// regionZones returns all AWS availability zones which have been correctly
// configured within the given region.
func (p *Provider) regionZones(region string, allZones []string) (zones []string, _ error) {
	r := p.opts.Config.getRegion(region)
	if r == nil {
		return nil, fmt.Errorf("region %s not found", region)
	}
	for _, z := range allZones {
		for _, az := range r.AvailabilityZones {
			if az.name == z {
				zones = append(zones, z)
				break
			}
		}
	}
	return zones, nil
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
	err := p.runJSONCommand(args, &data)
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
	// There exist different flags to control the machine type when ssd is true.
	// This enables sane defaults for either setting but the behavior can be
	// confusing when a user attempts to use `--aws-machine-type` and the command
	// succeeds but the flag is ignored. Rather than permit this behavior we
	// return an error instructing the user to use the other flag.
	if opts.SSDOpts.UseLocalSSD &&
		p.opts.MachineType != defaultMachineType &&
		p.opts.SSDMachineType == defaultSSDMachineType {
		return errors.Errorf("use the --aws-machine-type-ssd flag to set the " +
			"machine type when --local-ssd=true")
	} else if !opts.SSDOpts.UseLocalSSD &&
		p.opts.MachineType == defaultMachineType &&
		p.opts.SSDMachineType != defaultSSDMachineType {
		return errors.Errorf("use the --aws-machine-type flag to set the " +
			"machine type when --local-ssd=false")
	}

	az, ok := p.opts.Config.azByName[zone]
	if !ok {
		return fmt.Errorf("no region in %v corresponds to availability zone %v",
			p.opts.Config.regionNames(), zone)
	}

	keyName, err := p.sshKeyName()
	if err != nil {
		return err
	}

	var machineType string
	if opts.SSDOpts.UseLocalSSD {
		machineType = p.opts.SSDMachineType
	} else {
		machineType = p.opts.MachineType
	}

	cpuOptions := p.opts.CPUOptions

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

	// Create AWS startup script file.
	extraMountOpts := ""
	// Dynamic args.
	if opts.SSDOpts.UseLocalSSD {
		if opts.SSDOpts.NoExt4Barrier {
			extraMountOpts = "nobarrier"
		}
	}
	filename, err := writeStartupScript(extraMountOpts, p.opts.UseMultipleDisks)
	if err != nil {
		return errors.Wrapf(err, "could not write AWS startup script to temp file")
	}
	defer func() {
		_ = os.Remove(filename)
	}()

	withFlagOverride := func(cfg string, fl *string) string {
		if *fl == "" {
			return cfg
		}
		return *fl
	}

	args := []string{
		"ec2", "run-instances",
		"--associate-public-ip-address",
		"--count", "1",
		"--instance-type", machineType,
		"--image-id", withFlagOverride(az.region.AMI, &p.opts.ImageAMI),
		"--key-name", keyName,
		"--region", az.region.Name,
		"--security-group-ids", az.region.SecurityGroup,
		"--subnet-id", az.subnetID,
		"--tag-specifications", tagSpecs,
		"--user-data", "file://" + filename,
	}

	if cpuOptions != "" {
		args = append(args, "--cpu-options", cpuOptions)
	}

	if p.opts.IAMProfile != "" {
		args = append(args, "--iam-instance-profile", "Name="+p.opts.IAMProfile)
	}

	// The local NVMe devices are automatically mapped.  Otherwise, we need to map an EBS data volume.
	if !opts.SSDOpts.UseLocalSSD {
		if len(p.opts.EBSVolumes) == 0 && p.opts.DefaultEBSVolume.Disk.VolumeType == "" {
			p.opts.DefaultEBSVolume.Disk.VolumeType = defaultEBSVolumeType
		}

		if p.opts.DefaultEBSVolume.Disk.VolumeType != "" {
			// Add default volume to the list of volumes we'll setup.
			v := p.opts.EBSVolumes.newVolume()
			v.Disk = p.opts.DefaultEBSVolume.Disk
			p.opts.EBSVolumes = append(p.opts.EBSVolumes, v)
		}
	}

	osDiskVolume := &ebsVolume{
		DeviceName: "/dev/sda1",
		Disk: ebsDisk{
			VolumeType:          defaultEBSVolumeType,
			VolumeSize:          opts.OsVolumeSize,
			DeleteOnTermination: true,
		},
	}

	p.opts.EBSVolumes = append(p.opts.EBSVolumes, osDiskVolume)

	mapping, err := json.Marshal(p.opts.EBSVolumes)
	if err != nil {
		return err
	}

	deviceMapping, err := ioutil.TempFile("", "aws-block-device-mapping")
	if err != nil {
		return err
	}
	defer deviceMapping.Close()
	if _, err := deviceMapping.Write(mapping); err != nil {
		return err
	}
	args = append(args,
		"--block-device-mapping",
		"file://"+deviceMapping.Name(),
	)

	return p.runJSONCommand(args, &data)
}

// Active is part of the vm.Provider interface.
func (p *Provider) Active() bool {
	return true
}
