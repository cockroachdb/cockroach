// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package aws provides functionality for the aws provider.
package aws

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// ProviderName is aws.
const ProviderName = "aws"

// providerInstance is the instance to be registered into vm.Providers by Init.
var providerInstance = &Provider{}

//go:embed config.json
var configJson []byte

//go:embed old.json
var oldJson []byte

// Init initializes the AWS provider and registers it into vm.Providers.
//
// If the aws tool is not available on the local path, the provider is a stub.
func Init() error {
	// aws-cli version 1 automatically base64 encodes the string passed as --public-key-material.
	// Version 2 supports file:// and fileb:// prefixes for text and binary files.
	// The latter prefix will base64-encode the file contents. See
	// https://docs.aws.amazon.//com/cli/latest/userguide/cliv2-migration.html#cliv2-migration-binaryparam
	const unsupportedAwsCliVersionPrefix = "aws-cli/1."
	const unimplemented = "please install the AWS CLI utilities version 2+ " +
		"(https://docs.aws.amazon.com/cli/latest/userguide/installing.html)"
	const noCredentials = "missing AWS credentials, expected ~/.aws/credentials file or AWS_ACCESS_KEY_ID env var"

	configVal := awsConfigValue{awsConfig: *defaultConfig}
	providerInstance.Config = &configVal.awsConfig
	providerInstance.IAMProfile = "roachprod-testing"

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
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, unimplemented)
		return errors.New("doesn't have the required version")
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
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, noCredentials)
		return errors.New("missing/invalid credentials")
	}
	vm.Providers[ProviderName] = providerInstance
	return nil
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
			// 3000 is a base IOPs for gp3.
			d.IOPs = 3000
		}
		if d.Throughput == 0 {
			// 125MB/s is base throughput for gp3.
			d.Throughput = 125
		}

		if d.IOPs < d.Throughput*4 {
			d.IOPs = d.Throughput * 6
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

// DefaultProviderOpts returns a new aws.ProviderOpts with default values set.
func DefaultProviderOpts() *ProviderOpts {
	defaultEBSVolumeValue := ebsVolume{}
	defaultEBSVolumeValue.Disk.VolumeSize = ebsDefaultVolumeSizeGB
	defaultEBSVolumeValue.Disk.VolumeType = defaultEBSVolumeType
	return &ProviderOpts{
		MachineType:      defaultMachineType,
		SSDMachineType:   defaultSSDMachineType,
		RemoteUserName:   "ubuntu",
		DefaultEBSVolume: defaultEBSVolumeValue,
		CreateRateLimit:  2,
	}
}

// CreateProviderOpts returns a new aws.ProviderOpts with default values set.
func (p *Provider) CreateProviderOpts() vm.ProviderOpts {
	return DefaultProviderOpts()
}

// ProviderOpts provides user-configurable, aws-specific create options.
type ProviderOpts struct {
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

	// CreateZones stores the list of zones for used cluster creation.
	// When > 1 zone specified, geo is automatically used, otherwise, geo depends
	// on the geo flag being set. If no zones specified, defaultCreateZones are
	// used. See defaultCreateZones.
	CreateZones []string
	// CreateRateLimit specifies the rate limit used for aws instance creation.
	// The request limit from aws' side can vary across regions, as well as the
	// size of cluster being created.
	CreateRateLimit float64
	// use spot vms, spot vms are significantly cheaper, but can be preempted AWS.
	// see https://aws.amazon.com/ec2/spot/ for more details.
	UseSpot bool
}

// Provider implements the vm.Provider interface for AWS.
type Provider struct {
	// Profile to manage cluster in
	Profile string

	// Path to json for aws configuration, defaults to predefined configuration
	Config *awsConfig

	// IAMProfile designates the name of the instance profile to use for created
	// EC2 instances if non-empty.
	IAMProfile string
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

const (
	defaultSSDMachineType = "m6id.xlarge"
	defaultMachineType    = "m6i.xlarge"
)

var defaultConfig = func() (cfg *awsConfig) {
	cfg = new(awsConfig)
	if err := json.Unmarshal(configJson, cfg); err != nil {
		panic(errors.Wrap(err, "failed to embedded configuration"))
	}
	return cfg
}()

// defaultZones is the list of availability zones used by default for
// cluster creation. If the geo flag is specified, nodes are
// distributed between zones.
//
// NOTE: a number of AWS roachtests are dependent on us-east-2 for
// loading fixtures, out of s3://cockroach-fixtures-us-east-2. AWS
// doesn't support multi-regional buckets, thus resulting in material
// egress cost if the test loads from a different region. See
// https://github.com/cockroachdb/cockroach/issues/105968.
var defaultZones = []string{
	"us-east-2a",
	"us-west-2b",
	"eu-west-2b",
}

type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type Tags []Tag

func (t Tags) MakeMap() map[string]string {
	tagMap := make(map[string]string, len(t))
	for _, entry := range t {
		tagMap[entry.Key] = entry.Value
	}
	return tagMap
}

func (t Tags) String() string {
	var output []string
	for _, tag := range t {
		output = append(output, fmt.Sprintf("{Key=%s,Value=%s}", tag.Key, tag.Value))
	}
	return strings.Join(output, ",")
}

// ConfigureCreateFlags is part of the vm.ProviderOpts interface.
// This method sets up a lot of maps between the various EC2
// regions and the ids of the things we want to use there.  This is
// somewhat complicated because different EC2 regions may as well
// be parallel universes.
func (o *ProviderOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	// m5.xlarge is a 4core, 16Gb instance, approximately equal to a GCE n1-standard-4
	flags.StringVar(&o.MachineType, ProviderName+"-machine-type", o.MachineType,
		"Machine type (see https://aws.amazon.com/ec2/instance-types/)")

	// The m5 devices only support EBS volumes, so we need a different instance type
	// for directly-attached SSD support. This is 4 core, 16GB ram, 150GB ssd.
	flags.StringVar(&o.SSDMachineType, ProviderName+"-machine-type-ssd", o.SSDMachineType,
		"Machine type for --local-ssd (see https://aws.amazon.com/ec2/instance-types/)")

	flags.StringVar(&o.CPUOptions, ProviderName+"-cpu-options", o.CPUOptions,
		"Options to specify number of cores and threads per core (see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-optimize-cpu.html#instance-specify-cpu-options)")

	// AWS images generally use "ubuntu" or "ec2-user"
	flags.StringVar(&o.RemoteUserName, ProviderName+"-user",
		o.RemoteUserName, "Name of the remote user to SSH as")

	flags.StringVar(&o.DefaultEBSVolume.Disk.VolumeType, ProviderName+"-ebs-volume-type",
		o.DefaultEBSVolume.Disk.VolumeType, "Type of the EBS volume, only used if local-ssd=false")
	flags.IntVar(&o.DefaultEBSVolume.Disk.VolumeSize, ProviderName+"-ebs-volume-size",
		o.DefaultEBSVolume.Disk.VolumeSize, "Size in GB of EBS volume, only used if local-ssd=false")
	flags.IntVar(&o.DefaultEBSVolume.Disk.IOPs, ProviderName+"-ebs-iops",
		o.DefaultEBSVolume.Disk.IOPs, "Number of IOPs to provision for supported disk types (io1, io2, gp3)")
	flags.IntVar(&o.DefaultEBSVolume.Disk.Throughput, ProviderName+"-ebs-throughput",
		o.DefaultEBSVolume.Disk.Throughput, "Additional throughput to provision, in MiB/s")

	flags.VarP(&o.EBSVolumes, ProviderName+"-ebs-volume", "",
		`Additional EBS disk to attached, repeated for extra disks; specified as JSON: {"VolumeType":"io2","VolumeSize":213,"Iops":321}`)

	flags.StringSliceVar(&o.CreateZones, ProviderName+"-zones", o.CreateZones,
		fmt.Sprintf("aws availability zones to use for cluster creation. If zones are formatted\n"+
			"as AZ:N where N is an integer, the zone will be repeated N times. If > 1\n"+
			"zone specified, the cluster will be spread out evenly by zone regardless\n"+
			"of geo (default [%s])", strings.Join(defaultZones, ",")))
	flags.StringVar(&o.ImageAMI, ProviderName+"-image-ami",
		o.ImageAMI, "Override image AMI to use.  See https://awscli.amazonaws.com/v2/documentation/api/latest/reference/ec2/describe-images.html")
	flags.BoolVar(&o.UseMultipleDisks, ProviderName+"-enable-multiple-stores",
		false, "Enable the use of multiple stores by creating one store directory per disk. "+
			"Default is to raid0 stripe all disks. "+
			"See repeating --"+ProviderName+"-ebs-volume for adding extra volumes.")
	flags.Float64Var(&o.CreateRateLimit, ProviderName+"-create-rate-limit", o.CreateRateLimit, "aws"+
		" rate limit (per second) for instance creation. This is used to avoid hitting the request"+
		" limits from aws, which can vary based on the region, and the size of the cluster being"+
		" created. Try lowering this limit when hitting 'Request limit exceeded' errors.")
	flags.BoolVar(&o.UseSpot, ProviderName+"-use-spot",
		false, "use AWS Spot VMs, which are significantly cheaper, but can be preempted by AWS.")
	flags.StringVar(&providerInstance.IAMProfile, ProviderName+"-iam-profile", providerInstance.IAMProfile,
		"the IAM instance profile to associate with created VMs if non-empty")

}

// ConfigureClusterFlags implements vm.ProviderOpts.
func (o *ProviderOpts) ConfigureClusterFlags(flags *pflag.FlagSet, _ vm.MultipleProjectsOption) {
	flags.StringVar(&providerInstance.Profile, ProviderName+"-profile", providerInstance.Profile,
		"Profile to manage cluster in")
	configFlagVal := awsConfigValue{awsConfig: *defaultConfig}
	providerInstance.Config = &configFlagVal.awsConfig
	flags.Var(&configFlagVal, ProviderName+"-config",
		"Path to json for aws configuration, defaults to predefined configuration")
}

// CleanSSH is part of vm.Provider.  This implementation is a no-op,
// since we depend on the user's local identity file.
func (p *Provider) CleanSSH(l *logger.Logger) error {
	return nil
}

// ConfigSSH is part of the vm.Provider interface.
func (p *Provider) ConfigSSH(l *logger.Logger, zones []string) error {
	keyName, err := p.sshKeyName(l)
	if err != nil {
		return err
	}

	regions, err := p.allRegions(zones)
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
		// capture loop variable
		region := r
		g.Go(func() error {
			exists, err := p.sshKeyExists(l, keyName, region)
			if err != nil {
				return err
			}
			if !exists {
				err = p.sshKeyImport(l, keyName, region)
				if err != nil {
					return err
				}
				sshPublicKeyPath, err := config.SSHPublicKeyPath()
				if err != nil {
					return err
				}
				l.Printf("imported %s as %s in region %s", sshPublicKeyPath, keyName, region)
			}
			return nil
		})
	}

	return g.Wait()
}

// editLabels is a helper that adds or removes labels from the given VMs.
func (p *Provider) editLabels(
	l *logger.Logger, vms vm.List, labels map[string]string, remove bool,
) error {
	args := []string{"ec2"}
	if remove {
		args = append(args, "delete-tags")
	} else {
		args = append(args, "create-tags")
	}

	args = append(args, "--tags")
	tagArgs := make([]string, 0, len(labels))
	for key, value := range labels {
		if remove {
			tagArgs = append(tagArgs, fmt.Sprintf("Key=%s", key))
		} else {
			tagArgs = append(tagArgs, fmt.Sprintf("Key=%s,Value=%s", key, vm.SanitizeLabel(value)))
		}
	}
	args = append(args, tagArgs...)

	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}
	g := errgroup.Group{}
	for region, list := range byRegion {
		// Capture loop vars here
		regionArgs := make([]string, len(args))
		copy(regionArgs, args)

		regionArgs = append(regionArgs, "--region", region)
		regionArgs = append(regionArgs, "--resources")
		regionArgs = append(regionArgs, list.ProviderIDs()...)

		g.Go(func() error {
			_, err := p.runCommand(l, regionArgs)
			return err
		})
	}
	return g.Wait()
}

// AddLabels adds the given labels to the given VMs.
func (p *Provider) AddLabels(l *logger.Logger, vms vm.List, labels map[string]string) error {
	return p.editLabels(l, vms, labels, false)
}

// RemoveLabels removes the given labels from the given VMs.
func (p *Provider) RemoveLabels(l *logger.Logger, vms vm.List, labels []string) error {
	labelMap := make(map[string]string, len(labels))
	for _, label := range labels {
		labelMap[label] = ""
	}
	return p.editLabels(l, vms, labelMap, true)
}

// Create is part of the vm.Provider interface.
func (p *Provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, vmProviderOpts vm.ProviderOpts,
) error {
	providerOpts := vmProviderOpts.(*ProviderOpts)
	// There exist different flags to control the machine type when ssd is true.
	// This enables sane defaults for either setting but the behavior can be
	// confusing when a user attempts to use `--aws-machine-type` and the command
	// succeeds but the flag is ignored. Rather than permit this behavior we
	// return an error instructing the user to use the other flag.
	if opts.SSDOpts.UseLocalSSD &&
		providerOpts.MachineType != defaultMachineType &&
		providerOpts.SSDMachineType == defaultSSDMachineType {
		return errors.Errorf("use the --aws-machine-type-ssd flag to set the " +
			"machine type when --local-ssd=true")
	} else if !opts.SSDOpts.UseLocalSSD &&
		providerOpts.MachineType == defaultMachineType &&
		providerOpts.SSDMachineType != defaultSSDMachineType {
		return errors.Errorf("use the --aws-machine-type flag to set the " +
			"machine type when --local-ssd=false")
	}
	var machineType string
	if opts.SSDOpts.UseLocalSSD {
		machineType = providerOpts.SSDMachineType
	} else {
		machineType = providerOpts.MachineType
	}
	machineType = strings.ToLower(machineType)

	expandedZones, err := vm.ExpandZonesFlag(providerOpts.CreateZones)
	if err != nil {
		return err
	}

	if len(expandedZones) == 0 {
		expandedZones = defaultZones
	}

	// We need to make sure that the SSH keys have been distributed to all regions.
	if err := p.ConfigSSH(l, expandedZones); err != nil {
		return err
	}

	regions, err := p.allRegions(expandedZones)
	if err != nil {
		return err
	}
	if len(regions) < 1 {
		return errors.Errorf("Please specify a valid region.")
	}

	var zones []string // contains an az corresponding to each entry in names
	if opts.GeoDistributed {
		// Distribute the nodes amongst availability zones if geo distributed.
		nodeZones := vm.ZonePlacement(len(expandedZones), len(names))
		zones = make([]string, len(nodeZones))
		for i, z := range nodeZones {
			zones[i] = expandedZones[z]
		}
	} else {
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
	}

	var g errgroup.Group
	limiter := rate.NewLimiter(rate.Limit(providerOpts.CreateRateLimit), 2 /* buckets */)
	for i := range names {
		index := i
		capName := names[i]
		placement := zones[i]
		res := limiter.Reserve()
		g.Go(func() error {
			time.Sleep(res.Delay())
			return p.runInstance(l, capName, index, placement, machineType, opts, providerOpts)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return p.waitForIPs(l, names, regions, providerOpts)
}

func (p *Provider) Grow(*logger.Logger, vm.List, string, []string) error {
	panic("unimplemented")
}

// waitForIPs waits until AWS reports both internal and external IP addresses
// for all newly created VMs. If we did not wait for these IPs then attempts to
// list the new VMs after the creation might find VMs without an external IP.
// We do a bad job at higher layers detecting this lack of IP which can lead to
// commands hanging indefinitely.
func (p *Provider) waitForIPs(
	l *logger.Logger, names []string, regions []string, opts *ProviderOpts,
) error {
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
		vms, err := p.listRegions(l, regions, *opts, vm.ListOptions{})
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
func (p *Provider) Delete(l *logger.Logger, vms vm.List) error {
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
			return p.runJSONCommand(l, args, &data)
		})
	}
	return g.Wait()
}

// Reset is part of vm.Provider. It is a no-op.
func (p *Provider) Reset(l *logger.Logger, vms vm.List) error {
	return nil // unimplemented
}

// Extend is part of the vm.Provider interface.
// This will update the Lifetime tag on the instances.
func (p *Provider) Extend(l *logger.Logger, vms vm.List, lifetime time.Duration) error {
	return p.AddLabels(l, vms, map[string]string{
		"Lifetime": lifetime.String(),
	})
}

// cachedActiveAccount memoizes the return value from FindActiveAccount
var cachedActiveAccount string

// FindActiveAccount is part of the vm.Provider interface.
// This queries the AWS command for the current IAM user or role.
func (p *Provider) FindActiveAccount(l *logger.Logger) (string, error) {
	if len(cachedActiveAccount) > 0 {
		return cachedActiveAccount, nil
	}
	var account string
	var err error
	if p.Profile == "" {
		account, err = p.iamGetUser(l)
		if err != nil {
			return "", err
		}
	} else {
		account, err = p.stsGetCallerIdentity(l)
		if err != nil {
			return "", err
		}
	}
	cachedActiveAccount = account
	return cachedActiveAccount, nil
}

// iamGetUser returns the identity of an IAM user.
func (p *Provider) iamGetUser(l *logger.Logger) (string, error) {
	var userInfo struct {
		User struct {
			UserName string
		}
	}
	args := []string{"iam", "get-user"}
	err := p.runJSONCommand(l, args, &userInfo)
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
func (p *Provider) stsGetCallerIdentity(l *logger.Logger) (string, error) {
	var userInfo struct {
		Arn string
	}
	args := []string{"sts", "get-caller-identity"}
	err := p.runJSONCommand(l, args, &userInfo)
	if err != nil {
		return "", err
	}
	s := strings.Split(userInfo.Arn, "/")
	if len(s) < 2 {
		return "", errors.Errorf("Could not parse caller identity ARN '%s'", userInfo.Arn)
	}
	return s[1], nil
}

// List is part of the vm.Provider interface.
func (p *Provider) List(l *logger.Logger, opts vm.ListOptions) (vm.List, error) {
	regions, err := p.allRegions(p.Config.availabilityZoneNames())
	if err != nil {
		return nil, err
	}
	defaultOpts := p.CreateProviderOpts().(*ProviderOpts)
	return p.listRegions(l, regions, *defaultOpts, opts)
}

// listRegions lists VMs in the regions passed.
// It ignores region-specific errors.
func (p *Provider) listRegions(
	l *logger.Logger, regions []string, opts ProviderOpts, listOpts vm.ListOptions,
) (vm.List, error) {
	var ret vm.List
	var mux syncutil.Mutex
	var g errgroup.Group

	for _, r := range regions {
		// capture loop variable
		region := r
		g.Go(func() error {
			vms, err := p.listRegion(l, region, opts, listOpts)
			if err != nil {
				l.Printf("Failed to list AWS VMs in region: %s\n%v\n", region, err)
				return nil
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
		az := p.Config.getAvailabilityZone(z)
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
	r := p.Config.getRegion(region)
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

func (p *Provider) getVolumesForInstance(
	l *logger.Logger, region, instanceID string,
) (vols map[string]vm.Volume, err error) {
	type describeVolume struct {
		Volumes []struct {
			Attachments []struct {
				AttachTime          time.Time `json:"AttachTime"`
				Device              string    `json:"Device"`
				InstanceID          string    `json:"InstanceId"`
				State               string    `json:"State"`
				VolumeID            string    `json:"VolumeId"`
				DeleteOnTermination bool      `json:"DeleteOnTermination"`
			} `json:"Attachments"`
			AvailabilityZone   string    `json:"AvailabilityZone"`
			CreateTime         time.Time `json:"CreateTime"`
			Encrypted          bool      `json:"Encrypted"`
			Size               int       `json:"Size"`
			SnapshotID         string    `json:"SnapshotId"`
			State              string    `json:"State"`
			VolumeID           string    `json:"VolumeId"`
			Iops               int       `json:"Iops"`
			VolumeType         string    `json:"VolumeType"`
			MultiAttachEnabled bool      `json:"MultiAttachEnabled"`
			Throughput         int       `json:"Throughput,omitempty"`
			Tags               Tags      `json:"Tags,omitempty"`
		} `json:"Volumes"`
	}

	vols = make(map[string]vm.Volume)
	var volumeOut describeVolume
	getVolumesArgs := []string{
		"ec2", "describe-volumes",
		"--region", region,
		"--filters", "Name=attachment.instance-id,Values=" + instanceID,
	}

	err = p.runJSONCommand(l, getVolumesArgs, &volumeOut)
	if err != nil {
		return vols, err
	}
	for _, vol := range volumeOut.Volumes {
		tagMap := vol.Tags.MakeMap()
		vols[vol.VolumeID] = vm.Volume{
			ProviderResourceID: vol.VolumeID,
			ProviderVolumeType: vol.VolumeType,
			Zone:               vol.AvailabilityZone,
			Encrypted:          vol.Encrypted,
			Labels:             tagMap,
			Size:               vol.Size,
			Name:               tagMap["Name"],
		}
	}
	return vols, err
}

// DescribeInstancesOutput represents the output of the aws ec2 describe-instances command
type DescribeInstancesOutput struct {
	Reservations []struct {
		Instances []struct {
			InstanceID   string `json:"InstanceId"`
			Architecture string
			LaunchTime   string
			Placement    struct {
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
			RootDeviceName string

			BlockDeviceMappings []struct {
				DeviceName string `json:"DeviceName"`
				Disk       struct {
					AttachTime          time.Time `json:"AttachTime"`
					DeleteOnTermination bool      `json:"DeleteOnTermination"`
					Status              string    `json:"Status"`
					VolumeID            string    `json:"VolumeId"`
				} `json:"Ebs"`
			} `json:"BlockDeviceMappings"`

			Tags Tags

			VpcID                 string `json:"VpcId"`
			InstanceType          string
			InstanceLifecycle     string `json:"InstanceLifecycle"`
			SpotInstanceRequestId string `json:"SpotInstanceRequestId"`
		}
	}
}

// CancelSpotInstanceRequestsOutput represents the output structure of the cancel-spot-instance-requests command.
type CancelSpotInstanceRequestsOutput struct {
	CancelledSpotInstanceRequests []struct {
		SpotInstanceRequestId string `json:"SpotInstanceRequestId"`
		State                 string `json:"State"`
	} `json:"CancelledSpotInstanceRequests"`
}

// DescribeSpotInstanceRequestsOutput represents the output of the aws ec2 describe-spot-instance-requests command
type DescribeSpotInstanceRequestsOutput struct {
	SpotInstanceRequests []struct {
		SpotInstanceRequestId string `json:"SpotInstanceRequestId"`
		InstanceId            string `json:"InstanceId"`
		State                 string `json:"State"`
		Status                struct {
			Code       string `json:"Code"`
			Message    string `json:"Message"`
			UpdateTime string `json:"UpdateTime"`
		} `json:"Status"`
	} `json:"SpotInstanceRequests"`
}

// RunInstancesOutput represents the output of the aws ec2 run-instances command
type RunInstancesOutput struct {
	Instances []struct {
		InstanceID string `json:"InstanceId"`
	}
}

// listRegion extracts the roachprod-managed instances in the
// given region.
func (p *Provider) listRegion(
	l *logger.Logger, region string, opts ProviderOpts, listOpt vm.ListOptions,
) (vm.List, error) {

	args := []string{
		"ec2", "describe-instances",
		"--region", region,
	}
	var describeInstancesResponse DescribeInstancesOutput
	err := p.runJSONCommand(l, args, &describeInstancesResponse)
	if err != nil {
		return nil, err
	}

	var ret vm.List
	for _, res := range describeInstancesResponse.Reservations {
	in:
		for _, in := range res.Instances {
			// Ignore any instances that are not pending or running
			if in.State.Name != "pending" && in.State.Name != "running" {
				continue in
			}
			_ = in.PublicDNSName // silence unused warning
			_ = in.State.Code    // silence unused warning

			// Convert the tag map into a more useful representation
			tagMap := in.Tags.MakeMap()

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

			var nonBootableVolumes []vm.Volume
			if listOpt.IncludeVolumes {
				var volMap map[string]vm.Volume
				rootDevice := in.RootDeviceName
				for _, bdm := range in.BlockDeviceMappings {
					if bdm.DeviceName != rootDevice {
						// volMap does not exist so lazy initialize it here
						if volMap == nil {
							// TODO(leon, jackson): Change this to fetch the volumes in a
							// batch instead of fetching them one at a time
							volMap, err = p.getVolumesForInstance(l, region, in.InstanceID)
							if err != nil {
								errs = append(errs, err)
							}
						}
						if vol, ok := volMap[bdm.Disk.VolumeID]; ok {
							nonBootableVolumes = append(nonBootableVolumes, vol)
						} else {
							errs = append(errs, errors.Newf(
								"Attempted to add volume %s however it is not in the attached volumes for instance %s",
								bdm.Disk.VolumeID,
								in.InstanceID,
							))
						}
					}
				}
			}

			m := vm.VM{
				CreatedAt:              createdAt,
				DNS:                    in.PrivateDNSName,
				Name:                   tagMap["Name"],
				Errors:                 errs,
				Lifetime:               lifetime,
				Labels:                 tagMap,
				PrivateIP:              in.PrivateIPAddress,
				Provider:               ProviderName,
				ProviderID:             in.InstanceID,
				PublicIP:               in.PublicIPAddress,
				RemoteUser:             opts.RemoteUserName,
				VPC:                    in.VpcID,
				MachineType:            in.InstanceType,
				CPUArch:                vm.ParseArch(in.Architecture),
				Zone:                   in.Placement.AvailabilityZone,
				NonBootAttachedVolumes: nonBootableVolumes,
				Preemptible:            in.InstanceLifecycle == "spot",
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
func (p *Provider) runInstance(
	l *logger.Logger,
	name string,
	instanceIdx int,
	zone string,
	machineType string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
) error {
	az, ok := p.Config.azByName[zone]
	if !ok {
		return fmt.Errorf("no region in %v corresponds to availability zone %v",
			p.Config.regionNames(), zone)
	}

	keyName, err := p.sshKeyName(l)
	if err != nil {
		return err
	}
	cpuOptions := providerOpts.CPUOptions

	// We avoid the need to make a second call to set the tags by jamming
	// all of our metadata into the tagSpec.
	m := vm.GetDefaultLabelMap(opts)
	m[vm.TagCreated] = timeutil.Now().Format(time.RFC3339)
	m["Name"] = name

	if providerOpts.UseSpot {
		m[vm.TagSpotInstance] = "true"
	}

	var awsLabelsNameMap = map[string]string{
		vm.TagCluster:      "Cluster",
		vm.TagCreated:      "Created",
		vm.TagLifetime:     "Lifetime",
		vm.TagRoachprod:    "Roachprod",
		vm.TagSpotInstance: "Spot",
	}

	var labelPairs []string
	addLabel := func(key, value string) {
		// N.B. AWS does not allow empty values.
		if value != "" {
			labelPairs = append(labelPairs, fmt.Sprintf("{Key=%s,Value=%s}", key, value))
		}
	}

	for key, value := range opts.CustomLabels {
		_, ok := m[strings.ToLower(key)]
		if ok {
			return fmt.Errorf("duplicate label name defined: %s", key)
		}
		addLabel(key, value)
	}
	for key, value := range m {
		if n, ok := awsLabelsNameMap[key]; ok {
			key = n
		}
		addLabel(key, value)
	}
	labels := strings.Join(labelPairs, ",")
	vmTagSpecs := fmt.Sprintf("ResourceType=instance,Tags=[%s]", labels)
	volumeTagSpecs := fmt.Sprintf("ResourceType=volume,Tags=[%s]", labels)

	// Create AWS startup script file.
	extraMountOpts := ""
	// Dynamic args.
	if opts.SSDOpts.UseLocalSSD {
		if opts.SSDOpts.NoExt4Barrier {
			extraMountOpts = "nobarrier"
		}
	}
	filename, err := writeStartupScript(name, extraMountOpts, providerOpts.UseMultipleDisks, opts.Arch == string(vm.ArchFIPS))
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
	imageID := withFlagOverride(az.region.AMI_X86_64, &providerOpts.ImageAMI)
	useArmAMI := strings.Index(machineType, "6g.") == 1 || strings.Index(machineType, "6gd.") == 1 ||
		strings.Index(machineType, "7g.") == 1 || strings.Index(machineType, "7gd.") == 1
	if useArmAMI && (opts.Arch != "" && opts.Arch != string(vm.ArchARM64)) {
		return errors.Errorf("machine type %s is arm64, but requested arch is %s", machineType, opts.Arch)
	}
	//TODO(srosenberg): remove this once we have a better way to detect ARM64 machines
	if useArmAMI {
		imageID = withFlagOverride(az.region.AMI_ARM64, &providerOpts.ImageAMI)
		// N.B. use arbitrary instanceIdx to suppress the same info for every other instance being created.
		if instanceIdx == 0 {
			l.Printf("Using ARM64 AMI: %s for machine type: %s", imageID, machineType)
		}
	}
	if opts.Arch == string(vm.ArchFIPS) {
		imageID = withFlagOverride(az.region.AMI_FIPS, &providerOpts.ImageAMI)
		if instanceIdx == 0 {
			l.Printf("Using FIPS-enabled AMI: %s for machine type: %s", imageID, machineType)
		}
	}
	args := []string{
		"ec2", "run-instances",
		"--associate-public-ip-address",
		"--count", "1",
		"--instance-type", machineType,
		"--image-id", imageID,
		"--key-name", keyName,
		"--region", az.region.Name,
		"--security-group-ids", az.region.SecurityGroup,
		"--subnet-id", az.subnetID,
		"--tag-specifications", vmTagSpecs, volumeTagSpecs,
		"--user-data", "file://" + filename,
	}

	if cpuOptions != "" {
		args = append(args, "--cpu-options", cpuOptions)
	}

	if p.IAMProfile != "" {
		args = append(args, "--iam-instance-profile", "Name="+p.IAMProfile)
	}
	ebsVolumes := assignEBSVolumes(&opts, providerOpts)
	args, err = genDeviceMapping(ebsVolumes, args)
	if err != nil {
		return err
	}

	if providerOpts.UseSpot {
		return runSpotInstance(l, p, args, az.region.Name)
		//todo(babusrithar): Add fallback to on-demand instances if spot instances are not available.
	}
	runInstancesOutput := RunInstancesOutput{}
	return p.runJSONCommand(l, args, &runInstancesOutput)
}

// runSpotInstance uses run-instances command to create a spot instance.
// It returns an error if the spot request is not fulfilled within 2 minutes.
// It uses describe-spot-instance-requests command to get the status of the spot request.
func runSpotInstance(l *logger.Logger, p *Provider, args []string, regionName string) error {
	waitForSpotDuration := 2 * time.Minute

	// Add spot instance options to the run-instances command.
	spotArgs := append(args, "--instance-market-options",
		fmt.Sprintf("MarketType=spot,SpotOptions={SpotInstanceType=one-time,"+
			"InstanceInterruptionBehavior=terminate}"))
	runInstancesOutput := RunInstancesOutput{}
	err := p.runJSONCommand(l, spotArgs, &runInstancesOutput)
	if err != nil {
		return err
	}
	// If the spot request is accepted, the run-instances command will return an instance-id.
	if len(runInstancesOutput.Instances) == 0 {
		return errors.Errorf("No instances found for spot request, likely the spot request had bad parameter")
	}
	instanceId := runInstancesOutput.Instances[0].InstanceID
	spotInstanceRequestId, err := getSpotInstanceRequestId(l, p, regionName, instanceId)
	if err != nil {
		return err
	}

	// Loop every 10 seconds till the spot instance is fulfilled, for a maximum of 2 minutes.
	startTime := timeutil.Now()
	duration := waitForSpotDuration
	for {
		describeSpotInstanceRequestsOutput, err := describeSpotInstanceRequest(l, p, regionName, spotInstanceRequestId)
		if err != nil {
			return err
		}
		spotRequestFulfilled, err := processSpotInstanceRequestStatus(l, describeSpotInstanceRequestsOutput, spotInstanceRequestId, instanceId)
		if err != nil {
			return err
		}
		if spotRequestFulfilled {
			return nil
		}
		// This part of the code depends on demand/supply of AWS and can be hard to test.
		// One way to manually test is tested by commenting out return nil above and check cancellation after 2 minutes.
		if timeutil.Since(startTime) >= duration {
			l.Printf("waitForSpotDuration passed, cancel the spot instance request and exit loop")
			err := cancelSpotRequest(l, p, regionName, spotInstanceRequestId)
			if err != nil {
				return err
			}
			return errors.New("waitForSpotDuration over")
		}
		l.Printf("Sleeping for 10 seconds before checking the status of the spot instance request again")
		time.Sleep(10 * time.Second)
	}
}

func cancelSpotRequest(
	l *logger.Logger, p *Provider, regionName string, spotInstanceRequestId string,
) error {
	// Cancel the spot instance request.
	csrArgs := []string{
		"ec2", "cancel-spot-instance-requests",
		"--region", regionName,
		"--spot-instance-request-ids", spotInstanceRequestId,
	}
	err := p.runJSONCommand(l, csrArgs, &CancelSpotInstanceRequestsOutput{})
	if err != nil {
		// This code path is not expected to be hit, but if it does, we should return the error, so that roachprod
		// can destroy the cluster being created.
		return err
	}
	return nil
}

func describeSpotInstanceRequest(
	l *logger.Logger, p *Provider, regionName string, spotInstanceRequestId string,
) (DescribeSpotInstanceRequestsOutput, error) {
	// Use describe-spot-instance-requests to get the status of the spot request.
	dsirArgs := []string{
		"ec2", "describe-spot-instance-requests",
		"--region", regionName,
		"--spot-instance-request-ids", spotInstanceRequestId,
	}
	var describeSpotInstanceRequestsOutput DescribeSpotInstanceRequestsOutput
	err := p.runJSONCommand(l, dsirArgs, &describeSpotInstanceRequestsOutput)
	if err != nil {
		return DescribeSpotInstanceRequestsOutput{}, err
	}
	return describeSpotInstanceRequestsOutput, nil
}

func processSpotInstanceRequestStatus(
	l *logger.Logger,
	describeSpotInstanceRequestsOutput DescribeSpotInstanceRequestsOutput,
	spotInstanceRequestId string,
	instanceId string,
) (fullFilled bool, err error) {
	if len(describeSpotInstanceRequestsOutput.SpotInstanceRequests) == 0 {
		return false, errors.Errorf("No Spot Instance Request found for instance-id: %s", instanceId)
	}
	requestState := describeSpotInstanceRequestsOutput.SpotInstanceRequests[0].State
	requestStatusCode := describeSpotInstanceRequestsOutput.SpotInstanceRequests[0].Status.Code
	if requestState == "closed" || requestState == "cancelled" || requestState == "failed" {
		return false, errors.Errorf("Spot request %s for instance %s not active with state: %s",
			spotInstanceRequestId, instanceId, requestState)
	}
	if requestStatusCode == "fulfilled" {
		l.Printf("Spot request %s for instance %s fulfilled.", spotInstanceRequestId, instanceId)
		return true, nil
	} else {
		// Spot instance request is not fulfilled yet, but active,  continue looping.
		l.Printf("Spot request %s for instance %s not fulfilled yet, status.code: %s., state: %s",
			spotInstanceRequestId, instanceId, requestStatusCode, requestState)
	}
	return false, nil
}

func getSpotInstanceRequestId(
	l *logger.Logger, p *Provider, regionName string, instanceId string,
) (string, error) {
	diArgs := []string{
		"ec2", "describe-instances",
		"--region", regionName,
		"--instance-ids", instanceId,
	}
	var describeInstancesResponse DescribeInstancesOutput
	err := p.runJSONCommand(l, diArgs, &describeInstancesResponse)
	if err != nil {
		return "", err
	}

	// Sanity check to make sure that the instance-id is valid.
	if len(describeInstancesResponse.Reservations) < 1 ||
		len(describeInstancesResponse.Reservations[0].Instances) < 1 ||
		describeInstancesResponse.Reservations[0].Instances[0].SpotInstanceRequestId == "" {
		return "", errors.Errorf("No SpotInstanceRequestId found for instance-id: %s", instanceId)
	}
	spotInstanceRequestId := describeInstancesResponse.Reservations[0].Instances[0].SpotInstanceRequestId
	return spotInstanceRequestId, nil
}

func genDeviceMapping(ebsVolumes ebsVolumeList, args []string) ([]string, error) {
	mapping, err := json.Marshal(ebsVolumes)
	if err != nil {
		return nil, err
	}

	deviceMapping, err := os.CreateTemp("", "aws-block-device-mapping")
	if err != nil {
		return nil, err
	}
	defer deviceMapping.Close()
	if _, err := deviceMapping.Write(mapping); err != nil {
		return nil, err
	}
	return append(args,
		"--block-device-mapping",
		"file://"+deviceMapping.Name(),
	), nil
}

func assignEBSVolumes(opts *vm.CreateOpts, providerOpts *ProviderOpts) ebsVolumeList {
	// Make a local copy of providerOpts.EBSVolumes to prevent data races
	ebsVolumes := providerOpts.EBSVolumes
	// The local NVMe devices are automatically mapped.  Otherwise, we need to map an EBS data volume.
	if !opts.SSDOpts.UseLocalSSD {
		if len(ebsVolumes) == 0 && providerOpts.DefaultEBSVolume.Disk.VolumeType == "" {
			providerOpts.DefaultEBSVolume.Disk.VolumeType = defaultEBSVolumeType
			providerOpts.DefaultEBSVolume.Disk.DeleteOnTermination = true
		}

		if providerOpts.DefaultEBSVolume.Disk.VolumeType != "" {
			// Add default volume to the list of volumes we'll setup.
			v := ebsVolumes.newVolume()
			v.Disk = providerOpts.DefaultEBSVolume.Disk
			v.Disk.DeleteOnTermination = true
			ebsVolumes = append(ebsVolumes, v)
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
	return append(ebsVolumes, osDiskVolume)
}

// Active is part of the vm.Provider interface.
func (p *Provider) Active() bool {
	return true
}

// ProjectActive is part of the vm.Provider interface.
func (p *Provider) ProjectActive(project string) bool {
	return project == ""
}

type attachJsonResponse struct {
	AttachTime string `json:"AttachTime"`
	InstanceID string `json:"InstanceId"`
	VolumeID   string `json:"VolumeId"`
	State      string `json:"State"`
	Device     string `json:"Device"`
}

func (p *Provider) AttachVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) (string, error) {
	// TODO(leon): what happens if this device already exists?
	deviceName := "/dev/sdf"
	args := []string{
		"ec2",
		"attach-volume",
		"--instance-id", vm.ProviderID,
		"--volume-id", volume.ProviderResourceID,
		"--device", deviceName,
		"--region", vm.Zone[:len(vm.Zone)-1],
	}

	var commandResponse attachJsonResponse
	err := p.runJSONCommand(l, args, &commandResponse)
	if err != nil {
		return "", err
	}
	if commandResponse.State != "attaching" && commandResponse.State != "in-use" {
		return "", errors.New("Command to attach succeeded but volume is not in the attached state")
	}

	args = []string{
		"ec2",
		"--region", vm.Zone[:len(vm.Zone)-1],
		"modify-instance-attribute",
		"--attribute", "blockDeviceMapping",
		"--instance-id", vm.ProviderID,
		"--block-device-mappings",
		"DeviceName=" + deviceName + ",Ebs={DeleteOnTermination=true,VolumeId=" + volume.ProviderResourceID + "}",
	}
	_, err = p.runCommand(l, args)
	if err != nil {
		return "", err
	}

	return "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_" +
		strings.Replace(volume.ProviderResourceID, "-", "", 1), nil
}

type createVolume struct {
	AvailabilityZone string    `json:"AvailabilityZone"`
	Encrypted        bool      `json:"Encrypted"`
	VolumeType       string    `json:"VolumeType"`
	VolumeID         string    `json:"VolumeId"`
	State            string    `json:"State"`
	Iops             int       `json:"Iops"`
	SnapshotID       string    `json:"SnapshotId"`
	CreateTime       time.Time `json:"CreateTime"`
	Size             int       `json:"Size"`
}

func (p *Provider) CreateVolume(
	l *logger.Logger, vco vm.VolumeCreateOpts,
) (vol vm.Volume, err error) {
	// TODO(leon): SourceSnapshotID and IOPS, are not handled
	if vco.SourceSnapshotID != "" || vco.IOPS != 0 {
		err = errors.New("Creating a volume with SourceSnapshotID or IOPS is not supported at this time.")
		return vol, err
	}

	region := vco.Zone[:len(vco.Zone)-1]
	args := []string{
		"ec2",
		"create-volume",
		"--availability-zone", vco.Zone,
		"--region", region,
	}
	if vco.Encrypted {
		args = append(args, "--encrypted")
	}
	var tags Tags

	if vco.Name != "" {
		// Add label to create options label which will be converted into Tags
		vco.Labels["Name"] = vco.Name
	}

	for key, value := range vco.Labels {
		tags = append(tags, Tag{
			key,
			value,
		})
	}

	if tags != nil {
		args = append(args, "--tag-specifications", "ResourceType=volume,Tags=["+tags.String()+"]")
	}

	switch vco.Type {
	case "gp2", "gp3", "io1", "io2", "st1", "sc1", "standard":
		args = append(args, "--volume-type", vco.Type)
	case "":
		// Use the default.
	default:
		return vol, errors.Newf("Invalid volume type %q", vco.Type)
	}

	if vco.Size == 0 {
		return vol, errors.New("Cannot create a volume of size 0")
	}
	args = append(args, "--size", strconv.Itoa(vco.Size))
	var volumeDetails createVolume
	err = p.runJSONCommand(l, args, &volumeDetails)
	if err != nil {
		return vol, err
	}

	waitForVolumeCloser := make(chan struct{})

	waitForVolume := retry.Start(retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		MaxRetries:     10,
		Closer:         waitForVolumeCloser,
	})

	var state []string
	args = []string{
		"ec2",
		"describe-volumes",
		"--volume-id", volumeDetails.VolumeID,
		"--region", region,
		"--query", "Volumes[*].State",
	}
	for waitForVolume.Next() {
		err = p.runJSONCommand(l, args, &state)
		if len(state) > 0 && state[0] == "available" {
			close(waitForVolumeCloser)
		}
	}

	if err != nil {
		return vm.Volume{}, err
	}

	vol = vm.Volume{
		ProviderResourceID: volumeDetails.VolumeID,
		ProviderVolumeType: volumeDetails.VolumeType,
		Encrypted:          volumeDetails.Encrypted,
		Zone:               vol.Zone,
		Size:               volumeDetails.Size,
		Labels:             vco.Labels,
		Name:               vco.Name,
	}
	return vol, err
}

func (p *Provider) DeleteVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) error {
	panic("unimplemented")
}

func (p *Provider) ListVolumes(l *logger.Logger, vm *vm.VM) ([]vm.Volume, error) {
	return vm.NonBootAttachedVolumes, nil
}

type snapshotOutput struct {
	Description string `json:"Description"`
	Tags        []struct {
		Value string `json:"Value"`
		Key   string `json:"Key"`
	} `json:"Tags"`
	Encrypted  bool      `json:"Encrypted"`
	VolumeID   string    `json:"VolumeId"`
	State      string    `json:"State"`
	VolumeSize int       `json:"VolumeSize"`
	StartTime  time.Time `json:"StartTime"`
	Progress   string    `json:"Progress"`
	OwnerID    string    `json:"OwnerId"`
	SnapshotID string    `json:"SnapshotId"`
}

func (p *Provider) CreateVolumeSnapshot(
	l *logger.Logger, volume vm.Volume, vsco vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	region := volume.Zone[:len(volume.Zone)-1]
	var tags []string
	for k, v := range vsco.Labels {
		tags = append(tags, fmt.Sprintf("{Key=%s,Value=%s}", k, v))
	}
	tags = append(tags, fmt.Sprintf("{Key=%s,Value=%s}", "Name", vsco.Name))

	args := []string{
		"ec2", "create-snapshot",
		"--description", vsco.Description,
		"--region", region,
		"--volume-id", volume.ProviderResourceID,
		"--tag-specifications", "ResourceType=snapshot,Tags=[" + strings.Join(tags, ",") + "]",
	}

	var so snapshotOutput
	if err := p.runJSONCommand(l, args, &so); err != nil {
		return vm.VolumeSnapshot{}, err
	}
	return vm.VolumeSnapshot{
		ID:   so.SnapshotID,
		Name: vsco.Name,
	}, nil
}

func (p *Provider) ListVolumeSnapshots(
	l *logger.Logger, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	panic("unimplemented")
}

func (p *Provider) DeleteVolumeSnapshots(l *logger.Logger, snapshots ...vm.VolumeSnapshot) error {
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
