// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package aws provides functionality for the aws provider.
package aws

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	awsststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/flagstub"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// ProviderName is aws.
const ProviderName = "aws"

//go:embed config.json
var configJson []byte

//go:embed old.json
var oldJson []byte

var (
	// TODO(golgeek, 2025-03-25): remove this in one year or so when all
	// resources are created with the unified tags.
	legacyTagsRemapping = map[string]string{
		"Cluster":   vm.TagCluster,
		"Created":   vm.TagCreated,
		"Lifetime":  vm.TagLifetime,
		"Roachprod": vm.TagRoachprod,
		"Spot":      vm.TagSpotInstance,
	}
)

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

	providerInstance, err := NewProvider()
	if err != nil {
		vm.Providers[ProviderName] = flagstub.New(
			&Provider{},
			fmt.Sprintf("unable to init aws provider: %s", err),
		)
		return errors.Wrap(err, "unable to init aws provider")
	}

	haveRequiredVersion := func() bool {
		// `aws --version` takes around 400ms on my machine.
		if os.Getenv("ROACHPROD_SKIP_AWSCLI_CHECK") == "true" {
			return true
		}
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
		// We assume SSO is enabled if either AWS_PROFILE is set or ~/.aws/config exists.
		// N.B. We can't check if the user explicitly passed `--aws-profile` because CLI parsing hasn't happened yet.
		if os.Getenv("AWS_PROFILE") != "" {
			return true
		}
		const configFile = "${HOME}/.aws/config"
		if _, err := os.Stat(os.ExpandEnv(configFile)); err == nil {
			return true
		}
		// Non-SSO authentication is deprecated and will be removed in the future. However, CI continues to use it.
		hasAuth := false
		const credFile = "${HOME}/.aws/credentials"
		if _, err := os.Stat(os.ExpandEnv(credFile)); err == nil {
			hasAuth = true
		}
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			hasAuth = true
		}
		if !hasAuth {
			// No known method of auth. was detected.
			return false
		}
		// Non-SSO auth. is deprecated, so let's display a warning.
		fmt.Fprintf(os.Stderr, "WARN: Non-SSO form of authentication is deprecated and will be removed in the future.\n")
		fmt.Fprintf(os.Stderr, "WARN:\tPlease set `AWS_PROFILE` or pass `--aws-profile`.\n")
		return true
	}
	if !haveCredentials() {
		vm.Providers[ProviderName] = flagstub.New(&Provider{}, noCredentials)
		return errors.New("missing/invalid credentials")
	}
	vm.Providers[ProviderName] = providerInstance
	return nil
}

func NewProvider(options ...Option) (*Provider, error) {
	p := &Provider{
		Config: awsConfigValue{
			awsConfig: *DefaultConfig,
		},
	}

	for _, option := range options {
		option.apply(p)
	}

	// If AssumeSTSRole is set, we need to set a default session name
	// if none was provided.
	if p.AssumeSTSRole != "" && p.AssumeSTSSessionName == "" {
		if hostname, err := os.Hostname(); err != nil {
			p.AssumeSTSSessionName = fmt.Sprintf("roachprod-%d", timeutil.Now().Unix())
		} else {
			p.AssumeSTSSessionName = fmt.Sprintf("roachprod-%s", hostname)
		}
	}

	return p, nil
}

func (p *Provider) getEnvironmentAWSCredentials() ([]string, error) {

	// If we don't need to assume a role, return an empty slice.
	if p.AssumeSTSRole == "" || len(p.AccountIDs) == 0 {
		return []string{}, nil
	}

	// Our provider needs to assume a role.

	// In case we need to assume a role, we need to generate and return temporary
	// credentials.
	// We lock the provider to avoid concurrent generations.
	p.mu.Lock()
	defer p.mu.Unlock()

	// If we never fetched the credentials or they're about to expire, fetch them.
	if p.mu.credentials == nil || p.mu.credentials.Expiration.Before(timeutil.Now().Add(time.Minute*2)) {
		cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion("us-east-1"))
		if err != nil {
			return nil, errors.Wrap(err, "assumeRole: failed to load config")
		}

		roleArn := fmt.Sprintf("arn:aws:iam::%s:role/%s", p.AccountIDs[0], p.AssumeSTSRole)
		roleSessionName := fmt.Sprintf("%s-%s", p.AssumeSTSSessionName, p.AccountIDs[0])

		stsClient := sts.NewFromConfig(cfg)
		tmpCredentials, err := stsClient.AssumeRole(context.TODO(), &sts.AssumeRoleInput{
			RoleArn:         aws.String(roleArn),
			RoleSessionName: aws.String(roleSessionName),
		})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to assume role %s", roleArn)
		}

		p.mu.credentials = tmpCredentials.Credentials
	}

	return []string{
		fmt.Sprintf("%s=%s", amazon.AWSAccessKeyParam, *p.mu.credentials.AccessKeyId),
		fmt.Sprintf("%s=%s", amazon.AWSSecretParam, *p.mu.credentials.SecretAccessKey),
		fmt.Sprintf("%s=%s", amazon.AWSTempTokenParam, *p.mu.credentials.SessionToken),
	}, nil
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
	Disk       ebsDisk `json:"Ebs,omitempty"`
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
		if d.IOPs > 80000 {
			return errors.AssertionFailedf("Iops required for gp3 disk: [3000, 80000]")
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
		EBSVolumeCount:   1,
		CreateRateLimit:  2,
		IAMProfile:       "roachprod-testing",
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

	// EBSVolumeCount is the number of additional EBS volumes to attach.
	// Only used if local-ssd=false, and is superseded by EBSVolumes.
	EBSVolumeCount int

	// IAMProfile designates the name of the instance profile to use for created
	// EC2 instances if non-empty.
	IAMProfile string

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
	// BootDiskOnly ensures that no additional disks will be attached, other than
	// the boot disk.
	BootDiskOnly bool

	// Managed uses a launch template and auto scaling group to create VMs.
	// This enables cluster resizing via Grow/Shrink operations.
	Managed bool
}

// Provider implements the vm.Provider interface for AWS.
type Provider struct {
	// Profile to manage cluster in
	Profile string

	// Path to json for aws configuration, defaults to predefined configuration
	Config awsConfigValue

	// aws accounts to perform action in, used by gcCmd only as it clean ups multiple aws accounts
	AccountIDs []string

	// If AssumeSTSRole is set to a non-empty string, the provider will use STS
	// to assume the role. It should be set to the role part of the ARN to assume.
	// e.g. "arn:aws:iam::123456789012:role/{MyRole}"
	AssumeSTSRole        string
	AssumeSTSSessionName string

	mu struct {
		syncutil.Mutex

		// credentials are the AWS credentials.
		credentials *awsststypes.Credentials
	}

	dnsProvider vm.DNSProvider
}

func (p *Provider) SupportsSpotVMs() bool {
	return true
}

// IsCentralizedProvider returns true because AWS is a remote provider.
func (p *Provider) IsCentralizedProvider() bool {
	return true
}

func (p *Provider) GetPreemptedSpotVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {
	byRegion, err := regionMap(vms)
	if err != nil {
		return nil, err
	}

	var preemptedVMs []vm.PreemptedVM
	for region, vmList := range byRegion {
		args := []string{
			"ec2", "describe-instances",
			"--region", region,
			"--instance-ids",
		}
		args = append(args, vmList.ProviderIDs()...)
		var describeInstancesResponse DescribeInstancesOutput
		err = p.runJSONCommand(l, args, &describeInstancesResponse)
		if err != nil {
			// if the describe-instances operation fails with the error InvalidInstanceID.NotFound,
			// we assume that the instance has been preempted and describe-instances operation is attempted one hour after the instance termination
			if strings.Contains(err.Error(), "InvalidInstanceID.NotFound") {
				l.Errorf("WARNING: received NotFound error when trying to find preemptions: %v", err)
				return vm.CreatePreemptedVMs(getInstanceIDsNotFound(err.Error())), nil
			}
			return nil, err
		}

		// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/finding-an-interrupted-Spot-Instance.html
		for _, r := range describeInstancesResponse.Reservations {
			for _, instance := range r.Instances {
				if instance.InstanceLifecycle == "spot" &&
					instance.State.Name == "terminated" &&
					instance.StateReason.Code == "Server.SpotInstanceTermination" {
					preemptedVMs = append(preemptedVMs, vm.PreemptedVM{Name: instance.InstanceID})
				}
			}
		}
	}

	return preemptedVMs, nil
}

// getInstanceIDsNotFound returns a list of instance IDs that were not found during the describe-instances command.
//
// Sample error message:
//
// ‹An error occurred (InvalidInstanceID.NotFound) when calling the DescribeInstances operation: The instance IDs 'i-02e9adfac0e5fa18f, i-0bc7869fda0299caa'
// do not exist›
func getInstanceIDsNotFound(errorMsg string) []string {
	// Regular expression pattern to find instance IDs between single quotes
	re := regexp.MustCompile(`'([^']*)'`)
	matches := re.FindStringSubmatch(errorMsg)
	if len(matches) > 1 {
		instanceIDsStr := matches[1]
		return strings.Split(instanceIDsStr, ", ")
	}
	return nil
}

func (p *Provider) GetHostErrorVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	return nil, nil
}

func (p *Provider) GetLiveMigrationVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	return nil, nil
}

// GetVMSpecs returns a map from VM.Name to a map of VM attributes, provided by AWS
func (p *Provider) GetVMSpecs(
	l *logger.Logger, vms vm.List,
) (map[string]map[string]interface{}, error) {
	if vms == nil {
		return nil, errors.New("vms cannot be nil")
	}

	byRegion, err := regionMap(vms)
	if err != nil {
		return nil, err
	}

	// Extract the spec of all VMs and create a map from VM name to spec.
	vmSpecs := make(map[string]map[string]interface{})
	for region, list := range byRegion {
		args := []string{
			"ec2", "describe-instances",
			"--region", region,
			"--instance-ids",
		}
		args = append(args, list.ProviderIDs()...)
		var describeInstancesResponse DescribeInstancesOutput
		err := p.runJSONCommand(l, args, &describeInstancesResponse)
		if err != nil {
			return nil, errors.Wrapf(err, "error describing instances in region %s: ", region)
		}
		if len(describeInstancesResponse.Reservations) == 0 {
			l.Errorf("failed to create spec files for instances in region %s: no Reservations found", region)
			continue
		}

		for _, r := range describeInstancesResponse.Reservations {
			for _, instance := range r.Instances {
				i := slices.IndexFunc(instance.Tags, func(tag Tag) bool {
					return tag.Key == "Name"
				})
				if i != -1 {
					instanceRecord, err := json.MarshalIndent(instance, "", " ")
					if err != nil {
						l.Errorf("Failed to marshal JSON: %v for instance \n%v", err, instance)
						continue
					}
					var vmSpec map[string]interface{}
					err = json.Unmarshal(instanceRecord, &vmSpec)
					if err != nil {
						l.Errorf("Failed to unmarshal JSON: %v for instance record \n%v", err, instanceRecord)
						continue
					}
					vmSpecs[instance.Tags[i].Value] = vmSpec
				}
			}
		}
	}
	return vmSpecs, nil
}

const (
	defaultSSDMachineType = "m6id.xlarge"
	defaultMachineType    = "m6i.xlarge"
)

var DefaultConfig = func() (cfg *awsConfig) {
	cfg = new(awsConfig)
	if err := json.Unmarshal(configJson, cfg); err != nil {
		panic(errors.Wrap(err, "failed to embedded configuration"))
	}
	return cfg
}()

// DefaultZones is the list of availability zones used by default for
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

		// If the resource was created with the legacy tags, we remap
		// to the unified ones.
		// TODO(golgeek, 2025-03-25): remove this in one year or so when all
		// resources are created with the unified tags.
		if tag, ok := legacyTagsRemapping[entry.Key]; ok {
			tagMap[tag] = entry.Value
		} else {
			tagMap[entry.Key] = entry.Value
		}

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

	flags.IntVar(&o.EBSVolumeCount, ProviderName+"-ebs-volume-count", 1,
		"Number of EBS volumes to create, only used if local-ssd=false and superseded by --aws-ebs-volume")

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
	flags.StringVar(&o.IAMProfile, ProviderName+"-iam-profile", o.IAMProfile,
		"the IAM instance profile to associate with created VMs if non-empty")
	flags.BoolVar(&o.BootDiskOnly, ProviderName+"-boot-disk-only", o.BootDiskOnly,
		"Only attach the boot disk. No additional volumes will be provisioned even if specified.")
	flags.BoolVar(&o.Managed, ProviderName+"-managed", o.Managed,
		"use a launch template and auto scaling group to create VMs (enables Grow/Shrink operations)")
}

// ConfigureClusterCleanupFlags implements ProviderOpts.
func (p *Provider) ConfigureClusterCleanupFlags(flags *pflag.FlagSet) {
	flags.StringSliceVar(&p.AccountIDs, ProviderName+"-account-ids", []string{},
		"AWS account ids as a comma-separated string")
}

// ConfigureProviderFlags is part of the vm.Provider interface.
func (p *Provider) ConfigureProviderFlags(flags *pflag.FlagSet, _ vm.MultipleProjectsOption) {
	flags.StringVar(&p.Profile, ProviderName+"-profile", os.Getenv("AWS_PROFILE"),
		"Profile to manage cluster in")
	flags.Var(&p.Config, ProviderName+"-config",
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
		g.Go(func() error {
			exists, err := p.sshKeyExists(l, keyName, r)
			if err != nil {
				return err
			}
			if !exists {
				err = p.sshKeyImport(l, keyName, r)
				if err != nil {
					return err
				}
				sshPublicKeyPath, err := config.SSHPublicKeyPath()
				if err != nil {
					return err
				}
				l.Printf("imported %s as %s in region %s", sshPublicKeyPath, keyName, r)
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
			tagArgs = append(tagArgs, fmt.Sprintf("Key=%s,Value=%s", key, value))
		}
	}
	args = append(args, tagArgs...)

	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}
	g := errgroup.Group{}
	for region, list := range byRegion {
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

// AddLabels adds (or updates) the given labels to the given VMs.
// N.B. If a VM contains a label with the same key, its value will be updated.
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
) (vm.List, error) {
	providerOpts := vmProviderOpts.(*ProviderOpts)
	// There exist different flags to control the machine type when ssd is true.
	// This enables sane defaults for either setting but the behavior can be
	// confusing when a user attempts to use `--aws-machine-type` and the command
	// succeeds but the flag is ignored. Rather than permit this behavior we
	// return an error instructing the user to use the other flag.
	if opts.SSDOpts.UseLocalSSD &&
		providerOpts.MachineType != defaultMachineType &&
		providerOpts.SSDMachineType == defaultSSDMachineType {
		return nil, errors.Errorf("use the --aws-machine-type-ssd flag to set the " +
			"machine type when --local-ssd=true")
	} else if !opts.SSDOpts.UseLocalSSD &&
		providerOpts.MachineType == defaultMachineType &&
		providerOpts.SSDMachineType != defaultSSDMachineType {
		return nil, errors.Errorf("use the --aws-machine-type flag to set the " +
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
		return nil, err
	}

	if len(expandedZones) == 0 {
		expandedZones = DefaultZones(opts.GeoDistributed)
	}

	// We need to make sure that the SSH keys have been distributed to all regions.
	if err := p.ConfigSSH(l, expandedZones); err != nil {
		return nil, err
	}

	regions, err := p.allRegions(expandedZones)
	if err != nil {
		return nil, err
	}
	if len(regions) < 1 {
		return nil, errors.Errorf("Please specify a valid region.")
	}

	// Distribute the nodes amongst availability zones.
	nodeZones := vm.ZonePlacement(len(expandedZones), len(names))
	zones := make([]string, len(nodeZones))
	for i, z := range nodeZones {
		zones[i] = expandedZones[z]
	}

	// Use managed cluster creation (launch template + ASG) if --aws-managed is specified.
	if providerOpts.Managed {
		return p.createManagedCluster(l, names, zones, regions, machineType, opts, providerOpts)
	}

	// Create instances using the shared createInstances function.
	if _, err := p.createInstances(l, names, zones, machineType, opts, providerOpts); err != nil {
		return nil, err
	}

	// Our initial list of VMs does not include the IP addresses or volumes.
	// waitForIPs() returns the VMs list with all information set, so we simply
	// overwrite the list with the result of waitForIPs().
	vmList, err := p.waitForIPs(context.Background(), l, names, regions, providerOpts)
	if err != nil {
		return nil, err
	}

	return vmList, nil
}

func DefaultZones(geoDistributed bool) []string {
	if geoDistributed {
		return defaultZones
	}
	return []string{defaultZones[0]}
}

// createInstances creates EC2 instances in parallel with rate limiting.
// It returns a map of region -> instance IDs for the created instances.
func (p *Provider) createInstances(
	l *logger.Logger,
	names []string,
	zones []string,
	machineType string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
) (map[string][]string, error) {
	l.Printf("Creating %d instances...", len(names))

	var mu syncutil.Mutex
	instanceIDsByRegion := make(map[string][]string)
	rateLimit := rate.Limit(providerOpts.CreateRateLimit)
	limiter := rate.NewLimiter(rateLimit, 2)

	var g errgroup.Group
	for i := range names {
		index := i
		vmName := names[i]
		zone := zones[i]
		az := p.Config.getAvailabilityZone(zone)
		region := az.Region.Name
		res := limiter.Reserve()

		g.Go(func() error {
			time.Sleep(res.Delay())
			v, err := p.runInstance(l, vmName, index, zone, machineType, opts, providerOpts)
			if err != nil {
				return errors.Wrapf(err, "failed to create instance %s", vmName)
			}

			mu.Lock()
			instanceIDsByRegion[region] = append(instanceIDsByRegion[region], v.ProviderID)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return instanceIDsByRegion, nil
}

// getIAMProfileForInstance returns the IAM instance profile name attached to the
// given EC2 instance. Returns an empty string if no profile is attached.
func (p *Provider) getIAMProfileForInstance(
	ctx context.Context, l *logger.Logger, region, instanceID string,
) (string, error) {
	args := []string{
		"ec2", "describe-instances",
		"--instance-ids", instanceID,
		"--region", region,
	}
	var descOutput DescribeInstancesOutput
	if err := p.runJSONCommandWithContext(ctx, l, args, &descOutput); err != nil {
		return "", err
	}
	if len(descOutput.Reservations) == 0 || len(descOutput.Reservations[0].Instances) == 0 {
		return "", nil
	}
	arn := descOutput.Reservations[0].Instances[0].IamInstanceProfile.Arn
	if arn == "" {
		return "", nil
	}
	// ARN format: arn:aws:iam::<account>:instance-profile/<profile-name>
	parts := strings.Split(arn, "/")
	if len(parts) < 2 {
		return "", errors.Errorf("unexpected IAM profile ARN format: %s", arn)
	}
	return parts[len(parts)-1], nil
}

// waitForInstancesStatus waits for instances to reach the specified target state.
// It polls AWS until all instances reach the target state or the timeout is reached.
func (p *Provider) waitForInstancesStatus(
	ctx context.Context, l *logger.Logger, region string, instanceIDs []string, targetState string,
) error {
	if len(instanceIDs) == 0 {
		return nil
	}

	l.Printf("Waiting for %d instances in %s to reach %s state...", len(instanceIDs), region, targetState)

	waitRetry := retry.Start(retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Second,
		MaxRetries:     30,
	})

	for waitRetry.Next() {
		descArgs := []string{
			"ec2", "describe-instances",
			"--instance-ids",
		}
		descArgs = append(descArgs, instanceIDs...)
		descArgs = append(descArgs, "--region", region)

		var descOutput DescribeInstancesOutput
		if err := p.runJSONCommandWithContext(ctx, l, descArgs, &descOutput); err != nil {
			// If instances not found, they're already terminated (relevant for termination waits)
			if strings.Contains(err.Error(), "InvalidInstanceID.NotFound") {
				if targetState == "terminated" {
					l.Printf("All %d instances in region %s terminated", len(instanceIDs), region)
					return nil
				}
			}
			l.Printf("Warning: failed to describe instances: %v", err)
			continue
		}

		matchCount := 0
		for _, res := range descOutput.Reservations {
			for _, inst := range res.Instances {
				if inst.State.Name == targetState {
					matchCount++
				}
			}
		}

		if matchCount == len(instanceIDs) {
			l.Printf("All %d instances in region %s reached %s state", len(instanceIDs), region, targetState)
			return nil
		}

		l.Printf("Waiting for instances in %s: %d/%d %s", region, matchCount, len(instanceIDs), targetState)
	}

	return errors.Errorf("timed out waiting for instances in region %s to reach %s state", region, targetState)
}

// attachInstancesToASG attaches instances to an Auto Scaling Group.
// The caller is responsible for ensuring the ASG has sufficient max size capacity
// before calling this function.
func (p *Provider) attachInstancesToASG(
	ctx context.Context, l *logger.Logger, asgName, region string, instanceIDs []string,
) error {
	l.Printf("Attaching %d instances to ASG %s", len(instanceIDs), asgName)
	attachArgs := []string{
		"autoscaling", "attach-instances",
		"--auto-scaling-group-name", asgName,
		"--instance-ids",
	}
	attachArgs = append(attachArgs, instanceIDs...)
	attachArgs = append(attachArgs, "--region", region)

	if _, err := p.runCommandWithContext(ctx, l, attachArgs); err != nil {
		return errors.Wrapf(err, "failed to attach instances to ASG")
	}

	return nil
}

// updateASGMaxSize updates the max size of an Auto Scaling Group.
func (p *Provider) updateASGMaxSize(
	ctx context.Context, l *logger.Logger, asgName, region string, maxSize int,
) error {
	l.Printf("Updating ASG %s max size to %d", asgName, maxSize)
	args := []string{
		"autoscaling", "update-auto-scaling-group",
		"--auto-scaling-group-name", asgName,
		"--max-size", strconv.Itoa(maxSize),
		"--region", region,
	}
	if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
		return errors.Wrapf(err, "failed to update ASG max size")
	}
	return nil
}

// asgInstance contains information about an instance in an Auto Scaling Group.
type asgInstance struct {
	InstanceID     string
	LifecycleState string
}

// asgInfo contains information about an Auto Scaling Group.
type asgInfo struct {
	Name            string
	DesiredCapacity int
	MaxSize         int
	MinSize         int
	TargetGroupARNs []string
	Instances       []asgInstance
}

// describeASG retrieves information about an Auto Scaling Group.
// Returns nil (not an error) if the ASG doesn't exist.
func (p *Provider) describeASG(
	ctx context.Context, l *logger.Logger, asgName, region string,
) (*asgInfo, error) {
	args := []string{
		"autoscaling", "describe-auto-scaling-groups",
		"--auto-scaling-group-names", asgName,
		"--region", region,
	}

	var output struct {
		AutoScalingGroups []struct {
			AutoScalingGroupName string   `json:"AutoScalingGroupName"`
			DesiredCapacity      int      `json:"DesiredCapacity"`
			MaxSize              int      `json:"MaxSize"`
			MinSize              int      `json:"MinSize"`
			TargetGroupARNs      []string `json:"TargetGroupARNs"`
			Instances            []struct {
				InstanceID     string `json:"InstanceId"`
				LifecycleState string `json:"LifecycleState"`
			} `json:"Instances"`
		} `json:"AutoScalingGroups"`
	}

	if err := p.runJSONCommandWithContext(ctx, l, args, &output); err != nil {
		return nil, errors.Wrapf(err, "failed to describe ASG %s", asgName)
	}

	if len(output.AutoScalingGroups) == 0 {
		return nil, nil
	}

	asg := output.AutoScalingGroups[0]
	instances := make([]asgInstance, len(asg.Instances))
	for i, inst := range asg.Instances {
		instances[i] = asgInstance{
			InstanceID:     inst.InstanceID,
			LifecycleState: inst.LifecycleState,
		}
	}

	return &asgInfo{
		Name:            asg.AutoScalingGroupName,
		DesiredCapacity: asg.DesiredCapacity,
		MaxSize:         asg.MaxSize,
		MinSize:         asg.MinSize,
		TargetGroupARNs: asg.TargetGroupARNs,
		Instances:       instances,
	}, nil
}

// createManagedCluster creates a cluster using Launch Templates and Auto Scaling Groups.
//
// AWS ASG doesn't support creating instances with custom names (unlike GCE MIG which has
// `create-instance --instance <NAME>`). To preserve roachprod's naming conventions
// (e.g., clustername-0001, clustername-0002), we:
// 1. Create the launch template (stores instance configuration)
// 2. Create the ASG with desiredCapacity=0 (management structure only)
// 3. Create instances using runInstance() with proper names
// 4. Attach instances to the ASG
func (p *Provider) createManagedCluster(
	l *logger.Logger,
	names []string,
	zones []string,
	regions []string,
	machineType string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
) (vm.List, error) {
	if len(names) == 0 {
		return nil, errors.New("no VM names provided")
	}

	clusterName := opts.ClusterName

	ctx := context.Background()

	// Group zones by region for ASG creation.
	// Each region gets one launch template and one ASG.
	zonesByRegion := make(map[string][]string)
	zoneForName := make(map[string]string)
	for i, zone := range zones {
		az := p.Config.getAvailabilityZone(zone)
		if az == nil {
			return nil, errors.Errorf("unknown availability zone %s", zone)
		}
		region := az.Region.Name
		if !slices.Contains(zonesByRegion[region], zone) {
			zonesByRegion[region] = append(zonesByRegion[region], zone)
		}
		zoneForName[names[i]] = zone
	}

	// Step 1: Create launch template and ASG (with desiredCapacity=0) in each region.
	g := ctxgroup.WithContext(ctx)
	for region, regionZones := range zonesByRegion {
		// Use the first zone for instance configuration (AMI lookup, etc.)
		zone := regionZones[0]
		cfg, az, err := p.getInstanceConfig(l, zone, machineType, opts, providerOpts)
		if err != nil {
			return nil, err
		}

		g.GoCtx(func(ctx context.Context) error {
			// Create launch template
			ltID, err := p.createLaunchTemplate(ctx, l, clusterName, region, cfg, az, opts, providerOpts)
			if err != nil {
				return errors.Wrapf(err, "failed to create launch template in region %s", region)
			}

			// Create ASG with desiredCapacity=0 (we'll create and attach instances separately).
			// Set maxSize upfront to allow room for the instances we'll attach plus future growth.
			if err := p.createAutoScalingGroup(ctx, l, clusterName, region, ltID, regionZones, 0, len(names)*2, opts); err != nil {
				return errors.Wrapf(err, "failed to create auto scaling group in region %s", region)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Step 2: Create instances with proper names using createInstances().
	// Add managed label so instances are tagged at creation time.
	if opts.CustomLabels == nil {
		opts.CustomLabels = make(map[string]string)
	}
	opts.CustomLabels[vm.TagManaged] = "true"
	instanceIDsByRegion, err := p.createInstances(l, names, zones, machineType, opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Step 3: Wait for instances to be running before attaching to ASG.
	l.Printf("Waiting for instances to be running...")
	for region, instanceIDs := range instanceIDsByRegion {
		if err := p.waitForInstancesStatus(ctx, l, region, instanceIDs, "running"); err != nil {
			return nil, err
		}
	}

	// Step 4: Attach instances to the ASG.
	// The ASG was created with sufficient maxSize to accommodate these instances.
	g = ctxgroup.WithContext(ctx)
	for region, instanceIDs := range instanceIDsByRegion {
		asgName := autoScalingGroupName(clusterName, region)
		g.GoCtx(func(ctx context.Context) error {
			return p.attachInstancesToASG(ctx, l, asgName, region, instanceIDs)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Wait for IPs and return the VM list.
	return p.waitForIPs(ctx, l, names, regions, providerOpts)
}

// Grow adds new VMs to a managed cluster.
// For AWS, we create new EC2 instances using the launch template and attach them to the ASG.
// This preserves roachprod's naming conventions while keeping the instances managed by the ASG.
func (p *Provider) Grow(
	l *logger.Logger, vms vm.List, clusterName string, names []string,
) (vm.List, error) {
	if !isManaged(vms) {
		return nil, errors.New("growing is only supported for managed clusters (created with --aws-managed)")
	}
	if len(names) == 0 {
		return nil, errors.New("no VM names provided for grow operation")
	}

	ctx := context.Background()

	// For simplicity, add new VMs to the first region.
	region, existingVMs, err := p.getFirstRegion(vms)
	if err != nil {
		return nil, err
	}

	// Validate ASG exists and expand capacity if needed.
	asgName := autoScalingGroupName(clusterName, region)
	asg, err := p.describeASG(ctx, l, asgName, region)
	if err != nil {
		return nil, err
	}
	if asg == nil {
		return nil, errors.Errorf("ASG %s not found", asgName)
	}
	if newCapacity := asg.DesiredCapacity + len(names); newCapacity > asg.MaxSize {
		if err := p.updateASGMaxSize(ctx, l, asgName, region, newCapacity*2); err != nil {
			return nil, err
		}
	}

	// Configure SSH and extract config from existing VMs.
	zone := existingVMs[0].Zone
	if err := p.ConfigSSH(l, []string{zone}); err != nil {
		return nil, errors.Wrap(err, "failed to configure SSH keys")
	}
	machineType := existingVMs[0].MachineType
	providerOpts := DefaultProviderOpts()

	// Inherit the IAM profile from the existing instances so that new nodes
	// have the same permissions (e.g., S3 access, CloudWatch, etc.).
	if iamProfile, err := p.getIAMProfileForInstance(ctx, l, region, existingVMs[0].ProviderID); err != nil {
		l.Printf("Warning: failed to get IAM profile from existing instance: %v", err)
	} else if iamProfile != "" {
		providerOpts.IAMProfile = iamProfile
	}

	opts := p.buildCreateOptsFromVM(clusterName, existingVMs[0])

	// Create instances in parallel.
	instanceIDs, err := p.createInstancesInParallel(ctx, l, names, zone, machineType, opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Wait for instances to be running, then attach to ASG.
	if err := p.waitForInstancesStatus(ctx, l, region, instanceIDs, "running"); err != nil {
		return nil, err
	}
	if err := p.attachInstancesToASG(ctx, l, asgName, region, instanceIDs); err != nil {
		return nil, err
	}

	// Wait for IPs and copy labels from existing VMs.
	newVMs, err := p.waitForIPs(ctx, l, names, []string{region}, providerOpts)
	if err != nil {
		return nil, err
	}
	if err := p.copyLabelsToNewVMs(l, existingVMs[0], newVMs); err != nil {
		l.Printf("Warning: failed to add labels to new VMs: %v", err)
	}

	l.Printf("Successfully added %d VMs to cluster %s", len(names), clusterName)
	return newVMs, nil
}

// getFirstRegion returns the first region and its VMs from the given VM list.
// This is used when we need to pick a single region for an operation.
func (p *Provider) getFirstRegion(vms vm.List) (string, vm.List, error) {
	byRegion, err := regionMap(vms)
	if err != nil {
		return "", nil, err
	}
	for region, regionVMs := range byRegion {
		return region, regionVMs, nil
	}
	return "", nil, errors.New("no regions found in VM list")
}

// buildCreateOptsFromVM builds CreateOpts by extracting configuration from an existing VM.
func (p *Provider) buildCreateOptsFromVM(clusterName string, existingVM vm.VM) vm.CreateOpts {
	opts := vm.DefaultCreateOpts()
	opts.ClusterName = clusterName
	if lifetimeStr, ok := existingVM.Labels[vm.TagLifetime]; ok {
		if lifetime, err := time.ParseDuration(lifetimeStr); err == nil {
			opts.Lifetime = lifetime
		}
	}
	return opts
}

// createInstancesInParallel creates EC2 instances in parallel and returns their IDs.
func (p *Provider) createInstancesInParallel(
	ctx context.Context,
	l *logger.Logger,
	names []string,
	zone, machineType string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
) ([]string, error) {
	var mu syncutil.Mutex
	var instanceIDs []string
	limiter := rate.NewLimiter(rate.Limit(providerOpts.CreateRateLimit), 2)
	g := ctxgroup.WithContext(ctx)

	for i, vmName := range names {
		index := i
		name := vmName
		res := limiter.Reserve()
		g.GoCtx(func(ctx context.Context) error {
			time.Sleep(res.Delay())
			v, err := p.runInstance(l, name, index, zone, machineType, opts, providerOpts)
			if err != nil {
				return errors.Wrapf(err, "failed to create instance %s", name)
			}
			mu.Lock()
			instanceIDs = append(instanceIDs, v.ProviderID)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return instanceIDs, nil
}

// copyLabelsToNewVMs copies labels from a source VM to new VMs.
// It skips instance-specific labels like Name and AWS-reserved tags.
func (p *Provider) copyLabelsToNewVMs(l *logger.Logger, sourceVM vm.VM, newVMs vm.List) error {
	labels := map[string]string{vm.TagManaged: "true"}
	for key, value := range sourceVM.Labels {
		// Skip instance-specific and AWS-reserved labels.
		if key == "Name" || key == vm.TagManaged || strings.HasPrefix(key, "aws:") {
			continue
		}
		labels[key] = value
	}
	return p.AddLabels(l, newVMs, labels)
}

// Shrink removes VMs from a managed cluster.
// For AWS, we use terminate-instance-in-auto-scaling-group to remove specific instances.
// This decrements the ASG's desired capacity and terminates the specified instances.
func (p *Provider) Shrink(l *logger.Logger, vmsToDelete vm.List, clusterName string) error {
	if !isManaged(vmsToDelete) {
		return errors.New("shrinking is only supported for managed clusters (created with --aws-managed)")
	}

	if len(vmsToDelete) == 0 {
		return nil
	}

	ctx := context.Background()

	// Group VMs by region
	byRegion, err := regionMap(vmsToDelete)
	if err != nil {
		return err
	}

	// Terminate instances in each region
	g := ctxgroup.WithContext(ctx)
	for region, regionVMs := range byRegion {
		asgName := autoScalingGroupName(clusterName, region)

		// Terminate each instance
		for _, v := range regionVMs {
			instanceID := v.ProviderID
			g.GoCtx(func(ctx context.Context) error {
				l.Printf("Terminating instance %s from ASG %s", instanceID, asgName)

				// Use terminate-instance-in-auto-scaling-group to properly remove from ASG
				// and decrement desired capacity
				args := []string{
					"autoscaling", "terminate-instance-in-auto-scaling-group",
					"--instance-id", instanceID,
					"--should-decrement-desired-capacity",
					"--region", region,
				}

				if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
					return errors.Wrapf(err, "failed to terminate instance %s", instanceID)
				}

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Wait for instances to be fully terminated in each region.
	for region, regionVMs := range byRegion {
		if err := p.waitForInstancesStatus(ctx, l, region, regionVMs.ProviderIDs(), "terminated"); err != nil {
			return err
		}
	}

	l.Printf("Successfully removed %d VMs from cluster", len(vmsToDelete))
	return nil
}

// waitForIPs waits until AWS reports both internal and external IP addresses
// for all newly created VMs. If we did not wait for these IPs then attempts to
// list the new VMs after the creation might find VMs without an external IP.
// We do a bad job at higher layers detecting this lack of IP which can lead to
// commands hanging indefinitely.
func (p *Provider) waitForIPs(
	ctx context.Context, l *logger.Logger, names []string, regions []string, opts *ProviderOpts,
) ([]vm.VM, error) {
	waitForIPRetry := retry.Start(retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		MaxRetries:     120, // wait a bit less than 90s for IPs
	})
	for waitForIPRetry.Next() {
		// We need to list the VMs in each region to get their IPs.
		// We also include volumes in the list because they were not included
		// in the initial list of VMs as they're only returned by run-instances
		// when ready.
		vms, err := p.listRegionsFiltered(ctx, l, regions, names, *opts, vm.ListOptions{
			IncludeVolumes: true,
		})
		if err != nil {
			return nil, err
		}
		ipAddressesFound := make(map[string]struct{})
		for _, vm := range vms {
			if vm.PublicIP != "" && vm.PrivateIP != "" {
				ipAddressesFound[vm.Name] = struct{}{}
			}
		}
		if len(ipAddressesFound) == len(names) {
			return vms, nil
		}
	}
	return nil, fmt.Errorf("failed to retrieve IPs for all vms")
}

// Delete is part of vm.Provider.
// This will delete all instances in a single AWS command.
// For managed clusters, it also deletes the ASG and launch template.
func (p *Provider) Delete(l *logger.Logger, vms vm.List) error {
	if len(vms) == 0 {
		return nil
	}

	// Clean up any load balancers that may exist.
	if err := p.DeleteLoadBalancer(l, vms, 0); err != nil {
		l.Printf("Warning: failed to delete load balancers: %v", err)
	}

	// For managed clusters, delete the ASG and launch template in addition to instances.
	if isManaged(vms) {
		return p.deleteManagedCluster(l, vms)
	}

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

// deleteManagedCluster deletes a managed cluster's ASG, launch template, and instances.
func (p *Provider) deleteManagedCluster(l *logger.Logger, vms vm.List) error {
	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return err
	}

	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}

	ctx := context.Background()
	g := ctxgroup.WithContext(ctx)

	for region := range byRegion {
		g.GoCtx(func(ctx context.Context) error {
			// Delete the ASG first (this terminates instances)
			if err := p.deleteAutoScalingGroup(ctx, l, clusterName, region); err != nil {
				l.Printf("Warning: failed to delete ASG: %v", err)
			}

			// Delete the launch template
			if err := p.deleteLaunchTemplate(ctx, l, clusterName, region); err != nil {
				l.Printf("Warning: failed to delete launch template: %v", err)
			}

			return nil
		})
	}

	return g.Wait()
}

// Reset is part of vm.Provider.
func (p *Provider) Reset(l *logger.Logger, vms vm.List) error {
	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}
	g := errgroup.Group{}
	for region, list := range byRegion {
		args := []string{
			"ec2", "reboot-instances",
			"--region", region,
			"--instance-ids",
		}
		args = append(args, list.ProviderIDs()...)
		g.Go(func() error {
			_, e := p.runCommand(l, args)
			return e
		})
	}
	return g.Wait()
}

// Extend is part of the vm.Provider interface.
// This will update the Lifetime tag on the instances.
func (p *Provider) Extend(l *logger.Logger, vms vm.List, lifetime time.Duration) error {
	return p.AddLabels(l, vms, map[string]string{
		vm.TagLifetime: lifetime.String(),
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
			// iamGetUser fails with role-based credentials;
			// fall back to stsGetCallerIdentity.
			account, err = p.stsGetCallerIdentity(l)
		}
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
func (p *Provider) List(
	ctx context.Context, l *logger.Logger, opts vm.ListOptions,
) (vm.List, error) {
	regions, err := p.allRegions(p.Config.availabilityZoneNames())
	if err != nil {
		return nil, err
	}
	defaultOpts := p.CreateProviderOpts().(*ProviderOpts)
	return p.listRegions(ctx, l, regions, *defaultOpts, opts)
}

// listRegions lists VMs in the regions passed.
// It ignores region-specific errors.
func (p *Provider) listRegions(
	ctx context.Context,
	l *logger.Logger,
	regions []string,
	opts ProviderOpts,
	listOpts vm.ListOptions,
) (vm.List, error) {
	return p.listRegionsFiltered(ctx, l, regions, nil, opts, listOpts)
}

// listRegionsFiltered lists VMs in the regions with a filter on instance names.
// The filter makes it more efficient to list specific VMs than listRegions.
func (p *Provider) listRegionsFiltered(
	ctx context.Context,
	l *logger.Logger,
	regions, names []string,
	opts ProviderOpts,
	listOpts vm.ListOptions,
) (vm.List, error) {
	var ret vm.List
	var mux syncutil.Mutex
	g := ctxgroup.WithContext(ctx)

	// Create a filter for the instance names.
	var namesFilter string
	if names != nil {
		namesFilter = fmt.Sprintf("Name=tag:Name,Values=%s", strings.Join(names, ","))
	}

	for _, r := range regions {
		g.GoCtx(func(ctx context.Context) error {
			vms, err := p.describeInstances(ctx, l, r, opts, listOpts, namesFilter)
			if err != nil {
				l.Printf("Failed to list AWS VMs in region: %s\n%v\n", r, err)
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
		if _, have := byName[az.Region.Name]; !have {
			byName[az.Region.Name] = struct{}{}
			regions = append(regions, az.Region.Name)
		}
	}
	return regions, nil
}

func (p *Provider) getVolumesForInstances(
	ctx context.Context, l *logger.Logger, region string, instanceIDs []string,
) (vols map[string]map[string]vm.Volume, err error) {
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

	vols = make(map[string]map[string]vm.Volume)
	var volumeOut describeVolume
	getVolumesArgs := []string{
		"ec2", "describe-volumes",
		"--region", region,
		"--filters",
		"Name=attachment.instance-id,Values=" + strings.Join(instanceIDs, ","),
	}

	err = p.runJSONCommandWithContext(ctx, l, getVolumesArgs, &volumeOut)
	if err != nil {
		return vols, err
	}
	for _, vol := range volumeOut.Volumes {
		tagMap := vol.Tags.MakeMap()
		volume := vm.Volume{
			ProviderResourceID: vol.VolumeID,
			ProviderVolumeType: vol.VolumeType,
			Zone:               vol.AvailabilityZone,
			Encrypted:          vol.Encrypted,
			Labels:             tagMap,
			Size:               vol.Size,
			Name:               tagMap["Name"],
		}

		for _, attachment := range vol.Attachments {
			if vols[attachment.InstanceID] == nil {
				vols[attachment.InstanceID] = make(map[string]vm.Volume)
			}
			vols[attachment.InstanceID][vol.VolumeID] = volume
		}
	}
	return vols, err
}

// DescribeInstancesOutput represents the output of the aws ec2 describe-instances command
type DescribeInstancesOutput struct {
	Reservations []struct {
		Instances []DescribeInstancesOutputInstance
	}
}
type DescribeInstancesOutputInstance struct {
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
	StateReason struct {
		Code    string `json:"Code"`
		Message string `json:"Message"`
	} `json:"StateReason"`
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

	// Encodes IAM identifier for this instance.
	IamInstanceProfile struct {
		// Of the form "Arn": "arn:aws:iam::[0-9]+:instance-profile/roachprod-testing"
		Arn string `json:"Arn"`
		Id  string `json:"Id"`
	}
}

// toVM converts an ec2 instance to a vm.VM struct.
func (in *DescribeInstancesOutputInstance) toVM(
	volumes map[string]vm.Volume, remoteUserName string, dnsProvider vm.DNSProvider,
) *vm.VM {

	// Convert the tag map into a more useful representation
	tagMap := in.Tags.MakeMap()

	var errs []vm.VMError
	createdAt, err := time.Parse(time.RFC3339, in.LaunchTime)
	if err != nil {
		errs = append(errs, vm.NewVMError(vm.ErrNoExpiration))
	}

	var lifetime time.Duration
	if lifeText, ok := tagMap[vm.TagLifetime]; ok {
		lifetime, err = time.ParseDuration(lifeText)
		if err != nil {
			errs = append(errs, vm.NewVMError(err))
		}
	} else {
		errs = append(errs, vm.NewVMError(vm.ErrNoExpiration))
	}

	var nonBootableVolumes []vm.Volume
	if len(volumes) > 0 {
		rootDevice := in.RootDeviceName
		for _, bdm := range in.BlockDeviceMappings {
			if bdm.DeviceName != rootDevice {
				if vol, ok := volumes[bdm.Disk.VolumeID]; ok {
					nonBootableVolumes = append(nonBootableVolumes, vol)
				} else {
					errs = append(errs, vm.NewVMError(errors.Newf(
						"Attempted to add volume %s however it is not in the attached volumes for instance %s",
						bdm.Disk.VolumeID,
						in.InstanceID,
					)))
				}
			}
		}
	}
	// Parse IamInstanceProfile.Arn to extract IAM identifier.
	// The ARN is of the form "arn:aws:iam::[0-9]+:instance-profile/roachprod-testing"
	iamIdentifier := ""
	if in.IamInstanceProfile.Arn != "" {
		iamIdentifier = strings.Split(strings.TrimPrefix(in.IamInstanceProfile.Arn, "arn:aws:iam::"), ":")[0]
	}

	// Get public DNS info from the DNS provider if it is configured.
	publicDns, publicDnsZone, dnsProviderName := vm.GetVMDNSInfo(context.Background(), tagMap["Name"], dnsProvider)

	return &vm.VM{
		CreatedAt:              createdAt,
		DNS:                    in.PrivateDNSName,
		Name:                   tagMap["Name"],
		Errors:                 errs,
		Lifetime:               lifetime,
		Labels:                 tagMap,
		PrivateIP:              in.PrivateIPAddress,
		Provider:               ProviderName,
		ProviderID:             in.InstanceID,
		ProviderAccountID:      iamIdentifier,
		PublicIP:               in.PublicIPAddress,
		PublicDNS:              publicDns,
		PublicDNSZone:          publicDnsZone,
		DNSProvider:            dnsProviderName,
		RemoteUser:             remoteUserName,
		VPC:                    in.VpcID,
		MachineType:            in.InstanceType,
		CPUArch:                vm.ParseArch(in.Architecture),
		Zone:                   in.Placement.AvailabilityZone,
		NonBootAttachedVolumes: nonBootableVolumes,
		Preemptible:            in.InstanceLifecycle == "spot",
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
	Instances []DescribeInstancesOutputInstance
}

// describeInstances executes the ec2 describe-instances command
// with the given filters.
func (p *Provider) describeInstances(
	ctx context.Context,
	l *logger.Logger,
	region string,
	opts ProviderOpts,
	listOpt vm.ListOptions,
	filters string,
) (vm.List, error) {

	args := []string{
		"ec2", "describe-instances",
		"--region", region,
	}
	if filters != "" {
		args = append(args, "--filters", filters)
	}
	var describeInstancesResponse DescribeInstancesOutput
	err := p.runJSONCommandWithContext(ctx, l, args, &describeInstancesResponse)
	if err != nil {
		return nil, err
	}

	var instances = make(map[string]DescribeInstancesOutputInstance)
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
			if tagMap[vm.TagRoachprod] != "true" {
				continue in
			}

			instances[in.InstanceID] = in
		}
	}

	// Fetch volume info for all instances at once
	var volumes map[string]map[string]vm.Volume
	if listOpt.IncludeVolumes && len(instances) > 0 {
		volumes, err = p.getVolumesForInstances(ctx, l, region, maps.Keys(instances))
		if err != nil {
			return nil, err
		}
	}

	var ret vm.List
	for _, in := range instances {
		v := in.toVM(volumes[in.InstanceID], opts.RemoteUserName, p.dnsProvider)
		ret = append(ret, *v)
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
) (*vm.VM, error) {
	cfg, az, err := p.getInstanceConfig(l, zone, machineType, opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Log AMI selection
	if instanceIdx == 0 {
		if isArmMachineType(machineType) {
			l.Printf("Using ARM64 AMI: %s for machine type: %s", cfg.imageID, machineType)
		} else if opts.Arch == string(vm.ArchFIPS) {
			l.Printf("Using FIPS-enabled AMI: %s for machine type: %s", cfg.imageID, machineType)
		}
	}

	// We avoid the need to make a second call to set the tags by jamming
	// all of our metadata into the tagSpec.
	m := vm.GetDefaultLabelMap(opts)
	m[vm.TagCreated] = timeutil.Now().Format(time.RFC3339)
	m["Name"] = name

	// TODO(golgeek, 2025-03-25): In an effort to unify tags in lowercase across
	// all providers, AWS cost analysis dashboard will break as they look for a
	// capitalized `Cluster` tag. We duplicate the tag for now and will remove it
	// once all resources are created with the unified tags in a year or so.
	m["Cluster"] = m[vm.TagCluster]

	if providerOpts.UseSpot {
		m[vm.TagSpotInstance] = "true"
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
			return nil, fmt.Errorf("duplicate label name defined: %s", key)
		}
		addLabel(key, value)
	}
	for key, value := range m {
		addLabel(key, value)
	}
	labels := strings.Join(labelPairs, ",")
	vmTagSpecs := fmt.Sprintf("ResourceType=instance,Tags=[%s]", labels)
	volumeTagSpecs := fmt.Sprintf("ResourceType=volume,Tags=[%s]", labels)

	// Create AWS startup script file.
	extraMountOpts := ""
	if opts.SSDOpts.UseLocalSSD && opts.SSDOpts.NoExt4Barrier && opts.SSDOpts.FileSystem == vm.Ext4 {
		extraMountOpts = "nobarrier"
	}

	filename, err := writeStartupScript(
		name,
		extraMountOpts,
		opts.SSDOpts.FileSystem,
		providerOpts.UseMultipleDisks,
		opts.Arch == string(vm.ArchFIPS),
		providerOpts.RemoteUserName,
		providerOpts.BootDiskOnly,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not write AWS startup script to temp file")
	}
	defer func() {
		_ = os.Remove(filename)
	}()

	args := []string{
		"ec2", "run-instances",
		"--associate-public-ip-address",
		"--count", "1",
		"--instance-type", cfg.machineType,
		"--image-id", cfg.imageID,
		"--key-name", cfg.keyName,
		"--region", az.Region.Name,
		"--security-group-ids", cfg.securityGroup,
		"--subnet-id", az.SubnetID,
		"--tag-specifications", vmTagSpecs, volumeTagSpecs,
		"--user-data", "file://" + filename,
	}

	if cfg.cpuOptions != "" {
		args = append(args, "--cpu-options", cfg.cpuOptions)
	}

	if cfg.iamProfile != "" {
		args = append(args, "--iam-instance-profile", "Name="+cfg.iamProfile)
	}

	args, err = genDeviceMapping(cfg.ebsVolumes, args)
	if err != nil {
		return nil, err
	}

	if providerOpts.UseSpot {
		//todo(babusrithar): Add fallback to on-demand instances if spot instances are not available.
		return runSpotInstance(l, p, args, az.Region.Name, providerOpts)
	}

	runInstancesOutput := RunInstancesOutput{}
	err = p.runJSONCommand(l, args, &runInstancesOutput)
	if err != nil {
		return nil, err
	}

	if len(runInstancesOutput.Instances) == 0 {
		return nil, errors.Errorf("No instances found for run-instances command")
	}

	// Volumes are attached to the instance only after the instance is running.
	// We will fill in the volume information during the waitForIPs call.
	v := runInstancesOutput.Instances[0].toVM(
		map[string]vm.Volume{}, providerOpts.RemoteUserName, p.dnsProvider,
	)
	return v, err
}

// isArmMachineType returns true if the machine type uses ARM64 architecture.
func isArmMachineType(machineType string) bool {
	return strings.Index(machineType, "6g.") == 1 || strings.Index(machineType, "6gd.") == 1 ||
		strings.Index(machineType, "7g.") == 1 || strings.Index(machineType, "7gd.") == 1 ||
		strings.Index(machineType, "8g.") == 1 || strings.Index(machineType, "8gd.") == 1
}

// runSpotInstance uses run-instances command to create a spot instance.
// It returns an error if the spot request is not fulfilled within 2 minutes.
// It uses describe-spot-instance-requests command to get the status of the spot request.
func runSpotInstance(
	l *logger.Logger, p *Provider, args []string, regionName string, providerOpts *ProviderOpts,
) (*vm.VM, error) {
	waitForSpotDuration := 2 * time.Minute

	// Add spot instance options to the run-instances command.
	spotArgs := append(args, "--instance-market-options",
		fmt.Sprintf("MarketType=spot,SpotOptions={SpotInstanceType=one-time,"+
			"InstanceInterruptionBehavior=terminate}"))
	runInstancesOutput := RunInstancesOutput{}
	err := p.runJSONCommand(l, spotArgs, &runInstancesOutput)
	if err != nil {
		return nil, err
	}
	// If the spot request is accepted, the run-instances command will return an instance-id.
	if len(runInstancesOutput.Instances) == 0 {
		return nil, errors.Errorf("No instances found for spot request, likely the spot request had bad parameter")
	}

	v := runInstancesOutput.Instances[0].toVM(map[string]vm.Volume{}, providerOpts.RemoteUserName, p.dnsProvider)

	instanceId := runInstancesOutput.Instances[0].InstanceID
	spotInstanceRequestId, err := getSpotInstanceRequestId(l, p, regionName, instanceId)
	if err != nil {
		return nil, err
	}

	// Loop every 10 seconds till the spot instance is fulfilled, for a maximum of 2 minutes.
	startTime := timeutil.Now()
	duration := waitForSpotDuration
	for {
		describeSpotInstanceRequestsOutput, err := describeSpotInstanceRequest(l, p, regionName, spotInstanceRequestId)
		if err != nil {
			return nil, err
		}
		spotRequestFulfilled, err := processSpotInstanceRequestStatus(l, describeSpotInstanceRequestsOutput, spotInstanceRequestId, instanceId)
		if err != nil {
			return nil, err
		}
		if spotRequestFulfilled {
			return v, nil
		}
		// This part of the code depends on demand/supply of AWS and can be hard to test.
		// One way to manually test is tested by commenting out return nil above and check cancellation after 2 minutes.
		if timeutil.Since(startTime) >= duration {
			l.Printf("waitForSpotDuration passed, cancel the spot instance request and exit loop")
			err := cancelSpotRequest(l, p, regionName, spotInstanceRequestId)
			if err != nil {
				return nil, err
			}
			return nil, errors.New("waitForSpotDuration over")
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

// calculateProvisionedIOPS calculates the appropriate IOPS for io1/io2 volumes
// based on volume size, respecting AWS constraints.
//
// AWS enforces maximum IOPS-to-size ratios:
// - io1: 50 IOPS/GB (max 64,000 IOPS)
// - io2: 500 IOPS/GB for standard, 1000 IOPS/GB for Block Express (max 256,000 IOPS)
//
// We use 10 IOPS/GB as a baseline to match Azure's ultra-disk default ratio,
// with a minimum of 3,000 IOPS (matching gp3 baseline), but we must respect
// AWS's IOPS-to-size ratio constraints.
func calculateProvisionedIOPS(volumeType string, volumeSize int) int {
	if volumeType != "io1" && volumeType != "io2" {
		return 0
	}

	// Calculate baseline: 10 IOPS/GB
	iops := volumeSize * 10

	// Determine AWS constraints for this volume type
	var maxIOPSPerGB int
	var absoluteMaxIOPS int
	switch volumeType {
	case "io1":
		maxIOPSPerGB = 50
		absoluteMaxIOPS = 64000
	case "io2":
		// As of April 2025, all io2 volumes are Block Express with 1000 IOPS/GB.
		// We use the more conservative 500 IOPS/GB for compatibility.
		maxIOPSPerGB = 500
		absoluteMaxIOPS = 64000 // Use 64k for compatibility; Block Express supports 256k
	}

	// Apply constraint-aware minimum
	if iops < 3000 {
		// Set a minimum of 3,000 IOPS (matching gp3 baseline)
		iops = 3000

		// But if that exceeds the maximum allowed IOPS for this volume size,
		// set to the maximum allowed instead.
		maxAllowedIOPS := volumeSize * maxIOPSPerGB
		if iops > maxAllowedIOPS {
			iops = maxAllowedIOPS
		}
	} else if iops > absoluteMaxIOPS {
		iops = absoluteMaxIOPS
	}

	return iops
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

	osDiskVolume := &ebsVolume{
		DeviceName: "/dev/sda1",
		Disk: ebsDisk{
			VolumeType:          defaultEBSVolumeType,
			VolumeSize:          opts.OsVolumeSize,
			DeleteOnTermination: true,
		},
	}

	// If local SSD or boot disk only is requested, return that immediately.
	// Local SSDs cannot be configured and will be automatically mapped by AWS
	// depending on the instance type.
	if opts.SSDOpts.UseLocalSSD || providerOpts.BootDiskOnly {
		return ebsVolumeList{osDiskVolume}
	}

	// aws-ebs-volume supersedes other volume settings, if none are provided,
	// we build a list based on count and provided settings.
	if len(ebsVolumes) == 0 {
		for range providerOpts.EBSVolumeCount {
			v := ebsVolumes.newVolume()
			v.Disk = providerOpts.DefaultEBSVolume.Disk
			v.Disk.DeleteOnTermination = true

			// io2/io1 volumes require IOPS to be specified. If not already set,
			// calculate based on volume size using AWS-compliant logic.
			if v.Disk.IOPs == 0 {
				v.Disk.IOPs = calculateProvisionedIOPS(v.Disk.VolumeType, v.Disk.VolumeSize)
			}

			ebsVolumes = append(ebsVolumes, v)
		}
	}

	// Add the OS disk to the list of volumes to be created.
	ebsVolumes = append(ebsVolumes, osDiskVolume)

	return ebsVolumes
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

func (p *Provider) DeleteVolume(l *logger.Logger, volume vm.Volume, _ *vm.VM) error {
	return vm.UnimplementedError
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
	return nil, vm.UnimplementedError
}

func (p *Provider) DeleteVolumeSnapshots(l *logger.Logger, snapshots ...vm.VolumeSnapshot) error {
	return vm.UnimplementedError
}

// loadBalancerResourceName returns the name of a load balancer resource.
// The format is {cluster}-{port}-{resource-type}-roachprod.
// AWS has a 32-character limit for NLB and target group names, so the name
// is truncated if necessary, ensuring it doesn't end with a hyphen.
func loadBalancerResourceName(clusterName string, port int, resourceType string) string {
	const maxLen = 32
	suffix := fmt.Sprintf("-%d-%s-roachprod", port, resourceType)
	maxClusterLen := maxLen - len(suffix)

	if maxClusterLen <= 0 {
		name := fmt.Sprintf("%s%s", clusterName, suffix)
		return strings.TrimRight(name[:min(len(name), maxLen)], "-")
	}

	truncatedCluster := clusterName
	if len(clusterName) > maxClusterLen {
		truncatedCluster = strings.TrimRight(clusterName[:maxClusterLen], "-")
	}

	return fmt.Sprintf("%s%s", truncatedCluster, suffix)
}

// elbv2TargetGroup represents an AWS ELBv2 target group.
type elbv2TargetGroup struct {
	TargetGroupArn  string `json:"TargetGroupArn"`
	TargetGroupName string `json:"TargetGroupName"`
	VpcId           string `json:"VpcId"`
	Port            int    `json:"Port"`
}

// elbv2LoadBalancer represents an AWS ELBv2 load balancer.
type elbv2LoadBalancer struct {
	LoadBalancerArn  string `json:"LoadBalancerArn"`
	LoadBalancerName string `json:"LoadBalancerName"`
	DNSName          string `json:"DNSName"`
	State            struct {
		Code string `json:"Code"`
	} `json:"State"`
	AvailabilityZones []struct {
		ZoneName string `json:"ZoneName"`
		SubnetId string `json:"SubnetId"`
	} `json:"AvailabilityZones"`
}

// elbv2Listener represents an AWS ELBv2 listener.
type elbv2Listener struct {
	ListenerArn     string `json:"ListenerArn"`
	LoadBalancerArn string `json:"LoadBalancerArn"`
	Port            int    `json:"Port"`
	Protocol        string `json:"Protocol"`
}

// describeTargetGroupsOutput represents the output of describe-target-groups.
type describeTargetGroupsOutput struct {
	TargetGroups []elbv2TargetGroup `json:"TargetGroups"`
}

// describeLoadBalancersOutput represents the output of describe-load-balancers.
type describeLoadBalancersOutput struct {
	LoadBalancers []elbv2LoadBalancer `json:"LoadBalancers"`
}

// describeListenersOutput represents the output of describe-listeners.
type describeListenersOutput struct {
	Listeners []elbv2Listener `json:"Listeners"`
}

// elbv2TagDescription represents the tags for a single ELBv2 resource.
type elbv2TagDescription struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        Tags   `json:"Tags"`
}

// describeTagsOutput represents the output of describe-tags.
type describeTagsOutput struct {
	TagDescriptions []elbv2TagDescription `json:"TagDescriptions"`
}

// getLoadBalancerTags fetches tags for the given load balancer ARNs and returns
// a map from ARN to tag map.
func (p *Provider) getLoadBalancerTags(
	ctx context.Context, l *logger.Logger, region string, arns []string,
) (map[string]map[string]string, error) {
	if len(arns) == 0 {
		return nil, nil
	}

	result := make(map[string]map[string]string)

	// AWS DescribeTags API has a limit of 20 resources per call.
	const maxResourcesPerCall = 20
	for i := 0; i < len(arns); i += maxResourcesPerCall {
		end := i + maxResourcesPerCall
		if end > len(arns) {
			end = len(arns)
		}
		batch := arns[i:end]

		args := []string{
			"elbv2", "describe-tags",
			"--resource-arns",
		}
		args = append(args, batch...)
		args = append(args, "--region", region)

		var output describeTagsOutput
		if err := p.runJSONCommandWithContext(ctx, l, args, &output); err != nil {
			return nil, err
		}

		for _, td := range output.TagDescriptions {
			result[td.ResourceArn] = td.Tags.MakeMap()
		}
	}

	return result, nil
}

// CreateLoadBalancer creates a Network Load Balancer (NLB) for the given cluster and port.
// The NLB is created with a target group containing all the cluster's EC2 instances.
func (p *Provider) CreateLoadBalancer(l *logger.Logger, vms vm.List, port int) error {
	if len(vms) == 0 {
		return errors.New("no VMs provided for load balancer creation")
	}

	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return err
	}

	// Group VMs by region - AWS NLBs are regional
	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}

	// For simplicity, create an NLB in each region where VMs exist.
	// Use ctxgroup for cancellation support.
	ctx := context.Background()
	g := ctxgroup.WithContext(ctx)
	for region, regionVMs := range byRegion {
		g.GoCtx(func(ctx context.Context) error {
			return p.createRegionalLoadBalancer(ctx, l, clusterName, region, regionVMs, port)
		})
	}

	return g.Wait()
}

// createRegionalLoadBalancer creates an NLB in a specific region.
// On failure, any partially created resources are left behind; the caller
// (roachprod.CreateLoadBalancer) is expected to call DeleteLoadBalancer
// to clean up.
func (p *Provider) createRegionalLoadBalancer(
	ctx context.Context, l *logger.Logger, clusterName, region string, vms vm.List, port int,
) error {
	if len(vms) == 0 {
		return nil
	}

	vpcID := vms[0].VPC
	if vpcID == "" {
		return errors.New("VPC ID not found for VMs")
	}

	// Collect unique subnets from VMs
	subnetSet := make(map[string]struct{})
	for _, v := range vms {
		az, ok := p.Config.AZByName[v.Zone]
		if ok && az.SubnetID != "" {
			subnetSet[az.SubnetID] = struct{}{}
		}
	}
	if len(subnetSet) == 0 {
		return errors.Errorf("no subnets found for VMs in region %s", region)
	}
	subnets := maps.Keys(subnetSet)

	// Step 1: Create Target Group
	tgName := loadBalancerResourceName(clusterName, port, "tg")

	l.Printf("Creating target group %s in region %s", tgName, region)
	args := []string{
		"elbv2", "create-target-group",
		"--name", tgName,
		"--protocol", "TCP",
		"--port", strconv.Itoa(port),
		"--vpc-id", vpcID,
		"--target-type", "instance",
		"--health-check-protocol", "TCP",
		"--health-check-port", strconv.Itoa(port),
		"--health-check-interval-seconds", "30",
		"--healthy-threshold-count", "3",
		"--unhealthy-threshold-count", "3",
		"--region", region,
		"--tags",
		fmt.Sprintf("Key=%s,Value=%s", vm.TagCluster, clusterName),
		fmt.Sprintf("Key=%s,Value=true", vm.TagRoachprod),
		fmt.Sprintf("Key=Port,Value=%d", port),
	}

	var tgOutput struct {
		TargetGroups []elbv2TargetGroup `json:"TargetGroups"`
	}
	if err := p.runJSONCommandWithContext(ctx, l, args, &tgOutput); err != nil {
		return errors.Wrapf(err, "failed to create target group")
	}
	if len(tgOutput.TargetGroups) == 0 {
		return errors.New("no target group created")
	}
	tgArn := tgOutput.TargetGroups[0].TargetGroupArn

	// Step 2: Register targets with the target group.
	// For managed clusters, attach the target group to the ASG so new instances
	// are automatically registered. For non-managed clusters, manually register
	// each instance.
	if isManaged(vms) {
		asgName := autoScalingGroupName(clusterName, region)
		l.Printf("Attaching target group to ASG %s for automatic instance registration", asgName)
		args = []string{
			"autoscaling", "attach-load-balancer-target-groups",
			"--auto-scaling-group-name", asgName,
			"--target-group-arns", tgArn,
			"--region", region,
		}
		if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
			return errors.Wrapf(err, "failed to attach target group to ASG")
		}
	} else {
		// For non-managed clusters, manually register each instance
		l.Printf("Registering %d targets with target group", len(vms))
		targets := make([]string, 0, len(vms))
		for _, v := range vms {
			targets = append(targets, fmt.Sprintf("Id=%s", v.ProviderID))
		}

		args = []string{
			"elbv2", "register-targets",
			"--target-group-arn", tgArn,
			"--targets",
		}
		args = append(args, targets...)
		args = append(args, "--region", region)

		if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
			return errors.Wrapf(err, "failed to register targets")
		}
	}

	// Step 3: Create Network Load Balancer
	nlbName := loadBalancerResourceName(clusterName, port, "nlb")

	l.Printf("Creating Network Load Balancer %s in region %s", nlbName, region)
	args = []string{
		"elbv2", "create-load-balancer",
		"--name", nlbName,
		"--type", "network",
		"--scheme", "internet-facing",
		"--subnets",
	}
	args = append(args, subnets...)
	args = append(args,
		"--tags",
		fmt.Sprintf("Key=%s,Value=%s", vm.TagCluster, clusterName),
		fmt.Sprintf("Key=%s,Value=true", vm.TagRoachprod),
		fmt.Sprintf("Key=Port,Value=%d", port),
		"--region", region,
	)

	var nlbOutput struct {
		LoadBalancers []elbv2LoadBalancer `json:"LoadBalancers"`
	}
	if err := p.runJSONCommandWithContext(ctx, l, args, &nlbOutput); err != nil {
		return errors.Wrapf(err, "failed to create load balancer")
	}
	if len(nlbOutput.LoadBalancers) == 0 {
		return errors.New("no load balancer created")
	}
	nlbArn := nlbOutput.LoadBalancers[0].LoadBalancerArn

	// Wait for NLB to be active
	l.Printf("Waiting for load balancer to become active...")
	args = []string{
		"elbv2", "wait", "load-balancer-available",
		"--load-balancer-arns", nlbArn,
		"--region", region,
	}
	if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
		l.Printf("Warning: load balancer may not be fully active yet: %v", err)
	}

	// Step 4: Create Listener
	l.Printf("Creating listener on port %d", port)
	args = []string{
		"elbv2", "create-listener",
		"--load-balancer-arn", nlbArn,
		"--protocol", "TCP",
		"--port", strconv.Itoa(port),
		"--default-actions", fmt.Sprintf("Type=forward,TargetGroupArn=%s", tgArn),
		"--region", region,
	}

	if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
		return errors.Wrapf(err, "failed to create listener")
	}

	l.Printf("Load balancer created successfully: %s", nlbOutput.LoadBalancers[0].DNSName)
	return nil
}

// DeleteLoadBalancer deletes all NLBs and associated resources for the given cluster.
func (p *Provider) DeleteLoadBalancer(l *logger.Logger, vms vm.List, _ int) error {
	if len(vms) == 0 {
		return nil
	}

	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return err
	}

	byRegion, err := regionMap(vms)
	if err != nil {
		return err
	}

	ctx := context.Background()
	g := ctxgroup.WithContext(ctx)
	for region := range byRegion {
		g.GoCtx(func(ctx context.Context) error {
			return p.deleteLoadBalancerResources(ctx, l, clusterName, region)
		})
	}

	return g.Wait()
}

// deleteLoadBalancerResources deletes all LB resources for a cluster in a region.
// This function is lenient: it attempts to delete all resources and continues
// even if some deletions fail, logging warnings for individual failures.
// Target groups are always attempted even if load balancer operations fail.
func (p *Provider) deleteLoadBalancerResources(
	ctx context.Context, l *logger.Logger, clusterName, region string,
) error {
	// Find load balancers belonging to this cluster
	lbs, _, err := p.listClusterLoadBalancers(ctx, l, clusterName, region)
	if err != nil {
		// Log the error but continue to try deleting target groups
		l.Printf("Warning: failed to list load balancers in region %s: %v", region, err)
	}

	// Delete each load balancer
	for _, lb := range lbs {
		// Delete listeners first
		listenerArgs := []string{
			"elbv2", "describe-listeners",
			"--load-balancer-arn", lb.LoadBalancerArn,
			"--region", region,
		}
		var listenersOutput describeListenersOutput
		if err := p.runJSONCommandWithContext(ctx, l, listenerArgs, &listenersOutput); err == nil {
			for _, listener := range listenersOutput.Listeners {
				l.Printf("Deleting listener %s", listener.ListenerArn)
				deleteArgs := []string{
					"elbv2", "delete-listener",
					"--listener-arn", listener.ListenerArn,
					"--region", region,
				}
				if _, err := p.runCommand(l, deleteArgs); err != nil {
					l.Printf("Warning: failed to delete listener: %v", err)
				}
			}
		}

		// Delete the load balancer
		l.Printf("Deleting load balancer %s", lb.LoadBalancerName)
		deleteArgs := []string{
			"elbv2", "delete-load-balancer",
			"--load-balancer-arn", lb.LoadBalancerArn,
			"--region", region,
		}
		if _, err := p.runCommand(l, deleteArgs); err != nil {
			l.Printf("Warning: failed to delete load balancer: %v", err)
		}

		// Wait for deletion before deleting target group
		l.Printf("Waiting for load balancer deletion...")
		waitArgs := []string{
			"elbv2", "wait", "load-balancers-deleted",
			"--load-balancer-arns", lb.LoadBalancerArn,
			"--region", region,
		}
		if _, err := p.runCommand(l, waitArgs); err != nil {
			l.Printf("Warning: wait for load balancer deletion may have timed out: %v", err)
		}
	}

	// Delete all target groups for this cluster
	if err := p.deleteClusterTargetGroups(ctx, l, clusterName, region); err != nil {
		l.Printf("Warning: failed to delete target groups: %v", err)
	}

	return nil
}

// deleteClusterTargetGroups deletes all target groups for a cluster in a region.
// For managed clusters, it first detaches target groups from the ASG before deleting.
func (p *Provider) deleteClusterTargetGroups(
	ctx context.Context, l *logger.Logger, clusterName, region string,
) error {
	tgArgs := []string{
		"elbv2", "describe-target-groups",
		"--region", region,
	}

	var tgOutput describeTargetGroupsOutput
	if err := p.runJSONCommandWithContext(ctx, l, tgArgs, &tgOutput); err != nil {
		return errors.Wrap(err, "could not list target groups")
	}

	if len(tgOutput.TargetGroups) == 0 {
		return nil
	}

	// Collect target group ARNs to fetch tags
	tgArns := make([]string, len(tgOutput.TargetGroups))
	for i, tg := range tgOutput.TargetGroups {
		tgArns[i] = tg.TargetGroupArn
	}

	tgTagsByArn, err := p.getLoadBalancerTags(ctx, l, region, tgArns)
	if err != nil {
		return errors.Wrap(err, "could not get target group tags")
	}

	// Collect target group ARNs belonging to this cluster for potential ASG detachment
	var clusterTgArns []string
	for tgArn, tgTags := range tgTagsByArn {
		// Match by cluster name and roachprod tag
		if tgTags[vm.TagCluster] != clusterName || tgTags[vm.TagRoachprod] != "true" {
			continue
		}
		clusterTgArns = append(clusterTgArns, tgArn)
	}

	// For managed clusters, check which target groups are actually attached to the ASG
	// before attempting to detach them. This avoids spurious errors when no load balancer
	// was created.
	if len(clusterTgArns) > 0 {
		asgName := autoScalingGroupName(clusterName, region)

		// Query the ASG to get its attached target group ARNs
		attachedTgArns := make(map[string]struct{})
		if asg, err := p.describeASG(ctx, l, asgName, region); err == nil && asg != nil {
			for _, arn := range asg.TargetGroupARNs {
				attachedTgArns[arn] = struct{}{}
			}
		}

		// Only detach target groups that are actually attached to the ASG
		var tgArnsToDetach []string
		for _, tgArn := range clusterTgArns {
			if _, attached := attachedTgArns[tgArn]; attached {
				tgArnsToDetach = append(tgArnsToDetach, tgArn)
			}
		}

		if len(tgArnsToDetach) > 0 {
			l.Printf("Detaching %d target groups from ASG %s", len(tgArnsToDetach), asgName)
			detachArgs := []string{
				"autoscaling", "detach-load-balancer-target-groups",
				"--auto-scaling-group-name", asgName,
				"--target-group-arns",
			}
			detachArgs = append(detachArgs, tgArnsToDetach...)
			detachArgs = append(detachArgs, "--region", region)

			if _, err := p.runCommandWithContext(ctx, l, detachArgs); err != nil {
				// Log warning but continue - ASG may have been deleted already
				l.Printf("Warning: failed to detach target groups from ASG: %v", err)
			}
		}
	}

	// Now delete the target groups
	for _, tgArn := range clusterTgArns {
		if err := p.deleteTargetGroup(l, tgArn, region); err != nil {
			l.Printf("Warning: failed to delete target group: %v", err)
		}
	}

	return nil
}

// deleteTargetGroup deletes a target group by ARN.
func (p *Provider) deleteTargetGroup(l *logger.Logger, tgArn, region string) error {
	l.Printf("Deleting target group %s", tgArn)
	args := []string{
		"elbv2", "delete-target-group",
		"--target-group-arn", tgArn,
		"--region", region,
	}
	_, err := p.runCommand(l, args)
	return err
}

// ListLoadBalancers returns the list of load balancer addresses for the given VMs.
func (p *Provider) ListLoadBalancers(l *logger.Logger, vms vm.List) ([]vm.ServiceAddress, error) {
	if len(vms) == 0 {
		return nil, nil
	}

	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return nil, err
	}

	byRegion, err := regionMap(vms)
	if err != nil {
		return nil, err
	}

	var mu syncutil.Mutex
	addresses := make([]vm.ServiceAddress, 0)

	ctx := context.Background()
	g := ctxgroup.WithContext(ctx)
	for region := range byRegion {
		g.GoCtx(func(ctx context.Context) error {
			// Find load balancers belonging to this cluster
			lbs, tagsByArn, err := p.listClusterLoadBalancers(ctx, l, clusterName, region)
			if err != nil {
				return err
			}

			for _, lb := range lbs {
				tags := tagsByArn[lb.LoadBalancerArn]

				// Get port from the Port tag
				port := 0
				if portStr, ok := tags["Port"]; ok {
					port, _ = strconv.Atoi(portStr)
				}

				mu.Lock()
				addresses = append(addresses, vm.ServiceAddress{
					IP:   lb.DNSName,
					Port: port,
				})
				mu.Unlock()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return addresses, nil
}

// listClusterLoadBalancers lists all load balancers belonging to the given cluster in a region.
// It returns the load balancers and their tags (keyed by ARN).
func (p *Provider) listClusterLoadBalancers(
	ctx context.Context, l *logger.Logger, clusterName, region string,
) ([]elbv2LoadBalancer, map[string]map[string]string, error) {
	args := []string{
		"elbv2", "describe-load-balancers",
		"--region", region,
	}

	var output describeLoadBalancersOutput
	if err := p.runJSONCommandWithContext(ctx, l, args, &output); err != nil {
		return nil, nil, err
	}

	if len(output.LoadBalancers) == 0 {
		return nil, nil, nil
	}

	// Collect all LB ARNs to fetch their tags
	arns := make([]string, len(output.LoadBalancers))
	arnToLB := make(map[string]elbv2LoadBalancer)
	for i, lb := range output.LoadBalancers {
		arns[i] = lb.LoadBalancerArn
		arnToLB[lb.LoadBalancerArn] = lb
	}

	// Fetch tags for all load balancers
	tagsByArn, err := p.getLoadBalancerTags(ctx, l, region, arns)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get load balancer tags")
	}

	// Filter to only include load balancers belonging to this cluster
	var clusterLBs []elbv2LoadBalancer
	clusterTagsByArn := make(map[string]map[string]string)
	for arn, tags := range tagsByArn {
		if tags[vm.TagCluster] == clusterName && tags[vm.TagRoachprod] == "true" {
			clusterLBs = append(clusterLBs, arnToLB[arn])
			clusterTagsByArn[arn] = tags
		}
	}

	return clusterLBs, clusterTagsByArn, nil
}

// instanceConfig holds the configuration needed to create EC2 instances,
// either directly via run-instances or via a launch template.
type instanceConfig struct {
	imageID       string
	machineType   string
	keyName       string
	securityGroup string
	iamProfile    string
	ebsVolumes    ebsVolumeList
	cpuOptions    string
}

// getInstanceConfig builds the common instance configuration from the given options.
// This is used by both runInstance (for unmanaged clusters) and createLaunchTemplate
// (for managed clusters).
func (p *Provider) getInstanceConfig(
	l *logger.Logger, zone string, machineType string, opts vm.CreateOpts, providerOpts *ProviderOpts,
) (*instanceConfig, *availabilityZone, error) {
	az, ok := p.Config.AZByName[zone]
	if !ok {
		return nil, nil, fmt.Errorf("no region in %v corresponds to availability zone %v",
			p.Config.regionNames(), zone)
	}

	keyName, err := p.sshKeyName(l)
	if err != nil {
		return nil, nil, err
	}

	// Determine the AMI to use based on machine type and architecture
	withFlagOverride := func(cfg string, fl *string) string {
		if *fl == "" {
			return cfg
		}
		return *fl
	}
	imageID := withFlagOverride(az.Region.AMI_X86_64, &providerOpts.ImageAMI)
	useArmAMI := strings.Index(machineType, "6g.") == 1 || strings.Index(machineType, "6gd.") == 1 ||
		strings.Index(machineType, "7g.") == 1 || strings.Index(machineType, "7gd.") == 1 ||
		strings.Index(machineType, "8g.") == 1 || strings.Index(machineType, "8gd.") == 1
	if useArmAMI && (opts.Arch != "" && opts.Arch != string(vm.ArchARM64)) {
		return nil, nil, errors.Errorf("machine type %s is arm64, but requested arch is %s", machineType, opts.Arch)
	}
	if useArmAMI {
		imageID = withFlagOverride(az.Region.AMI_ARM64, &providerOpts.ImageAMI)
	}
	if opts.Arch == string(vm.ArchFIPS) {
		imageID = withFlagOverride(az.Region.AMI_FIPS, &providerOpts.ImageAMI)
	}

	ebsVolumes := assignEBSVolumes(&opts, providerOpts)

	return &instanceConfig{
		imageID:       imageID,
		machineType:   machineType,
		keyName:       keyName,
		securityGroup: az.Region.SecurityGroup,
		iamProfile:    providerOpts.IAMProfile,
		ebsVolumes:    ebsVolumes,
		cpuOptions:    providerOpts.CPUOptions,
	}, az, nil
}

// launchTemplateName returns the name of the launch template for a cluster.
func launchTemplateName(clusterName string) string {
	return fmt.Sprintf("%s-lt", clusterName)
}

// autoScalingGroupName returns the name of the auto scaling group for a cluster in a region.
// AWS ASGs are regional (can span multiple AZs within a region).
func autoScalingGroupName(clusterName, region string) string {
	return fmt.Sprintf("%s-%s-asg", clusterName, region)
}

// isManaged returns true if the VMs belong to a managed cluster (created with --aws-managed).
// A managed cluster uses launch templates and auto scaling groups.
func isManaged(vms vm.List) bool {
	if len(vms) == 0 {
		return false
	}
	return vms[0].Labels[vm.TagManaged] == "true"
}

// createLaunchTemplateOutput represents the output of create-launch-template.
type createLaunchTemplateOutput struct {
	LaunchTemplate struct {
		LaunchTemplateID   string `json:"LaunchTemplateId"`
		LaunchTemplateName string `json:"LaunchTemplateName"`
	} `json:"LaunchTemplate"`
}

// createLaunchTemplate creates an EC2 launch template for the cluster.
// The launch template captures all instance configuration (AMI, instance type,
// security groups, volumes, etc.) so that the ASG can launch identical instances.
func (p *Provider) createLaunchTemplate(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	region string,
	cfg *instanceConfig,
	az *availabilityZone,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
) (string, error) {
	ltName := launchTemplateName(clusterName)

	// Create startup script for user-data
	extraMountOpts := ""
	if opts.SSDOpts.UseLocalSSD {
		if opts.SSDOpts.NoExt4Barrier && opts.SSDOpts.FileSystem == vm.Ext4 {
			extraMountOpts = "nobarrier"
		}
	}

	// For launch templates, we use a generic name since instances will be named by ASG
	userData, err := generateStartupScript(
		clusterName,
		extraMountOpts,
		opts.SSDOpts.FileSystem,
		providerOpts.UseMultipleDisks,
		opts.Arch == string(vm.ArchFIPS),
		providerOpts.RemoteUserName,
		providerOpts.BootDiskOnly,
	)
	if err != nil {
		return "", errors.Wrapf(err, "could not generate AWS startup script")
	}

	// Build launch template data as JSON
	// We need to use a JSON file for complex launch template configurations
	ltData := map[string]interface{}{
		"ImageId":      cfg.imageID,
		"InstanceType": cfg.machineType,
		"KeyName":      cfg.keyName,
		"SecurityGroupIds": []string{
			cfg.securityGroup,
		},
		"UserData": userData,
		"TagSpecifications": []map[string]interface{}{
			{
				"ResourceType": "instance",
				"Tags": []map[string]string{
					{"Key": vm.TagCluster, "Value": clusterName},
					{"Key": vm.TagRoachprod, "Value": "true"},
					{"Key": vm.TagManaged, "Value": "true"},
					{"Key": "Cluster", "Value": clusterName}, // Legacy tag for cost analysis
				},
			},
			{
				"ResourceType": "volume",
				"Tags": []map[string]string{
					{"Key": vm.TagCluster, "Value": clusterName},
					{"Key": vm.TagRoachprod, "Value": "true"},
				},
			},
		},
		"BlockDeviceMappings": cfg.ebsVolumes,
	}

	if cfg.iamProfile != "" {
		ltData["IamInstanceProfile"] = map[string]string{
			"Name": cfg.iamProfile,
		}
	}

	if cfg.cpuOptions != "" {
		// Parse cpuOptions string (format: "CoreCount=X,ThreadsPerCore=Y")
		ltData["CpuOptions"] = cfg.cpuOptions
	}

	// Write launch template data to a temp file
	ltDataJSON, err := json.Marshal(ltData)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal launch template data")
	}

	ltDataFile, err := os.CreateTemp("", "aws-launch-template-data")
	if err != nil {
		return "", errors.Wrap(err, "failed to create temp file for launch template data")
	}
	defer func() {
		ltDataFile.Close()
		_ = os.Remove(ltDataFile.Name())
	}()

	if _, err := ltDataFile.Write(ltDataJSON); err != nil {
		return "", errors.Wrap(err, "failed to write launch template data")
	}
	ltDataFile.Close()

	l.Printf("Creating launch template %s in region %s", ltName, region)
	args := []string{
		"ec2", "create-launch-template",
		"--launch-template-name", ltName,
		"--launch-template-data", "file://" + ltDataFile.Name(),
		"--tag-specifications",
		fmt.Sprintf("ResourceType=launch-template,Tags=[{Key=%s,Value=%s},{Key=%s,Value=true},{Key=%s,Value=true}]",
			vm.TagCluster, clusterName, vm.TagRoachprod, vm.TagManaged),
		"--region", region,
	}

	var output createLaunchTemplateOutput
	if err := p.runJSONCommandWithContext(ctx, l, args, &output); err != nil {
		return "", errors.Wrapf(err, "failed to create launch template")
	}

	l.Printf("Created launch template: %s (ID: %s)", ltName, output.LaunchTemplate.LaunchTemplateID)
	return output.LaunchTemplate.LaunchTemplateID, nil
}

// createAutoScalingGroup creates an Auto Scaling Group for the cluster in a region.
// The ASG manages the desired number of instances using the launch template.
// maxSize specifies the maximum number of instances the ASG can hold.
func (p *Provider) createAutoScalingGroup(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	region string,
	launchTemplateID string,
	zones []string,
	desiredCapacity int,
	maxSize int,
	opts vm.CreateOpts,
) error {
	asgName := autoScalingGroupName(clusterName, region)

	// Collect subnet IDs for the zones
	var subnetIDs []string
	for _, zone := range zones {
		az, ok := p.Config.AZByName[zone]
		if ok && az.SubnetID != "" {
			subnetIDs = append(subnetIDs, az.SubnetID)
		}
	}

	if len(subnetIDs) == 0 {
		return errors.Errorf("no subnets found for zones %v", zones)
	}

	l.Printf("Creating Auto Scaling Group %s in region %s with %d instances", asgName, region, desiredCapacity)

	// Build tags for the ASG
	tags := []string{
		fmt.Sprintf("Key=%s,Value=%s,PropagateAtLaunch=true", vm.TagCluster, clusterName),
		fmt.Sprintf("Key=%s,Value=true,PropagateAtLaunch=true", vm.TagRoachprod),
		fmt.Sprintf("Key=%s,Value=true,PropagateAtLaunch=true", vm.TagManaged),
		fmt.Sprintf("Key=%s,Value=%s,PropagateAtLaunch=true", vm.TagLifetime, opts.Lifetime.String()),
		fmt.Sprintf("Key=Cluster,Value=%s,PropagateAtLaunch=true", clusterName), // Legacy tag
	}

	args := []string{
		"autoscaling", "create-auto-scaling-group",
		"--auto-scaling-group-name", asgName,
		"--launch-template", fmt.Sprintf("LaunchTemplateId=%s,Version=$Latest", launchTemplateID),
		"--min-size", "0",
		"--max-size", strconv.Itoa(maxSize),
		"--desired-capacity", strconv.Itoa(desiredCapacity),
		"--vpc-zone-identifier", strings.Join(subnetIDs, ","),
		"--tags",
	}
	args = append(args, tags...)
	args = append(args, "--region", region)

	if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
		return errors.Wrapf(err, "failed to create auto scaling group")
	}

	l.Printf("Created Auto Scaling Group: %s", asgName)

	// When desiredCapacity is 0, we don't need to wait for instances.
	// The caller will create and attach instances separately to preserve
	// roachprod naming conventions (AWS ASG doesn't support custom instance names).
	if desiredCapacity == 0 {
		return nil
	}

	// Wait for instances to be running
	l.Printf("Waiting for %d instances to launch...", desiredCapacity)
	waitRetry := retry.Start(retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Second,
		MaxRetries:     60, // Wait up to ~10 minutes
	})

	for waitRetry.Next() {
		// Check ASG status
		asg, err := p.describeASG(ctx, l, asgName, region)
		if err != nil {
			l.Printf("Warning: failed to describe ASG: %v", err)
			continue
		}
		if asg == nil {
			continue
		}

		inServiceCount := 0
		for _, inst := range asg.Instances {
			if inst.LifecycleState == "InService" {
				inServiceCount++
			}
		}

		if inServiceCount >= desiredCapacity {
			l.Printf("All %d instances are InService", inServiceCount)
			return nil
		}

		l.Printf("Waiting for instances: %d/%d InService", inServiceCount, desiredCapacity)
	}

	return errors.Errorf("timed out waiting for ASG instances to become InService")
}

// deleteLaunchTemplate deletes the launch template for a cluster.
func (p *Provider) deleteLaunchTemplate(
	ctx context.Context, l *logger.Logger, clusterName, region string,
) error {
	ltName := launchTemplateName(clusterName)

	l.Printf("Deleting launch template %s in region %s", ltName, region)
	args := []string{
		"ec2", "delete-launch-template",
		"--launch-template-name", ltName,
		"--region", region,
	}

	if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
		// Ignore "not found" errors
		if !strings.Contains(err.Error(), "InvalidLaunchTemplateName.NotFoundException") {
			return errors.Wrapf(err, "failed to delete launch template")
		}
		l.Printf("Launch template %s not found, skipping", ltName)
	}

	return nil
}

// deleteAutoScalingGroup deletes the Auto Scaling Group for a cluster in a region.
func (p *Provider) deleteAutoScalingGroup(
	ctx context.Context, l *logger.Logger, clusterName, region string,
) error {
	asgName := autoScalingGroupName(clusterName, region)

	l.Printf("Deleting Auto Scaling Group %s in region %s (with force-delete)", asgName, region)
	args := []string{
		"autoscaling", "delete-auto-scaling-group",
		"--auto-scaling-group-name", asgName,
		"--force-delete", // Terminates all instances
		"--region", region,
	}

	if _, err := p.runCommandWithContext(ctx, l, args); err != nil {
		// Ignore "not found" errors
		if !strings.Contains(err.Error(), "AutoScalingGroup name not found") {
			return errors.Wrapf(err, "failed to delete auto scaling group")
		}
		l.Printf("Auto Scaling Group %s not found, skipping", asgName)
	}

	// Wait for ASG to be deleted
	l.Printf("Waiting for Auto Scaling Group deletion...")
	waitRetry := retry.Start(retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Second,
		MaxRetries:     30, // Wait up to ~5 minutes
	})

	for waitRetry.Next() {
		asg, err := p.describeASG(ctx, l, asgName, region)
		if err != nil {
			l.Printf("Warning: failed to describe ASG: %v", err)
			continue
		}

		if asg == nil {
			l.Printf("Auto Scaling Group %s deleted", asgName)
			return nil
		}
	}

	return errors.Errorf("timed out waiting for ASG deletion")
}

// String returns a human-readable string representation of the Provider.
func (p *Provider) String() string {
	return fmt.Sprintf("%s-%s", ProviderName, strings.Join(p.AccountIDs, "_"))
}
