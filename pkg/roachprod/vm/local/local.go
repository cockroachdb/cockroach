// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// ProviderName is config.Local.
const ProviderName = config.Local

// VMDir returns the local directory for a given node in a cluster.
// Node indexes start at 1.
//
// If the cluster name is "local", node 1 directory is:
//
//	${HOME}/local/1
//
// If the cluster name is "local-foo", node 1 directory is:
//
//	${HOME}/local/foo-1
//
// WARNING: when we destroy a local cluster, we remove these directories so it's
// important that this function never returns things like "" or "/".
func VMDir(clusterName string, nodeIdx int) string {
	if nodeIdx < 1 {
		panic("invalid nodeIdx")
	}
	localDir := os.ExpandEnv("${HOME}/local")
	if clusterName == config.Local {
		return filepath.Join(localDir, fmt.Sprintf("%d", nodeIdx))
	}
	name := strings.TrimPrefix(clusterName, config.Local+"-")
	return filepath.Join(localDir, fmt.Sprintf("%s-%d", name, nodeIdx))
}

// Init initializes the Local provider and registers it into vm.Providers.
func Init(storage VMStorage) error {
	vm.Providers[ProviderName] = &Provider{
		clusters:    make(cloud.Clusters),
		storage:     storage,
		DNSProvider: NewDNSProvider(config.DNSDir, "local-zone"),
	}
	return nil
}

// AddCluster adds the metadata of a local cluster; used when loading the saved
// metadata for local clusters.
func AddCluster(cluster *cloud.Cluster) {
	p := vm.Providers[ProviderName].(*Provider)
	p.clusters[cluster.Name] = cluster
}

// DeleteCluster destroys a local cluster. It assumes that the cockroach
// processes are stopped.
func DeleteCluster(l *logger.Logger, name string) error {
	p := vm.Providers[ProviderName].(*Provider)
	c := p.clusters[name]
	if c == nil {
		return fmt.Errorf("local cluster %s does not exist", name)
	}
	l.Printf("Deleting local cluster %s\n", name)

	for i := range c.VMs {
		path := VMDir(c.Name, i+1)
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	if err := p.storage.DeleteCluster(l, name); err != nil {
		return err
	}

	delete(p.clusters, name)

	// Local clusters are expected to specifically use the local DNS provider
	// implementation, and should clean up any DNS records in the local file
	// system cache.
	return p.DeleteRecordsBySubdomain(context.Background(), c.Name)
}

// Clusters returns a list of all known local clusters.
func Clusters() []string {
	p := vm.Providers[ProviderName].(*Provider)
	return p.clusters.Names()
}

// VMStorage is the interface for saving metadata for local clusters.
type VMStorage interface {
	// SaveCluster saves the metadata for a local cluster. It is expected that
	// when the program runs again, this same metadata will be reported via
	// AddCluster.
	SaveCluster(l *logger.Logger, cluster *cloud.Cluster) error

	// DeleteCluster deletes the metadata for a local cluster.
	DeleteCluster(l *logger.Logger, name string) error
}

// A Provider is used to create stub VM objects.
type Provider struct {
	clusters cloud.Clusters
	storage  VMStorage
	vm.DNSProvider
}

func (p *Provider) ConfigureProviderFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}

func (p *Provider) SupportsSpotVMs() bool {
	return false
}

func (p *Provider) GetPreemptedSpotVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {
	return nil, nil
}

func (p *Provider) GetHostErrorVMs(
	l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	return nil, nil
}

func (p *Provider) GetVMSpecs(
	l *logger.Logger, vms vm.List,
) (map[string]map[string]interface{}, error) {
	return nil, nil
}

func (p *Provider) CreateVolumeSnapshot(
	l *logger.Logger, volume vm.Volume, vsco vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	return vm.VolumeSnapshot{}, nil
}

func (p *Provider) CreateVolume(*logger.Logger, vm.VolumeCreateOpts) (vm.Volume, error) {
	return vm.Volume{}, nil
}

func (p *Provider) DeleteVolume(l *logger.Logger, volume vm.Volume, vm *vm.VM) error {
	return nil
}

func (p *Provider) ListVolumes(l *logger.Logger, vm *vm.VM) ([]vm.Volume, error) {
	return vm.NonBootAttachedVolumes, nil
}

func (p *Provider) ListVolumeSnapshots(
	l *logger.Logger, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	return nil, nil
}

func (p *Provider) DeleteVolumeSnapshots(l *logger.Logger, snapshots ...vm.VolumeSnapshot) error {
	return nil
}

func (p *Provider) AttachVolume(*logger.Logger, vm.Volume, *vm.VM) (string, error) {
	return "", nil
}

// No-op implementation of vm.ProviderOpts
type providerOpts struct{}

// ConfigureCreateFlags is part of ProviderOpts.  This implementation is a no-op.
func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
}

// ConfigureClusterCleanupFlags is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) ConfigureClusterCleanupFlags(flags *pflag.FlagSet) {
}

// CleanSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) CleanSSH(l *logger.Logger) error {
	return nil
}

// ConfigSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) ConfigSSH(l *logger.Logger, zones []string) error {
	return nil
}

func (p *Provider) AddLabels(l *logger.Logger, vms vm.List, labels map[string]string) error {
	return nil
}

func (p *Provider) RemoveLabels(l *logger.Logger, vms vm.List, labels []string) error {
	return nil
}

func (p *Provider) CreateLoadBalancer(*logger.Logger, vm.List, int) error {
	return nil
}

func (p *Provider) DeleteLoadBalancer(*logger.Logger, vm.List, int) error {
	return nil
}

func (p *Provider) ListLoadBalancers(*logger.Logger, vm.List) ([]vm.ServiceAddress, error) {
	return nil, nil
}

func (p *Provider) createVM(clusterName string, index int, creationTime time.Time) (vm.VM, error) {
	cVM := vm.VM{
		Name:             "localhost",
		CreatedAt:        creationTime,
		Lifetime:         time.Hour,
		PrivateIP:        "127.0.0.1",
		Provider:         ProviderName,
		DNSProvider:      ProviderName,
		ProviderID:       ProviderName,
		PublicIP:         "127.0.0.1",
		PublicDNS:        "localhost",
		RemoteUser:       config.OSUser.Username,
		VPC:              ProviderName,
		MachineType:      ProviderName,
		Zone:             ProviderName,
		LocalClusterName: clusterName,
	}
	path := VMDir(clusterName, index+1)
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return vm.VM{}, err
	}
	return cVM, nil
}

// Create just creates fake host-info entries in the local filesystem
func (p *Provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, unusedProviderOpts vm.ProviderOpts,
) (vm.List, error) {
	now := timeutil.Now()
	c := &cloud.Cluster{
		Name:      opts.ClusterName,
		CreatedAt: now,
		Lifetime:  time.Hour,
		VMs:       make(vm.List, len(names)),
	}

	if !config.IsLocalClusterName(c.Name) {
		return nil, errors.Errorf("'%s' is not a valid local cluster name", c.Name)
	}

	for i := range names {
		var err error
		c.VMs[i], err = p.createVM(c.Name, i, now)
		if err != nil {
			return nil, err
		}
	}
	if err := p.storage.SaveCluster(l, c); err != nil {
		return nil, err
	}
	p.clusters[c.Name] = c
	return c.VMs, nil
}

func (p *Provider) Grow(
	l *logger.Logger, vms vm.List, clusterName string, names []string,
) (vm.List, error) {
	now := timeutil.Now()
	offset := p.clusters[clusterName].VMs.Len()
	newVms := make(vm.List, len(names))
	for i := range names {
		cVM, err := p.createVM(clusterName, i+offset, now)
		if err != nil {
			return nil, err
		}
		newVms[i] = cVM
	}
	p.clusters[clusterName].VMs = append(p.clusters[clusterName].VMs, newVms...)
	return newVms, p.storage.SaveCluster(l, p.clusters[clusterName])
}

func (p *Provider) Shrink(l *logger.Logger, vmsToDelete vm.List, clusterName string) error {
	keepCount := p.clusters[clusterName].VMs.Len() - len(vmsToDelete)
	for i := 0; i < p.clusters[clusterName].VMs.Len(); i++ {
		if i < keepCount {
			continue
		}
		path := VMDir(clusterName, i+1)
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	p.clusters[clusterName].VMs = p.clusters[clusterName].VMs[:keepCount]
	return p.storage.SaveCluster(l, p.clusters[clusterName])
}

// Delete is part of the vm.Provider interface.
func (p *Provider) Delete(l *logger.Logger, vms vm.List) error {
	panic("DeleteCluster should be used")
}

// Reset is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) Reset(l *logger.Logger, vms vm.List) error {
	return nil
}

// Extend is part of the vm.Provider interface.  This implementation returns an error.
func (p *Provider) Extend(l *logger.Logger, vms vm.List, lifetime time.Duration) error {
	return errors.New("local clusters have unlimited lifetime")
}

// FindActiveAccount is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) FindActiveAccount(l *logger.Logger) (string, error) {
	return "", nil
}

// CreateProviderOpts is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) CreateProviderOpts() vm.ProviderOpts {
	return &providerOpts{}
}

// List reports all the local cluster "VM" instances.
func (p *Provider) List(l *logger.Logger, opts vm.ListOptions) (vm.List, error) {
	var result vm.List
	for _, clusterName := range p.clusters.Names() {
		c := p.clusters[clusterName]
		result = append(result, c.VMs...)
	}
	return result, nil
}

// Name returns the name of the Provider, which will also surface in VM.Provider
func (p *Provider) Name() string {
	return ProviderName
}

// Active is part of the vm.Provider interface.
func (p *Provider) Active() bool {
	return true
}

// ProjectActive is part of the vm.Provider interface.
func (p *Provider) ProjectActive(project string) bool {
	return project == ""
}
