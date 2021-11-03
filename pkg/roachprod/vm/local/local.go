// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package local

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// ProviderName is config.Local.
const ProviderName = config.Local

// Init initializes the Local provider and registers it into vm.Providers.
func Init(storage VMStorage) {
	vm.Providers[ProviderName] = &Provider{
		clusters: make(cloud.Clusters),
		storage:  storage,
	}
}

// IsLocal returns true if the given cluster name is a local cluster.
func IsLocal(clusterName string) bool {
	return clusterName == config.Local
}

// AddCluster adds information about a local cluster; used when loading the
// saved metadata for local clusters.
func AddCluster(cluster *cloud.Cluster) {
	p := vm.Providers[ProviderName].(*Provider)
	p.clusters[cluster.Name] = cluster
}

// VMStorage is the interface for saving metadata for local clusters.
type VMStorage interface {
	// SaveCluster saves the metadata for a local cluster. It is expected that
	// when the program runs again, this same metadata will be reported via
	// AddCluster.
	SaveCluster(cluster *cloud.Cluster) error
}

// A Provider is used to create stub VM objects.
type Provider struct {
	clusters cloud.Clusters

	storage VMStorage
}

// No-op implementation of ProviderFlags
type emptyFlags struct{}

// ConfigureCreateFlags is part of ProviderFlags.  This implementation is a no-op.
func (o *emptyFlags) ConfigureCreateFlags(flags *pflag.FlagSet) {
}

// ConfigureClusterFlags is part of ProviderFlags.  This implementation is a no-op.
func (o *emptyFlags) ConfigureClusterFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}

// ConfigureProviderOpts is part of ProviderFlags.  This implementation is a no-op.
func (o *emptyFlags) ConfigureProviderOpts(newOpts interface{}) {
}

// CleanSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) CleanSSH() error {
	return nil
}

// ConfigSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) ConfigSSH() error {
	return nil
}

// Create just creates fake host-info entries in the local filesystem
func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	now := timeutil.Now()
	c := &cloud.Cluster{
		Name:      opts.ClusterName,
		CreatedAt: now,
		Lifetime:  time.Hour,
		VMs:       make(vm.List, len(names)),
	}
	for i := range names {
		c.VMs[i] = vm.VM{
			Name:        "localhost",
			CreatedAt:   now,
			Lifetime:    time.Hour,
			PrivateIP:   "127.0.0.1",
			Provider:    ProviderName,
			ProviderID:  ProviderName,
			PublicIP:    "127.0.0.1",
			RemoteUser:  config.OSUser.Username,
			VPC:         ProviderName,
			MachineType: ProviderName,
			Zone:        ProviderName,
		}
	}
	if err := p.storage.SaveCluster(c); err != nil {
		return err
	}
	p.clusters[c.Name] = c
	return nil
}

// Delete is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) Delete(vms vm.List) error {
	return nil
}

// Reset is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) Reset(vms vm.List) error {
	return nil
}

// Extend is part of the vm.Provider interface.  This implementation returns an error.
func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	return errors.New("local clusters have unlimited lifetime")
}

// FindActiveAccount is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) FindActiveAccount() (string, error) {
	return "", nil
}

// Flags is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) Flags() vm.ProviderFlags {
	return &emptyFlags{}
}

// List reports all the local cluster "VM" instances.
func (p *Provider) List() (vm.List, error) {
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
