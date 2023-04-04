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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
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
		clusters: make(cloud.Clusters),
		storage:  storage,
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
	return nil
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

	storage VMStorage
}

func (p *Provider) SnapshotVolume(
	l *logger.Logger, volume vm.Volume, name, description string, labels map[string]string,
) (string, error) {
	return "", nil
}

func (p *Provider) CreateVolume(*logger.Logger, vm.VolumeCreateOpts) (vm.Volume, error) {
	return vm.Volume{}, nil
}

func (p *Provider) AttachVolumeToVM(*logger.Logger, vm.Volume, *vm.VM) (string, error) {
	return "", nil
}

// No-op implementation of vm.ProviderOpts
type providerOpts struct{}

// ConfigureCreateFlags is part of ProviderOpts.  This implementation is a no-op.
func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
}

// ConfigureClusterFlags is part of ProviderOpts.  This implementation is a no-op.
func (o *providerOpts) ConfigureClusterFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}

// CleanSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) CleanSSH(l *logger.Logger) error {
	return nil
}

// ConfigSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) ConfigSSH(l *logger.Logger, zones []string) error {
	return nil
}

// Create just creates fake host-info entries in the local filesystem
func (p *Provider) Create(
	l *logger.Logger, names []string, opts vm.CreateOpts, unusedProviderOpts vm.ProviderOpts,
) error {
	now := timeutil.Now()
	c := &cloud.Cluster{
		Name:      opts.ClusterName,
		CreatedAt: now,
		Lifetime:  time.Hour,
		VMs:       make(vm.List, len(names)),
	}

	if !config.IsLocalClusterName(c.Name) {
		return errors.Errorf("'%s' is not a valid local cluster name", c.Name)
	}

	// We will need to assign ports to the nodes, and they must not conflict with
	// any other local clusters.
	var portsTaken intsets.Fast
	for _, c := range p.clusters {
		for i := range c.VMs {
			portsTaken.Add(c.VMs[i].SQLPort)
			portsTaken.Add(c.VMs[i].AdminUIPort)
		}
	}
	sqlPort := config.DefaultSQLPort
	adminUIPort := config.DefaultAdminUIPort

	// getPort returns the first available port (starting at *port), and modifies
	// (*port) to be the following value.
	getPort := func(port *int) int {
		for portsTaken.Contains(*port) {
			(*port)++
		}
		result := *port
		portsTaken.Add(result)
		(*port)++
		return result
	}

	for i := range names {
		c.VMs[i] = vm.VM{
			Name:             "localhost",
			CreatedAt:        now,
			Lifetime:         time.Hour,
			PrivateIP:        "127.0.0.1",
			Provider:         ProviderName,
			ProviderID:       ProviderName,
			PublicIP:         "127.0.0.1",
			RemoteUser:       config.OSUser.Username,
			VPC:              ProviderName,
			MachineType:      ProviderName,
			Zone:             ProviderName,
			SQLPort:          getPort(&sqlPort),
			AdminUIPort:      getPort(&adminUIPort),
			LocalClusterName: c.Name,
		}
		path := VMDir(c.Name, i+1)
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return err
		}
	}
	if err := p.storage.SaveCluster(l, c); err != nil {
		return err
	}
	p.clusters[c.Name] = c
	return nil
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
