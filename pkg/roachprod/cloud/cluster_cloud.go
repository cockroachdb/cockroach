// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// Cloud contains information about all known clusters (across multiple cloud
// providers).
type Cloud struct {
	Clusters Clusters `json:"clusters"`
	// BadInstances contains the VMs that have the Errors field populated. They
	// are not part of any Cluster.
	BadInstances vm.List `json:"bad_instances"`
}

// BadInstanceErrors returns all bad VM instances, grouped by error.
func (c *Cloud) BadInstanceErrors() map[error]vm.List {
	ret := map[error]vm.List{}

	// Expand instances and errors
	for _, vm := range c.BadInstances {
		for _, err := range vm.Errors {
			ret[err] = append(ret[err], vm)
		}
	}

	// Sort each List to make the output prettier
	for _, v := range ret {
		sort.Sort(v)
	}

	return ret
}

// Clusters contains a set of clusters (potentially across multiple providers),
// keyed by the cluster name.
type Clusters map[string]*Cluster

// Names returns all cluster names, in alphabetical order.
func (c Clusters) Names() []string {
	result := make([]string, 0, len(c))
	for n := range c {
		result = append(result, n)
	}
	sort.Strings(result)
	return result
}

// FilterByName creates a new Clusters map that only contains the clusters with
// name matching the given regexp.
func (c Clusters) FilterByName(pattern *regexp.Regexp) Clusters {
	result := make(Clusters)
	for name, cluster := range c {
		if pattern.MatchString(name) {
			result[name] = cluster
		}
	}
	return result
}

// A Cluster is created by querying various vm.Provider instances.
//
// TODO(benesch): unify with syncedCluster.
type Cluster struct {
	Name string `json:"name"`
	User string `json:"user"`
	// This is the earliest creation and shortest lifetime across VMs.
	CreatedAt time.Time     `json:"created_at"`
	Lifetime  time.Duration `json:"lifetime"`
	VMs       vm.List       `json:"vms"`
}

// Clouds returns the names of all of the various cloud providers used
// by the VMs in the cluster.
func (c *Cluster) Clouds() []string {
	present := make(map[string]bool)
	for _, m := range c.VMs {
		p := m.Provider
		if m.Project != "" {
			p = fmt.Sprintf("%s:%s", m.Provider, m.Project)
		}
		present[p] = true
	}

	var ret []string
	for provider := range present {
		ret = append(ret, provider)
	}
	sort.Strings(ret)
	return ret
}

// ExpiresAt TODO(peter): document
func (c *Cluster) ExpiresAt() time.Time {
	return c.CreatedAt.Add(c.Lifetime)
}

// GCAt TODO(peter): document
func (c *Cluster) GCAt() time.Time {
	// NB: GC is performed every hour. We calculate the lifetime of the cluster
	// taking the GC time into account to accurately reflect when the cluster
	// will be destroyed.
	return c.ExpiresAt().Add(time.Hour - 1).Truncate(time.Hour)
}

// LifetimeRemaining TODO(peter): document
func (c *Cluster) LifetimeRemaining() time.Duration {
	return time.Until(c.GCAt())
}

func (c *Cluster) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s: %d", c.Name, len(c.VMs))
	if !c.IsLocal() {
		fmt.Fprintf(&buf, " (%s)", c.LifetimeRemaining().Round(time.Second))
	}
	return buf.String()
}

// PrintDetails TODO(peter): document
func (c *Cluster) PrintDetails(logger *logger.Logger) {
	logger.Printf("%s: %s ", c.Name, c.Clouds())
	if !c.IsLocal() {
		l := c.LifetimeRemaining().Round(time.Second)
		if l <= 0 {
			logger.Printf("expired %s ago", -l)
		} else {
			logger.Printf("%s remaining", l)
		}
	} else {
		logger.Printf("(no expiration)")
	}
	for _, vm := range c.VMs {
		logger.Printf("  %s\t%s\t%s\t%s", vm.Name, vm.DNS, vm.PrivateIP, vm.PublicIP)
	}
}

// IsLocal returns true if c is a local cluster.
func (c *Cluster) IsLocal() bool {
	return config.IsLocalClusterName(c.Name)
}

const vmNameFormat = "user-<clusterid>-<nodeid>"

// namesFromVM determines the user name and the cluster name from a VM.
func namesFromVM(v vm.VM) (userName string, clusterName string, _ error) {
	if v.IsLocal() {
		return config.Local, v.LocalClusterName, nil
	}
	name := v.Name
	parts := strings.Split(name, "-")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("expected VM name in the form %s, got %s", vmNameFormat, name)
	}
	return parts[0], strings.Join(parts[:len(parts)-1], "-"), nil
}

// ListCloud returns information about all instances (across all available
// providers).
func ListCloud(l *logger.Logger, options vm.ListOptions) (*Cloud, error) {
	cloud := &Cloud{
		Clusters: make(Clusters),
	}

	providerNames := vm.AllProviderNames()
	providerVMs := make([]vm.List, len(providerNames))
	var g errgroup.Group
	for i, providerName := range providerNames {
		// Capture loop variable.
		index := i
		provider := vm.Providers[providerName]
		g.Go(func() error {
			var err error
			providerVMs[index], err = provider.List(l, options)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, vms := range providerVMs {
		for _, v := range vms {
			// Parse cluster/user from VM name, but only for non-local VMs
			userName, clusterName, err := namesFromVM(v)
			if err != nil {
				v.Errors = append(v.Errors, vm.ErrInvalidName)
			}

			// Anything with an error gets tossed into the BadInstances slice, and we'll correct
			// the problem later on.
			if len(v.Errors) > 0 {
				cloud.BadInstances = append(cloud.BadInstances, v)
				continue
			}

			if _, ok := cloud.Clusters[clusterName]; !ok {
				cloud.Clusters[clusterName] = &Cluster{
					Name:      clusterName,
					User:      userName,
					CreatedAt: v.CreatedAt,
					Lifetime:  v.Lifetime,
					VMs:       nil,
				}
			}

			// Bound the cluster creation time and overall lifetime to the earliest and/or shortest VM
			c := cloud.Clusters[clusterName]
			c.VMs = append(c.VMs, v)
			if v.CreatedAt.Before(c.CreatedAt) {
				c.CreatedAt = v.CreatedAt
			}
			if v.Lifetime < c.Lifetime {
				c.Lifetime = v.Lifetime
			}
		}
	}

	// Sort VMs for each cluster. We want to make sure we always have the same order.
	for _, c := range cloud.Clusters {
		sort.Sort(c.VMs)
	}

	return cloud, nil
}

// CreateCluster TODO(peter): document
func CreateCluster(
	l *logger.Logger,
	nodes int,
	opts vm.CreateOpts,
	providerOptsContainer vm.ProviderOptionsContainer,
) error {
	providerCount := len(opts.VMProviders)
	if providerCount == 0 {
		return errors.New("no VMProviders configured")
	}

	// Allocate vm names over the configured providers
	vmLocations := map[string][]string{}
	for i, p := 1, 0; i <= nodes; i++ {
		pName := opts.VMProviders[p]
		vmName := vm.Name(opts.ClusterName, i)
		vmLocations[pName] = append(vmLocations[pName], vmName)

		p = (p + 1) % providerCount
	}

	return vm.ProvidersParallel(opts.VMProviders, func(p vm.Provider) error {
		return p.Create(l, vmLocations[p.Name()], opts, providerOptsContainer[p.Name()])
	})
}

// DestroyCluster TODO(peter): document
func DestroyCluster(l *logger.Logger, c *Cluster) error {
	return vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		// Enable a fast-path for providers that can destroy a cluster in one shot.
		if x, ok := p.(vm.DeleteCluster); ok {
			return x.DeleteCluster(l, c.Name)
		}
		return p.Delete(l, vms)
	})
}

// ExtendCluster TODO(peter): document
func ExtendCluster(l *logger.Logger, c *Cluster, extension time.Duration) error {
	// Round new lifetime to nearest second.
	newLifetime := (c.Lifetime + extension).Round(time.Second)
	return vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.Extend(l, vms, newLifetime)
	})
}
