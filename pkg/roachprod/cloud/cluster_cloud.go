// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/promhelperclient"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// The following constants are headers that are used for printing the VM details.
	headerName           = "Name"
	headerDNS            = "DNS"
	headerPrivateIP      = "Private IP"
	headerPublicIP       = "Public IP"
	headerMachineType    = "Machine Type"
	headerCPUArch        = "CPU Arch"
	headerCPUFamily      = "CPU Family"
	headerProvisionModel = "Provision Model"

	// Provisional models that are used for printing VM details.
	spotProvisionModel     = "spot"
	onDemandProvisionModel = "ondemand"

	errNoVMsCreated = "No VMs were created by the providers"
)

// printDetailsColumnHeaders are the headers to be printed in the defined sequence.
var printDetailsColumnHeaders = []string{
	headerName, headerDNS, headerPrivateIP, headerPublicIP, headerMachineType, headerCPUArch, headerCPUFamily,
	headerProvisionModel,
}

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
	// CostPerHour is an estimate, in dollars, of how much this cluster costs to
	// run per hour. 0 if the cost estimate is unavailable.
	CostPerHour float64
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
func (c *Cluster) PrintDetails(logger *logger.Logger) error {
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
	// Align columns left and separate with at least two spaces.
	tw := tabwriter.NewWriter(logger.Stdout, 0, 8, 2, ' ', 0)
	logPrettifiedHeader(tw, printDetailsColumnHeaders)

	for _, vm := range c.VMs {
		provisionModel := onDemandProvisionModel
		if vm.Preemptible {
			provisionModel = spotProvisionModel
		}
		fmt.Fprintf(tw, "%s\n", prettifyRow(printDetailsColumnHeaders, map[string]string{
			headerName: vm.Name, headerDNS: vm.DNS, headerPrivateIP: vm.PrivateIP, headerPublicIP: vm.PublicIP,
			headerMachineType: vm.MachineType, headerCPUArch: string(vm.CPUArch), headerCPUFamily: vm.CPUFamily,
			headerProvisionModel: provisionModel,
		}))
	}
	return tw.Flush()
}

// logPrettifiedHeader writes a prettified row of headers to the tab writer.
func logPrettifiedHeader(tw *tabwriter.Writer, headers []string) {
	for _, header := range headers {
		fmt.Fprintf(tw, "%s\t", header)
	}
	fmt.Fprint(tw, "\n")
}

// prettifyRow returns a prettified row of values. the sequence of the header is maintained.
func prettifyRow(headers []string, rowMap map[string]string) string {
	row := ""
	for _, header := range headers {
		value := ""
		if v, ok := rowMap[header]; ok {
			value = v
		}
		row = fmt.Sprintf("%s%s\t", row, value)
	}
	return row
}

// IsLocal returns true if c is a local cluster.
func (c *Cluster) IsLocal() bool {
	return config.IsLocalClusterName(c.Name)
}

// IsEmptyCluster returns true if a cluster has no resources.
func (c *Cluster) IsEmptyCluster() bool {
	return c.VMs[0].EmptyCluster
}

// ListCloud returns information about all instances (across all available
// providers).
func ListCloud(l *logger.Logger, options vm.ListOptions) (*Cloud, error) {
	cloud := &Cloud{
		Clusters: make(Clusters),
	}

	providerNames := vm.AllProviderNames()
	if len(options.IncludeProviders) > 0 {
		providerNames = options.IncludeProviders
	}
	providerVMs := make([]vm.List, len(providerNames))
	var g errgroup.Group
	for i, providerName := range providerNames {
		provider := vm.Providers[providerName]
		g.Go(func() error {
			var err error
			providerVMs[i], err = provider.List(l, options)
			return errors.Wrapf(err, "provider %s", provider.Name())
		})
	}
	providerErr := g.Wait()
	if providerErr != nil {
		// We continue despite the error as we don't want to fail for all providers if only one
		// has an issue. The function that calls ListCloud may not even use the erring provider,
		// so log a warning and let the caller decide how to handle the error.
		l.Printf("WARNING: Error listing VMs, continuing but list may be incomplete. %s \n", providerErr.Error())
	}

	for _, vms := range providerVMs {
		for _, v := range vms {
			// Parse cluster/user from VM name, but only for non-local VMs
			userName, err := v.UserName()
			if err != nil {
				v.Errors = append(v.Errors, vm.ErrInvalidName)
			}
			clusterName, err := v.ClusterName()
			if err != nil {
				v.Errors = append(v.Errors, vm.ErrInvalidName)
			}

			// Anything with an error gets tossed into the BadInstances slice, and we'll correct
			// the problem later on. Ignore empty clusters since BadInstances will be destroyed on
			// the VM level. GC will destroy them instead.
			if len(v.Errors) > 0 && !v.EmptyCluster {
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
			c.CostPerHour += v.CostPerHour
		}
	}

	// Sort VMs for each cluster. We want to make sure we always have the same order.
	// Also check and warn if we find an empty cluster.
	for _, c := range cloud.Clusters {
		if len(c.VMs) == 0 {
			l.Printf("WARNING: found no VMs in cluster %s\n", c.Name)
		}

		// `roachprod.Start` expects nodes/vms to be in sorted order
		// see https://github.com/cockroachdb/cockroach/pull/133647 for more details
		sort.Sort(c.VMs)
	}

	return cloud, providerErr
}

type ClusterCreateOpts struct {
	// Nodes indicates how many nodes in the cluster should be created with the
	// respective CreateOpts and ProviderOpts.
	Nodes                 int
	CreateOpts            vm.CreateOpts
	ProviderOptsContainer vm.ProviderOptionsContainer
}

// Extracts o.CreateOpts.VMProviders from the provided opts.
func Providers(opts ...*ClusterCreateOpts) []string {
	providers := []string{}
	for _, o := range opts {
		providers = append(providers, o.CreateOpts.VMProviders...)
	}
	// Remove dupes, if any.
	slices.Sort(providers)
	return slices.Compact(providers)
}

// CreateCluster TODO(peter): document
// opts is a slice of all node VM specs to be provisioned for the cluster. Generally,
// non uniform VM specs are not supported for a CRDB cluster, but we often want to provision
// an additional "workload node". This node often times does not need the same CPU count as
// the rest of the cluster. i.e. it is overkill for a 3 node 32 CPU cluster to have a 32 CPU
// workload node, but a 50 node 8 CPU cluster might find a 8 CPU workload node inadequate.
func CreateCluster(l *logger.Logger, opts []*ClusterCreateOpts) (*Cluster, error) {

	c := &Cluster{
		Name:      opts[0].CreateOpts.ClusterName,
		CreatedAt: timeutil.Now(),
		Lifetime:  opts[0].CreateOpts.Lifetime,
	}

	// Keep track of the total number of nodes created, as we append all cluster names
	// with the node count.
	var nodesCreated int
	vmName := func(name string) string {
		nodesCreated++
		return vm.Name(name, nodesCreated)
	}
	for _, o := range opts {
		providerCount := len(o.CreateOpts.VMProviders)
		if providerCount == 0 {
			return nil, errors.New("no VMProviders configured")
		}

		// Allocate vm names over the configured providers
		// N.B., nodeIdx starts at 1 as nodes are one-based, i.e. n1, n2, ...
		vmLocations := map[string][]string{}
		for nodeIdx, p := 1, 0; nodeIdx <= o.Nodes; nodeIdx++ {
			pName := o.CreateOpts.VMProviders[p]
			vmLocations[pName] = append(vmLocations[pName], vmName(o.CreateOpts.ClusterName))
			p = (p + 1) % providerCount
		}

		var vmList vm.List
		var vmListLock syncutil.Mutex
		// Create VMs in parallel across all providers.
		// Each provider will return the list of VMs it created, and we append
		// them to the cached Cluster.
		if err := vm.ProvidersParallel(o.CreateOpts.VMProviders, func(p vm.Provider) error {
			providerVmList, err := p.Create(
				l, vmLocations[p.Name()], o.CreateOpts, o.ProviderOptsContainer[p.Name()],
			)
			if err != nil {
				return err
			}
			vmListLock.Lock()
			defer vmListLock.Unlock()
			vmList = append(vmList, providerVmList...)
			return nil
		}); err != nil {
			return nil, err
		}

		c.VMs = append(c.VMs, vmList...)
	}

	// Clusters can end up being empty (due to Azure or GCE dangling resources),
	// but can't be created with no VMs.
	if len(c.VMs) == 0 {
		return nil, errors.New(errNoVMsCreated)
	}

	// Set the cluster user to the user of the first VM.
	// This is the method also used in ListCloud() above.
	var err error
	c.User, err = c.VMs[0].UserName()
	if err != nil {
		return nil, err
	}

	// `roachprod.Start` expects nodes/vms to be in sorted order
	sort.Sort(c.VMs)

	return c, nil
}

// GrowCluster adds new nodes to an existing cluster.
func GrowCluster(l *logger.Logger, c *Cluster, numNodes int) error {
	names := make([]string, 0, numNodes)
	offset := len(c.VMs) + 1
	for i := offset; i < offset+numNodes; i++ {
		vmName := vm.Name(c.Name, i)
		names = append(names, vmName)
	}

	provider := c.VMs[0].Provider
	if !c.IsLocal() {
		providers := c.Clouds()
		// Only GCE supports expanding a cluster.
		if len(providers) != 1 || provider != gce.ProviderName {
			return errors.Errorf("cannot grow cluster %s, growing a cluster is currently only supported on %s",
				c.Name, gce.ProviderName)
		}
	}

	err := vm.ForProvider(provider, func(p vm.Provider) error {
		addedVms, err := p.Grow(l, c.VMs, c.Name, names)
		if err != nil {
			return err
		}

		// Update the list of VMs in the cluster.
		c.VMs = append(c.VMs, addedVms...)

		return nil
	})
	if err != nil {
		return err
	}

	// `roachprod.Start` expects nodes/vms to be in sorted order
	sort.Sort(c.VMs)

	return nil
}

// ShrinkCluster removes tail nodes from an existing cluster.
func ShrinkCluster(l *logger.Logger, c *Cluster, numNodes int) error {
	provider := c.VMs[0].Provider
	if !c.IsLocal() {
		providers := c.Clouds()
		// Only GCE supports shrinking a cluster.
		if len(providers) != 1 || provider != gce.ProviderName {
			return errors.Errorf("cannot shrink cluster %s, shrinking a cluster is currently only supported on %s",
				c.Name, gce.ProviderName)
		}
	}

	if numNodes >= len(c.VMs) {
		return errors.Errorf("cannot shrink cluster %s by %d nodes, only %d nodes in cluster",
			c.Name, numNodes, len(c.VMs))
	}
	// Always delete from the tail.
	vmsToDelete := c.VMs[len(c.VMs)-numNodes:]

	err := vm.ForProvider(provider, func(p vm.Provider) error {
		return p.Shrink(l, vmsToDelete, c.Name)
	})
	if err != nil {
		return err
	}

	// Update the list of VMs in the cluster.
	c.VMs = c.VMs[:len(c.VMs)-numNodes]
	return nil
}

func (c *Cluster) DeletePrometheusConfig(ctx context.Context, l *logger.Logger) error {

	cl := promhelperclient.NewPromClient()

	stopSpinner := ui.NewDefaultSpinner(l, "Destroying Prometheus configs").Start()
	defer stopSpinner()

	for _, node := range c.VMs {

		reachability := promhelperclient.ProviderReachability(
			node.Provider,
			promhelperclient.CloudEnvironment(node.Project),
		)
		if reachability == promhelperclient.None {
			continue
		}

		err := cl.DeleteClusterConfig(ctx, c.Name, false, false /* insecure */, l)
		if err != nil {

			if !promhelperclient.IsNotFoundError(err) {
				return errors.Wrapf(
					err,
					"failed to delete the cluster config with cluster as secure",
				)
			}

			// TODO(bhaskar): Obtain secure cluster information.
			// Cluster does not have the information on secure or not.
			// So, we retry as insecure  if delete fails with cluster as secure.
			if err = cl.DeleteClusterConfig(ctx, c.Name, false, true /* insecure */, l); err != nil {
				return errors.Wrapf(
					err,
					"failed to delete the cluster config with cluster as insecure and secure",
				)
			}

		}
		break

	}

	return nil
}

// DestroyCluster TODO(peter): document
func DestroyCluster(l *logger.Logger, c *Cluster) error {

	if err := c.DeletePrometheusConfig(context.Background(), l); err != nil {
		l.Printf("WARNING: failed to delete the prometheus config (already wiped?): %s", err)
	}

	// DNS entries are destroyed first to ensure that the GC job will not try
	// and clean-up entries prematurely.
	stopSpinner := ui.NewDefaultSpinner(l, "Destroying DNS entries").Start()
	dnsErr := vm.FanOutDNS(c.VMs, func(p vm.DNSProvider, vms vm.List) error {
		return p.DeleteRecordsBySubdomain(context.Background(), c.Name)
	})
	stopSpinner()

	stopSpinner = ui.NewDefaultSpinner(l, "Destroying VMs").Start()
	// Allow both DNS and VM operations to run before returning any errors.
	clusterErr := vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		// Enable a fast-path for providers that can destroy a cluster in one shot.
		if x, ok := p.(vm.DeleteCluster); ok {
			return x.DeleteCluster(l, c.Name)
		}
		return p.Delete(l, vms)
	})
	stopSpinner()
	return errors.CombineErrors(dnsErr, clusterErr)
}

// ExtendCluster TODO(peter): document
func ExtendCluster(l *logger.Logger, c *Cluster, extension time.Duration) error {
	// Round new lifetime to nearest second.
	newLifetime := (c.Lifetime + extension).Round(time.Second)
	err := vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.Extend(l, vms, newLifetime)
	})
	if err != nil {
		return err
	}
	c.Lifetime = newLifetime
	return nil
}
