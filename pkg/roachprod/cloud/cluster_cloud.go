// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"slices"
	"sort"
	"time"

	roachprodcentralized "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
	"github.com/cockroachdb/cockroach/pkg/roachprod/centralizedapi"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	errNoVMsCreated = "No VMs were created by the providers"
)

// Cloud contains information about all known clusters (across multiple cloud
// providers).
type Cloud struct {
	Clusters cloudcluster.Clusters `json:"clusters"`
	// BadInstances contains the VMs that have the Errors field populated. They
	// are not part of any Cluster.
	BadInstances vm.List `json:"bad_instances"`

	// providers supported for this Cloud instance
	centralizedProviders []vm.Provider
	localProviders       []vm.Provider
}

// BadInstanceErrors returns all bad VM instances, grouped by error message.
// Note: errors are grouped by their string representation to handle cases
// where errors have been serialized/deserialized and lost pointer identity.
func (c *Cloud) BadInstanceErrors() map[string]vm.List {
	ret := map[string]vm.List{}

	// Expand instances and errors
	for _, v := range c.BadInstances {
		for _, err := range v.Errors {
			msg := err.Error()
			ret[msg] = append(ret[msg], v)
		}
	}

	// Sort each List to make the output prettier
	for _, v := range ret {
		sort.Sort(v)
	}

	return ret
}

// ListCloud returns information about all instances (across all available
// providers).
func ListCloud(l *logger.Logger, options vm.ListOptions) (*Cloud, error) {

	// Build the list of providers to use with the static func.
	providers := []vm.Provider{}
	providerNames := vm.AllProviderNames()
	if len(options.IncludeProviders) > 0 {
		providerNames = options.IncludeProviders
	}
	for _, providerName := range providerNames {
		providers = append(providers, vm.Providers[providerName])
	}

	// Init a new Cloud struct with the computed providers.
	cloud := NewCloud(WithProviders(providers))

	err := cloud.ListCloud(context.Background(), l, options)
	if err != nil {
		return nil, err
	}

	return cloud, nil
}

func NewCloud(options ...Option) *Cloud {
	c := &Cloud{
		Clusters:     make(cloudcluster.Clusters),
		BadInstances: vm.List{},
	}
	for _, o := range options {
		o.apply(c)
	}
	return c
}

// Option is a functional option for configuring the Client.
type Option interface {
	apply(*Cloud)
}

// OptionFunc is a function that implements the Option interface.
type OptionFunc func(*Cloud)

func (o OptionFunc) apply(c *Cloud) { o(c) }

// WithProviders configures the Cloud with the given providers.
func WithProviders(providers []vm.Provider) OptionFunc {
	return func(c *Cloud) {
		c.addProviders(providers)
	}
}

// addProviders adds the given providers to the Cloud struct. Local providers
// are added to the localProviders field, while remote providers are added to
// the providers field.
func (c *Cloud) addProviders(providers []vm.Provider) {
	if c.centralizedProviders == nil {
		c.centralizedProviders = []vm.Provider{}
	}
	if c.localProviders == nil {
		c.localProviders = []vm.Provider{}
	}
	for _, p := range providers {
		if p.IsCentralizedProvider() {
			c.centralizedProviders = append(c.centralizedProviders, p)
		} else {
			c.localProviders = append(c.localProviders, p)
		}
	}
}

// ListCloud populates the Cloud struct with all clusters and bad instances
// found across all providers.
func (c *Cloud) ListCloud(ctx context.Context, l *logger.Logger, options vm.ListOptions) error {

	// Reset the Clusters and BadInstance fields.
	c.Clusters = make(cloudcluster.Clusters)
	c.BadInstances = vm.List{}

	providers := append(c.centralizedProviders, c.localProviders...)

	apiClient := centralizedapi.GetCentralizedAPIClient()
	if apiClient.IsEnabled() {
		// Use the centralized roachprod service to list clusters.
		response, err := apiClient.ListClusters(context.Background(), l, roachprodcentralized.ListClustersOptions{})
		if err != nil {
			l.Errorf("Error listing clusters using the centralized API: %s", err)
		} else {
			c.Clusters = response.Clusters
			c.BadInstances = response.BadInstances

			// Remote providers are already listed by the centralized service,
			// so only use local providers in the local listing.
			providers = c.localProviders
		}
	}

	// List all VMs across all providers in parallel.
	providerVMs := make(map[string]vm.List)
	g := ctxgroup.WithContext(ctx)

	// Limit concurrent provider List() calls to prevent blocking all OS threads.
	// Each provider may spawn blocking syscalls (gcloud, aws) and concurrency
	// can starve the scheduler. Limit to ensure at least 1 CPU remains available.
	if options.LimitConcurrency > 0 {
		g.SetLimit(options.LimitConcurrency)
	}

	vmListLock := syncutil.Mutex{}
	for _, provider := range providers {
		g.GoCtx(func(ctx context.Context) error {
			pVms, err := provider.List(ctx, l, options)
			if err != nil {
				return errors.Wrapf(err, "provider %s", provider.String())
			}

			// Lock the map to avoid concurrent writes.
			vmListLock.Lock()
			defer vmListLock.Unlock()
			providerVMs[provider.String()] = pVms
			return nil
		})
	}
	providerErr := g.Wait()
	if providerErr != nil {
		if options.BailOnProviderError {
			// On the roachprod-centralized path, this is executed as part
			// of an async task, and we want to fail fast if a provider errors out
			// or the context is cancelled or its deadline is exceeded.
			// This avoids partial/incomplete listings when DNS entries are updated
			// or when the results are stored in the repository.
			return providerErr
		} else {
			// We continue despite the error as we don't want to fail for all providers if only one
			// has an issue. The function that calls ListCloud may not even use the erring provider,
			// so log a warning and let the caller decide how to handle the error.
			l.Printf("WARNING: Error listing VMs, continuing but list may be incomplete. %s \n", providerErr.Error())
		}
	}

	for providerID, vms := range providerVMs {
		for _, v := range vms {
			// Parse cluster/user from VM name, but only for non-local VMs
			userName, err := v.UserName()
			if err != nil {
				v.Errors = append(v.Errors, vm.NewVMError(vm.ErrInvalidUserName))
			}
			clusterName, err := v.ClusterName()
			if err != nil {
				v.Errors = append(v.Errors, vm.NewVMError(vm.ErrInvalidClusterName))
			}

			// Anything with an error gets tossed into the BadInstances slice, and we'll correct
			// the problem later on. Ignore empty clusters since BadInstances will be destroyed on
			// the VM level. GC will destroy them instead.
			if len(v.Errors) > 0 && !v.EmptyCluster {
				c.BadInstances = append(c.BadInstances, v)
				continue
			}

			if _, ok := c.Clusters[clusterName]; !ok {
				c.Clusters[clusterName] = &cloudcluster.Cluster{
					Name:           clusterName,
					User:           userName,
					CreatedAt:      v.CreatedAt,
					Lifetime:       v.Lifetime,
					VMs:            nil,
					CloudProviders: []string{providerID},
				}
			} else {
				found := false
				for _, id := range c.Clusters[clusterName].CloudProviders {
					if id == providerID {
						found = true
						break
					}
				}
				if !found {
					c.Clusters[clusterName].CloudProviders = append(c.Clusters[clusterName].CloudProviders, providerID)
				}
			}

			// Bound the cluster creation time and overall lifetime to the earliest and/or shortest VM
			c := c.Clusters[clusterName]
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
	for _, c := range c.Clusters {
		if len(c.VMs) == 0 {
			l.Printf("WARNING: found no VMs in cluster %s\n", c.Name)
		}

		// `roachprod.Start` expects nodes/vms to be in sorted order
		// see https://github.com/cockroachdb/cockroach/pull/133647 for more details
		sort.Sort(c.VMs)
	}

	return nil
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
func CreateCluster(l *logger.Logger, opts []*ClusterCreateOpts) (*cloudcluster.Cluster, error) {
	c := &cloudcluster.Cluster{
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

	apiClient := centralizedapi.GetCentralizedAPIClient()
	if apiClient.IsEnabled() {
		// Use the centralized roachprod service to upsert the cluster registration
		// in its final state.
		// Upsert is used here because the cluster may already exist in the
		// centralized service if the cluster was picked up during a background sync.
		if err = apiClient.RegisterClusterUpsert(context.Background(), l, c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// GrowCluster adds new nodes to an existing cluster.
func GrowCluster(l *logger.Logger, c *cloudcluster.Cluster, numNodes int) error {
	names := make([]string, 0, numNodes)
	offset := len(c.VMs) + 1
	for i := offset; i < offset+numNodes; i++ {
		vmName := vm.Name(c.Name, i)
		names = append(names, vmName)
	}

	provider := c.VMs[0].Provider
	if !c.IsLocal() {
		providers := c.Clouds()
		// Only GCE and AWS support expanding a cluster.
		if len(providers) != 1 || (provider != gce.ProviderName && provider != aws.ProviderName) {
			return errors.Errorf("cannot grow cluster %s, growing a cluster is currently only supported on %s and %s",
				c.Name, gce.ProviderName, aws.ProviderName)
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

	apiClient := centralizedapi.GetCentralizedAPIClient()
	if apiClient.IsEnabled() {
		// Use the centralized roachprod service to create the cluster.
		if err := apiClient.RegisterClusterUpdate(context.Background(), l, c); err != nil {
			return err
		}
	}

	return nil
}

// ShrinkCluster removes tail nodes from an existing cluster.
func ShrinkCluster(l *logger.Logger, c *cloudcluster.Cluster, numNodes int) error {
	provider := c.VMs[0].Provider
	if !c.IsLocal() {
		providers := c.Clouds()
		// Only GCE and AWS support shrinking a cluster.
		if len(providers) != 1 || (provider != gce.ProviderName && provider != aws.ProviderName) {
			return errors.Errorf("cannot shrink cluster %s, shrinking a cluster is currently only supported on %s and %s",
				c.Name, gce.ProviderName, aws.ProviderName)
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

	apiClient := centralizedapi.GetCentralizedAPIClient()
	if apiClient.IsEnabled() {
		// Use the centralized roachprod service to create the cluster.
		if err := apiClient.RegisterClusterUpdate(context.Background(), l, c); err != nil {
			return err
		}
	}

	return nil
}

// DestroyCluster TODO(peter): document
func DestroyCluster(l *logger.Logger, c *cloudcluster.Cluster) error {

	if err := c.DeletePrometheusConfig(context.Background(), l); err != nil {
		l.Printf("WARNING: failed to delete the prometheus config (already wiped?): %s", err)
	}

	// DNS entries are destroyed first to ensure that the GC job will not try
	// and clean-up entries prematurely.
	stopSpinner := ui.NewDefaultSpinner(l, "Destroying DNS entries").Start()
	publicRecords := make([]string, 0, len(c.VMs))
	for _, v := range c.VMs {
		publicRecords = append(publicRecords, v.PublicDNS)
	}
	dnsErr := vm.FanOutDNS(c.VMs, func(p vm.DNSProvider, vms vm.List) error {
		var publicRecordsErr error
		if !centralizedapi.GetCentralizedAPIClient().IsEnabled() {
			publicRecordsErr = p.DeletePublicRecordsByName(context.Background(), publicRecords...)
		}
		srvRecordsErr := p.DeleteSRVRecordsBySubdomain(context.Background(), c.Name)
		return errors.CombineErrors(publicRecordsErr, srvRecordsErr)
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

	if clusterErr == nil {
		apiClient := centralizedapi.GetCentralizedAPIClient()
		if apiClient.IsEnabled() {
			if err := apiClient.RegisterClusterDelete(context.Background(), l, c.Name); err != nil {
				l.Printf("WARNING: failed to delete cluster %s from centralized service: %s", c.Name, err)
			}
		}
	}
	return errors.CombineErrors(dnsErr, clusterErr)
}

// ExtendCluster TODO(peter): document
func ExtendCluster(l *logger.Logger, c *cloudcluster.Cluster, extension time.Duration) error {
	// Round new lifetime to nearest second.
	newLifetime := (c.Lifetime + extension).Round(time.Second)
	err := vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.Extend(l, vms, newLifetime)
	})
	if err != nil {
		return err
	}
	c.Lifetime = newLifetime

	apiClient := centralizedapi.GetCentralizedAPIClient()
	if apiClient.IsEnabled() {
		// Use the centralized roachprod service to create the cluster.
		if err := apiClient.RegisterClusterUpdate(context.Background(), l, c); err != nil {
			return err
		}
	}
	return nil
}
