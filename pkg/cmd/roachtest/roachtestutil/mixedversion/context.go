// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

type (
	// ServiceContext contains the mixed-version context data for a
	// specific service deployed during the test. A service can be the
	// system tenant itself, or a secondary tenant created for the
	// purposes of the test.
	ServiceContext struct {
		// Descriptor is the descriptor associated with this context.
		Descriptor *ServiceDescriptor
		// Stage is the UpgradeStage when the step is scheduled to run.
		Stage UpgradeStage
		// FromVersion is the version the nodes are migrating from.
		FromVersion *clusterupgrade.Version
		// ToVersion is the version the nodes are migrating to.
		ToVersion *clusterupgrade.Version
		// Finalizing indicates whether the cluster version is in the
		// process of upgrading (i.e., all nodes in the cluster have been
		// upgraded to a certain version, and the migrations are being
		// executed).
		Finalizing bool

		// nodesByVersion maps released versions to which nodes are
		// currently running that version.
		nodesByVersion map[clusterupgrade.Version]*intsets.Fast
	}

	// Context wraps the context passed to predicate functions that
	// dictate when a mixed-version hook will run during a test. Both
	// the system tenant and a secondary tenant (when available) have
	// their own mixed-version context.
	Context struct {
		System *ServiceContext
		Tenant *ServiceContext
	}
)

// clone creates a copy of the caller service context.
func (sc *ServiceContext) clone() *ServiceContext {
	// This allows us to call clone on `nil` instances of this struct,
	// which is useful in tests that don't deploy a tenant (in which
	// case the tenant context is nil).
	if sc == nil {
		return nil
	}

	newDescriptor := &ServiceDescriptor{
		Name:  sc.Descriptor.Name,
		Nodes: append(option.NodeListOption{}, sc.Descriptor.Nodes...),
	}

	nodesByVersion := make(map[clusterupgrade.Version]*intsets.Fast)
	for v, nodes := range sc.nodesByVersion {
		newSet := nodes.Copy()
		nodesByVersion[v] = &newSet
	}

	fromVersion := sc.FromVersion.Version
	toVersion := sc.ToVersion.Version

	return &ServiceContext{
		Descriptor:     newDescriptor,
		Stage:          sc.Stage,
		FromVersion:    &clusterupgrade.Version{Version: fromVersion},
		ToVersion:      &clusterupgrade.Version{Version: toVersion},
		Finalizing:     sc.Finalizing,
		nodesByVersion: nodesByVersion,
	}
}

// nodesInVersion returns a list of all nodes running the version
// passed, if any.
func (sc *ServiceContext) nodesInVersion(v *clusterupgrade.Version) option.NodeListOption {
	set, ok := sc.nodesByVersion[*v]
	if !ok {
		return nil
	}

	return set.Ordered()
}

// startUpgrade is called when the test is starting the upgrade to the
// given version. This should be called once every node is already
// running that version and the cluster version has finished reaching
// the logical version corresponding to that release.
func (sc *ServiceContext) startUpgrade(nextRelease *clusterupgrade.Version) {
	sc.FromVersion = sc.ToVersion
	sc.ToVersion = nextRelease
}

// changeVersion is used to indicate that the given `node` is now
// running release version `v`.
func (sc *ServiceContext) changeVersion(node int, v *clusterupgrade.Version) error {
	currentVersion, err := sc.NodeVersion(node)
	if err != nil {
		return err
	}

	sc.nodesByVersion[*currentVersion].Remove(node)
	if _, exists := sc.nodesByVersion[*v]; !exists {
		sc.nodesByVersion[*v] = intSetP()
	}

	sc.nodesByVersion[*v].Add(node)
	return nil
}

func (sc *ServiceContext) IsSystem() bool {
	return sc.Descriptor.Name == install.SystemInterfaceName
}

// NodeVersion returns the release version the given `node` is
// currently running. Returns an error if the node is not valid (i.e.,
// the underlying service is not deployed on the node passed).
func (sc *ServiceContext) NodeVersion(node int) (*clusterupgrade.Version, error) {
	for version, nodes := range sc.nodesByVersion {
		if nodes.Contains(node) {
			return &version, nil
		}
	}

	return nil, fmt.Errorf(
		"invalid node %d, %s nodes: %v",
		node, sc.Descriptor.Name, sc.Descriptor.Nodes,
	)
}

// NodesInPreviousVersion returns a list of nodes running the version
// we are upgrading from.
func (sc *ServiceContext) NodesInPreviousVersion() option.NodeListOption {
	return sc.nodesInVersion(sc.FromVersion)
}

// NodesInNextVersion returns the list of nodes running the version we
// are upgrading to.
func (sc *ServiceContext) NodesInNextVersion() option.NodeListOption {
	return sc.nodesInVersion(sc.ToVersion)
}

// MixedBinary indicates if the cluster is currently in mixed-binary
// mode, i.e., not all nodes in the cluster are running the same
// released binary version.
func (sc *ServiceContext) MixedBinary() bool {
	return len(sc.NodesInPreviousVersion()) > 0 && len(sc.NodesInNextVersion()) > 0
}

// newContext creates a new mixed-version context for an upgrade
// `from` a given version `to` another version. `systemNodes` is the
// set of nodes where the system tenant is running. If this test sets
// up a virtual cluster (tenant) as well, callers should pass a
// ServiceDescriptor for that tenant.
func newContext(
	from, to *clusterupgrade.Version,
	stage UpgradeStage,
	systemNodes option.NodeListOption,
	tenant *ServiceDescriptor,
) *Context {
	makeContext := func(name string, nodes option.NodeListOption) *ServiceContext {
		sort.Ints(nodes)
		return &ServiceContext{
			Descriptor: &ServiceDescriptor{
				Name:  name,
				Nodes: nodes,
			},
			Stage:       stage,
			FromVersion: from,
			ToVersion:   to,
			nodesByVersion: map[clusterupgrade.Version]*intsets.Fast{
				*from: intSetP(nodes...),
			},
		}
	}

	var tenantContext *ServiceContext
	if tenant != nil {
		tenantContext = makeContext(tenant.Name, tenant.Nodes)
	}

	return &Context{
		System: makeContext(install.SystemInterfaceName, systemNodes),
		Tenant: tenantContext,
	}
}

// newInitialContext creates the context to be used when starting a
// new mixed-version test. Both `from` and `to` versions are set to
// the `initialRelease`, as they are changed by the planner as the
// upgrades plans are generated.
func newInitialContext(
	initialRelease *clusterupgrade.Version,
	systemNodes option.NodeListOption,
	tenant *ServiceDescriptor,
) *Context {
	return newContext(
		initialRelease, initialRelease, SystemSetupStage, systemNodes, tenant,
	)
}

func (c *Context) NodeVersion(node int) (*clusterupgrade.Version, error) {
	return c.DefaultService().NodeVersion(node)
}

func (c *Context) NodesInPreviousVersion() option.NodeListOption {
	return c.DefaultService().NodesInPreviousVersion()
}

func (c *Context) NodesInNextVersion() option.NodeListOption {
	return c.DefaultService().NodesInNextVersion()
}

func (c *Context) MixedBinary() bool {
	return c.DefaultService().MixedBinary()
}

func (c *Context) FromVersion() *clusterupgrade.Version {
	return c.DefaultService().FromVersion
}

func (c *Context) ToVersion() *clusterupgrade.Version {
	return c.DefaultService().ToVersion
}

func (c *Context) Nodes() option.NodeListOption {
	return c.DefaultService().Descriptor.Nodes
}

// Finalizing returns whether the cluster is known to be
// finalizing. Since virtual clusters rely on the system tenant for
// various operations, this function returns `true` if either the
// system or virtual cluster are in the process of finalizing the
// upgrade.
func (c *Context) Finalizing() bool {
	systemFinalizing := c.System.Finalizing

	var tenantFinalizing bool
	if c.Tenant != nil {
		tenantFinalizing = c.Tenant.Finalizing
	}

	return systemFinalizing || tenantFinalizing
}

// DefaultService returns the `ServiceContext` associated with the
// "default" service in the test. If a virtual cluster was created, it
// is the default service, otherwise we use the system service.
func (c *Context) DefaultService() *ServiceContext {
	if c.Tenant == nil {
		return c.System
	}

	return c.Tenant
}

// clone copies the caller Context and returns the copy.
func (c *Context) clone() Context {
	return Context{
		System: c.System.clone(),
		Tenant: c.Tenant.clone(),
	}
}

func intSetP(ns ...int) *intsets.Fast {
	set := intsets.MakeFast(ns...)
	return &set
}
