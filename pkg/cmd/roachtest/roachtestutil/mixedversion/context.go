// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

type (
	// Context wraps the context passed to predicate functions that
	// dictate when a mixed-version hook will run during a test
	Context struct {
		// CockroachNodes is the set of cockroach nodes in the cluster
		// that are being part of the upgrade being performed.
		CockroachNodes option.NodeListOption
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
)

func newInitialContext(
	initialRelease *clusterupgrade.Version, crdbNodes option.NodeListOption,
) *Context {
	return &Context{
		CockroachNodes: crdbNodes,
		FromVersion:    initialRelease,
		ToVersion:      initialRelease,
		nodesByVersion: map[clusterupgrade.Version]*intsets.Fast{
			*initialRelease: intSetP(crdbNodes...),
		},
	}
}

// newLongRunningContext is the test context passed to long running
// tasks (background functions and the like). In these scenarios,
// `FromVersion` and `ToVersion` correspond to, respectively, the
// initial version the cluster is started at, and the final version
// once the test finishes. Background functions should *not* rely on
// context functions since the context is not dynamically updated as
// the test makes progress during the background function's execution.
func newLongRunningContext(
	from, to *clusterupgrade.Version, crdbNodes option.NodeListOption,
) *Context {
	return &Context{
		CockroachNodes: crdbNodes,
		FromVersion:    from,
		ToVersion:      to,
		nodesByVersion: map[clusterupgrade.Version]*intsets.Fast{
			*from: intSetP(crdbNodes...),
		},
	}
}

// clone copies the caller Context and returns the copy.
func (c *Context) clone() Context {
	nodesByVersion := make(map[clusterupgrade.Version]*intsets.Fast)
	for v, nodes := range c.nodesByVersion {
		newSet := nodes.Copy()
		nodesByVersion[v] = &newSet
	}

	fromVersion := c.FromVersion.Version
	toVersion := c.ToVersion.Version

	return Context{
		CockroachNodes: append(option.NodeListOption{}, c.CockroachNodes...),
		FromVersion:    &clusterupgrade.Version{Version: fromVersion},
		ToVersion:      &clusterupgrade.Version{Version: toVersion},
		Finalizing:     c.Finalizing,
		nodesByVersion: nodesByVersion,
	}
}

// startUpgrade is called when the test is starting the upgrade to the
// given version. This should be called once every node is already
// running that version and the cluster version has finished reaching
// the logical version corresponding to that release.
func (c *Context) startUpgrade(nextRelease *clusterupgrade.Version) {
	c.FromVersion = c.ToVersion
	c.ToVersion = nextRelease
}

// changeVersion is used to indicate that the given `node` is now
// running release version `v`.
func (c *Context) changeVersion(node int, v *clusterupgrade.Version) {
	currentVersion := c.NodeVersion(node)
	c.nodesByVersion[*currentVersion].Remove(node)
	if _, exists := c.nodesByVersion[*v]; !exists {
		c.nodesByVersion[*v] = intSetP()
	}

	c.nodesByVersion[*v].Add(node)
}

// nodesInVersion returns a list of all nodes running the version
// passed, if any.
func (c *Context) nodesInVersion(v *clusterupgrade.Version) option.NodeListOption {
	set, ok := c.nodesByVersion[*v]
	if !ok {
		return nil
	}

	return set.Ordered()
}

// NodeVersion returns the release version the given `node` is
// currently running. Panics if the node is not valid.
func (c *Context) NodeVersion(node int) *clusterupgrade.Version {
	for version, nodes := range c.nodesByVersion {
		if nodes.Contains(node) {
			return &version
		}
	}

	panic(fmt.Errorf("NodeVersion error: invalid node %d, cockroach nodes: %v", node, c.CockroachNodes))
}

// NodesInPreviousVersion returns a list of nodes running the version
// we are upgrading from.
func (c *Context) NodesInPreviousVersion() option.NodeListOption {
	return c.nodesInVersion(c.FromVersion)
}

// NodesInNextVersion returns the list of nodes running the version we
// are upgrading to.
func (c *Context) NodesInNextVersion() option.NodeListOption {
	return c.nodesInVersion(c.ToVersion)
}

// MixedBinary indicates if the cluster is currently in mixed-binary
// mode, i.e., not all nodes in the cluster are running the same
// released binary version.
func (c *Context) MixedBinary() bool {
	return len(c.NodesInPreviousVersion()) > 0 && len(c.NodesInNextVersion()) > 0
}

func intSetP(ns ...int) *intsets.Fast {
	set := intsets.MakeFast(ns...)
	return &set
}
