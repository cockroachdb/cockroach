// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

var parameterRe = regexp.MustCompile(`{[^{}]*}`)

// The following regex expressions may contain up to 3 sections. Since
// we need to resolve both an IP and port, in most cases, but multiple services
// could be running on a single node, it may be required to narrow things down
// by specifying more selectors.
// 1. Nodes Selector: `(:[-,0-9]+|:(?i)lb)?` This can be either a single node
// "1", or a node range "1-3", or "LB" can be specified to indicate that a load
// balancer should be resolved.
// 2. Tenant Selector: `(:[a-z0-9\-]+)?` This section can optionally specify a
// tenant name. For example "system" could be specified here.
// 3. Instance Selector: `(:[0-9]+)?` If the target nodes have more than one
// external process serving the same tenant, if a tenant was specified in the
// last section we can specify the ith tenant process instance.
// Examples:
// {pgurl:3:tenant1:0} - Get the postgres URL for node 3, tenant1, instance 0.
var pgURLRe = regexp.MustCompile(`{pgurl(:[-,0-9]+|:(?i)lb)?(:[a-z0-9\-]+)?(:[0-9]+)?}`)
var pgHostRe = regexp.MustCompile(`{pghost(:[-,0-9]+|:(?i)lb)?(:[a-z0-9\-]+)?(:[0-9]+)?}`)
var pgPortRe = regexp.MustCompile(`{pgport(:[-,0-9]+)?(:[a-z0-9\-]+)?(:[0-9]+)?}`)
var uiPortRe = regexp.MustCompile(`{uiport(:[-,0-9]+)}`)
var ipAddressRe = regexp.MustCompile(`{ip(:\d+([-,]\d+)?)(:public|:private)?}`)
var storeDirRe = regexp.MustCompile(`{store-dir(:[0-9]+)?}`)
var logDirRe = regexp.MustCompile(`{log-dir(:[a-z0-9\-]+)?(:[0-9]+)?}`)
var certsDirRe = regexp.MustCompile(`{certs-dir}`)

type ExpanderConfig struct {
	DefaultVirtualCluster string
}

// expander expands a string which contains templated parameters for cluster
// attributes like pgurl, pghost, pgport, uiport, store-dir, and log-dir with
// the corresponding values.
type expander struct {
	node Node

	pgURLs     map[string]map[Node]string
	pgHosts    map[Node]string
	pgPorts    map[Node]string
	uiPorts    map[Node]string
	publicIPs  map[Node]string
	privateIPs map[Node]string
}

// expanderFunc is a function which may expand a string with a templated value.
type expanderFunc func(context.Context, *logger.Logger, *SyncedCluster, ExpanderConfig, string) (expanded string, didExpand bool, err error)

// expand will expand arg if it contains an expander template.
func (e *expander) expand(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, cfg ExpanderConfig, arg string,
) (string, error) {
	var err error
	s := parameterRe.ReplaceAllStringFunc(arg, func(s string) string {
		if err != nil {
			return ""
		}
		expanders := []expanderFunc{
			e.maybeExpandPgURL,
			e.maybeExpandPgHost,
			e.maybeExpandPgPort,
			e.maybeExpandUIPort,
			e.maybeExpandStoreDir,
			e.maybeExpandLogDir,
			e.maybeExpandCertsDir,
			e.maybeExpandIPAddress,
		}
		for _, f := range expanders {
			v, expanded, fErr := f(ctx, l, c, cfg, s)
			if fErr != nil {
				err = fErr
				return ""
			}
			if expanded {
				return v
			}
		}
		return s
	})
	if err != nil {
		return "", err
	}
	return s, nil
}

// maybeExpandMap is used by other expanderFuncs to return a space separated
// list of strings which correspond to the values in m for each node specified
// by nodeSpec.
func (e *expander) maybeExpandMap(
	c *SyncedCluster, m map[Node]string, nodeSpec string,
) (string, error) {
	if nodeSpec == "" {
		nodeSpec = "all"
	} else {
		nodeSpec = nodeSpec[1:]
	}

	nodes, err := ListNodes(nodeSpec, len(c.VMs))
	if err != nil {
		return "", err
	}

	var result []string
	for _, node := range nodes {
		if s, ok := m[node]; ok {
			result = append(result, s)
		}
	}
	if len(result) != len(nodes) {
		return "", errors.Errorf("failed to expand nodes %v, given node map %v", nodes, m)
	}
	return strings.Join(result, " "), nil
}

// extractVirtualClusterInfo extracts the virtual cluster name and
// instance from the given group match, if available. If no default or
// custom tenant is provided, an empty string is returned. During
// service discovery, this will mean that the service for the system
// tenant is used.
func extractVirtualClusterInfo(
	matches []string, defaultVirtualCluster string,
) (string, int, error) {
	// Defaults if the passed in group match is empty.
	virtualClusterName := defaultVirtualCluster
	var sqlInstance int

	// Extract the cluster name and instance matches.
	trim := func(s string) string {
		// Trim off the leading ':' in the capture group.
		return s[1:]
	}
	virtualClusterNameMatch := matches[0]
	sqlInstanceMatch := matches[1]

	if virtualClusterNameMatch != "" {
		virtualClusterName = trim(virtualClusterNameMatch)
	}
	if sqlInstanceMatch != "" {
		var err error
		sqlInstance, err = strconv.Atoi(trim(sqlInstanceMatch))
		if err != nil {
			return "", 0, err
		}
	}
	return virtualClusterName, sqlInstance, nil
}

// maybeExpandPgURL is an expanderFunc for {pgurl:<nodeSpec>[:virtualCluster[:sqlInstance]]}
func (e *expander) maybeExpandPgURL(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, cfg ExpanderConfig, s string,
) (string, bool, error) {
	var err error
	m := pgURLRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.pgURLs == nil {
		e.pgURLs = make(map[string]map[Node]string)
	}
	virtualClusterName, sqlInstance, err := extractVirtualClusterInfo(m[2:], cfg.DefaultVirtualCluster)
	if err != nil {
		return "", false, err
	}
	switch strings.ToLower(m[1]) {
	case ":lb":
		url, err := c.loadBalancerURL(ctx, l, virtualClusterName, sqlInstance, DefaultAuthMode())
		return url, url != "", err
	default:
		if e.pgURLs[virtualClusterName] == nil {
			e.pgURLs[virtualClusterName], err = c.pgurls(ctx, l, allNodes(len(c.VMs)), virtualClusterName, sqlInstance)
			if err != nil {
				return "", false, err
			}
		}
		s, err = e.maybeExpandMap(c, e.pgURLs[virtualClusterName], m[1])
		return s, err == nil, err
	}
}

// maybeExpandPgHost is an expanderFunc for {pghost:<nodeSpec>}
func (e *expander) maybeExpandPgHost(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, cfg ExpanderConfig, s string,
) (string, bool, error) {
	m := pgHostRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}
	virtualClusterName, sqlInstance, err := extractVirtualClusterInfo(m[2:], cfg.DefaultVirtualCluster)
	if err != nil {
		return "", false, err
	}

	switch strings.ToLower(m[1]) {
	case ":lb":
		services, err := c.DiscoverServices(ctx, virtualClusterName, ServiceTypeSQL, ServiceInstancePredicate(sqlInstance))
		if err != nil {
			return "", false, err
		}
		port := config.DefaultSQLPort
		for _, svc := range services {
			if svc.VirtualClusterName == virtualClusterName && svc.Instance == sqlInstance {
				port = svc.Port
				break
			}
		}
		addr, err := c.FindLoadBalancer(l, port)
		if err != nil {
			return "", false, err
		}
		return addr.IP, true, nil
	default:
		if e.pgHosts == nil {
			var err error
			e.pgHosts, err = c.pghosts(ctx, l, allNodes(len(c.VMs)))
			if err != nil {
				return "", false, err
			}
		}
		s, err := e.maybeExpandMap(c, e.pgHosts, m[1])
		return s, err == nil, err
	}
}

// maybeExpandPgURL is an expanderFunc for {pgport:<nodeSpec>}
func (e *expander) maybeExpandPgPort(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, cfg ExpanderConfig, s string,
) (string, bool, error) {
	m := pgPortRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}
	virtualClusterName, sqlInstance, err := extractVirtualClusterInfo(m[2:], cfg.DefaultVirtualCluster)
	if err != nil {
		return "", false, err
	}

	if e.pgPorts == nil {
		e.pgPorts = make(map[Node]string, len(c.VMs))
		for _, node := range allNodes(len(c.VMs)) {
			desc, err := c.DiscoverService(ctx, node, virtualClusterName, ServiceTypeSQL, sqlInstance)
			if err != nil {
				return s, false, err
			}
			e.pgPorts[node] = fmt.Sprint(desc.Port)
		}
	}

	s, err = e.maybeExpandMap(c, e.pgPorts, m[1])
	return s, err == nil, err
}

// maybeExpandPgURL is an expanderFunc for {uiport:<nodeSpec>}
func (e *expander) maybeExpandUIPort(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, _ ExpanderConfig, s string,
) (string, bool, error) {
	m := uiPortRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.uiPorts == nil {
		e.uiPorts = make(map[Node]string, len(c.VMs))
		for _, node := range allNodes(len(c.VMs)) {
			// TODO(herko): Add support for separate-process services.
			e.uiPorts[node] = fmt.Sprint(c.NodeUIPort(ctx, node, "" /* virtualClusterName */, 0 /* sqlInstance */))
		}
	}

	s, err := e.maybeExpandMap(c, e.uiPorts, m[1])
	return s, err == nil, err
}

// maybeExpandStoreDir is an expanderFunc for "{store-dir[:<storeIndex>])}",
// where storeIndex is optional and defaults to 1. Note that storeIndex is the
// store's index on multi-store nodes, not the store ID.
func (e *expander) maybeExpandStoreDir(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, _ ExpanderConfig, s string,
) (string, bool, error) {
	m := storeDirRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}
	storeIndex := 1
	if len(m) > 1 && m[1] != "" { // the optional submatch is always included
		var err error
		storeIndex, err = strconv.Atoi(m[1][1:]) // skip the :
		if err != nil {
			return "", false, errors.Wrapf(err, "invalid store index %q in %q", m[1], s)
		}
	}
	return c.NodeDir(e.node, storeIndex), true, nil
}

// maybeExpandLogDir is an expanderFunc for "{log-dir}"
func (e *expander) maybeExpandLogDir(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, cfg ExpanderConfig, s string,
) (string, bool, error) {
	m := logDirRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}
	virtualClusterName, sqlInstance, err := extractVirtualClusterInfo(m[1:], cfg.DefaultVirtualCluster)
	if err != nil {
		return "", false, err
	}
	return c.LogDir(e.node, virtualClusterName, sqlInstance), true, nil
}

// maybeExpandCertsDir is an expanderFunc for "{certs-dir}"
func (e *expander) maybeExpandCertsDir(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, _ ExpanderConfig, s string,
) (string, bool, error) {
	if !certsDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.CertsDir(e.node), true, nil
}

// maybeExpandIPAddress is an expanderFunc for {ipaddress:<nodeSpec>[:public|private]}
func (e *expander) maybeExpandIPAddress(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, cfg ExpanderConfig, s string,
) (string, bool, error) {
	m := ipAddressRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	var err error
	switch m[3] {
	case ":public":
		if e.publicIPs == nil {
			e.publicIPs = make(map[Node]string, len(c.VMs))
			for _, node := range allNodes(len(c.VMs)) {
				e.publicIPs[node] = c.Host(node)
			}
		}

		s, err = e.maybeExpandMap(c, e.publicIPs, m[1])
	default:
		if e.privateIPs == nil {
			e.privateIPs = make(map[Node]string, len(c.VMs))
			for _, node := range allNodes(len(c.VMs)) {
				ip, err := c.GetInternalIP(node)
				if err != nil {
					return "", false, err
				}
				e.privateIPs[node] = ip
			}
		}

		s, err = e.maybeExpandMap(c, e.privateIPs, m[1])
	}
	return s, err == nil, err
}
