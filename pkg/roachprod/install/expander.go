// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

var parameterRe = regexp.MustCompile(`{[^{}]*}`)
var pgURLRe = regexp.MustCompile(`{pgurl(:[-,0-9]+)?(:[a-z0-9\-]+)?}`)
var pgHostRe = regexp.MustCompile(`{pghost(:[-,0-9]+)?}`)
var pgPortRe = regexp.MustCompile(`{pgport(:[-,0-9]+)?}`)
var uiPortRe = regexp.MustCompile(`{uiport(:[-,0-9]+)?}`)
var storeDirRe = regexp.MustCompile(`{store-dir}`)
var logDirRe = regexp.MustCompile(`{log-dir}`)
var certsDirRe = regexp.MustCompile(`{certs-dir}`)

// expander expands a string which contains templated parameters for cluster
// attributes like pgurl, pghost, pgport, uiport, store-dir, and log-dir with
// the corresponding values.
type expander struct {
	node Node

	pgURLs  map[string]map[Node]string
	pgHosts map[Node]string
	pgPorts map[Node]string
	uiPorts map[Node]string
}

// expanderFunc is a function which may expand a string with a templated value.
type expanderFunc func(context.Context, *logger.Logger, *SyncedCluster, string) (expanded string, didExpand bool, err error)

// expand will expand arg if it contains an expander template.
func (e *expander) expand(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, arg string,
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
		}
		for _, f := range expanders {
			v, expanded, fErr := f(ctx, l, c, s)
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

// maybeExpandPgURL is an expanderFunc for {pgurl:<nodeSpec>}
func (e *expander) maybeExpandPgURL(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, s string,
) (string, bool, error) {
	m := pgURLRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.pgURLs == nil {
		e.pgURLs = make(map[string]map[Node]string)
	}
	tenant := "system"
	if m[2] != "" {
		// Trim off the leading ':' in the capture group.
		tenant = m[2][1:]
	}
	if e.pgURLs[tenant] == nil {
		var err error
		e.pgURLs[tenant], err = c.pgurls(ctx, l, allNodes(len(c.VMs)), tenant)
		if err != nil {
			return "", false, err
		}
	}

	s, err := e.maybeExpandMap(c, e.pgURLs[tenant], m[1])
	return s, err == nil, err
}

// maybeExpandPgHost is an expanderFunc for {pghost:<nodeSpec>}
func (e *expander) maybeExpandPgHost(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, s string,
) (string, bool, error) {
	m := pgHostRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

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

// maybeExpandPgURL is an expanderFunc for {pgport:<nodeSpec>}
func (e *expander) maybeExpandPgPort(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, s string,
) (string, bool, error) {
	m := pgPortRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.pgPorts == nil {
		e.pgPorts = make(map[Node]string, len(c.VMs))
		for _, node := range allNodes(len(c.VMs)) {
			e.pgPorts[node] = fmt.Sprint(c.NodePort(node))
		}
	}

	s, err := e.maybeExpandMap(c, e.pgPorts, m[1])
	return s, err == nil, err
}

// maybeExpandPgURL is an expanderFunc for {uiport:<nodeSpec>}
func (e *expander) maybeExpandUIPort(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, s string,
) (string, bool, error) {
	m := uiPortRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.uiPorts == nil {
		e.uiPorts = make(map[Node]string, len(c.VMs))
		for _, node := range allNodes(len(c.VMs)) {
			e.uiPorts[node] = fmt.Sprint(c.NodeUIPort(node))
		}
	}

	s, err := e.maybeExpandMap(c, e.uiPorts, m[1])
	return s, err == nil, err
}

// maybeExpandStoreDir is an expanderFunc for "{store-dir}"
func (e *expander) maybeExpandStoreDir(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, s string,
) (string, bool, error) {
	if !storeDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.NodeDir(e.node, 1 /* storeIndex */), true, nil
}

// maybeExpandLogDir is an expanderFunc for "{log-dir}"
func (e *expander) maybeExpandLogDir(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, s string,
) (string, bool, error) {
	if !logDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.LogDir(e.node), true, nil
}

// maybeExpandCertsDir is an expanderFunc for "{certs-dir}"
func (e *expander) maybeExpandCertsDir(
	ctx context.Context, l *logger.Logger, c *SyncedCluster, s string,
) (string, bool, error) {
	if !certsDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.CertsDir(e.node), true, nil
}
