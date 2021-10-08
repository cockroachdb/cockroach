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
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

var parameterRe = regexp.MustCompile(`{[^{}]*}`)
var pgURLRe = regexp.MustCompile(`{pgurl(:[-,0-9]+)?}`)
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
	node int

	pgURLs  map[int]string
	pgHosts map[int]string
	pgPorts map[int]string
	uiPorts map[int]string
}

// expanderFunc is a function which may expand a string with a templated value.
type expanderFunc func(*SyncedCluster, string) (expanded string, didExpand bool, err error)

// expand will expand arg if it contains an expander template.
func (e *expander) expand(c *SyncedCluster, arg string) (string, error) {
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
			v, expanded, fErr := f(c, s)
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
	c *SyncedCluster, m map[int]string, nodeSpec string,
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
	for _, i := range nodes {
		if s, ok := m[i]; ok {
			result = append(result, s)
		}
	}
	if len(result) != len(nodes) {
		return "", errors.Errorf("failed to expand nodes %v, given node map %v", nodes, m)
	}
	return strings.Join(result, " "), nil
}

// maybeExpandPgURL is an expanderFunc for {pgurl:<nodeSpec>}
func (e *expander) maybeExpandPgURL(c *SyncedCluster, s string) (string, bool, error) {
	m := pgURLRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.pgURLs == nil {
		e.pgURLs = c.pgurls(allNodes(len(c.VMs)))
	}

	s, err := e.maybeExpandMap(c, e.pgURLs, m[1])
	return s, err == nil, err
}

// maybeExpandPgHost is an expanderFunc for {pghost:<nodeSpec>}
func (e *expander) maybeExpandPgHost(c *SyncedCluster, s string) (string, bool, error) {
	m := pgHostRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.pgHosts == nil {
		e.pgHosts = c.pghosts(allNodes(len(c.VMs)))
	}

	s, err := e.maybeExpandMap(c, e.pgHosts, m[1])
	return s, err == nil, err
}

// maybeExpandPgURL is an expanderFunc for {pgport:<nodeSpec>}
func (e *expander) maybeExpandPgPort(c *SyncedCluster, s string) (string, bool, error) {
	m := pgPortRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.pgPorts == nil {
		e.pgPorts = make(map[int]string, len(c.VMs))
		for _, i := range allNodes(len(c.VMs)) {
			e.pgPorts[i] = fmt.Sprint(c.Impl.NodePort(c, i))
		}
	}

	s, err := e.maybeExpandMap(c, e.pgPorts, m[1])
	return s, err == nil, err
}

// maybeExpandPgURL is an expanderFunc for {uiport:<nodeSpec>}
func (e *expander) maybeExpandUIPort(c *SyncedCluster, s string) (string, bool, error) {
	m := uiPortRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.uiPorts == nil {
		e.uiPorts = make(map[int]string, len(c.VMs))
		for _, i := range allNodes(len(c.VMs)) {
			e.uiPorts[i] = fmt.Sprint(c.Impl.NodeUIPort(c, i))
		}
	}

	s, err := e.maybeExpandMap(c, e.uiPorts, m[1])
	return s, err == nil, err
}

// maybeExpandStoreDir is an expanderFunc for "{store-dir}"
func (e *expander) maybeExpandStoreDir(c *SyncedCluster, s string) (string, bool, error) {
	if !storeDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.Impl.NodeDir(c, e.node, 1 /* storeIndex */), true, nil
}

// maybeExpandLogDir is an expanderFunc for "{log-dir}"
func (e *expander) maybeExpandLogDir(c *SyncedCluster, s string) (string, bool, error) {
	if !logDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.Impl.LogDir(c, e.node), true, nil
}

// maybeExpandCertsDir is an expanderFunc for "{certs-dir}"
func (e *expander) maybeExpandCertsDir(c *SyncedCluster, s string) (string, bool, error) {
	if !certsDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.Impl.CertsDir(c, e.node), true, nil
}
