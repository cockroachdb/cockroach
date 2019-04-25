// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package install

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var parameterRe = regexp.MustCompile(`{[^}]*}`)
var pgURLRe = regexp.MustCompile(`{pgurl(:[-,0-9]+)?}`)
var pgPortRe = regexp.MustCompile(`{pgport(:[-,0-9]+)?}`)
var uiPortRe = regexp.MustCompile(`{uiport(:[-,0-9]+)?}`)
var storeDirRe = regexp.MustCompile(`{store-dir}`)
var logDirRe = regexp.MustCompile(`{log-dir}`)

type expander struct {
	node    int
	pgURLs  map[int]string
	pgPorts map[int]string
	uiPorts map[int]string
}

func (e *expander) maybeExpandMap(
	c *SyncedCluster, m map[int]string, nodeSpec string,
) (string, bool, error) {
	if nodeSpec == "" {
		nodeSpec = "all"
	} else {
		nodeSpec = nodeSpec[1:]
	}

	nodes, err := ListNodes(nodeSpec, len(c.VMs))
	if err != nil {
		return "", false, err
	}

	var result []string
	for _, i := range nodes {
		if s, ok := m[i]; ok {
			result = append(result, s)
		}
	}
	if len(result) != len(nodes) {
		return "", false, errors.Errorf("failed to expand nodes %v, given node map %v", nodes, m)
	}
	return strings.Join(result, " "), true, nil
}

func (e *expander) maybeExpandPgURL(c *SyncedCluster, s string) (string, bool, error) {
	m := pgURLRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}

	if e.pgURLs == nil {
		e.pgURLs = c.pgurls(allNodes(len(c.VMs)))
	}

	return e.maybeExpandMap(c, e.pgURLs, m[1])
}

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

	return e.maybeExpandMap(c, e.pgPorts, m[1])
}

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

	return e.maybeExpandMap(c, e.uiPorts, m[1])
}

func (e *expander) maybeExpandStoreDir(c *SyncedCluster, s string) (string, bool, error) {
	if !storeDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.Impl.NodeDir(c, e.node), true, nil
}

func (e *expander) maybeExpandLogDir(c *SyncedCluster, s string) (string, bool, error) {
	if !logDirRe.MatchString(s) {
		return s, false, nil
	}
	return c.Impl.LogDir(c, e.node), true, nil
}

type expanderFunc func(*SyncedCluster, string) (string, bool, error)

func (e *expander) expand(c *SyncedCluster, arg string) (string, error) {
	var err error
	s := parameterRe.ReplaceAllStringFunc(arg, func(s string) string {
		if err != nil {
			return ""
		}
		expanders := []expanderFunc{
			e.maybeExpandPgURL,
			e.maybeExpandPgPort,
			e.maybeExpandUIPort,
			e.maybeExpandStoreDir,
			e.maybeExpandLogDir,
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
