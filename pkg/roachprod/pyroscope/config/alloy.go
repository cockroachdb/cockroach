// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
	"github.com/grafana/alloy/syntax"
	"github.com/grafana/alloy/syntax/parser"
	"github.com/grafana/alloy/syntax/vm"
)

var (
	// invalidIdentifierChars matches characters that are not valid in Alloy identifiers
	invalidIdentifierChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)
)

type (
	// AlloyConfig represents the root configuration for Alloy.
	AlloyConfig struct {
		PyroscopeWrite  []*PyroscopeWriteConfig  `alloy:"pyroscope.write,block,optional"`
		PyroscopeScrape []*PyroscopeScrapeConfig `alloy:"pyroscope.scrape,block,optional"`
	}

	// PyroscopeWriteConfig configures the pyroscope.write component.
	PyroscopeWriteConfig struct {
		Label     string            `alloy:",label"`
		Endpoints []*EndpointConfig `alloy:"endpoint,block"`
	}

	// EndpointConfig defines a single endpoint for writing profiles.
	EndpointConfig struct {
		URL string `alloy:"url,attr"`
	}

	// PyroscopeScrapeConfig configures the pyroscope.scrape component.
	PyroscopeScrapeConfig struct {
		Label                  string              `alloy:",label"`
		Targets                []map[string]string `alloy:"targets,attr"`
		Scheme                 string              `alloy:"scheme,attr,optional"`
		HTTPHeaders            map[string][]string `alloy:"http_headers,attr,optional"`
		TLSConfig              *TLSConfig          `alloy:"tls_config,block,optional"`
		ScrapeInterval         string              `alloy:"scrape_interval,attr,optional"`
		DeltaProfilingDuration string              `alloy:"delta_profiling_duration,attr,optional"`
		ProfilingConfig        *ProfilingConfig    `alloy:"profiling_config,block,optional"`
		ForwardTo              []string            `alloy:"forward_to,attr,optional"`
	}

	// TLSConfig configures TLS settings for scraping.
	TLSConfig struct {
		InsecureSkipVerify bool `alloy:"insecure_skip_verify,attr,optional"`
	}

	// ProfilingConfig defines which profile types to enable.
	ProfilingConfig struct {
		ProcessCPU *ProfileConfig `alloy:"profile.process_cpu,block,optional"`
		Memory     *ProfileConfig `alloy:"profile.memory,block,optional"`
		Goroutine  *ProfileConfig `alloy:"profile.goroutine,block,optional"`
		Mutex      *ProfileConfig `alloy:"profile.mutex,block,optional"`
		Block      *ProfileConfig `alloy:"profile.block,block,optional"`
	}

	// ProfileConfig enables/disables a specific profile type.
	ProfileConfig struct {
		Enabled bool `alloy:"enabled,attr,optional"`
	}
)

const receiverMarker = "RECEIVER_MARKER"

// sanitizeClusterName converts a cluster name to a valid Alloy identifier by replacing
// any non-alphanumeric characters with underscores.
func sanitizeClusterName(name string) string {
	return invalidIdentifierChars.ReplaceAllString(name, "_")
}

// Decode reads and parses Alloy configuration from the reader into the provided config
// struct.
func Decode(r io.Reader, cfg *AlloyConfig) error {
	bb, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	f, err := parser.ParseFile("", bb)
	if err != nil {
		return err
	}

	// We define a custom scope to handle the pyroscope receiver reference, since
	// we don't care about linking the actual references here. This marker is
	// replaced again in Encode.
	scope := &vm.Scope{
		Variables: map[string]interface{}{
			"pyroscope": map[string]map[string]map[string]string{
				"write": {"local": {"receiver": receiverMarker}},
			},
		},
	}
	eval := vm.New(f)
	return eval.Evaluate(scope, cfg)
}

// Encode writes the Alloy configuration to the writer in Alloy syntax format.
func Encode(w io.Writer, cfg *AlloyConfig) error {
	buf, err := syntax.Marshal(cfg) // nolint:protomarshal
	if err != nil {
		return err
	}
	// Restore the receiver reference.
	buf = []byte(
		strings.ReplaceAll(string(buf),
			fmt.Sprintf(`"%s"`, receiverMarker),
			"pyroscope.write.local.receiver",
		),
	)
	_, err = w.Write(buf)
	return err
}

// ContainsTarget reports whether the config includes a scrape target for the given
// cluster.
func (c *AlloyConfig) ContainsTarget(clusterName string) bool {
	sanitized := sanitizeClusterName(clusterName)
	for _, scrape := range c.PyroscopeScrape {
		if scrape.Label == sanitized {
			return true
		}
	}
	return false
}

// UpdateTarget creates or updates the scrape configuration for a cluster with optional
// secure cookie authentication.
func (c *AlloyConfig) UpdateTarget(clusterName string, secureCookie string) {
	sanitized := sanitizeClusterName(clusterName)
	var scrapeConfig *PyroscopeScrapeConfig
	found := false
	for _, scrape := range c.PyroscopeScrape {
		if scrape.Label == sanitized {
			scrapeConfig = scrape
			found = true
			break
		}
	}

	if !found {
		scrapeConfig = &PyroscopeScrapeConfig{
			Label:                  sanitized,
			Targets:                []map[string]string{},
			ScrapeInterval:         "1m",
			DeltaProfilingDuration: "10s",
			ProfilingConfig: &ProfilingConfig{
				ProcessCPU: &ProfileConfig{Enabled: true},
				Memory:     &ProfileConfig{Enabled: true},
				Goroutine:  &ProfileConfig{Enabled: true},
				Mutex:      &ProfileConfig{Enabled: true},
				Block:      &ProfileConfig{Enabled: true},
			},
			ForwardTo: []string{receiverMarker},
		}
		c.PyroscopeScrape = append(c.PyroscopeScrape, scrapeConfig)
	}

	scrapeConfig.Scheme = "http"
	scrapeConfig.HTTPHeaders = nil
	scrapeConfig.TLSConfig = nil
	if secureCookie != "" {
		scrapeConfig.Scheme = "https"
		scrapeConfig.HTTPHeaders = map[string][]string{
			"Cookie": {secureCookie},
		}
		scrapeConfig.TLSConfig = &TLSConfig{
			InsecureSkipVerify: true,
		}
	}
}

// AddNodes adds the specified nodes to the scrape targets for a cluster, skipping
// duplicates.
func (c *AlloyConfig) AddNodes(clusterName string, addresses map[install.Node]string) error {
	// Get existing nodes to avoid duplicates
	existingNodes, err := c.GetNodes(clusterName)
	if err != nil {
		return err
	}

	sanitized := sanitizeClusterName(clusterName)
	for _, scrape := range c.PyroscopeScrape {
		if scrape.Label != sanitized {
			continue
		}

		for node, address := range addresses {
			if _, exists := existingNodes[node]; exists {
				continue
			}
			target := map[string]string{
				"__address__":  address,
				"node":         fmt.Sprintf("%d", node),
				"service_name": clusterName,
			}
			scrape.Targets = append(scrape.Targets, target)
			// Sort the targets by node number for consistent ordering.
			sort.Slice(scrape.Targets, func(i, j int) bool {
				var nodeI, nodeJ int
				_, _ = fmt.Sscanf(scrape.Targets[i]["node"], "%d", &nodeI)
				_, _ = fmt.Sscanf(scrape.Targets[j]["node"], "%d", &nodeJ)
				return nodeI < nodeJ
			})
		}
		return nil
	}
	return errors.Newf("cluster %q not found in configuration", clusterName)
}

// RemoveNodes removes the specified nodes from the scrape targets for a cluster.
func (c *AlloyConfig) RemoveNodes(clusterName string, nodes install.Nodes) error {
	sanitized := sanitizeClusterName(clusterName)
	for _, scrape := range c.PyroscopeScrape {
		if scrape.Label != sanitized {
			continue
		}
		// Create a set of nodes to remove for a quick lookup
		nodeSet := make(map[int]bool)
		for _, node := range nodes {
			nodeSet[int(node)] = true
		}

		// Filter out targets whose node is in the removal set
		var newTargets []map[string]string
		for _, target := range scrape.Targets {
			nodeStr := target["node"]
			var nodeNum int
			if _, err := fmt.Sscanf(nodeStr, "%d", &nodeNum); err == nil {
				if !nodeSet[nodeNum] {
					newTargets = append(newTargets, target)
				}
			} else {
				// Keep targets we can't parse
				newTargets = append(newTargets, target)
			}
		}
		scrape.Targets = newTargets
		return nil
	}
	return errors.Newf("cluster %q not found in configuration", clusterName)
}

// GetNodes returns a set of node numbers for the given cluster.
func (c *AlloyConfig) GetNodes(clusterName string) (map[install.Node]struct{}, error) {
	sanitized := sanitizeClusterName(clusterName)
	for _, scrape := range c.PyroscopeScrape {
		if scrape.Label != sanitized {
			continue
		}
		nodes := make(map[install.Node]struct{}, len(scrape.Targets))
		for _, target := range scrape.Targets {
			nodeStr := target["node"]
			var nodeNum int
			if _, err := fmt.Sscanf(nodeStr, "%d", &nodeNum); err == nil {
				nodes[install.Node(nodeNum)] = struct{}{}
			}
		}
		return nodes, nil
	}
	return nil, errors.Newf("cluster %q not found in configuration", clusterName)
}
