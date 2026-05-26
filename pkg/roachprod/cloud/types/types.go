// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package types provides shared types used by both the cloud package and its clients.
package types

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/promhelperclient"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
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
)

// printDetailsColumnHeaders are the headers to be printed in the defined sequence.
var printDetailsColumnHeaders = []string{
	headerName, headerDNS, headerPrivateIP, headerPublicIP, headerMachineType, headerCPUArch, headerCPUFamily,
	headerProvisionModel,
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

	CloudProviders []string
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

func (c *Cluster) DeletePrometheusConfig(ctx context.Context, l *logger.Logger) error {

	stopSpinner := ui.NewDefaultSpinner(l, "Destroying Prometheus configs").Start()
	defer stopSpinner()

	// We first iterate on all VMs to determine if any machine of the cluster
	// was reachable by Prometheus and if we need to delete its config.
	// This is done this way to avoid authenticating the promhelper client
	// in case we don't need to delete any config.
	needDelete := false
	for _, node := range c.VMs {

		reachability := promhelperclient.ProviderReachability(
			node.Provider,
			promhelperclient.CloudEnvironment(node.Project),
		)
		if reachability == promhelperclient.None {
			continue
		}

		needDelete = true
		break
	}

	if needDelete {
		cl, err := promhelperclient.NewPromClient()
		if err != nil {
			return err
		}

		err = cl.DeleteClusterConfig(ctx, c.Name, l)
		if err != nil {
			return err
		}
	}

	return nil
}
