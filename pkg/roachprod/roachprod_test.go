// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

func TestSelectGCClouds(t *testing.T) {
	selected, err := selectGCClouds(nil)
	require.NoError(t, err)
	require.Equal(t, supportedGCClouds, selected)

	selected, err = selectGCClouds([]string{"gce", "aws", "gce"})
	require.NoError(t, err)
	require.Equal(t, []string{"gce", "aws"}, selected)

	_, err = selectGCClouds([]string{"local"})
	require.ErrorContains(t, err, "unsupported cloud provider")
}

func TestGCGCEInventoryFailureSkipsDestructiveOperations(t *testing.T) {
	var (
		mu     syncutil.Mutex
		called = make(map[string]int)
	)
	recordCall := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		called[name]++
	}

	operations := gcOperations{
		loadClusters: func() error {
			recordCall("load")
			return nil
		},
		providerActive: func(string) bool { return true },
		gcAWS:          func(*logger.Logger, bool) error { return nil },
		gcAzure:        func(*logger.Logger, bool) error { return nil },
		gcIBM:          func(*logger.Logger, bool) error { return nil },
		listGCE: func(*logger.Logger) (*cloud.Cloud, error) {
			recordCall("list-gce")
			return nil, errors.New("inventory unavailable")
		},
		gcClusters: func(*logger.Logger, *cloud.Cloud, bool) error {
			recordCall("gc-clusters")
			return nil
		},
		gcDNS: func(*logger.Logger, *cloud.Cloud, bool) error {
			recordCall("gc-dns")
			return nil
		},
	}

	err := gc(nilLogger(), GCOptions{Clouds: []string{"gce"}}, operations)
	require.ErrorContains(t, err, "listing GCE resources for gc")
	require.Equal(t, 1, called["load"])
	require.Equal(t, 1, called["list-gce"])
	require.Zero(t, called["gc-clusters"])
	require.Zero(t, called["gc-dns"])
}

func TestGCProviderFailuresAreIndependent(t *testing.T) {
	var (
		mu     syncutil.Mutex
		called = make(map[string]int)
	)
	recordCall := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		called[name]++
	}

	operations := gcOperations{
		loadClusters: func() error { return nil },
		providerActive: func(name string) bool {
			return name != "aws"
		},
		gcAWS:   func(*logger.Logger, bool) error { return nil },
		gcAzure: func(*logger.Logger, bool) error { return nil },
		gcIBM:   func(*logger.Logger, bool) error { return nil },
		listGCE: func(*logger.Logger) (*cloud.Cloud, error) {
			recordCall("list-gce")
			return cloud.NewCloud(), nil
		},
		gcClusters: func(*logger.Logger, *cloud.Cloud, bool) error {
			recordCall("gc-clusters")
			return nil
		},
		gcDNS: func(*logger.Logger, *cloud.Cloud, bool) error {
			recordCall("gc-dns")
			return nil
		},
	}

	err := gc(nilLogger(), GCOptions{Clouds: []string{"aws", "gce"}}, operations)
	require.ErrorContains(t, err, `cloud provider "aws" is not active`)
	require.Equal(t, 1, called["list-gce"])
	require.Equal(t, 1, called["gc-clusters"])
	require.Equal(t, 1, called["gc-dns"])
}

func TestIPExternalRequiresPublicIP(t *testing.T) {
	cluster := &install.SyncedCluster{
		Cluster: cloud.Cluster{VMs: vm.List{{
			PrivateIP: "10.0.0.1",
		}}},
		Nodes: install.Nodes{1},
	}

	_, err := clusterIPs(cluster, true)
	require.ErrorContains(t, err, "no public IP for node 1")

	cluster.VMs[0].PublicIP = "192.0.2.1"
	ips, err := clusterIPs(cluster, true)
	require.NoError(t, err)
	require.Equal(t, []string{"192.0.2.1"}, ips)

	ips, err = clusterIPs(cluster, false)
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.1"}, ips)
}

func TestPGURLIPsUseHost(t *testing.T) {
	cluster := &install.SyncedCluster{
		Cluster: cloud.Cluster{VMs: vm.List{
			{PrivateIP: "10.0.0.1", PublicIP: "192.0.2.1"},
			{PrivateIP: "10.0.0.2"},
		}},
		Nodes: install.Nodes{1, 2},
	}

	ips, err := pgURLIPs(cluster, cluster.Nodes, PGURLOptions{UseHost: true})
	require.NoError(t, err)
	require.Equal(t, []string{"192.0.2.1", "10.0.0.2"}, ips)

	ips, err = pgURLIPs(cluster, cluster.Nodes, PGURLOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, ips)

	_, err = pgURLIPs(cluster, cluster.Nodes, PGURLOptions{External: true})
	require.ErrorContains(t, err, "no public IP for node 2")
}

func TestURLGeneratorUseHost(t *testing.T) {
	cluster := &install.SyncedCluster{
		Cluster: cloud.Cluster{VMs: vm.List{
			{PrivateIP: "10.0.0.1", PublicIP: "192.0.2.1"},
			{PrivateIP: "10.0.0.2"},
		}},
		Nodes: install.Nodes{1, 2},
	}

	urls, err := urlGenerator(
		context.Background(), cluster, nilLogger(), cluster.Nodes,
		urlConfig{useHost: true, port: 26258},
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"http://192.0.2.1:26258/",
		"http://10.0.0.2:26258/",
	}, urls)

	cluster.VMs[1].PrivateIP = ""
	_, err = urlGenerator(
		context.Background(), cluster, nilLogger(), cluster.Nodes,
		urlConfig{useHost: true, port: 26258},
	)
	require.ErrorContains(t, err, "no host address for node 2")
}

func TestURLGeneratorDNSFallbackUsesHost(t *testing.T) {
	cluster := &install.SyncedCluster{
		Cluster: cloud.Cluster{
			Name: "private-cluster",
			VMs: vm.List{
				{PrivateIP: "10.0.0.1"},
				{PrivateIP: "10.0.0.2"},
			},
		},
		Nodes: install.Nodes{1, 2},
	}
	var lookups int
	urls, err := urlGenerator(
		context.Background(), cluster, nilLogger(), cluster.Nodes,
		urlConfig{
			port:      26258,
			dnsDomain: "roachprod.test",
			lookupHost: func(string) ([]string, error) {
				lookups++
				return nil, errors.New("DNS unavailable")
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, lookups)
	require.Equal(t, []string{
		"http://10.0.0.1:26258/",
		"http://10.0.0.2:26258/",
	}, urls)
}

func TestURLGeneratorRejectsEmptyPublicIP(t *testing.T) {
	cluster := &install.SyncedCluster{
		Cluster: cloud.Cluster{VMs: vm.List{{PrivateIP: "10.0.0.1"}}},
		Nodes:   install.Nodes{1},
	}

	_, err := urlGenerator(
		context.Background(), cluster, nilLogger(), cluster.Nodes,
		urlConfig{usePublicIP: true, port: 26258},
	)
	require.ErrorContains(t, err, "no public IP for node 1")
}

func TestVerifyClusterName(t *testing.T) {
	findActiveAccounts = func(l *logger.Logger) (map[string]string, error) {
		return map[string]string{"1": "user1", "2": "user2", "3": "USER4"}, nil
	}
	defer func() {
		findActiveAccounts = vm.FindActiveAccounts
	}()
	cases := []struct {
		description, clusterName, username string
		errorExpected                      bool
	}{
		{
			"username found", "user1-clustername", "", false,
		},
		{
			"username not found", "user3-clustername", "", true,
		},
		{
			"specified username", "user3-clustername", "user3", false,
		},
		{
			"specified username that doesn't match", "user1-clustername", "fakeuser", true,
		},
		{
			"clustername not sanitized", "UserName-clustername", "", true,
		},
		{
			"no username", "clustername", "", true,
		},
		{
			"no clustername", "user1", "", true,
		},
		{
			"unsanitized found username", "user4-clustername", "", false,
		},
		{
			"unsanitized specified username", "user3-clustername", "USER3", false,
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			if c.errorExpected {
				assert.Error(t, verifyClusterName(nilLogger(), c.clusterName, c.username))
			} else {
				assert.NoError(t, verifyClusterName(nilLogger(), c.clusterName, c.username))
			}
		})
	}
}
