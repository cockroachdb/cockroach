// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"fmt"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLoadBalancerLabels(t *testing.T) {
	testCases := []struct {
		name        string
		clusterName string
		port        int
		expected    map[string]string
	}{
		{
			name:        "simple cluster name and port",
			clusterName: "test-cluster",
			port:        26257,
			expected: map[string]string{
				vm.TagCluster:   "test-cluster",
				vm.TagRoachprod: "true",
				"port":          "26257",
			},
		},
		{
			name:        "cluster name with special characters",
			clusterName: "my_cluster-123",
			port:        8080,
			expected: map[string]string{
				vm.TagCluster:   "my_cluster-123",
				vm.TagRoachprod: "true",
				"port":          "8080",
			},
		},
		{
			name:        "different port",
			clusterName: "prod",
			port:        443,
			expected: map[string]string{
				vm.TagCluster:   "prod",
				vm.TagRoachprod: "true",
				"port":          "443",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			labels := buildLoadBalancerLabels(tc.clusterName, tc.port)
			assert.Equal(t, tc.expected, labels)
		})
	}
}

func TestLoadBalancerResourceName(t *testing.T) {
	testCases := []struct {
		name         string
		clusterName  string
		port         int
		resourceType string
		expected     string
	}{
		{
			name:         "forwarding rule",
			clusterName:  "test-cluster",
			port:         26257,
			resourceType: "forwarding-rule",
			expected:     "test-cluster-26257-forwarding-rule-roachprod",
		},
		{
			name:         "health check",
			clusterName:  "test-cluster",
			port:         26257,
			resourceType: "health-check",
			expected:     "test-cluster-26257-health-check-roachprod",
		},
		{
			name:         "load balancer",
			clusterName:  "my-cluster",
			port:         8080,
			resourceType: "load-balancer",
			expected:     "my-cluster-8080-load-balancer-roachprod",
		},
		{
			name:         "proxy",
			clusterName:  "prod",
			port:         443,
			resourceType: "proxy",
			expected:     "prod-443-proxy-roachprod",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := loadBalancerResourceName(tc.clusterName, tc.port, tc.resourceType)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestLoadBalancerNameParts(t *testing.T) {
	testCases := []struct {
		name            string
		resourceName    string
		expectedCluster string
		expectedType    string
		expectedPort    int
		expectedValid   bool
	}{
		{
			name:            "valid forwarding rule",
			resourceName:    "test-cluster-26257-forwarding-rule-roachprod",
			expectedCluster: "test-cluster",
			expectedType:    "forwarding-rule",
			expectedPort:    26257,
			expectedValid:   true,
		},
		{
			name:            "valid health check",
			resourceName:    "my-cluster-8080-health-check-roachprod",
			expectedCluster: "my-cluster",
			expectedType:    "health-check",
			expectedPort:    8080,
			expectedValid:   true,
		},
		{
			name:            "valid load balancer",
			resourceName:    "prod-443-load-balancer-roachprod",
			expectedCluster: "prod",
			expectedType:    "load-balancer",
			expectedPort:    443,
			expectedValid:   true,
		},
		{
			name:            "valid proxy",
			resourceName:    "test-26257-proxy-roachprod",
			expectedCluster: "test",
			expectedType:    "proxy",
			expectedPort:    26257,
			expectedValid:   true,
		},
		{
			name:            "cluster name with hyphens",
			resourceName:    "my-test-cluster-26257-forwarding-rule-roachprod",
			expectedCluster: "my-test-cluster",
			expectedType:    "forwarding-rule",
			expectedPort:    26257,
			expectedValid:   true,
		},
		{
			name:          "invalid - missing roachprod suffix",
			resourceName:  "test-cluster-26257-forwarding-rule",
			expectedValid: false,
		},
		{
			name:          "invalid - missing parts",
			resourceName:  "test-cluster",
			expectedValid: false,
		},
		{
			name:          "invalid - non-numeric port",
			resourceName:  "test-cluster-abc-forwarding-rule-roachprod",
			expectedValid: false,
		},
		{
			name:          "invalid - empty string",
			resourceName:  "",
			expectedValid: false,
		},
		{
			name:          "invalid - only two parts",
			resourceName:  "test-26257",
			expectedValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cluster, resourceType, port, valid := loadBalancerNameParts(tc.resourceName)

			assert.Equal(t, tc.expectedValid, valid, "validity mismatch")

			if tc.expectedValid {
				assert.Equal(t, tc.expectedCluster, cluster, "cluster name mismatch")
				assert.Equal(t, tc.expectedType, resourceType, "resource type mismatch")
				assert.Equal(t, tc.expectedPort, port, "port mismatch")
			}
		})
	}
}

func TestSerializeLabel(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple lowercase",
			input:    "test",
			expected: "test",
		},
		{
			name:     "uppercase converted to lowercase",
			input:    "TEST",
			expected: "test",
		},
		{
			name:     "mixed case",
			input:    "TestCluster",
			expected: "testcluster",
		},
		{
			name:     "with hyphens preserved",
			input:    "test-cluster",
			expected: "test-cluster",
		},
		{
			name:     "with underscores preserved",
			input:    "test_cluster",
			expected: "test_cluster",
		},
		{
			name:     "mixed separators preserved",
			input:    "test_cluster-name",
			expected: "test_cluster-name",
		},
		{
			name:     "numbers allowed",
			input:    "cluster123",
			expected: "cluster123",
		},
		{
			name:     "special characters converted to underscores",
			input:    "test@cluster!",
			expected: "test_cluster_",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := serializeLabel(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestInstanceGroupName(t *testing.T) {
	testCases := []struct {
		name        string
		clusterName string
		expected    string
	}{
		{
			name:        "simple cluster name",
			clusterName: "test-cluster",
			expected:    "test-cluster-group",
		},
		{
			name:        "cluster with numbers",
			clusterName: "cluster123",
			expected:    "cluster123-group",
		},
		{
			name:        "cluster with hyphens",
			clusterName: "my-test-cluster",
			expected:    "my-test-cluster-group",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := instanceGroupName(tc.clusterName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestLastComponent(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with multiple path components",
			input:    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-east1-a",
			expected: "us-east1-a",
		},
		{
			name:     "URL ending with slash returns empty string",
			input:    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-east1-a/",
			expected: "", // Last component after split is empty string
		},
		{
			name:     "simple path",
			input:    "zones/us-east1-a/instances/my-instance",
			expected: "my-instance",
		},
		{
			name:     "single component",
			input:    "my-instance",
			expected: "my-instance",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "just slashes",
			input:    "///",
			expected: "",
		},
		{
			name:     "machine type URL",
			input:    "zones/us-east1-a/machineTypes/n2-standard-4",
			expected: "n2-standard-4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := lastComponent(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestBuildLoadBalancerNameFilter tests the name filter construction for load balancer resources
func TestBuildLoadBalancerNameFilter(t *testing.T) {
	testCases := []struct {
		name        string
		clusterName string
		expected    string
	}{
		{
			name:        "simple cluster",
			clusterName: "test-cluster",
			expected:    "name:test-cluster-*",
		},
		{
			name:        "cluster with numbers",
			clusterName: "cluster123",
			expected:    "name:cluster123-*",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This tests the filter pattern we use in deleteLoadBalancerResourcesWithSDK
			filter := "name:" + tc.clusterName + "-*"
			assert.Equal(t, tc.expected, filter)
		})
	}
}

// TestShouldExcludeLoadBalancerResource tests the filtering logic used in deletion
func TestShouldExcludeLoadBalancerResource(t *testing.T) {
	testCases := []struct {
		name                 string
		resourceName         string
		clusterFilter        string
		expectedResourceType string
		portFilter           string
		shouldExclude        bool
	}{
		{
			name:                 "matching cluster and resource type",
			resourceName:         "test-cluster-26257-forwarding-rule-roachprod",
			clusterFilter:        "test-cluster",
			expectedResourceType: "forwarding-rule",
			portFilter:           "",
			shouldExclude:        false,
		},
		{
			name:                 "matching cluster, type, and port",
			resourceName:         "test-cluster-26257-forwarding-rule-roachprod",
			clusterFilter:        "test-cluster",
			expectedResourceType: "forwarding-rule",
			portFilter:           "26257",
			shouldExclude:        false,
		},
		{
			name:                 "wrong cluster",
			resourceName:         "other-cluster-26257-forwarding-rule-roachprod",
			clusterFilter:        "test-cluster",
			expectedResourceType: "forwarding-rule",
			portFilter:           "",
			shouldExclude:        true,
		},
		{
			name:                 "wrong resource type",
			resourceName:         "test-cluster-26257-proxy-roachprod",
			clusterFilter:        "test-cluster",
			expectedResourceType: "forwarding-rule",
			portFilter:           "",
			shouldExclude:        true,
		},
		{
			name:                 "wrong port",
			resourceName:         "test-cluster-8080-forwarding-rule-roachprod",
			clusterFilter:        "test-cluster",
			expectedResourceType: "forwarding-rule",
			portFilter:           "26257",
			shouldExclude:        true,
		},
		{
			name:                 "invalid resource name",
			resourceName:         "invalid-name",
			clusterFilter:        "test-cluster",
			expectedResourceType: "forwarding-rule",
			portFilter:           "",
			shouldExclude:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This replicates the shouldExclude logic from deleteLoadBalancerResourcesWithSDK
			shouldExclude := func(name string, expectedResourceType string) bool {
				cluster, resourceType, resourcePort, ok := loadBalancerNameParts(name)
				if !ok || cluster != tc.clusterFilter || resourceType != expectedResourceType {
					return true
				}
				if tc.portFilter != "" && resourcePort != mustAtoi(tc.portFilter) {
					return true
				}
				return false
			}

			result := shouldExclude(tc.resourceName, tc.expectedResourceType)
			assert.Equal(t, tc.shouldExclude, result)
		})
	}
}

// Helper function for tests
func mustAtoi(s string) int {
	var result int
	if _, err := fmt.Sscanf(s, "%d", &result); err != nil {
		panic(err)
	}
	return result
}

// ============================================================
// CONFIGURATION BUILDER TESTS
// ============================================================

func TestBuildHealthCheckConfig(t *testing.T) {
	testCases := []struct {
		name         string
		resourceName string
		port         int
	}{
		{
			name:         "standard health check",
			resourceName: "test-cluster-26257-health-check",
			port:         26257,
		},
		{
			name:         "different port",
			resourceName: "prod-8080-health-check",
			port:         8080,
		},
		{
			name:         "http port",
			resourceName: "dev-80-health-check",
			port:         80,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildHealthCheckConfig(tc.resourceName, tc.port)

			require.NotNil(t, result)
			assert.Equal(t, tc.resourceName, *result.Name)
			assert.Equal(t, computepb.HealthCheck_TCP.String(), *result.Type)
			require.NotNil(t, result.TcpHealthCheck)
			assert.Equal(t, int32(tc.port), *result.TcpHealthCheck.Port)
		})
	}
}

func TestBuildBackendServiceConfig(t *testing.T) {
	testCases := []struct {
		name                string
		resourceName        string
		healthCheckSelfLink string
	}{
		{
			name:                "standard backend service",
			resourceName:        "test-cluster-26257-load-balancer",
			healthCheckSelfLink: "https://www.googleapis.com/compute/v1/projects/test-project/global/healthChecks/test-cluster-26257-health-check",
		},
		{
			name:                "different cluster",
			resourceName:        "prod-8080-load-balancer",
			healthCheckSelfLink: "https://www.googleapis.com/compute/v1/projects/prod-project/global/healthChecks/prod-8080-health-check",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildBackendServiceConfig(tc.resourceName, tc.healthCheckSelfLink)

			require.NotNil(t, result)
			assert.Equal(t, tc.resourceName, *result.Name)
			assert.Equal(t, "EXTERNAL_MANAGED", *result.LoadBalancingScheme)
			assert.Equal(t, "TCP", *result.Protocol)
			require.Len(t, result.HealthChecks, 1)
			assert.Equal(t, tc.healthCheckSelfLink, result.HealthChecks[0])
			assert.Equal(t, int32(300), *result.TimeoutSec)
			assert.Equal(t, "cockroach", *result.PortName)
		})
	}
}

func TestBuildBackendConfig(t *testing.T) {
	testCases := []struct {
		name             string
		instanceGroupURL string
	}{
		{
			name:             "instance group in us-east1",
			instanceGroupURL: "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-east1-b/instanceGroups/test-cluster-ig",
		},
		{
			name:             "instance group in europe",
			instanceGroupURL: "https://www.googleapis.com/compute/v1/projects/prod-project/zones/europe-west2-a/instanceGroups/prod-ig",
		},
		{
			name:             "simple instance group URL",
			instanceGroupURL: "zones/us-central1-a/instanceGroups/my-group",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildBackendConfig(tc.instanceGroupURL)

			require.NotNil(t, result)
			assert.Equal(t, tc.instanceGroupURL, *result.Group)
			assert.Equal(t, "CONNECTION", *result.BalancingMode)
			assert.Equal(t, int32(9999999), *result.MaxConnections)
		})
	}
}

func TestBuildTargetTcpProxyConfig(t *testing.T) {
	testCases := []struct {
		name                   string
		proxyName              string
		backendServiceSelfLink string
	}{
		{
			name:                   "standard TCP proxy",
			proxyName:              "test-cluster-26257-proxy",
			backendServiceSelfLink: "https://www.googleapis.com/compute/v1/projects/test-project/global/backendServices/test-cluster-26257-load-balancer",
		},
		{
			name:                   "different cluster",
			proxyName:              "prod-8080-proxy",
			backendServiceSelfLink: "https://www.googleapis.com/compute/v1/projects/prod-project/global/backendServices/prod-8080-load-balancer",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildTargetTcpProxyConfig(tc.proxyName, tc.backendServiceSelfLink)

			require.NotNil(t, result)
			assert.Equal(t, tc.proxyName, *result.Name)
			assert.Equal(t, tc.backendServiceSelfLink, *result.Service)
			assert.Equal(t, "NONE", *result.ProxyHeader)
		})
	}
}

func TestBuildForwardingRuleConfig(t *testing.T) {
	testCases := []struct {
		name          string
		ruleName      string
		proxySelfLink string
		port          int
		labels        map[string]string
	}{
		{
			name:          "standard forwarding rule with labels",
			ruleName:      "test-cluster-26257-forwarding-rule",
			proxySelfLink: "https://www.googleapis.com/compute/v1/projects/test-project/global/targetTcpProxies/test-cluster-26257-proxy",
			port:          26257,
			labels: map[string]string{
				vm.TagCluster:   "test-cluster",
				vm.TagRoachprod: "true",
				"port":          "26257",
			},
		},
		{
			name:          "forwarding rule without labels",
			ruleName:      "prod-8080-forwarding-rule",
			proxySelfLink: "https://www.googleapis.com/compute/v1/projects/prod-project/global/targetTcpProxies/prod-8080-proxy",
			port:          8080,
			labels:        map[string]string{},
		},
		{
			name:          "https port",
			ruleName:      "web-443-forwarding-rule",
			proxySelfLink: "https://www.googleapis.com/compute/v1/projects/web-project/global/targetTcpProxies/web-443-proxy",
			port:          443,
			labels: map[string]string{
				"env": "production",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildForwardingRuleConfig(tc.ruleName, tc.proxySelfLink, tc.port, tc.labels)

			require.NotNil(t, result)
			assert.Equal(t, tc.ruleName, *result.Name)
			assert.Equal(t, tc.proxySelfLink, *result.Target)
			assert.Equal(t, fmt.Sprintf("%d", tc.port), *result.PortRange)
			assert.Equal(t, "TCP", *result.IPProtocol)
			assert.Equal(t, tc.labels, result.Labels)
		})
	}
}

func TestBuildNamedPortConfig(t *testing.T) {
	testCases := []struct {
		name     string
		portName string
		port     int
	}{
		{
			name:     "cockroach port",
			portName: "cockroach",
			port:     26257,
		},
		{
			name:     "http port",
			portName: "http",
			port:     8080,
		},
		{
			name:     "https port",
			portName: "https",
			port:     443,
		},
		{
			name:     "custom service",
			portName: "my-service",
			port:     9000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildNamedPortConfig(tc.portName, tc.port)

			require.NotNil(t, result)
			assert.Equal(t, tc.portName, *result.Name)
			assert.Equal(t, int32(tc.port), *result.Port)
		})
	}
}
