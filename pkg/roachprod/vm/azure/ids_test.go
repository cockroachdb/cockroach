// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import "testing"

func TestParseAzureID(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedString string
		expectErr      bool
		expected       azureID
	}{
		{
			name:           "Valid VM",
			input:          "/subscriptions/1234-abcd/resourceGroups/my-rg/providers/Microsoft.Compute/virtualMachines/n01",
			expectedString: "/subscriptions/1234-abcd/resourceGroups/my-rg/providers/Microsoft.Compute/virtualMachines/n01",
			expected: azureID{
				subscription:  "1234-abcd",
				resourceGroup: "my-rg",
				provider:      "Microsoft.Compute",
				resourceType:  "virtualMachines",
				resourceName:  "n01",
			},
		},
		{
			name:           "Valid NIC",
			input:          "/subscriptions/1234-abcd/resourceGroups/test-rg/providers/Microsoft.Network/networkInterfaces/test-nic01",
			expectedString: "/subscriptions/1234-abcd/resourceGroups/test-rg/providers/Microsoft.Network/networkInterfaces/test-nic01",
			expected: azureID{
				subscription:  "1234-abcd",
				resourceGroup: "test-rg",
				provider:      "Microsoft.Network",
				resourceType:  "networkInterfaces",
				resourceName:  "test-nic01",
			},
		},
		{
			// Since child resources are not implemented, all we are checking here is
			// that the resourceName gets selected out properly
			name:           "Valid with child resource",
			input:          "/subscriptions/1111/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/lb01/frontendIPConfigurations/ipconfig1",
			expectedString: "/subscriptions/1111/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/lb01",
			expected: azureID{
				subscription:  "1111",
				resourceGroup: "rg1",
				provider:      "Microsoft.Network",
				resourceType:  "loadBalancers",
				resourceName:  "lb01",
			},
		},
		{
			name:      "Invalid - missing fields",
			input:     "/subscriptions/abcd/resourceGroups//providers/Microsoft.Compute/virtualMachines/vm1",
			expectErr: true,
		},
		{
			name:      "Invalid - not an Azure ID",
			input:     "/this/is/not/an/azure/id",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := parseAzureID(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			// Compare fields
			if actual.subscription != tt.expected.subscription {
				t.Errorf("subscription: actual %q, expected %q", actual.subscription, tt.expected.subscription)
			}
			if actual.resourceGroup != tt.expected.resourceGroup {
				t.Errorf("resourceGroup: actual %q, expected %q", actual.resourceGroup, tt.expected.resourceGroup)
			}
			if actual.provider != tt.expected.provider {
				t.Errorf("provider: actual %q, expected %q", actual.provider, tt.expected.provider)
			}
			if actual.resourceType != tt.expected.resourceType {
				t.Errorf("resourceType: actual %q, expected %q", actual.resourceType, tt.expected.resourceType)
			}
			if actual.resourceName != tt.expected.resourceName {
				t.Errorf("resourceName: actual %q, expected %q", actual.resourceName, tt.expected.resourceName)
			}
			// Ensure String() reconstructs the same input
			if actual.String() != tt.expectedString {
				t.Errorf("String(): actual %q, expected %q", actual.String(), tt.expectedString)
			}
		})
	}
}
